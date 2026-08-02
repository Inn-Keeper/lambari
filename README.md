# Lambari — real-time fraud scoring pipeline (PoC)

A full-stack anti-fraud proof of concept: a Go worker-pool engine that scores
payment transactions against velocity, geo, amount, and merchant-risk rules,
fed either over HTTP or Kafka, with a live React dashboard streaming verdicts
over SSE.

**Measured on a developer laptop** — not a tuned deployment:

- ~130,000 tx/sec engine throughput (`make bench`), p99 scoring latency under 50µs.
- ~260,000 tx/sec accepted over the HTTP ingest path with 8 concurrent senders,
  at which point the engine's buffer saturates and ingest begins shedding load.
  The load generator reports what the server *kept*, not what it was offered.
- Delivery is **at-least-once** end to end, proven by a test that SIGKILLs the
  consumer mid-batch against a real broker and asserts nothing was lost
  (`make e2e`).

Includes **case management**: every review/decline verdict opens a case in
a review queue (worst score first). Analyst resolutions — confirmed fraud
or false positive — are stored as labels, i.e. training data for a future
ML rule. In-memory for the PoC behind a `Store` interface; `schema.sql`
defines the identical Postgres shape.

The queue is live but holds still while your pointer is over it — at a few
thousand transactions per second the server re-ranks cases faster than a human
can aim at a button. Resolving is optimistic and rolls back with a visible
error if the server refuses.

## Stack

| Layer | Choice | Why |
|---|---|---|
| Engine/API | Go 1.22+, stdlib `net/http` | Goroutine worker pool, method-pattern routing, zero framework overhead |
| Event bus | Kafka API via [franz-go](https://github.com/twmb/franz-go), Redpanda broker | Fastest pure-Go client; Redpanda = Kafka without ZooKeeper/JVM |
| Dashboard | React 19 + TypeScript 5.7 + Vite 6 | |
| Styling | Tailwind CSS v4 (CSS-first `@theme`) | |
| Motion | [motion](https://motion.dev) v12 (`motion/react`) | Feed entry animations, animated bars |
| Primitives | Radix UI (Slider, Switch) | Accessible simulator controls |
| Live updates | Server-Sent Events | One-directional stats push — simpler than WebSocket, auto-reconnects |

## Prerequisites

- Go 1.22+
- Node 22 + pnpm (`corepack enable` picks up the pinned version)
- Docker (only for Kafka mode)

## Quickstart (no Kafka needed)

```bash
make setup       # go mod tidy + pnpm install
make run-api     # terminal 1 — engine + API on :8080
make run-web     # terminal 2 — dashboard on :5173
```

To compile standalone binaries instead of `go run`:

```bash
make build       # → backend/api, backend/loadgen
./backend/api
```

Open http://localhost:5173, flip the **Simulator** switch, drag the rate
slider. The built-in generator produces realistic traffic with ~8–10%
embedded fraud patterns (card-testing rings, velocity attacks, stolen-card
geo mismatches, high-risk merchants).

### External IO load (the PoC path)

```bash
make loadgen     # POSTs 5000 tx/s in JSON batches to /api/transactions for 30s
make bench       # raw engine throughput benchmark
make test        # Go tests
make test-web    # React tests (vitest + Testing Library)
```

To find a ceiling rather than hold a rate, run the generator unthrottled and
add senders until throughput stops rising:

```bash
go run ./cmd/loadgen -rate 0 -workers 8 -batch 500 -duration 30s
```

Every run reports the rate it *achieved*, plus anything the server shed —
a paced generator can never report more than you asked it for, and a 200
response that quietly dropped half the batch is not throughput.

## Kafka mode

```bash
make kafka-up        # Redpanda on :19092, console UI on :8081
make kafka-run       # API consumes from the `transactions` topic
make kafka-loadgen   # produce 5000 tx/s into the topic
make e2e             # crash-replay proof: SIGKILL the consumer, assert no loss
```

Records are keyed by card token so one card's events stay ordered within a
partition — the velocity rules depend on that. Backpressure is natural:
if the engine saturates, polling slows and consumer lag becomes visible
(and alertable) in Kafka.

### Delivery semantics

The pipeline is **at-least-once with idempotent effects**:

- Offsets are committed only after the engine has finished scoring the batch —
  `SubmitBatch` blocks until every transaction is done. Committing after
  *enqueue* would be at-most-once, losing whatever was still buffered on a
  crash, silently.
- The verdict and DLQ producers are flushed **before** every commit. They
  publish asynchronously, so committing first would let a crash discard
  buffered publishes the offsets already claimed were handled.
- Records that fail to decode go to `transactions.dlq` with origin headers
  rather than a log line, and don't block the rest of the batch.
- `OnPartitionsRevoked` flushes and commits before a rebalance, bounding
  redelivery to one in-flight batch.
- Duplicates are harmless by construction: cases dedupe on transaction id and
  verdicts are keyed by it. Replay after a crash is also how the in-memory
  velocity windows rebuild, so a dedup cache would do more harm than good.

`make e2e` proves it: it streams transactions, SIGKILLs the real consumer
mid-batch against Redpanda, restarts it, and asserts every verdict arrived at
least once. A graceful shutdown can't stand in for a crash — the revoke hook
correctly commits on the way out, so the test kills by signal.

## Architecture

```
                    ┌──────────────────────────────────────────────┐
 loadgen ──HTTP──►  │  API :8080                                   │
                    │   POST /api/transactions  ─┐                 │
 loadgen ──Kafka─►  │  franz-go consumer        ─┤                 │
 (topic:            │   (group: lambari-scoring)│                 │
  transactions)     │                            ▼                 │
                    │   ┌────────────────────────────────┐         │
                    │   │ Engine: chan (16k buf)         │         │
                    │   │  → N×2 CPU workers             │         │
                    │   │  → rules: amount · velocity ·  │         │
                    │   │     ip fan-out · geo · MCC     │         │
                    │   │  → score ⇒ approve/review/     │         │
                    │   │            decline             │         │
                    │   └────────────────────────────────┘         │
                    │   atomics + sharded sliding windows (256)    │
                    │                            │                 │
                    │   GET /api/stream (SSE) ◄──┤                 │
                    │   GET /metrics (Prom)   ◄──┘                 │
                    └──────────────┬───────────────────────────────┘
                                   ▼
                        React dashboard :5173
              stats · throughput chart · live verdict feed
```

### Engine design notes

- **Worker pool, not per-request goroutines**: a bounded channel (16,384) in
  front of `2 × NumCPU` workers. `Submit` blocks when full — backpressure
  propagates upstream instead of OOMing.
- **Sharded velocity state**: sliding-window counters live in 256 mutex
  shards keyed by FNV hash, so thousands of concurrent workers don't fight
  over one lock. A background sweeper evicts idle keys to keep memory flat.
- **Lock-free reads**: counters are atomics; the stats endpoint never stalls
  the hot path. Latency lands in a lock-free bucketed histogram, and the
  dashboard's p50/p99 are read back out of those same buckets.
- **Scoring is additive**: each rule returns points + a flag; ≥40 ⇒ review,
  ≥70 ⇒ decline. Thresholds and the rule chain live in one place
  (`internal/engine/rules.go`) so tuning is a one-line change.

## API

| Endpoint | Purpose |
|---|---|
| `POST /api/transactions` | Batch JSON ingest (the HTTP IO path). `503 + Retry-After` when saturated — see below |
| `POST /api/simulate` | `{"rate": 5000}` starts the built-in generator, `0` stops |
| `GET /api/stats` | Engine snapshot |
| `GET /api/stream` | SSE: stats + recent verdicts + case counts every 400ms |
| `GET /api/cases` | Review queue (`?status=resolved` for labeled history) |
| `POST /api/cases/{id}/resolve` | `{"resolution":"confirmed_fraud"\|"false_positive"}` |
| `GET /metrics` | Prometheus scrape (root-level by convention, not under `/api`) |

### Backpressure at the ingest boundary

When the engine's buffer is full, `POST /api/transactions` accepts a **prefix**
of the batch and sheds the rest, answering `503` with `Retry-After: 1` and
`{"accepted": N, "rejected": M}`. Accepting a prefix rather than scattered
transactions is what makes `accepted: N` actionable — it means "the first N,
resend from there".

503 rather than 429 because the constraint is this server's capacity, not this
caller's rate. Retrying is safe by construction: cases dedupe on transaction id
and verdicts are keyed by it, so a partially-landed batch cannot double-count
on the way back. Shed load is counted in `lambari_submissions_rejected_total`.

The Kafka path deliberately answers the same question differently — it
*blocks* (`SubmitBatch`) rather than shedding. One consumer loop slowing down
turns into consumer lag: visible, alertable, and already wired to autoscaling.
Blocking at an HTTP boundary would instead spread the pressure across N
uncoordinated callers as latency creep until their timeouts fire, telling
nobody to slow down. Same problem, opposite answer, because the context
differs.

### Metrics

`GET /metrics` serves Prometheus text exposition, hand-written so the repo
keeps its single dependency: transactions scored, decisions by type, rule
fires by name, queue depth against capacity, throughput, uptime, and — in
Kafka mode only — consumer lag per partition plus a summed total.

Scoring latency is exported as a **bucketed histogram**, not a pre-computed
percentile, because a percentile calculated inside one process cannot be
aggregated with another's: averaging eight pods' p99s is meaningless.
Prometheus gets raw buckets it can sum across pods, and the dashboard's p50/p99
are read back out of the same buckets. That makes the dashboard numbers
bucket-quantized (a p99 reads `250µs`, not `123µs`) — the price of having one
latency mechanism in the engine instead of two.

Consumer lag comes from the high watermark that every fetch already carries —
no admin client, no extra round-trips. It is the signal to autoscale on: a
saturated consumer can sit at low CPU while lag climbs, so CPU-based scaling
would never react.

In Kafka mode, every flagged verdict is also published to the `verdicts`
topic (keyed by tx id) for downstream consumers — notifications, data lake,
model training.

## Docs

- [docs/architecture.html](docs/architecture.html) — the as-built system
  diagram, a one-transaction end-to-end walkthrough, the production-shape
  board with the tradeoffs behind each box, and a glossary (standalone page,
  open directly in a browser)
- [docs/interview-kafka-at-least-once.md](docs/interview-kafka-at-least-once.md)
  — why at-least-once over the alternatives, where the crash windows are, and
  what the e2e test actually proves
- [docs/knowledge-base.md](docs/knowledge-base.md) — full architecture,
  scoring model, decision record, verified performance, roadmap
- [docs/diagrams.md](docs/diagrams.md) — UML: component, sequence, class,
  case-lifecycle state diagram (plain Markdown, GitHub-rendered)
- [docs/diagrams.html](docs/diagrams.html) — same four diagrams as a
  standalone navigable page (open directly in a browser)

## Where this would go next (production deltas)

Known gaps, stated plainly — the delta between this and production is more
interesting than pretending there isn't one:

- **Velocity state dies on rebalance.** The sliding windows live in process
  memory, so when partitions move to another consumer the affected keys start
  from zero: no error, just quietly worse detection. Redis with a pipelined
  sliding window, or Flink keyed state with checkpoints, is the real fix — and
  the honest question is what distributing that state costs in throughput.
- **The case store is in memory.** Cases and their labels do not survive a
  restart. `schema.sql` is the Postgres shape; the `Store` interface is the
  swap point.
- Swap `cases.MemStore` for a pgx implementation of `cases.Store`
  (`schema.sql` is ready)
- A Python sidecar serving an ML model as one more `Rule` in the chain,
  trained on the resolution labels the queue already collects
- Kubernetes with KEDA scaling on the exported consumer-lag metric, capped at
  the partition count — a consumer group can't usefully exceed it, and idle
  pods still bill you
- OpenTelemetry traces around `score()`; the p99 budget is the SLO
- AuthN on the API (deliberately omitted from this PoC)

## Layout

```
backend/
  cmd/api/          # entrypoint: HTTP + optional Kafka consumer
  cmd/loadgen/      # traffic flood tool (HTTP or Kafka transport)
  internal/engine/  # worker pool, rules, sharded state, latency histogram
  internal/model/   # transaction/verdict types, synthetic generator
  internal/kafka/   # franz-go consumer + producers, lag tracking, crash-replay e2e
  internal/cases/   # review queue store + tests (schema.sql = Postgres shape)
  internal/metrics/ # Prometheus text exposition (no client library)
  internal/api/     # HTTP handlers, SSE stream, simulator, /metrics
frontend/
  src/lib/useStream.ts        # SSE hook over a pure, tested reducer
  src/lib/api.ts              # every fetch, throwing a typed HttpError on non-2xx
  src/lib/casesReducer.ts     # review queue: optimistic resolve, rollback, pause
  src/components/             # StatCard, ThroughputChart, Breakdown,
                              # LiveFeed, SimControl, ReviewQueue
docs/               # knowledge base + UML diagrams
docker-compose.yml  # Redpanda + Redpanda Console
Makefile            # every workflow, one word each
pnpm-workspace.yaml # pnpm workspace root (frontend)
```
