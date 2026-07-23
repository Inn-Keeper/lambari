# Tripwire — real-time fraud scoring pipeline (PoC)

A full-stack anti-fraud proof of concept: a Go worker-pool engine that scores
payment transactions against velocity, geo, amount, and merchant-risk rules,
fed either over HTTP or Kafka, with a live React dashboard streaming verdicts
over SSE.

**Verified in CI-like conditions:** ~130,000 tx/sec engine throughput
(benchmark), 5,000 tx/sec sustained end-to-end with p99 scoring latency
under 50µs.

Includes **case management**: every review/decline verdict opens a case in
a review queue (worst score first). Analyst resolutions — confirmed fraud
or false positive — are stored as labels, i.e. training data for a future
ML rule. In-memory for the PoC behind a `Store` interface; `schema.sql`
defines the identical Postgres shape.

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
make test        # rule + engine tests
```

## Kafka mode

```bash
make kafka-up        # Redpanda on :19092, console UI on :8081
make kafka-run       # API consumes from the `transactions` topic
make kafka-loadgen   # produce 5000 tx/s into the topic
```

Records are keyed by card token so one card's events stay ordered within a
partition — the velocity rules depend on that. Backpressure is natural:
if the engine saturates, polling slows and consumer lag becomes visible
(and alertable) in Kafka.

## Architecture

```
                    ┌──────────────────────────────────────────────┐
 loadgen ──HTTP──►  │  API :8080                                   │
                    │   POST /api/transactions  ─┐                 │
 loadgen ──Kafka─►  │  franz-go consumer        ─┤                 │
 (topic:            │   (group: tripwire-scoring)│                 │
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
                    │   GET /api/stream (SSE) ◄──┘                 │
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
  the hot path. Latency percentiles come from a sampled reservoir (every 8th
  transaction).
- **Scoring is additive**: each rule returns points + a flag; ≥40 ⇒ review,
  ≥70 ⇒ decline. Thresholds and the rule chain live in one place
  (`internal/engine/rules.go`) so tuning is a one-line change.

## API

| Endpoint | Purpose |
|---|---|
| `POST /api/transactions` | Batch JSON ingest (the HTTP IO path) |
| `POST /api/simulate` | `{"rate": 5000}` starts the built-in generator, `0` stops |
| `GET /api/stats` | Engine snapshot |
| `GET /api/stream` | SSE: stats + recent verdicts + case counts every 400ms |
| `GET /api/cases` | Review queue (`?status=resolved` for labeled history) |
| `POST /api/cases/{id}/resolve` | `{"resolution":"confirmed_fraud"\|"false_positive"}` |

In Kafka mode, every flagged verdict is also published to the `verdicts`
topic (keyed by tx id) for downstream consumers — notifications, data lake,
model training.

## Where this would go next (production deltas)

- Swap `cases.MemStore` for a pgx implementation of `cases.Store`
  (`schema.sql` is ready); Redis if velocity state must survive restarts or
  be shared across instances
- A Python sidecar serving an ML model as one more `Rule` in the chain,
  trained on the resolution labels the queue already collects
- OpenTelemetry traces around `score()`; the p99 budget is the SLO
- AuthN on the API (deliberately omitted from this PoC)

## Layout

```
backend/
  cmd/api/          # entrypoint: HTTP + optional Kafka consumer
  cmd/loadgen/      # traffic flood tool (HTTP or Kafka transport)
  internal/engine/  # worker pool, rules, sharded state, tests + benchmark
  internal/model/   # transaction/verdict types, synthetic generator
  internal/kafka/   # franz-go consumer + producers (transactions in, verdicts out)
  internal/cases/   # review queue store + tests (schema.sql = Postgres shape)
  internal/api/     # HTTP handlers, SSE stream, simulator
frontend/
  src/lib/useStream.ts        # typed SSE hook
  src/components/             # StatCard, ThroughputChart, Breakdown,
                              # LiveFeed, SimControl
docker-compose.yml  # Redpanda + Redpanda Console
Makefile            # every workflow, one word each
pnpm-workspace.yaml # pnpm workspace root (frontend)
```
