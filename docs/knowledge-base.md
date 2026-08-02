# Lambari — Product Knowledge Base

*Real-time anti-fraud scoring pipeline. Full-stack proof of concept.*
*Last updated: 2026-07-23 · Status: PoC, verified working end-to-end*

---

## 1. Problem framing

Fraud detection is fundamentally a **high-throughput, low-latency stream
processing problem**: score thousands of payment transactions per second
against rules (and eventually models), decide approve / review / decline,
and surface flagged traffic to analysts fast enough to act on it.

The scoring path has a latency budget measured in microseconds-to-milliseconds
because it sits inline with payment authorization. Everything else (case
review, analytics, model training) is asynchronous and rides downstream of
the verdict stream.

## 2. Stack decision record

| Decision | Choice | Rationale |
|---|---|---|
| Backend language | **Go 1.22+** | Near-Rust performance for I/O-bound stream workloads; goroutines + channels map directly onto event pipelines; far faster hiring and iteration than Rust. Fraud rules change weekly — iteration speed is a security property. |
| Where Rust *would* fit | Hot-path scoring engine only | Justified only if sub-millisecond p99 or heavy in-memory feature computation is required. Not needed: Go hits p99 < 50µs here. |
| Where PHP fits | Nowhere new | Could run the back-office dashboard fine, but no reason to pick it over TS when the team is already there. |
| HTTP layer | stdlib `net/http`, Go 1.22 method-pattern routing | 6 endpoints do not justify a framework. Zero dependency overhead. |
| Event bus | **Kafka API** via [franz-go](https://github.com/twmb/franz-go) v1.18 | Fastest, most actively maintained pure-Go client. |
| Broker | **Redpanda** (docker-compose) | Kafka-API compatible, single binary, no ZooKeeper/JVM. Dev friction ≈ zero. |
| Frontend | **React 19 + TypeScript 5.7 + Vite 6** | |
| Styling | **Tailwind CSS v4** (CSS-first `@theme` tokens) | No `tailwind.config.js`; design tokens live in `index.css`. |
| Motion | **motion v12** (`motion/react`) | Feed entry/exit animations, animated bars. Successor package to framer-motion. |
| Primitives | **Radix UI** (Slider, Switch) | Accessible controls without adopting a full component library. |
| Live updates | **Server-Sent Events** | One-directional stats push; simpler than WebSocket, auto-reconnects natively. 400ms tick. |
| Persistence (PoC) | In-memory behind `cases.Store` interface | `schema.sql` defines the identical Postgres shape; swap = one new implementation, zero handler changes. |
| Production data plan | Postgres (cases), Redis (shared velocity state), Python sidecar (ML) | See §10 roadmap. |

**Dependency discipline:** exactly one external Go dependency (franz-go).
Five frontend runtime dependencies. Every addition needs to pay rent.

## 3. Architecture

```
                    ┌──────────────────────────────────────────────┐
 loadgen ──HTTP──►  │  API :8080                                   │
                    │   POST /api/transactions  ─┐                 │
 loadgen ──Kafka─►  │  franz-go consumer        ─┤                 │
 (topic:            │   (group: lambari-scoring)│                 │
  transactions)     │                            ▼                 │
                    │   ┌────────────────────────────────┐         │
                    │   │ Engine: chan (16,384 buffer)   │         │
                    │   │  → 2×NumCPU workers            │         │
                    │   │  → rules: amount · velocity ·  │         │
                    │   │     ip fan-out · geo · MCC     │         │
                    │   │  → score ⇒ approve/review/     │         │
                    │   │            decline             │         │
                    │   └───────────┬────────────────────┘         │
                    │               │ OnFlagged hook               │
                    │        ┌──────┴──────┐                       │
                    │        ▼             ▼                       │
                    │   case store    verdicts topic (kafka mode)  │
                    │        │                                     │
                    │   GET /api/stream (SSE, 400ms) ──────────►   │
                    └──────────────────────────────────────────────┘
                                        │
                                        ▼
                          React dashboard :5173
            stats · throughput chart · review queue · live feed
```

### Data flow
1. Transactions enter via HTTP batch (`POST /api/transactions`), Kafka
   (`transactions` topic), or the built-in simulator.
2. The engine's bounded channel feeds a worker pool; each worker runs the
   rule chain and produces a `Verdict`.
3. Every review/decline verdict fires the `OnFlagged` hook → opens a case
   and (in Kafka mode) publishes to the `verdicts` topic keyed by tx id.
4. The dashboard consumes SSE (stats + recent verdicts + case counts) and
   REST (case queue).

### Engine internals — the four load-bearing decisions
- **Worker pool, not per-request goroutines.** Bounded channel (16,384) in
  front of `2 × NumCPU` workers. `Submit` blocks when full — backpressure
  propagates upstream instead of OOMing. In Kafka mode this surfaces as
  consumer lag, which is visible and alertable.
- **Sharded velocity state.** Sliding-window counters in 256 mutex shards
  keyed by FNV-1a hash, so concurrent workers don't fight over one lock.
  A background sweeper (2-min tick) evicts idle keys to keep memory flat.
  *Honest caveat: 256 was a reasoned guess, never benchmarked against 64
  or 512.*
- **Lock-free reads.** Counters are atomics; the stats endpoint never
  stalls the hot path. Latency goes into a lock-free bucketed histogram —
  one atomic add per transaction — and the p50/p99 the dashboard shows are
  read back out of those buckets, so they are bucket-quantized.
- **Additive scoring, one tuning surface.** Each rule returns points + a
  flag. Thresholds and the rule chain live in `internal/engine/rules.go`
  only.

## 4. Rules and scoring model

| Rule | Signal | Points |
|---|---|---|
| `amount_high` / `amount_extreme` | ≥ 1,500 / ≥ 5,000 | 20 / 45 |
| `card_velocity` / `_extreme` | Same card ≥ 4 / ≥ 8 hits in 60s | 25 / 50 |
| `ip_fanout` / `_extreme` | One IP ≥ 12 / ≥ 30 events in 5 min (card-testing signature) | 20 / 40 |
| `geo_mismatch` | Card BIN country ≠ transaction country | 25 |
| `high_risk_mcc` | Gambling, crypto/quasi-cash, wire transfer, inbound telemarketing | 15 |

**Decision thresholds:** score ≥ 70 ⇒ decline · ≥ 40 ⇒ review · else approve.

Known simplifications (deliberate, documented):
- BIN→country is a toy 9-entry table; production licenses a BIN database.
- IP fan-out counts *events* per IP, not *distinct cards* per IP. Production
  wants a HyperLogLog per key — counting distinct cards is the true
  card-testing signature.

## 5. Case management and the labeling loop

Every flagged verdict opens a **case** (1:1 with tx id for the PoC). The
review queue sorts worst-score-first, capped at 200 open — at 5k tx/s you
triage by recency and severity or you drown.

Analyst resolutions are binary labels: `confirmed_fraud` or
`false_positive`. **This is the strategic part:** the queue is quietly
collecting exactly the training data a future ML rule needs. The feedback
loop (rules flag → analyst labels → model trains → model becomes a rule) is
designed in from day one, even though the ML piece doesn't exist yet.

Storage: in-memory `MemStore` behind the `cases.Store` interface. Resolved
cases move to a capped history (bounded memory — the permanent record is
Postgres's job). `schema.sql` defines the production shape including a
partial index for training-data export.

## 6. API reference

| Endpoint | Purpose |
|---|---|
| `POST /api/transactions` | Batch JSON ingest (the HTTP IO path). Returns accepted/rejected counts; rejects = engine saturated (backpressure made visible). |
| `POST /api/simulate` | `{"rate": 5000}` starts built-in generator, `{"rate": 0}` stops. Max 100,000. |
| `GET /api/stats` | Engine snapshot: counters, rate, p50/p99, queue depth, rule fires. |
| `GET /api/stream` | SSE every 400ms: stats + recent verdicts + sim state + case counts. |
| `GET /api/cases` | Review queue, worst first. `?status=resolved` for labeled history. |
| `POST /api/cases/{id}/resolve` | Body `{"resolution":"confirmed_fraud"\|"false_positive"}`. 404 on double-resolve. |
| `GET /api/health` | Liveness + mode (`inline` / `kafka`). |

**Kafka topics:** `transactions` in (keyed by card token → per-card ordering
within a partition, which velocity rules depend on) · `verdicts` out (keyed
by tx id).

## 7. Verified performance (measured, not estimated)

All numbers from live runs in a containerized environment (Intel Xeon 2.80GHz):

| Metric | Value | How measured |
|---|---|---|
| Engine throughput | **~154,000 tx/sec** | `go test -bench`, full worker pool, drain included in timing |
| Sustained end-to-end | 5,000–6,000 tx/s | Built-in simulator + SSE-observed rate |
| Scoring latency p50 | ~2µs | Under 5k tx/s load; now reported as the enclosing bucket (`≤2µs`) |
| Scoring latency p99 | 33–123µs | Same; reported as `≤50µs` / `≤250µs` |
| Flagged rate on synthetic traffic | ~5.5–5.8% | Generator embeds ~8–10% fraud patterns; some score below thresholds |
| Queue depth under 6k tx/s | 0 / 16,384 | Engine never saturated at demo rates |
| Velocity windows lost on a clean rebalance | 12–15 of 24 cards | `make rebalance`: two consumers, 6 partitions, SIGTERM one (four runs) |

Historical note: the first benchmark read ~130k tx/sec; removing a dead map
write from the hot path (see §9, fix 1) raised it to ~154k.

## 8. Frontend design system

**Aesthetic:** "night-shift ops room" — deep navy base, deliberately *not*
the black-plus-acid-green cliché. Amber = review, red = decline, muted
sage = approve, steel blue accent for chrome.

**Tokens** (Tailwind v4 `@theme` in `index.css`):
`base #0e1420 · panel #151d2d · line #263248 · ink #e8edf6 · muted #7c8aa5 ·
approve #62b584 · review #f5b341 · decline #f0564a · accent #5b8def`

**Type:** Space Grotesk (display) · Inter (body) · IBM Plex Mono with
tabular numerals for **all** data — numbers never jitter as they update.

**Signature element:** the live verdict feed is a signal wire — a vertical
wire down the left edge with nodes that light up amber/red as flagged
transactions land. One bold idea, executed consistently; everything else
stays disciplined.

**Layout:** header (status + simulator controls) → 4 stat cards →
throughput chart (SVG, custom, no chart library) + decision split → review
queue → live feed + rule breakdown.

**Motion rules:** entry/exit animations on feed and queue rows, animated
bar widths, pulse on the live indicator. `prefers-reduced-motion` respected
globally.

## 9. Engineering principles and the code-review log

Principles applied:
- Interfaces only where a swap is planned (`cases.Store` is the only one).
- Backpressure over buffering-to-death; make saturation observable.
- Hot path owes rent: every operation per-transaction must justify itself.
- Deliberate omissions beat accidental ones: auth, persistence, and config
  are absent *by decision*, stated in the README.
- **First-pass output always needs a review pass — including (especially)
  AI-generated code.**

The self-review found and fixed five real defects (kept here as a log,
because the *pattern* of each is reusable):

| # | Defect | Class | Impact | Fix |
|---|---|---|---|---|
| 1 | `RuleIPFanOut` did a wasted map write per tx + `_ = n` discard | Dead code on hot path | ~18% throughput loss (130k→154k after fix) | Single `touch` per IP |
| 2 | `\|\| true` in the SSE hook's history condition | Dead condition (AI residue) | Misleading code, no functional bug | Deleted the condition |
| 3 | Resolved cases never left the map | Unbounded memory growth | Slow leak on long runs | Capped resolved history; open count became O(1) counter |
| 4 | `score()` called `processed.Load()` twice after `Add(1)` already returned the value | Redundant atomics on hot path | Minor cost, sloppy | Capture `pn := Add(1)` once |
| 5 | `cmd/api` started the consumer with `go consumer.Run(ctx)` and never waited for it | Shutdown ordering | SIGTERM could exit before `LeaveGroup`, stranding the member's partitions for the ~45s session timeout; `eng.Stop()` could also close the buffer under an in-flight `SubmitBatch` | Wait on a `consumerDone` channel before stopping the engine (found while building `make rebalance`, which needs a clean leave) |

## 10. Known tradeoffs (documented, not hidden)

- **Velocity state is per-instance, and partition affinity is not a fix.**
  Producing keyed by card token keeps one card on one partition, which is
  what makes the windows coherent — but the window still lives in the
  memory of whoever owns that partition today. `make rebalance` measures
  the consequence: stopping one of two consumers with SIGTERM (a rolling
  deploy) left 12–15 of 24 cards mid-attack scoring as first-time traffic,
  with no error raised anywhere. External state (Redis, Flink) is the only
  real fix; the open question is what it costs in throughput.
- **SSE marshals per connection.** Fine for a dashboard; a fleet of
  consumers needs a single-marshal broadcaster.
- **Case = tx (1:1).** Production groups related transactions (same card,
  same ring) into one case.
- **No auth.** Stated PoC scope cut.

## 11. Roadmap (production deltas, in order)

1. **pgx implementation of `cases.Store`** against `schema.sql` — the
   interface and schema are already aligned.
2. **ML sidecar**: Python service trained on resolution labels, exposed to
   Go as one more `Rule` in the chain (score contribution like any other).
3. **Redis-backed velocity state** if multi-instance without partition
   affinity is needed.
4. **OpenTelemetry** traces around `score()`; the p99 budget becomes the SLO.
5. **AuthN/AuthZ** on the API; analyst identity on resolutions
   (`resolved_by` column already in schema).
6. **Case grouping** (ring detection) and a `verdicts` topic consumer for
   notifications / data lake.

## 12. Operational runbook

```bash
make setup          # go mod tidy + pnpm install
make run-api        # engine + API on :8080 (inline mode, no Kafka needed)
make run-web        # dashboard on :5173
make sim            # start built-in simulator at 5000 tx/s
make loadgen        # external HTTP flood: 5000 tx/s for 30s
make test           # backend tests
make test-web       # frontend tests (vitest)
make bench          # engine throughput benchmark

make kafka-up       # Redpanda :19092 + console UI :8081
make kafka-run      # API consuming from `transactions` topic
make kafka-loadgen  # produce 5000 tx/s into the topic
make e2e            # crash-replay: SIGKILL a consumer, assert no verdict is lost
make rebalance      # state loss: SIGTERM one of two consumers, count lost windows
```

Env vars: `LAMBARI_ADDR` (default `:8080`) ·
`LAMBARI_KAFKA_BROKERS` (unset = inline mode).

## 13. Repository layout

```
backend/
  cmd/api/          # entrypoint: HTTP + optional Kafka consumer + hooks
  cmd/loadgen/      # traffic flood tool (HTTP or Kafka transport)
  internal/engine/  # worker pool, rules, sharded state, tests + benchmark
  internal/model/   # transaction/verdict types, synthetic generator
  internal/kafka/   # franz-go consumer + producers (transactions in, verdicts out)
  internal/cases/   # review queue store + tests
  go.mod            # one dependency: franz-go
frontend/
  src/lib/useStream.ts   # typed SSE hook
  src/components/        # StatCard, ThroughputChart, Breakdown, LiveFeed,
                         # ReviewQueue, SimControl
  src/index.css          # Tailwind v4 @theme design tokens
schema.sql          # Postgres shape for cases.Store
docker-compose.yml  # Redpanda + Redpanda Console
Makefile            # every workflow, one word each
```
