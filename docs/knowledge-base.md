# Lambari — Product Knowledge Base

*Real-time anti-fraud scoring pipeline. Full-stack proof of concept.*
*Last updated: 2026-08-02 · Status: PoC, verified working end-to-end*

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
                    │   │ Engine: 16,384 slots, per card │         │
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
4. The dashboard consumes SSE (stats + recent verdicts + case counts + the
   top open cases) and uses REST only to resolve cases.

### Engine internals — the four load-bearing decisions
- **Worker pool, not per-request goroutines.** `2 × NumCPU` workers, each with
  its own queue (16,384 slots in total), and each card always on the same one
  so it is scored in order. `Submit` blocks when full — backpressure
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

See the README: [API](../README.md#api).

## 7. Verified performance

See the README: [Throughput ceiling (measured)](../README.md#throughput-ceiling-measured).

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

## 10. Known tradeoffs

See the README: [Where this would go next](../README.md#where-this-would-go-next-production-deltas) and [Delivery semantics](../README.md#delivery-semantics).

## 11. Roadmap

See the README: [Where this would go next](../README.md#where-this-would-go-next-production-deltas).

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

See the README: [Layout](../README.md#layout).
