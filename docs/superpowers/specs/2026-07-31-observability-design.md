# Observability: scrapeable metrics — design

Date: 2026-07-31
Status: approved

## Goal

Close the "monitored" gap: export the engine's counters and the Kafka
consumer's lag in Prometheus text format, so the claim "built, tested,
deployed, monitored" is true and lag-based autoscaling (KEDA) has something
to read.

## Constraints

- **No new dependencies.** The repo has exactly one (franz-go) and keeps it.
  The Prometheus text exposition format is `# HELP` / `# TYPE` / `name value`
  lines — roughly 60 lines to write, versus pulling in protobuf, client_model,
  common, and procfs for the same result.
- No changes to the SSE/dashboard contract.

## Design

### 1. Latency histogram in the engine

Fixed microsecond buckets: 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500,
5000, +Inf. One `atomic.Int64` per bucket plus an atomic sum, incremented on
the existing score path — lock-free, so no contention added to a 130k/s hot
path. Exposed as `Engine.Histogram()` returning bucket bounds, cumulative
counts, sum and count.

The existing sampled reservoir stays. The two serve different readers and
that is the point worth stating out loud: **pre-computed per-pod percentiles
cannot be aggregated** — averaging the p99s of eight pods is meaningless — so
the histogram exports raw buckets that Prometheus can aggregate across pods,
while the reservoir keeps feeding the dashboard the precise live number a
human is watching.

> **Revised 2026-08-02.** The reservoir was removed: keeping two latency
> mechanisms cost a mutex, a 4,096-element copy and a sort on every
> `Snapshot()` (once per SSE frame) to make one dashboard number exact.
> `Histogram.Quantile` now derives p50/p99 from the same buckets. The
> aggregation argument above still stands — it is why the *histogram* is what
> survived. Three details of the contract above have changed since:
>
> - **The bucket range ends at 1s, not 5ms**: `…, 2500, 5000, 10000, 100000,
>   1000000, +Inf`. The ceiling ramp produced GC assist waves of 1.4s, so
>   millisecond-scale scoring latency happens here and pinning at 5ms would
>   have been a three-orders-of-magnitude lie.
> - **`Quantile` returns `engine.Overflow` (-1)** above the largest bucket
>   rather than the top bound, because a bound rendered verbatim reads as a
>   measurement. Every other return is a bucket *upper bound*, so the dashboard
>   renders `≤250µs` / `off scale` / `—` rather than a bare number.
> - **Rank semantics follow Prometheus**: the φ-quantile is the first bucket
>   whose cumulative count *reaches* φ·N. Requiring it to exceed φ·N walks one
>   bucket too far whenever the rank lands on a bucket edge, which reported the
>   maximum as the p99 for round observation counts.

### 2. `internal/metrics` package

A pure function, HTTP-free and therefore testable as data-in/text-out:

```go
func Write(w io.Writer, s engine.Stats, h engine.Histogram, lag map[int32]int64) error
```

Metric families (Prometheus naming: snake_case, `_total` for counters, base
units in the name):

| Metric | Type | Notes |
| --- | --- | --- |
| `lambari_transactions_scored_total` | counter | |
| `lambari_decisions_total{decision}` | counter | approve / review / decline |
| `lambari_rule_fires_total{rule}` | counter | one series per named flag |
| `lambari_scoring_duration_microseconds` | histogram | `_bucket{le}`, `_sum`, `_count` |
| `lambari_queue_depth` / `lambari_queue_capacity` | gauge | backpressure headroom |
| `lambari_throughput_per_second` | gauge | rolling rate |
| `lambari_uptime_seconds` | gauge | |
| `lambari_kafka_consumer_lag{partition}` | gauge | omitted entirely in inline mode |
| `lambari_kafka_consumer_lag_total` | gauge | sum across partitions — the KEDA signal |

Label values are escaped (`\`, `"`, newline) even though rule names are
internal constants: an exposition format is a serialization boundary, and
one unescaped quote silently corrupts a scrape.

### 3. Consumer lag without an admin client

Every `FetchPartition` franz-go hands back already carries `HighWatermark`.
A pure `lagTracker` in the kafka package consumes it:

```go
func (t *lagTracker) Observe(partition int32, highWatermark, lastOffset int64)
func (t *lagTracker) Snapshot() map[int32]int64
```

`lastOffset` is the final record's offset in that partition for this fetch,
or `-1` when the fetch returned no records — in which case the tracker
reuses the offset it stored previously, so a caught-up partition correctly
reports 0 rather than freezing at its last non-empty value. Lag is
`max(0, highWatermark - lastOffset - 1)`. The consumer's poll loop feeds it
via `fetches.EachPartition`; no admin client, no extra round-trips, no new
goroutine.

### 4. Wiring

`GET /metrics` on the existing mux (not under `/api` — scrape endpoints are
conventionally root-level). The API server gains
`SetLagSource(func() map[int32]int64)`, called only in Kafka mode from
`cmd/api`; nil in inline mode, where the Kafka metric families are omitted
rather than reported as zero. This keeps the `api` package free of any Kafka
import.

## Testing

- `metrics`: exposition output contains each family with correct HELP/TYPE;
  histogram buckets are cumulative and `_count` equals the final bucket;
  labels escaped; Kafka families absent when lag is nil and present with a
  correct total when it isn't.
- `engine`: an observation lands in the right bucket, and sum/count track it.
- `kafka`: lag math — a partition with records, a caught-up partition
  reporting an empty fetch, and a partition never seen (absent, not zero).

## Out of scope

- Prometheus/Grafana in docker-compose (the local Docker has no compose
  plugin; shipping unverifiable infra is worse than shipping none).
- Surfacing lag in the React console — lag is an autoscaler's signal, not an
  analyst's.
- Tracing/OpenTelemetry.
