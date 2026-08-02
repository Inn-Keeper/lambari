# Kafka at-least-once delivery — design

Date: 2026-07-31
Status: approved

## Goal

Upgrade the Kafka consumer path from at-most-once to at-least-once delivery
with idempotent effects, and prove it against a real broker. This is the
delivery-semantics story for the pipeline: a consumer crash must never lose a
transaction; duplicates are permitted and made harmless.

## Core mechanism (already in working tree, kept as-is)

- `kgo.DisableAutoCommit()` — offsets are only committed explicitly.
- Poll → decode → score → commit, in that order. `Engine.SubmitBatch` blocks
  until every transaction in the batch has been scored, so the commit after it
  covers only genuinely finished work. A crash between scoring and commit
  replays the batch instead of losing it.
- Undecodable records go to `transactions.dlq` with origin headers (topic,
  partition, offset, cause) — never silently dropped, and they don't block the
  rest of the batch.
- `kgo.OnPartitionsRevoked` commits uncommitted offsets before a rebalance
  hands partitions to another member, bounding redelivery to the one in-flight
  batch.
- **Flush before commit** (added during implementation): verdict and DLQ
  publishes are async and buffered; both producers are flushed before every
  offset commit (including the revoke hook), otherwise a crash right after a
  commit silently loses the buffered publishes the commit covered. If the
  flush fails, the commit is skipped — replay on restart, never loss.

## Idempotency: effect-level, not dedup

No dedup cache, no processed-set. The argument:

1. **Replay is state recovery.** The engine's sliding-window counters live in
   memory. After a crash-restart they are empty; replaying the uncommitted
   tail *rebuilds* them. A dedup cache would actively prevent that recovery.
2. **The duplicate-visible effects are already idempotent.**
   `cases.MemStore.Open` returns early when the `TxID` already has an open
   case, and verdicts are published to the `verdicts` topic keyed by `TxID`,
   so downstream consumers can dedupe (or the topic can be log-compacted).
3. **Rebalance redelivery is bounded** by the commit-on-revoke hook, so the
   duplicate window in steady operation is at most one batch.

Rejected alternatives:

- **TxID LRU dedup in the consumer** — blocks window-state rebuild after a
  crash (the one time redelivery actually happens at scale) to solve a
  duplicate window that commit-on-revoke already made tiny.
- **Persistent processed-set / Kafka transactions (exactly-once)** — adds a
  synchronous durable write to the hot path of a ~130k tx/s additive-scoring
  engine. Wrong cost/benefit: a duplicate score is harmless by construction,
  so we buy nothing with the throughput we'd spend.

## Verification

Unit (existing, in working tree):

- `TestSubmitBatchReturnsOnlyAfterScoring` — SubmitBatch cannot return while
  scoring is blocked; returning early would reintroduce at-most-once.
- `TestUndecodableRecordGoesToDLQNotSilentlyDropped` — bad records reach the
  DLQ with a cause; good records in the same batch are scored before
  `handleRecords` returns.

Unit (new):

- `Cases.Open` called twice with the same `TxID` yields exactly one case —
  pins the load-bearing half of the idempotency argument.

End-to-end (new): `backend/internal/kafka/e2e_test.go`, skipped unless
`LAMBARI_KAFKA_BROKERS` is set. Against the repo's Redpanda
(`localhost:19092`):

1. Build the real `cmd/api` binary — the test exercises production wiring,
   not a test-only consumer. An in-process context-cancel cannot simulate a
   crash: graceful shutdown (correctly) commits via the revoke hook, so the
   process must die by SIGKILL.
2. Stream a first wave of 5,000 transactions with unique `TxID`s, crafted to
   always flag (`Amount ≥ 5000`), *while* the consumer is live — a
   pre-produced wave would arrive as one giant fetch, scored and committed in
   milliseconds, leaving the kill nothing to catch. SIGKILL the api once a
   fifth of the verdicts have appeared on the `verdicts` topic.
3. Produce a second wave of 5,000 while the consumer is down, then start a
   fresh api process (same consumer group); it resumes from the last commit
   and picks up everything the dead process never committed.
4. A group-less watcher tails the `verdicts` topic and asserts every one of
   the 10,000 `TxID`s appears at least once — that assertion is the
   invariant (loss = at-most-once bug). Duplicate publishes are reported,
   not required: they occur only when the kill lands inside a
   poll-score-commit cycle a few milliseconds wide, and are legal — that
   *is* the semantics.

`make e2e` brings up Redpanda and runs the test.

> **Revised 2026-08-02:** `make e2e` no longer starts the broker. Bringing it
> up is `make kafka-up`, which needs the Compose plugin that not every Docker
> install has; the experiment only needs a reachable broker and checks for one.
> The out-of-scope list below has also moved on: metrics shipped (`/metrics`,
> including consumer lag), and a second experiment, `make rebalance`, now
> measures what at-least-once does *not* cover — the in-memory velocity state,
> which dies when a partition moves to another consumer.

## Out of scope

- Exactly-once / Kafka transactions (rejected above).
- Configurable topic/consumer-group names — nothing needs them yet.
- Metrics/observability — separate follow-up (next on the roadmap).
  *(Shipped 2026-07-31; see the revision note above.)*
