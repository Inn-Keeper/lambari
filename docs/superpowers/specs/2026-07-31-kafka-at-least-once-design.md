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

1. Produce ~500 transactions with unique `TxID`s, crafted to always flag.
2. Run consumer 1 with `engine.OnFlagged` collecting scored TxIDs; cancel its
   context mid-stream — after some records scored, before the tail is
   committed. That lands in the exact crash window (scored-but-uncommitted).
3. Run consumer 2, same group, fresh engine; it resumes from the last commit
   and replays the uncommitted tail.
4. Assert every produced `TxID` was scored at least once across both runs;
   report the duplicate count (duplicates are expected — that *is* the
   semantics).

`make e2e` brings up Redpanda and runs the test.

## Out of scope

- Exactly-once / Kafka transactions (rejected above).
- Configurable topic/consumer-group names — nothing needs them yet.
- Metrics/observability — separate follow-up (next on the roadmap).
