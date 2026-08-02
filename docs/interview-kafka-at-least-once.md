# Interview talk-track: Kafka at-least-once in Lambari

The one-liner: **"My consumer is at-least-once. Duplicates are absorbed where I
own the effect — case creation dedupes on transaction id — and keyed where I
don't, so a downstream consumer can dedupe for itself. I have a test that
SIGKILLs the real binary mid-stream against a real broker to prove nothing is
ever lost."** Commit `52520f4`, test: `make e2e`.

Say it that way rather than "at-least-once with idempotent effects". The
shorter phrasing sounds better and is wrong: keying a topic by transaction id
makes downstream deduplication *possible*, it does not perform it, and this
repo configures neither log compaction nor a downstream dedupe. Claiming
idempotency you don't control is precisely the kind of thing a good interviewer
pulls on — and the accurate answer is the more interesting one, because it
names the boundary of the system you own.

> The canonical statement of the contract is **Delivery semantics** in the
> README. This document is the talk-track for it: how to tell the story, what
> to say when pushed. Where the two ever disagree, the README wins and this
> file is wrong — that failure has already happened four times.

## The design, as a story

### 1. Why at-least-once (and not the alternatives)

- **At-most-once** (commit on fetch, before processing) is what the naive
  consumer does — and what Lambari did originally: `Submit` pushed into a
  buffered channel and offsets were committed immediately. A crash loses
  everything still buffered. For fraud scoring that's silently unscored
  transactions — the worst failure mode, because nobody knows.
- **Exactly-once** (Kafka transactions / a durable processed-set) is the wrong
  trade here: it puts a synchronous durable write into the hot path of a
  ~130k tx/s engine, and the duplicates it removes are the ones this system
  already absorbs cheaply (see "where duplicates land" below — the honest
  version, which is narrower than it first looks).
- **At-least-once**: commit only after the work is done; accept duplicates;
  make duplicates harmless. That's the sweet spot for additive scoring.

### 2. The mechanism — four pieces, each with a why

1. **`DisableAutoCommit` + commit after scoring.** The engine's
   `SubmitBatch(txs)` blocks until every transaction in the batch is actually
   scored (each job carries a `WaitGroup`), so the commit that follows covers
   only finished work. Crash between scoring and commit ⇒ replay, never loss.
2. **DLQ instead of log-and-drop.** A record that won't decode is either a
   producer bug or an attack — both worth keeping. It goes to
   `transactions.dlq` with origin topic/partition/offset/cause headers, and it
   doesn't block the rest of the batch. "Scored or durable elsewhere" is the
   precondition for committing.
3. **Flush producers before every commit.** Verdict and DLQ publishes are
   async and buffered (5 ms linger). Without a flush, this sequence loses
   data: score → buffer verdict → *commit offsets* → crash → verdict gone,
   and the offset says the work is done. So both producers are flushed before
   each commit; if the flush fails, the commit is skipped and the batch
   replays. **This was a real bug I found while designing the crash test** —
   the kind you only see when you think about where exactly the process can
   die.
4. **Commit on partition revoke.** Before a rebalance moves partitions to
   another group member, uncommitted offsets are committed (after a flush),
   bounding steady-state redelivery to at most one in-flight batch.

### 3. Idempotency: effect-level, no dedup cache

The counterintuitive bit that makes the design elegant:

- **Replay is state recovery, not corruption.** The sliding-window velocity
  counters live in memory. After a crash-restart they're empty; replaying the
  uncommitted tail *rebuilds* them. A TxID dedup cache would actively block
  that recovery — the one time redelivery actually happens at scale.
- **Where duplicates land, exactly.** Say this before you are asked; it is the
  part that does not survive scrutiny otherwise.
  - *The case queue* suppresses a duplicate **while the case is still open** —
    pinned by a unit test. Not forever: resolving or evicting a case forgets
    its `TxID`, so a replay after either reopens it. Remembering every `TxID`
    forever is the dedup cache I rejected above, and it would block the window
    rebuild. After a restart the store is empty anyway, so replay repopulates
    rather than duplicates.
  - *Counters and velocity windows* advance twice — accepted, because that
    second pass **is** the state rebuild.
  - *The verdicts topic* is keyed by `TxID`, which enables downstream dedupe or
    log compaction but performs neither. Nothing here configures either, so a
    consumer reading the live stream sees the record twice and needs its own
    dedupe on the key. That is a contract handed to whoever builds it, not a
    property of this system.
- So duplicates cost little inside the process, and preventing them would cost
  correctness (blocked state rebuild) plus memory plus code. What they cost
  outside it is a contract you hand to whoever consumes the topic.

### 4. Backpressure for free

The consumer blocks in `SubmitBatch` when the engine's bounded channel
(16,384) is full ⇒ polling slows ⇒ **consumer lag becomes visible in Kafka**,
which is exactly where you want to alert and autoscale on it. No custom
backpressure machinery.

## The proof

`backend/internal/kafka/e2e_test.go` (`make e2e`), against Redpanda:

1. Build and run the **real `cmd/api` binary** — production wiring, not a
   test-only consumer.
2. Stream 5,000 always-flagging transactions *while* the consumer runs, then
   **SIGKILL it** once ~1,000 verdicts have appeared.
3. Produce 5,000 more while the consumer is down (traffic doesn't stop for a
   crash), start a fresh process in the same group.
4. A group-less watcher tails the `verdicts` topic: **all 10,000 TxIDs must
   appear at least once.** Duplicates are reported, not required.

Observed run: `all 10000 verdicts arrived; 42 duplicate publishes (allowed
under at-least-once)` — the kill landed inside a poll-score-commit cycle, the
batch replayed, nothing was lost. That log line *is* at-least-once.

Two subtleties worth volunteering:

- **Graceful shutdown cannot test crash semantics.** Cancelling the context
  runs the revoke hook, which correctly flushes and commits on the way out —
  a clean exit has nothing to replay. The process has to die by signal.
  (Corollary: kill -9 in the test is a feature, not laziness.)
- **A streamed load, not a pre-produced one.** A pre-produced wave arrives as
  one giant fetch, scored and committed in milliseconds — the kill can never
  catch work in flight. Streaming keeps the consumer in small
  poll-score-commit cycles with a real crash window.

Bonus find: the producers never opted into topic auto-creation
(`kgo.AllowAutoTopicCreation`) — first e2e run failed with
`UNKNOWN_TOPIC_OR_PARTITION` on a fresh broker. The Kafka path had likely
never run against a clean cluster before. E2e tests earn their keep.

## Likely follow-up questions, with answers

- *"What if the DLQ publish itself fails?"* Flush fails ⇒ commit skipped ⇒
  batch replays on restart. Loud log either way. Records are never silently
  dropped; the failure mode is duplication, not loss.
- *"What about duplicates across group members?"* Commit-on-revoke bounds it
  to one in-flight batch, and the case store suppresses the duplicate for any
  case still open, whichever member re-scores. Each member has its own window
  state though — which is a bigger problem than the duplicate, and `make
  rebalance` measures it.
- *"Why not Kafka transactions for exactly-once?"* Be precise here, because
  the sloppy version is easy to catch. A transactional read-process-write loop
  **would** stop duplicate verdicts appearing in the output topic: the produce
  and the offset commit land atomically, and a `read_committed` consumer never
  sees the aborted attempt. What it would *not* cover is my velocity state,
  which is in-memory by design (latency) and is not part of any transaction —
  a replay still re-advances those windows, and that is the replay I rely on to
  rebuild them. Nor does it cover a side effect outside Kafka, like a
  notification already sent. So transactions would buy a cleaner output topic
  at the cost of broker round-trips per batch, and leave the part I actually
  care about unchanged.
- *"How does this scale out?"* Partitioned by `CardHash`, so one card's
  events stay ordered within a partition and each member's window state is
  self-contained for the cards it owns. Add partitions + members; rebalance
  redelivery stays bounded; the missing piece is distributed window state
  (Redis/Flink) if a card's traffic must survive member reassignment — known
  gap, on the roadmap.
