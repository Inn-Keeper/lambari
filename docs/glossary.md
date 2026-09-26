# Glossary

The words this project uses, grouped by domain. Each entry says what the term
means here and, where it helps, where to find it in the code.

## Payments and fraud

**Approve / Review / Decline** — The three decisions. A score of 40 or more
opens a review; 70 or more declines. `engine.Decide` in `rules.go`.

**BIN** (Bank Identification Number) — The first six digits of a card number.
They identify the issuing bank and its country. `Transaction.CardBIN`; the
lookup table is `model.BINCountry`.

**Card hash / card token** — A stand-in for the card number (PAN) that is safe
to store. Velocity rules count per card hash, and Kafka records are keyed by it.
`Transaction.CardHash`.

**Card testing** — Fraudsters trying many stolen cards with small amounts from
a few IPs to find the live ones. Caught by the IP fan-out rule.

**Case** — A flagged verdict waiting for an analyst. One per transaction,
worst score first. `internal/cases`.

**Chargeback** — A payment reversed by the card issuer after a dispute. The
cost that high-risk merchant categories are flagged for.

**Confirmed fraud / False positive** — The two ways an analyst resolves a case.
Each resolution is stored as a **label**: training data for a future ML rule.

**Flag** — The name a rule returns when it adds points, such as
`card_velocity_extreme`. The flags on a verdict explain the decision.

**Geo mismatch** — A card used in a country other than the one that issued it.
`RuleGeoMismatch`.

**IP fan-out** — Heavy activity from one IP address within 5 minutes.
`RuleIPFanOut`. The PoC counts all events; production would count distinct
cards.

**Issuer** — The bank that issued the card.

**MCC** (Merchant Category Code) — A four-digit code for the kind of business.
Gambling (7995), crypto (6051), direct marketing (5967) and wire transfers
(4829) are treated as high risk. `RuleHighRiskMCC`.

**PAN** (Primary Account Number) — The full card number. Never handled here;
the card hash replaces it.

**Rule** — A function that looks at one transaction and returns points and a
flag. Five run for every transaction, in order. `DefaultRules` in `rules.go`.

**Score** — The sum of points from every rule that fired. Additive, so a
decision can always be explained by listing its flags.

**Velocity** — How often a key (card or IP) appears within a time window.
Velocity attacks hammer one card quickly. `RuleCardVelocity` (60s window).

**Verdict** — The scored outcome for one transaction: score, flags, decision
and scoring latency. `model.Verdict`.

## Scoring engine

**Backpressure** — Letting a slow stage slow down the stage feeding it instead
of buffering without limit. Here: a full engine blocks the Kafka consumer, or
makes HTTP ingest answer 503.

**Bounded channel / buffer** — The queues in front of the workers, one per
worker, 16,384 slots in total. A full queue means backpressure. `bufferSize` in
`engine.go`.

**Ring buffer** — The last 64 interesting verdicts, kept for the live feed.
Overwrites the oldest entry. `Engine.Recent`.

**Shard** — One of 256 slices of the velocity state, each with its own lock, so
workers rarely wait on each other. Chosen by an **FNV** hash of the key, a fast
hash that isn't meant to be secure.

**Sliding window** — The timestamps a key was seen at within the last N
seconds, kept sorted by timestamp and capped at the newest 30. An event counts
what falls in the window ending at its own time, taken before the cap trims
anything and limited to 30; one older than the whole window is scored alone.
`shard.touch`.

**Sweeper** — A background task that, every 2 minutes, deletes keys idle for
longer than the longest rule window (5 minutes). `State.StartSweeper`.

**Worker pool** — A fixed set of goroutines (2 × CPU count) that score from
their queues, instead of one goroutine per request. A card always goes to the
same worker, so its transactions are scored in order.

## Kafka and delivery

**At-least-once** — Every transaction gets a verdict, possibly more than once
after a crash. The pipeline's guarantee. Canonical statement:
[README, Delivery semantics](../README.md#delivery-semantics).

**At-most-once / Exactly-once** — The alternatives. At-most-once can lose
records (commit before processing). Exactly-once needs Kafka transactions and
still doesn't cover effects outside Kafka. Neither is used.

**BlockRebalanceOnPoll** — A franz-go setting that holds a rebalance until the
consumer calls `AllowRebalance`, so partitions never move mid-batch.

**Commit / Offset** — An offset is the consumer's position in a partition.
Committing it tells the broker everything before it is done. Here it happens
only after scoring and a successful flush.

**Consumer group** — The consumers sharing a topic's partitions, each partition
owned by one member. This one is `lambari-scoring` (`kafka.Group`).

**Consumer lag** — How many records a consumer is behind the end of its
partition. Exported per partition and in total; the right signal to autoscale
on. `lag.go`.

**DLQ** (dead letter queue) — `transactions.dlq`: records that can't be parsed
or fail validation, with headers pointing to where they came from.

**Duplicate / Replay** — After a crash, records since the last commit are
consumed again. Their verdicts can be published twice. Where duplicates land is
in the README's delivery semantics.

**Flush** — Waiting until every buffered publish has finished. franz-go's
`Flush` doesn't report failed records, so the producers count failures
themselves (`publishErrors`).

**High watermark** — The offset of the next record to be written to a
partition. Lag is the high watermark minus the consumer's position.

**Key** — The field that picks a record's partition. Transactions are keyed by
card hash, so one card's events stay in order. Verdicts are keyed by
transaction id.

**Partition** — A slice of a topic, the unit of both ordering and parallelism.
More consumers than partitions do nothing.

**Rebalance** — Partitions being reassigned when a consumer joins or leaves.
Velocity windows stay behind in the old process, which `make rebalance`
measures.

**Redpanda** — A Kafka-compatible broker in one container, used locally and in
CI.

**Revoke hook** — `OnPartitionsRevoked`: runs before partitions are taken away,
and flushes and commits.

**Topic** — A named stream: `transactions` in, `verdicts` out,
`transactions.dlq` for rejects.

## API and dashboard

**Accepted prefix** — When ingest is full it keeps the first N transactions of
a batch and rejects the rest. The caller resends from N.

**Ingest** — `POST /api/transactions`: batches of transactions over HTTP.

**415** — The answer to a POST that isn't `Content-Type: application/json`.
Stops other websites from sending writes without a CORS preflight.

**Load generator** — `cmd/loadgen`: sends synthetic traffic over HTTP or
Kafka and reports the rate the server actually kept.

**Optimistic resolve / Rollback** — The dashboard removes a case as soon as the
analyst clicks, and puts it back with an error if the server refuses.
`casesReducer.ts`.

**Pause** — The review queue stops re-ordering while the pointer or keyboard
focus is inside it.

**Shed load** — Transactions rejected because the engine was full. Counted
separately and never mixed into throughput.

**Simulator** — The built-in traffic generator started from the dashboard.
`POST /api/simulate`.

**SSE** (Server-Sent Events) — The one-way stream the dashboard reads every
400ms: stats, recent verdicts, case counts and the top open cases.
`GET /api/stream`.

**503 vs 429** — Ingest answers 503 (server at capacity) rather than 429
(caller over its rate limit), with `Retry-After: 1`.

## Metrics and operations

**Bucket / Histogram** — Scoring latency is counted in fixed ranges (1µs up to
1s) rather than stored per request. `histogram.go`.

**Colima** — A free tool that runs Docker on macOS in a small Linux VM. Used to
run the broker locally.

**KEDA / HPA** — Kubernetes autoscalers. HPA scales on CPU by default, which a
lagging consumer may not show. KEDA can scale on consumer lag.

**Overflow** — What a quantile reports when it falls above the largest bucket
(slower than 1s). The dashboard shows "off scale".

**p50 / p99** — Median and 99th-percentile scoring latency. Read from the
histogram, so they are bounds: "≤50µs", not "50µs".

**Prometheus exposition** — The text format `GET /metrics` serves, written by
hand in `internal/metrics`.
