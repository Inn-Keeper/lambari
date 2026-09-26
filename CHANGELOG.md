# Changelog

Notable changes, newest first. Earlier history is in `git log`.

## 2026-09-24

### Fixed
- Engine: late events no longer erase an active velocity window. Windows kept
  the last 30 *arrivals* and expired entries relative to the incoming event's
  own time, so 30 events from six minutes ago pushed out 30 current ones and
  the next current event scored 0 instead of 40. Windows are now sorted by
  timestamp, expire relative to the newest one, and keep the newest 30; an
  event older than the whole window is scored alone.
- Engine: a late event in a full window is counted before the 30-entry cap
  trims the oldest timestamp. It lost one timestamp inside its own window and
  could miss a threshold (29 counted instead of 30 for `ip_fanout_extreme`).
- Engine: one card's transactions are scored in the order they were
  submitted. Workers used to share one queue, so the extreme-velocity flag
  could land on an earlier transaction for the card (88 of 200 test runs);
  each card now always goes to the same worker's queue.
- Kafka: records from healthy partitions are scored when another partition
  in the same fetch errors, and records from the last poll are scored on
  shutdown. Both used to be dropped while still counting as polled, so a
  later commit could skip them.
- Kafka: lag is no longer reported for partitions this consumer lost.
- API: POSTs must be `application/json` (else `415`). A cross-origin
  `text/plain` POST needs no CORS preflight and could resolve cases.
- Case store: resolving removes the case from the insertion order, which
  grew without bound and could evict a reopened case too early.
- Load generator: Kafka throughput is reported after the flush and excludes
  failed publishes, which it used to count as accepted.
- Dashboard: a case resolved while the queue was paused no longer reappears
  on resume.
- CI: the rebalance experiment waits until both consumers own partitions.
  On a fast runner warm-up finished before the second one joined, so every
  card moved and the run failed as a full reset.
- Kafka: a failed verdict or DLQ publish no longer gets its offsets committed.
  kgo's `Flush` returns nil for failed records, so the producers now count
  failures; one failure stops all commits and the process exits non-zero to
  replay from the last good commit.
- Kafka: rebalances wait for the in-flight batch (`BlockRebalanceOnPoll`), so
  the revoke hook can no longer commit a batch that is still being scored.
- Kafka: verdicts and DLQ records buffered during a graceful shutdown are no
  longer failed by the cancelled context and then committed past.
- Shutdown no longer panics with `send on closed channel` while the simulator
  runs, and open SSE streams no longer hold shutdown for its 5s timeout.
- Ingest rejects bodies over 8 MiB, and transactions without `id` or
  `card_hash` or dated more than 5s ahead (a future timestamp wiped the card's
  velocity window). Kafka routes the same records to the DLQ.
- Dashboard: the review queue holds still under keyboard focus, not only under
  the pointer. Case ids are URL-encoded when resolving.
- `roundCents` in the generator rounded down instead of rounding.

### Changed
- The sweeper keeps velocity entries for 5 minutes, the longest rule window,
  instead of 10.
- Velocity windows keep at most 30 timestamps per key (the largest rule
  threshold). Engine benchmark: ~620–760k → ~1.2–1.5M tx/s on the same machine.
- The SSE frame carries the top 8 open cases; the dashboard no longer polls
  `/api/cases` every 400ms.
- The simulator rate slider posts once on release instead of on every step,
  each of which restarted the simulator.
- The dashboard shows shed load on the Throughput card.
- Removed the `Access-Control-Allow-Origin: *` middleware; the dashboard is
  same-origin through the Vite proxy.
- The BIN→country table lives once, in `model`. `make setup` uses
  `go mod download` instead of rewriting `go.mod` with `go mod tidy`.
- Trimmed long code comments to what the code needs.
- Docs: README "Run it locally" section; architecture page figures corrected;
  `knowledge-base.md` links to the README instead of copying five sections;
  removed `docs/diagrams.html` (duplicate of `docs/diagrams.md`).

### Added
- Deploy to Render's free plan: `backend/Dockerfile` (17.5 MB distroless
  image) and a `render.yaml` Blueprint. README "Deploy" section covers Render
  and the Vercel dashboard.
- `LAMBARI_ALLOWED_ORIGINS`: CORS for listed origins only, preflight included.
- Demo limits for a public deploy: `LAMBARI_SIM_MAX_RATE`,
  `LAMBARI_SIM_MAX_DURATION` (the simulator stops itself) and
  `LAMBARI_DISABLE_INGEST`. The API listens on `PORT` when it is set.
- Dashboard: `VITE_API_BASE` points it at a separate backend; the simulator
  slider follows the server's rate cap.
- `docs/glossary.md`: the project's vocabulary, grouped by domain.
- GitHub Actions CI: gofmt, vet and race tests; dashboard tests and build; and
  the crash-replay and rebalance experiments against a real Redpanda broker.
- `AGENTS.md` for this repo, replacing one copied from another project.
