# Changelog

Notable changes, newest first. Earlier history is in `git log`.

## 2026-09-24

### Fixed
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
- `docs/glossary.md`: the project's vocabulary, grouped by domain.
- GitHub Actions CI: gofmt, vet and race tests; dashboard tests and build; and
  the crash-replay and rebalance experiments against a real Redpanda broker.
- `AGENTS.md` for this repo, replacing one copied from another project.
