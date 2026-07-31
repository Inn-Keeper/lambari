# Kafka At-Least-Once Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove and complete the at-least-once Kafka pipeline: flush async producers before offset commits, pin cases idempotency with a unit test, and verify no-loss end-to-end by SIGKILLing a real consumer process against Redpanda.

**Architecture:** The at-least-once core (DisableAutoCommit, score-then-commit via `SubmitBatch`, DLQ, commit-on-revoke) is already in the working tree. This plan adds the missing durability edge — verdict/DLQ producer flush *before* every offset commit, otherwise a crash right after a commit loses buffered publishes — plus the tests that make the claim checkable.

**Tech Stack:** Go 1.x, franz-go (`kgo`), Redpanda via docker-compose (`localhost:19092`), stdlib `testing` + `os/exec`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-31-kafka-at-least-once-design.md`.
- No new dependencies. No dedup cache (effect-level idempotency per spec).
- Commits attributed to the user only — no Claude/Anthropic trailers.
- All working-tree WIP + this plan's changes land as **one feature commit** at the end (Task 4); the e2e uses `LAMBARI_ADDR=:18080` to avoid a dev server on `:8080`.

---

### Task 1: Pin cases idempotency

**Files:**
- Test: `backend/internal/cases/cases_test.go` (append)

**Interfaces:**
- Consumes: existing `NewMemStore(maxOpen int)`, `Open(v model.Verdict)`, `Counts()`, `List(status, limit)`, and the test helper `v(id string, score int) model.Verdict` already in this file.
- Produces: nothing new — a pinning test only.

- [ ] **Step 1: Write the test** (expected to pass — it pins existing uncommitted behavior; a failure means the dedupe is broken and must be fixed before proceeding)

```go
// Under at-least-once delivery the same verdict can arrive twice (crash
// between scoring and offset commit). Open must be idempotent per TxID or
// every replay would fork a duplicate case for the analyst queue.
func TestOpenIsIdempotentPerTxID(t *testing.T) {
	s := NewMemStore(10)
	s.Open(v("tx_dup", 50))
	s.Open(v("tx_dup", 90)) // redelivery of the same transaction
	open, _, _ := s.Counts()
	if open != 1 {
		t.Fatalf("duplicate verdict opened %d cases, want 1", open)
	}
	got := s.List(Open, 10)
	if len(got) != 1 || got[0].Verdict.Score != 50 {
		t.Fatalf("first verdict must win, got %+v", got)
	}
}
```

- [ ] **Step 2: Run it**

Run: `cd backend && go test -run TestOpenIsIdempotentPerTxID ./internal/cases/ -v`
Expected: PASS (if FAIL, fix `MemStore.Open`'s existing early-return before continuing)

---

### Task 2: Flush producers before every offset commit

**Files:**
- Modify: `backend/internal/kafka/kafka.go` (Consumer struct, `NewConsumer`, `Run`, revoke hook; add `Flush` to `DLQProducer` and `VerdictProducer`)
- Modify: `backend/cmd/api/main.go:32-53` (create `VerdictProducer` before the consumer, pass it in)

**Interfaces:**
- Consumes: `kgo.Client.Flush(ctx) error`.
- Produces: `NewConsumer(brokers []string, eng *engine.Engine, dlq *DLQProducer, verdicts *VerdictProducer) (*Consumer, error)` — note the new 4th param; `(*DLQProducer).Flush(ctx) error`; `(*VerdictProducer).Flush(ctx) error`. Task 3's e2e is the test for this behavior (not unit-testable without a broker).

- [ ] **Step 1: Add Flush methods** (one near each producer's Close)

```go
func (p *DLQProducer) Flush(ctx context.Context) error { return p.client.Flush(ctx) }
```

```go
func (p *VerdictProducer) Flush(ctx context.Context) error { return p.client.Flush(ctx) }
```

- [ ] **Step 2: Thread a flush func through the Consumer**

Struct gains:

```go
	// flush forces the async producers (verdicts, DLQ) to durable storage.
	// Called before every offset commit: committing first would mean a crash
	// silently loses whatever publishes were still buffered.
	flush func(ctx context.Context) error
```

`NewConsumer` becomes:

```go
func NewConsumer(brokers []string, eng *engine.Engine, dlq *DLQProducer, verdicts *VerdictProducer) (*Consumer, error) {
	c := &Consumer{eng: eng, dlq: dlq.Send, flush: func(ctx context.Context) error {
		if err := dlq.Flush(ctx); err != nil {
			return err
		}
		return verdicts.Flush(ctx)
	}}
```

and the revoke hook flushes first, skipping the commit if it cannot:

```go
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			if err := c.flush(ctx); err != nil {
				slog.Error("flush on partition revoke — skipping commit, records will replay", "err", err)
				return
			}
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				slog.Error("commit on partition revoke", "revoked", revoked, "err", err)
			}
		}),
```

- [ ] **Step 3: Flush in the Run loop, before the commit**

Between `c.handleRecords(ctx, recs)` and `CommitUncommittedOffsets` in `Run`:

```go
		if err := c.flush(ctx); err != nil {
			// Not committing is the safe failure: these records redeliver on
			// restart instead of their verdicts vanishing.
			if ctx.Err() == nil {
				slog.Error("producer flush before commit — skipping commit", "err", err)
			}
			continue
		}
```

- [ ] **Step 4: Rewire main.go** — create `verdictPub` before the consumer and pass it:

```go
		dlq, err := kafka.NewDLQProducer(seeds)
		if err != nil {
			slog.Error("dlq producer failed", "err", err)
			os.Exit(1)
		}
		defer dlq.Close()
		if verdictPub, err = kafka.NewVerdictProducer(seeds); err != nil {
			slog.Error("verdict producer failed", "err", err)
			os.Exit(1)
		}
		consumer, err := kafka.NewConsumer(seeds, eng, dlq, verdictPub)
		if err != nil {
			slog.Error("kafka connect failed", "err", err)
			os.Exit(1)
		}
		go consumer.Run(ctx)
```

- [ ] **Step 5: Everything still compiles and unit tests pass**

Run: `cd backend && go vet ./... && go test ./...`
Expected: PASS (existing `kafka_test.go` builds the Consumer struct literal directly and never calls flush — unaffected)

---

### Task 3: Crash-replay e2e test + make target

**Files:**
- Create: `backend/internal/kafka/e2e_test.go`
- Modify: `Makefile` (add `e2e` target, extend `.PHONY`)

**Interfaces:**
- Consumes: `NewProducer(seeds)` / `(*Producer).Send` / `Close` (flushes), `VerdictTopic`, `model.Transaction`, `model.Verdict` (field `TxID`), Task 2's flush-before-commit (without it, this test fails: verdicts committed-but-unflushed die with the SIGKILL).
- Produces: `make e2e`.

- [ ] **Step 1: Write the e2e test**

```go
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"lambari/internal/model"
)

// TestCrashReplayAtLeastOnce proves the delivery-semantics claim against a
// real broker: SIGKILL the consumer mid-stream, restart it, and every
// transaction's verdict still arrives. Duplicates are allowed — that is what
// at-least-once means. A graceful shutdown can't stand in for the crash: the
// revoke hook would (correctly) commit on the way out, so the process dies by
// signal, never by context.
//
// Skipped unless LAMBARI_KAFKA_BROKERS is set — run via `make e2e`.
func TestCrashReplayAtLeastOnce(t *testing.T) {
	brokers := os.Getenv("LAMBARI_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set LAMBARI_KAFKA_BROKERS to run (make e2e)")
	}
	seeds := strings.Split(brokers, ",")
	const wave = 250 // two waves of transactions; the kill lands inside the first
	runID := fmt.Sprintf("e2e_%d", time.Now().UnixNano())

	bin := filepath.Join(t.TempDir(), "api")
	build := exec.Command("go", "build", "-o", bin, "./cmd/api")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// Watcher tails the verdicts topic from the start (no group); old runs'
	// records are filtered out by the runID prefix.
	watcher, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(VerdictTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	seen := map[string]int{} // TxID -> times its verdict appeared

	waitVerdicts := func(want int, deadline time.Duration) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()
		for len(seen) < want {
			fetches := watcher.PollFetches(ctx)
			if ctx.Err() != nil {
				t.Fatalf("at-least-once violated: only %d of %d verdicts arrived within %v",
					len(seen), want, deadline)
			}
			fetches.EachRecord(func(rec *kgo.Record) {
				var v model.Verdict
				if json.Unmarshal(rec.Value, &v) == nil && strings.HasPrefix(v.TxID, runID) {
					seen[v.TxID]++
				}
			})
		}
	}

	produce := func(from, n int) {
		t.Helper()
		prod, err := NewProducer(seeds)
		if err != nil {
			t.Fatal(err)
		}
		defer prod.Close() // Close flushes
		for i := from; i < from+n; i++ {
			tx := model.Transaction{
				ID:      fmt.Sprintf("%s_%d", runID, i),
				CardBIN: "520082", CardHash: fmt.Sprintf("%s_c%d", runID, i),
				Amount:   9999, // ≥5000 → amount_extreme → always flagged → always a verdict
				Currency: "SEK", Country: "SE",
				IP: fmt.Sprintf("10.9.%d.%d", i/200, i%200), MerchantID: "m_e2e", MCC: "5411",
				Timestamp: time.Now(),
			}
			if err := prod.Send(context.Background(), tx); err != nil {
				t.Fatal(err)
			}
		}
	}

	startAPI := func() *exec.Cmd {
		t.Helper()
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"LAMBARI_KAFKA_BROKERS="+brokers,
			"LAMBARI_ADDR=:18080", // stay clear of a dev server on :8080
		)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}

	produce(0, wave)
	api := startAPI()
	waitVerdicts(wave/5, time.Minute) // some scored, more still in flight —
	if err := api.Process.Kill(); err != nil { // — SIGKILL: a real crash, no graceful commit
		t.Fatal(err)
	}
	_ = api.Wait()

	produce(wave, wave) // traffic keeps arriving while the consumer is down
	api2 := startAPI()
	defer func() { _ = api2.Process.Kill(); _ = api2.Wait() }()

	waitVerdicts(2*wave, 2*time.Minute)

	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups++
		}
	}
	t.Logf("all %d verdicts arrived; %d duplicates (expected under at-least-once)", 2*wave, dups)
}
```

- [ ] **Step 2: Compile check (skips without broker)**

Run: `cd backend && go test -run TestCrashReplayAtLeastOnce ./internal/kafka/ -v`
Expected: SKIP with "set LAMBARI_KAFKA_BROKERS to run (make e2e)"

- [ ] **Step 3: Add the make target**

`.PHONY` line gains ` e2e`; target (after `kafka-loadgen`):

```makefile
e2e:              ## crash-replay at-least-once test against Redpanda (SIGKILLs a real consumer)
	docker compose up -d --wait redpanda
	cd backend && LAMBARI_KAFKA_BROKERS=localhost:19092 go test -run TestCrashReplayAtLeastOnce -v -timeout 5m -count=1 ./internal/kafka/
```

---

### Task 4: Verify, align spec, ship

**Files:**
- Modify: `docs/superpowers/specs/2026-07-31-kafka-at-least-once-design.md` (Verification section)
- Commit: everything (WIP + Tasks 1-3)

- [ ] **Step 1: Update the spec's e2e paragraph** to match the built reality — replace the numbered e2e list with: SIGKILL of a real `cmd/api` process (graceful cancel can't simulate a crash — the revoke hook correctly commits on shutdown), verdict observation via the `verdicts` topic, two waves of 250, and add flush-before-commit to the "Core mechanism" section (async verdict/DLQ publishes must be durable before the offsets covering them are committed).

- [ ] **Step 2: Full unit run**

Run: `cd backend && go vet ./... && go test ./...`
Expected: all PASS

- [ ] **Step 3: e2e against Redpanda**

Run: `make e2e`
Expected: PASS, log line "all 500 verdicts arrived; N duplicates (expected under at-least-once)"

- [ ] **Step 4: One feature commit** (user attribution only, no Claude trailer)

```bash
git add -A backend/ Makefile docs/superpowers/specs/2026-07-31-kafka-at-least-once-design.md docs/superpowers/plans/2026-07-31-kafka-at-least-once.md
git commit -m "Make Kafka pipeline at-least-once, verified by crash-replay e2e

- Commit offsets only after the engine has scored the batch (SubmitBatch
  blocks until done); DisableAutoCommit + commit-on-revoke bound rebalance
  redelivery to one in-flight batch
- Route undecodable records to transactions.dlq with origin headers
- Flush verdict/DLQ producers before every offset commit so a crash after
  the commit cannot lose buffered publishes
- Effect-level idempotency: cases dedupe by TxID (pinned by test), verdicts
  keyed by TxID for downstream dedupe/compaction
- e2e: SIGKILL a real consumer mid-stream against Redpanda, restart, assert
  every verdict arrives at least once (make e2e)"
```

- [ ] **Step 5: Memory update** — correct `lambari-open-gaps.md`: Kafka at-most-once gap is closed (at-least-once + e2e proof); real-e2e gap partially closed (crash-replay exists; full-pipeline ceiling still open); metrics + K8s remain.
