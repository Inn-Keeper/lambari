package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"lambari/internal/model"
)

// TestCrashReplayAtLeastOnce proves the verdict-delivery claim against a real
// broker: SIGKILL the consumer mid-stream, restart it, and every transaction's
// verdict still arrives. It does not cover the in-memory case queue. Duplicates
// are allowed — that is what at-least-once means. A graceful shutdown can't
// stand in for the crash: the revoke hook would (correctly) commit on the way
// out, so the process dies by signal, never by context.
//
// Skipped unless LAMBARI_KAFKA_BROKERS is set — run via `make e2e`.
func TestCrashReplayAtLeastOnce(t *testing.T) {
	brokers := os.Getenv("LAMBARI_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set LAMBARI_KAFKA_BROKERS to run (make e2e)")
	}
	seeds := strings.Split(brokers, ",")
	// Two waves of transactions; the kill lands inside the first. The wave is
	// large on purpose: the engine chews a small one in milliseconds, and the
	// SIGKILL has to land while scored-but-uncommitted work is in flight for
	// the replay path to actually be exercised.
	const wave = 5000
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

	// produce streams [from, from+n) with `delay` between records. t.Error,
	// not t.Fatal: wave 1 runs on a goroutine, and a produce failure shows up
	// anyway as missing verdicts in waitVerdicts.
	produce := func(from, n int, delay time.Duration) {
		prod, err := NewProducer(seeds)
		if err != nil {
			t.Error(err)
			return
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
				t.Error(err)
				return
			}
			time.Sleep(delay)
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
		// Never leak the process, even when an assertion Fatals mid-test: an
		// orphan would keep :18080 and stay in the consumer group, poisoning
		// the next run. SIGTERM rather than SIGKILL so the survivor leaves the
		// group on the way out — a killed member lingers until its session
		// times out (~45s), and the next test to join this group waits for the
		// ghost. Signalling an already-dead process just errors — ignored.
		t.Cleanup(func() { _ = cmd.Process.Signal(syscall.SIGTERM); _, _ = cmd.Process.Wait() })
		return cmd
	}

	api := startAPI()
	// Stream wave 1 while the consumer is live: a pre-produced wave arrives as
	// one giant fetch that is scored and committed in milliseconds, and the
	// kill can never catch work in flight. Streaming keeps the consumer in
	// small poll-score-commit cycles instead.
	go produce(0, wave, 250*time.Microsecond)
	waitVerdicts(wave/5, time.Minute)          // some scored, more still in flight —
	if err := api.Process.Kill(); err != nil { // — SIGKILL: a real crash, no graceful commit
		t.Fatal(err)
	}
	_ = api.Wait()

	produce(wave, wave, 0) // traffic keeps arriving while the consumer is down
	startAPI()             // fresh consumer, same group: resumes from the last commit

	waitVerdicts(2*wave, 2*time.Minute)

	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups++
		}
	}
	// Duplicates appear only when the kill lands inside a poll-score-commit
	// cycle (a few ms wide) — allowed and correct, but not guaranteed per run.
	// The assertion above is the invariant: no expected verdict is lost.
	t.Logf("all %d verdicts arrived; %d duplicate publishes (allowed under at-least-once)", 2*wave, dups)
}
