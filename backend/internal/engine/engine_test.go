package engine

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lambari/internal/model"
)

func benignTx(i int) model.Transaction {
	return model.Transaction{
		ID: fmt.Sprintf("tx_%d", i), CardBIN: "520082", CardHash: fmt.Sprintf("tok_%d", i),
		Amount: 49.90, Currency: "SEK", Country: "SE",
		IP: fmt.Sprintf("10.0.%d.%d", i%255, (i/255)%255), MerchantID: "m_001", MCC: "5411",
		Timestamp: time.Now(),
	}
}

func TestBenignTransactionApproves(t *testing.T) {
	e := New()
	e.Start()
	e.Submit(benignTx(1))
	waitProcessed(t, e, 1)
	e.Stop()
	s := e.Snapshot()
	if s.Approved != 1 {
		t.Fatalf("expected 1 approval, got %+v", s)
	}
}

func TestVelocityAttackDeclines(t *testing.T) {
	e := New()
	e.Start()
	for i := 0; i < 10; i++ {
		tx := benignTx(i)
		tx.CardHash = "tok_hot" // same card, 10 hits in one window
		e.Submit(tx)
	}
	waitProcessed(t, e, 10)
	e.Stop()
	s := e.Snapshot()
	if s.RuleFires["card_velocity_extreme"] == 0 {
		t.Fatalf("expected extreme velocity fires, got %+v", s.RuleFires)
	}
}

func TestGeoMismatchPlusAmountReviews(t *testing.T) {
	e := New()
	e.Start()
	tx := benignTx(1)
	tx.Country = "RU" // card issued SE (BIN 520082), used in RU → 25
	tx.Amount = 1600  // high amount → 20 ⇒ total 45 ⇒ review
	e.Submit(tx)
	waitProcessed(t, e, 1)
	e.Stop()
	s := e.Snapshot()
	if s.Reviewed != 1 {
		t.Fatalf("expected 1 review, got %+v", s)
	}
}

func TestDecideThresholds(t *testing.T) {
	cases := []struct {
		score int
		want  model.Decision
	}{{0, model.Approve}, {39, model.Approve}, {40, model.Review}, {69, model.Review}, {70, model.Decline}, {120, model.Decline}}
	for _, c := range cases {
		if got := Decide(c.score); got != c.want {
			t.Errorf("Decide(%d) = %s, want %s", c.score, got, c.want)
		}
	}
}

// SubmitBatch is what lets a Kafka consumer commit offsets safely: it must not
// return until every transaction in the batch has actually been scored.
// Returning early is at-most-once — a crash after the commit loses whatever is
// still sitting in the buffer, silently.
func TestSubmitBatchReturnsOnlyAfterScoring(t *testing.T) {
	e := New()
	release := make(chan struct{})
	var scored atomic.Int64
	e.rules = []Rule{func(_ *model.Transaction, _ *State) (int, string) {
		<-release
		scored.Add(1)
		return 0, ""
	}}
	e.Start()

	txs := make([]model.Transaction, 8)
	for i := range txs {
		txs[i] = benignTx(i)
	}

	returned := make(chan struct{})
	go func() {
		e.SubmitBatch(txs)
		close(returned)
	}()

	select {
	case <-returned:
		t.Fatal("SubmitBatch returned while scoring was still blocked — committing offsets here would be at-most-once")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitBatch never returned after scoring was released")
	}

	if got := scored.Load(); got != int64(len(txs)) {
		t.Fatalf("SubmitBatch returned with %d of %d scored", got, len(txs))
	}
	e.Stop()
}

func waitProcessed(t *testing.T, e *Engine, n int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for e.processed.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d processed (have %d)", n, e.processed.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// BenchmarkEngineThroughput measures end-to-end scoring throughput through
// the full worker pool — the number this PoC exists to demonstrate.
// Run with: go test -bench=. -benchtime=3s ./internal/engine/
func BenchmarkEngineThroughput(b *testing.B) {
	e := New()
	e.Start()
	gen := model.NewGenerator(42)
	txs := make([]model.Transaction, b.N)
	for i := range txs {
		txs[i] = gen.Next()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Submit(txs[i])
	}
	e.Stop() // waits for drain — includes all scoring work in the timing
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/sec")
}

// Shed load that nobody counts makes a saturated engine look identical to an
// idle one — the transactions simply never appear anywhere.
func TestRecordRejectedAccumulates(t *testing.T) {
	e := New() // no Start: nothing drains the buffer, so it fills and stays full
	filled := 0
	for e.TrySubmit(benignTx(filled)) {
		filled++
	}
	if filled == 0 {
		t.Fatal("buffer refused the very first transaction")
	}
	if got := e.Snapshot().Rejected; got != 0 {
		t.Fatalf("Rejected = %d before anything was reported, want 0", got)
	}

	// A caller that gave up on the rest of a 40-transaction batch reports all
	// 40, not the single refusal it happened to observe.
	e.RecordRejected(40)
	e.RecordRejected(2)
	if got := e.Snapshot().Rejected; got != 42 {
		t.Fatalf("Rejected = %d, want 42", got)
	}
}

// A hot key must not grow without bound: each touch scans the whole slice. The
// cap still has to let the largest threshold (30) be reached.
func TestVelocityWindowIsCappedButStillSaturates(t *testing.T) {
	sh := &shard{seen: map[string][]int64{}}
	var n int
	for i := int64(0); i < 1000; i++ {
		n = sh.touch("ip", 1_000+i, 300_000)
	}
	if got := len(sh.seen["ip"]); got != maxWindowEvents {
		t.Fatalf("kept %d timestamps, want %d", got, maxWindowEvents)
	}
	if n < 30 {
		t.Fatalf("count saturated at %d, below the ip_fanout_extreme threshold", n)
	}
	if last := sh.seen["ip"][maxWindowEvents-1]; last != 1_999 {
		t.Fatalf("newest kept timestamp = %d, want 1999 (oldest must be dropped)", last)
	}
}

// One card's transactions must be scored in submission order, or the
// card_velocity_extreme flag (the 8th hit in 60s) can land on an earlier,
// possibly legitimate, transaction.
func TestOneCardIsScoredInSubmissionOrder(t *testing.T) {
	e := New()
	var mu sync.Mutex
	var extreme []string
	e.OnFlagged(func(v model.Verdict) {
		for _, f := range v.Flags {
			if f == "card_velocity_extreme" {
				mu.Lock()
				extreme = append(extreme, v.TxID)
				mu.Unlock()
			}
		}
	})
	e.Start()
	defer e.Stop()

	start := time.Now()
	txs := make([]model.Transaction, 8)
	for i := range txs {
		txs[i] = benignTx(i)
		txs[i].CardHash = "tok_hot"
		txs[i].Timestamp = start.Add(time.Duration(i) * time.Millisecond)
	}
	e.SubmitBatch(txs)

	if len(extreme) != 1 || extreme[0] != "tx_7" {
		t.Fatalf("card_velocity_extreme on %v, want only the 8th (tx_7)", extreme)
	}
}

// Late events must not displace newer history. The review's reproduction:
// 30 current events, 30 valid ones from six minutes ago, then one more
// current event, which must still see the active window.
func TestLateEventsDoNotEraseActiveWindow(t *testing.T) {
	sh := &shard{seen: map[string][]int64{}}
	const window = 300_000 // IP window, 5 min
	now := int64(10_000_000)
	for i := int64(0); i < 30; i++ {
		sh.touch("ip", now+i, window)
	}
	for i := int64(0); i < 30; i++ {
		if n := sh.touch("ip", now-6*60_000+i, window); n != 1 {
			t.Fatalf("event older than the window counted %d, want 1 (scored alone)", n)
		}
	}
	if n := sh.touch("ip", now+30, window); n < 30 {
		t.Fatalf("current event counted %d after late events, want ≥30 (ip_fanout_extreme)", n)
	}
}

// Arrival order must not change a count: in-window events that arrive late
// are inserted by timestamp, and each event counts what precedes it in time.
func TestOutOfOrderEventsAreCountedByTimestamp(t *testing.T) {
	sh := &shard{seen: map[string][]int64{}}
	const window = 60_000
	for _, ts := range []int64{5_000, 1_000, 3_000, 2_000, 4_000} {
		sh.touch("card", ts, window)
	}
	if got := sh.seen["card"]; !slices.IsSorted(got) || len(got) != 5 {
		t.Fatalf("window = %v, want 5 timestamps in order", got)
	}
	if n := sh.touch("card", 6_000, window); n != 6 {
		t.Fatalf("count = %d, want 6", n)
	}
	// A late event counts only what came before it in time: 1000..2500.
	if n := sh.touch("card", 2_500, window); n != 3 {
		t.Fatalf("late event counted %d, want 3", n)
	}
}
