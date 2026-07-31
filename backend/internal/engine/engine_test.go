package engine

import (
	"fmt"
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
