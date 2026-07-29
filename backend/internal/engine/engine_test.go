package engine

import (
	"fmt"
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
