package cases

import (
	"testing"

	"lambari/internal/model"
)

func v(id string, score int) model.Verdict {
	return model.Verdict{TxID: id, Score: score, Decision: model.Review}
}

func TestOpenResolveFlow(t *testing.T) {
	s := NewMemStore(10)
	s.Open(v("tx_1", 50))
	s.Open(v("tx_2", 90))

	open := s.List(Open, 10)
	if len(open) != 2 || open[0].ID != "tx_2" {
		t.Fatalf("expected 2 open, highest score first, got %+v", open)
	}

	c, err := s.Resolve("tx_1", FalsePositive)
	if err != nil || c.Resolution != FalsePositive {
		t.Fatalf("resolve failed: %v %+v", err, c)
	}
	if _, err := s.Resolve("tx_1", ConfirmedFraud); err != ErrNotFound {
		t.Fatalf("double-resolve should fail, got %v", err)
	}

	o, conf, fp := s.Counts()
	if o != 1 || conf != 0 || fp != 1 {
		t.Fatalf("counts wrong: open=%d conf=%d fp=%d", o, conf, fp)
	}
}

func TestEvictionKeepsNewest(t *testing.T) {
	s := NewMemStore(3)
	for i := 0; i < 5; i++ {
		s.Open(v(string(rune('a'+i)), 40))
	}
	if open, _, _ := s.Counts(); open != 3 {
		t.Fatalf("expected 3 open after eviction, got %d", open)
	}
}

func TestResolvedHistoryIsBounded(t *testing.T) {
	s := NewMemStore(3)
	for i := 0; i < 6; i++ {
		id := string(rune('a' + i))
		s.Open(v(id, 40))
		if _, err := s.Resolve(id, ConfirmedFraud); err != nil {
			t.Fatalf("resolve %s: %v", id, err)
		}
	}
	if got := len(s.List(Resolved, 100)); got != 3 {
		t.Fatalf("history should be capped at 3, got %d", got)
	}
	open, conf, _ := s.Counts()
	if open != 0 || conf != 6 {
		t.Fatalf("counts should survive eviction: open=%d conf=%d", open, conf)
	}
}
