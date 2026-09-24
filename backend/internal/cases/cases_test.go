package cases

import (
	"fmt"
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

// Under at-least-once delivery the same verdict can arrive twice (crash
// between scoring and offset commit). Open must suppress the duplicate or
// every replay would fork a duplicate case for the analyst queue.
func TestOpenSuppressesDuplicateWhileCaseIsOpen(t *testing.T) {
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

// Suppression lasts only as long as the case: once resolved, a replay reopens
// it. This pins MemStore's behaviour, not the Store contract.
func TestResolvedCaseIsReopenedByAReplay(t *testing.T) {
	s := NewMemStore(10)
	s.Open(v("tx_dup", 50))
	if _, err := s.Resolve("tx_dup", FalsePositive); err != nil {
		t.Fatal(err)
	}
	if open, _, fp := s.Counts(); open != 0 || fp != 1 {
		t.Fatalf("after resolve: open=%d falsePos=%d, want 0 and 1", open, fp)
	}

	s.Open(v("tx_dup", 50)) // redelivery, after the analyst already ruled on it
	if open, _, _ := s.Counts(); open != 1 {
		t.Fatalf("replay after resolution left %d open cases, want 1 — if this ever "+
			"reads 0, Open started remembering resolved TxIDs and the docs need updating "+
			"in the other direction", open)
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

// Resolving must drop the id from the insertion order, or the list grows
// forever and a reopened case inherits its old, older position.
func TestInsertionOrderTracksOnlyOpenCases(t *testing.T) {
	s := NewMemStore(2)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("tx_%d", i)
		s.Open(v(id, 50))
		if _, err := s.Resolve(id, FalsePositive); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(s.openIDs); n != 0 {
		t.Fatalf("%d ids retained with no open cases", n)
	}

	s.Open(v("tx_a", 50))
	s.Open(v("tx_b", 50))
	if _, err := s.Resolve("tx_a", FalsePositive); err != nil {
		t.Fatal(err)
	}
	s.Open(v("tx_a", 50)) // replay reopens it: now the newest
	s.Open(v("tx_c", 50)) // over capacity: the oldest open case is tx_b

	ids := map[string]bool{}
	for _, c := range s.List(Open, 10) {
		ids[c.ID] = true
	}
	if !ids["tx_a"] || ids["tx_b"] || !ids["tx_c"] {
		t.Fatalf("open = %v, want tx_a and tx_c (tx_b evicted as oldest)", ids)
	}
}
