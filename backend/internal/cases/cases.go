// Package cases turns flagged verdicts into a review queue. This PoC keeps
// cases in memory behind a Store interface; schema.sql in the repo root
// defines the identical Postgres shape for production (swap in a pgx
// implementation without touching handlers).
//
// Analyst resolutions are kept as labels — confirmed fraud / false positive —
// which is exactly the training data a future ML rule needs.
package cases

import (
	"errors"
	"sort"
	"sync"
	"time"

	"lambari/internal/model"
)

type Status string

const (
	Open     Status = "open"
	Resolved Status = "resolved"
)

type Resolution string

const (
	ConfirmedFraud Resolution = "confirmed_fraud"
	FalsePositive  Resolution = "false_positive"
)

type Case struct {
	ID         string        `json:"id"` // = tx_id, 1:1 for the PoC
	Verdict    model.Verdict `json:"verdict"`
	Status     Status        `json:"status"`
	Resolution Resolution    `json:"resolution,omitempty"`
	OpenedAt   int64         `json:"opened_at"`
	ResolvedAt int64         `json:"resolved_at,omitempty"`
}

var ErrNotFound = errors.New("case not found")

// Store is the swap point for Postgres later.
type Store interface {
	Open(v model.Verdict)
	List(status Status, limit int) []Case
	Resolve(id string, r Resolution) (Case, error)
	Counts() (open, confirmed, falsePos int64)
}

// MemStore keeps the newest maxOpen open cases; older unreviewed ones are
// evicted (at 5k tx/s you triage by recency or you drown). Resolved cases
// move to a capped history so memory stays bounded on long runs — the
// permanent record is Postgres's job, not this store's.
type MemStore struct {
	mu       sync.Mutex
	open     map[string]*Case
	openIDs  []string // insertion order, oldest first
	maxOpen  int
	history  []Case // resolved, newest last, capped at maxOpen
	resolved struct{ confirmed, falsePos int64 }
}

func NewMemStore(maxOpen int) *MemStore {
	return &MemStore{open: make(map[string]*Case), maxOpen: maxOpen}
}

// Open creates a case for a flagged verdict, suppressing a duplicate **while
// the case is still open** — which covers the window that matters, since
// redelivery is bounded to one in-flight batch.
//
// It is not idempotent for all time, and the docs must not claim it is: the
// TxID is forgotten once the case is resolved or evicted, and a replay after
// either reopens the case. That is a property of *this* store being in-memory
// and capped, not a design principle — `schema.sql` makes tx_id the primary
// key, so a pgx implementation would dedupe for the row's whole lifetime and be
// strictly better here.
//
// It is specifically not an argument against durable dedupe. The thing this
// design rejects is a *consumer-level* dedup cache that skips a record before
// scoring, because that would block the velocity-window rebuild replay exists
// to perform. Open runs after Engine.score has already advanced those windows,
// so deduping here cannot block anything.
func (s *MemStore) Open(v model.Verdict) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.open[v.TxID]; exists {
		return
	}
	s.open[v.TxID] = &Case{ID: v.TxID, Verdict: v, Status: Open, OpenedAt: time.Now().UnixMilli()}
	s.openIDs = append(s.openIDs, v.TxID)
	// evict oldest; openIDs may hold ids already resolved (removed from the
	// map), so skip those until an actual eviction happens.
	for len(s.open) > s.maxOpen && len(s.openIDs) > 0 {
		evict := s.openIDs[0]
		s.openIDs = s.openIDs[1:]
		delete(s.open, evict)
	}
}

func (s *MemStore) List(status Status, limit int) []Case {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Case
	if status == Resolved {
		// newest resolutions first
		out = make([]Case, 0, min(limit, len(s.history)))
		for i := len(s.history) - 1; i >= 0 && len(out) < limit; i-- {
			out = append(out, s.history[i])
		}
		return out
	}
	out = make([]Case, 0, len(s.open))
	for _, c := range s.open {
		out = append(out, *c)
	}
	// worst cases surface on top: score desc, then newest first.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Verdict.Score != out[j].Verdict.Score {
			return out[i].Verdict.Score > out[j].Verdict.Score
		}
		return out[i].OpenedAt > out[j].OpenedAt
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (s *MemStore) Resolve(id string, r Resolution) (Case, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.open[id]
	if !ok {
		return Case{}, ErrNotFound
	}
	delete(s.open, id)
	c.Status = Resolved
	c.Resolution = r
	c.ResolvedAt = time.Now().UnixMilli()
	switch r {
	case ConfirmedFraud:
		s.resolved.confirmed++
	case FalsePositive:
		s.resolved.falsePos++
	}
	s.history = append(s.history, *c)
	if len(s.history) > s.maxOpen {
		s.history = s.history[1:]
	}
	return *c, nil
}

func (s *MemStore) Counts() (open, confirmed, falsePos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.open)), s.resolved.confirmed, s.resolved.falsePos
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
