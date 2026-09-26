package engine

import (
	"hash/fnv"
	"slices"
	"sort"
	"sync"
	"time"

	"lambari/internal/model"
)

// Rule scores one transaction. Returns (points, flagName) — flagName empty when the rule passes.
type Rule func(tx *model.Transaction, s *State) (int, string)

// ---- shared engine state -----------------------------------------------

const shardCount = 256

type shard struct {
	mu   sync.Mutex
	seen map[string][]int64 // key -> recent event timestamps (unix millis)
}

// State holds sliding-window counters, sharded to avoid lock contention
// when thousands of workers hammer it concurrently.
type State struct {
	cardShards [shardCount]*shard
	ipShards   [shardCount]*shard
}

func NewState() *State {
	s := &State{}
	for i := 0; i < shardCount; i++ {
		s.cardShards[i] = &shard{seen: make(map[string][]int64)}
		s.ipShards[i] = &shard{seen: make(map[string][]int64)}
	}
	return s
}

func shardFor(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % shardCount
}

// maxWindowEvents caps the timestamps kept per key. The rules only ask whether
// a count reached a threshold, and the largest threshold is 30
// (ip_fanout_extreme), so older entries past that change no decision. Without
// the cap, a hot key holds every event in its window and each touch scans all
// of them. Raise it if a rule ever needs a higher threshold.
const maxWindowEvents = 30

// Velocity windows. The sweeper keeps entries exactly as long as the longest
// one needs them.
const (
	cardWindowMS = 60_000
	ipWindowMS   = 300_000
)

// touch records an event at ts for key and returns how many events fall in
// the window ending at ts, itself included, saturating at maxWindowEvents.
//
// Events can arrive out of timestamp order: per-card queues order only one
// card, and many cards update the same IP window concurrently. So the window is
// kept sorted by timestamp, entries expire relative to the newest timestamp
// seen for the key, and the cap drops the oldest timestamps. A late event is
// inserted at its place and can never push newer history out. An event older
// than the whole window is scored alone and leaves the window unchanged.
func (sh *shard) touch(key string, ts int64, windowMS int64) int {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	events := sh.seen[key] // sorted ascending
	newest := ts
	if n := len(events); n > 0 && events[n-1] > newest {
		newest = events[n-1]
	}
	events = events[sort.Search(len(events), func(i int) bool { return events[i] >= newest-windowMS }):]
	if ts < newest-windowMS {
		sh.seen[key] = events
		return 1
	}

	at := sort.Search(len(events), func(i int) bool { return events[i] > ts })
	events = slices.Insert(events, at, ts) // an append when in order

	// Count before capping: the cap may drop timestamps inside this event's
	// window when the event is late.
	from := sort.Search(len(events), func(i int) bool { return events[i] >= ts-windowMS })
	count := min(at+1-from, maxWindowEvents)

	if n := len(events); n > maxWindowEvents {
		events = events[:copy(events, events[n-maxWindowEvents:])] // keep the newest
	}
	sh.seen[key] = events
	return count
}

// ---- rules ---------------------------------------------------------------

// highRiskMCC flags merchant categories with elevated chargeback rates.
var highRiskMCC = map[string]bool{
	"7995": true, // gambling
	"6051": true, // crypto / quasi-cash
	"5967": true, // direct marketing, inbound
	"4829": true, // wire transfer
}

// RuleAmount: unusually large single transaction.
func RuleAmount(tx *model.Transaction, _ *State) (int, string) {
	switch {
	case tx.Amount >= 5000:
		return 45, "amount_extreme"
	case tx.Amount >= 1500:
		return 20, "amount_high"
	}
	return 0, ""
}

// RuleCardVelocity: same card seen too many times inside 60s.
func RuleCardVelocity(tx *model.Transaction, s *State) (int, string) {
	now := tx.Timestamp.UnixMilli()
	n := s.cardShards[shardFor(tx.CardHash)].touch(tx.CardHash, now, cardWindowMS)
	switch {
	case n >= 8:
		return 50, "card_velocity_extreme"
	case n >= 4:
		return 25, "card_velocity"
	}
	return 0, ""
}

// RuleIPFanOut: heavy activity from one IP inside 5 minutes — the classic
// card-testing signature (a PoC simplification: production would count
// *distinct cards* per IP, e.g. with a HyperLogLog per key).
func RuleIPFanOut(tx *model.Transaction, s *State) (int, string) {
	now := tx.Timestamp.UnixMilli()
	total := s.ipShards[shardFor(tx.IP)].touch(tx.IP, now, ipWindowMS)
	switch {
	case total >= 30:
		return 40, "ip_fanout_extreme"
	case total >= 12:
		return 20, "ip_fanout"
	}
	return 0, ""
}

// RuleGeoMismatch: card issued in one country, transaction originating in another.
func RuleGeoMismatch(tx *model.Transaction, _ *State) (int, string) {
	issuer, ok := model.BINCountry[tx.CardBIN]
	if ok && issuer != tx.Country {
		return 25, "geo_mismatch"
	}
	return 0, ""
}

// RuleHighRiskMCC: merchant category with elevated fraud rates.
func RuleHighRiskMCC(tx *model.Transaction, _ *State) (int, string) {
	if highRiskMCC[tx.MCC] {
		return 15, "high_risk_mcc"
	}
	return 0, ""
}

// DefaultRules is the rule chain evaluated for every transaction, in order.
func DefaultRules() []Rule {
	return []Rule{
		RuleAmount,
		RuleCardVelocity,
		RuleIPFanOut,
		RuleGeoMismatch,
		RuleHighRiskMCC,
	}
}

// Decide converts an aggregate score into a decision.
func Decide(score int) model.Decision {
	switch {
	case score >= 70:
		return model.Decline
	case score >= 40:
		return model.Review
	default:
		return model.Approve
	}
}

// windowSweep is how often stale velocity entries get garbage-collected.
const windowSweep = 2 * time.Minute

// StartSweeper trims idle keys so memory stays flat under sustained load.
func (s *State) StartSweeper(done <-chan struct{}) {
	go func() {
		t := time.NewTicker(windowSweep)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				cutoff := time.Now().UnixMilli() - ipWindowMS
				for _, group := range [][shardCount]*shard{s.cardShards, s.ipShards} {
					for _, sh := range group {
						sh.mu.Lock()
						for k, evs := range sh.seen {
							if len(evs) == 0 || evs[len(evs)-1] < cutoff {
								delete(sh.seen, k)
							}
						}
						sh.mu.Unlock()
					}
				}
			}
		}
	}()
}
