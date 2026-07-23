package engine

import (
	"hash/fnv"
	"sync"
	"time"

	"tripwire/internal/model"
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

// touch records an event for key and returns how many events landed inside the window.
func (sh *shard) touch(key string, now int64, windowMS int64) int {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	events := sh.seen[key]
	cutoff := now - windowMS
	// drop expired entries in place
	kept := events[:0]
	for _, t := range events {
		if t >= cutoff {
			kept = append(kept, t)
		}
	}
	kept = append(kept, now)
	sh.seen[key] = kept
	return len(kept)
}

// ---- rules ---------------------------------------------------------------

// binCountry maps a card BIN prefix to its issuing country (toy table for the PoC;
// production would use a licensed BIN database).
var binCountry = map[string]string{
	"411111": "US", "455673": "GB", "510510": "DE",
	"520082": "SE", "530127": "SE", "601100": "US",
	"356600": "JP", "627780": "BR", "506699": "NG",
}

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
	n := s.cardShards[shardFor(tx.CardHash)].touch(tx.CardHash, now, 60_000)
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
	total := s.ipShards[shardFor(tx.IP)].touch(tx.IP, now, 300_000)
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
	issuer, ok := binCountry[tx.CardBIN]
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
				cutoff := time.Now().Add(-10 * time.Minute).UnixMilli()
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
