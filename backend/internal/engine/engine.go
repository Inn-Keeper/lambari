package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"lambari/internal/model"
)

// Engine is a bounded worker pool that scores transactions concurrently.
// Ingest is non-blocking up to the buffer size; beyond that, backpressure
// applies (Submit blocks), which is exactly what you want in front of Kafka.
// job carries one transaction to a worker. wg is non-nil only for batch
// submissions, where the caller needs to know when scoring actually finished.
type job struct {
	tx model.Transaction
	wg *sync.WaitGroup
}

type Engine struct {
	in    chan job
	rules []Rule
	state *State
	done  chan struct{}
	wg    sync.WaitGroup

	// counters (atomics — read by the stats endpoint at any time)
	processed atomic.Int64
	approved  atomic.Int64
	reviewed  atomic.Int64
	declined  atomic.Int64
	// rejected counts transactions refused because the buffer was full. Shed
	// load that nobody counts is indistinguishable from load that never
	// arrived.
	rejected atomic.Int64

	// per-rule fire counts
	ruleMu    sync.Mutex
	ruleFires map[string]int64

	// scoring latency, in microseconds — feeds both /metrics and the
	// dashboard's p50/p99 (see histogram.go)
	lat histogram

	// ring buffer of recent verdicts for the live feed
	ringMu sync.Mutex
	ring   []model.Verdict
	ringAt int

	// optional hook invoked for every non-approve verdict (case opening,
	// Kafka verdict publishing). Set before Start; not synchronized after.
	onFlagged func(model.Verdict)

	// rolling throughput: processed count snapshots per second
	lastSnap  atomic.Int64
	lastRate  atomic.Int64
	startedAt time.Time
}

const (
	bufferSize = 16_384
	ringSize   = 64
)

func New() *Engine {
	e := &Engine{
		in:        make(chan job, bufferSize),
		rules:     DefaultRules(),
		state:     NewState(),
		done:      make(chan struct{}),
		ruleFires: make(map[string]int64),
		ring:      make([]model.Verdict, ringSize),
		startedAt: time.Now(),
	}
	return e
}

// OnFlagged registers a callback fired for every review/decline verdict.
// Must be called before Start. The callback runs on worker goroutines, so
// it has to be fast and non-blocking.
func (e *Engine) OnFlagged(fn func(model.Verdict)) {
	e.onFlagged = fn
}

// Start launches the worker pool. Workers = 2× logical CPUs: scoring is
// CPU-light but lock-punctuated, so oversubscribing hides contention stalls.
func (e *Engine) Start() {
	workers := runtime.NumCPU() * 2
	for i := 0; i < workers; i++ {
		e.wg.Add(1)
		go e.worker()
	}
	e.state.StartSweeper(e.done)
	go e.rateTicker()
}

// Stop drains and shuts down.
func (e *Engine) Stop() {
	close(e.in)
	e.wg.Wait()
	close(e.done)
}

// Submit queues one transaction. Blocks only when the buffer is full.
// Fire-and-forget: it returns once the transaction is queued, not scored.
func (e *Engine) Submit(tx model.Transaction) {
	e.in <- job{tx: tx}
}

// TrySubmit queues without blocking; returns false if the engine is saturated.
// A false is shed load: report it with RecordRejected.
func (e *Engine) TrySubmit(tx model.Transaction) bool {
	select {
	case e.in <- job{tx: tx}:
		return true
	default:
		return false
	}
}

// RecordRejected accounts for transactions dropped because the engine was
// saturated. Callers report it rather than TrySubmit counting itself, because
// a caller that stops offering a batch at the first refusal knows how many it
// gave up on — and that total, not the number of refusals it happened to
// observe, is the shed load worth alerting on.
func (e *Engine) RecordRejected(n int) { e.rejected.Add(int64(n)) }

// SubmitBatch queues every transaction and blocks until all of them have been
// scored. Callers that own an offset — the Kafka consumer — need this: they
// can only commit once the work is genuinely done, otherwise a crash silently
// discards whatever is still buffered.
func (e *Engine) SubmitBatch(txs []model.Transaction) {
	var wg sync.WaitGroup
	wg.Add(len(txs))
	for _, tx := range txs {
		e.in <- job{tx: tx, wg: &wg}
	}
	wg.Wait()
}

func (e *Engine) worker() {
	defer e.wg.Done()
	for j := range e.in {
		e.score(&j.tx)
		if j.wg != nil {
			j.wg.Done()
		}
	}
}

func (e *Engine) score(tx *model.Transaction) {
	start := time.Now()
	total := 0
	var flags []string
	for _, rule := range e.rules {
		pts, flag := rule(tx, e.state)
		if pts > 0 {
			total += pts
			flags = append(flags, flag)
		}
	}
	decision := Decide(total)
	lat := time.Since(start).Microseconds()

	v := model.Verdict{
		TxID: tx.ID, CardBIN: tx.CardBIN, Amount: tx.Amount,
		Currency: tx.Currency, Country: tx.Country,
		Score: total, Flags: flags, Decision: decision,
		LatencyUS: lat, At: time.Now().UnixMilli(),
	}

	pn := e.processed.Add(1)
	switch decision {
	case model.Approve:
		e.approved.Add(1)
	case model.Review:
		e.reviewed.Add(1)
	case model.Decline:
		e.declined.Add(1)
	}

	if len(flags) > 0 {
		e.ruleMu.Lock()
		for _, f := range flags {
			e.ruleFires[f]++
		}
		e.ruleMu.Unlock()
	}

	e.lat.observe(lat)

	if decision != model.Approve && e.onFlagged != nil {
		e.onFlagged(v)
	}

	// keep flagged verdicts (and a trickle of approvals) for the live feed
	if decision != model.Approve || pn%97 == 0 {
		e.ringMu.Lock()
		e.ring[e.ringAt] = v
		e.ringAt = (e.ringAt + 1) % ringSize
		e.ringMu.Unlock()
	}
}

func (e *Engine) rateTicker() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-t.C:
			now := e.processed.Load()
			e.lastRate.Store(now - e.lastSnap.Load())
			e.lastSnap.Store(now)
		}
	}
}

// ---- read side ------------------------------------------------------------

type Stats struct {
	Processed   int64            `json:"processed"`
	Approved    int64            `json:"approved"`
	Reviewed    int64            `json:"reviewed"`
	Declined    int64            `json:"declined"`
	Rejected    int64            `json:"rejected"` // shed: buffer was full
	RatePerSec  int64            `json:"rate_per_sec"`
	P50US       int64            `json:"p50_us"` // bucket upper bound, not exact
	P99US       int64            `json:"p99_us"` // bucket upper bound, not exact
	QueueDepth  int              `json:"queue_depth"`
	QueueCap    int              `json:"queue_cap"`
	UptimeSec   int64            `json:"uptime_sec"`
	RuleFires   map[string]int64 `json:"rule_fires"`
	FlaggedRate float64          `json:"flagged_rate"` // (review+decline)/processed
}

func (e *Engine) Snapshot() Stats {
	p := e.processed.Load()
	rev, dec := e.reviewed.Load(), e.declined.Load()

	lat := e.lat.snapshot()

	e.ruleMu.Lock()
	fires := make(map[string]int64, len(e.ruleFires))
	for k, v := range e.ruleFires {
		fires[k] = v
	}
	e.ruleMu.Unlock()

	var flagged float64
	if p > 0 {
		flagged = float64(rev+dec) / float64(p)
	}

	return Stats{
		Processed: p, Approved: e.approved.Load(), Reviewed: rev, Declined: dec,
		Rejected:   e.rejected.Load(),
		RatePerSec: e.lastRate.Load(),
		P50US:      lat.Quantile(0.50), P99US: lat.Quantile(0.99),
		QueueDepth: len(e.in), QueueCap: cap(e.in),
		UptimeSec: int64(time.Since(e.startedAt).Seconds()),
		RuleFires: fires, FlaggedRate: flagged,
	}
}

// Recent returns the ring buffer newest-first.
func (e *Engine) Recent() []model.Verdict {
	e.ringMu.Lock()
	defer e.ringMu.Unlock()
	out := make([]model.Verdict, 0, ringSize)
	for i := 0; i < ringSize; i++ {
		idx := (e.ringAt - 1 - i + ringSize*2) % ringSize
		if e.ring[idx].TxID != "" {
			out = append(out, e.ring[idx])
		}
	}
	return out
}
