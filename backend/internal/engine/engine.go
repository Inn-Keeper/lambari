package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"lambari/internal/model"
)

// job carries one transaction to a worker. wg is non-nil only for SubmitBatch,
// whose caller needs to know when scoring finished.
type job struct {
	tx model.Transaction
	wg *sync.WaitGroup
}

// Engine is a bounded worker pool that scores transactions concurrently.
// Submit blocks once the buffer is full, so backpressure reaches the caller.
//
// Each worker has its own queue, and a card always goes to the same one, so
// one card's transactions are scored in the order they were submitted. The
// velocity rule depends on that: with a shared queue, two workers could score
// a card's 8th and 1st transactions in either order. The IP rule stays
// order-insensitive; one IP spans many cards, which Kafka never ordered.
type Engine struct {
	ins   []chan job // one per worker, picked by card hash
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
	// Workers = 2× logical CPUs: scoring is CPU-light but lock-punctuated, so
	// oversubscribing hides contention stalls.
	workers := runtime.NumCPU() * 2
	ins := make([]chan job, workers)
	for i := range ins {
		// Split bufferSize exactly, so capacity reads the same on any machine.
		size := bufferSize / workers
		if i < bufferSize%workers {
			size++
		}
		ins[i] = make(chan job, size)
	}
	e := &Engine{
		ins:       ins,
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

// Start launches one worker per queue.
func (e *Engine) Start() {
	for _, in := range e.ins {
		e.wg.Add(1)
		go e.worker(in)
	}
	e.state.StartSweeper(e.done)
	go e.rateTicker()
}

// Stop drains and shuts down.
func (e *Engine) Stop() {
	for _, in := range e.ins {
		close(in)
	}
	e.wg.Wait()
	close(e.done)
}

// Submit queues one transaction. Blocks only when the buffer is full.
// Fire-and-forget: it returns once the transaction is queued, not scored.
func (e *Engine) Submit(tx model.Transaction) {
	e.queueFor(tx) <- job{tx: tx}
}

// queueFor picks the card's queue. Same hash as the velocity shards.
func (e *Engine) queueFor(tx model.Transaction) chan job {
	return e.ins[shardFor(tx.CardHash)%uint32(len(e.ins))]
}

// TrySubmit queues without blocking; returns false if the engine is saturated.
// A false is shed load: report it with RecordRejected.
func (e *Engine) TrySubmit(tx model.Transaction) bool {
	select {
	case e.queueFor(tx) <- job{tx: tx}:
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
		e.queueFor(tx) <- job{tx: tx, wg: &wg}
	}
	wg.Wait()
}

func (e *Engine) worker(in <-chan job) {
	defer e.wg.Done()
	for j := range in {
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
	Processed  int64 `json:"processed"`
	Approved   int64 `json:"approved"`
	Reviewed   int64 `json:"reviewed"`
	Declined   int64 `json:"declined"`
	Rejected   int64 `json:"rejected"` // shed: buffer was full
	RatePerSec int64 `json:"rate_per_sec"`
	// P50US/P99US are histogram bucket *upper bounds*, not exact latencies:
	// 100000 means "in (10ms, 100ms]". Overflow (-1) means slower than the
	// largest bucket, 0 means nothing scored yet. Anything rendering these has
	// to qualify them — see formatLatencyBound in the dashboard.
	P50US       int64            `json:"p50_us"`
	P99US       int64            `json:"p99_us"`
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

	depth, capacity := 0, 0
	for _, in := range e.ins {
		depth += len(in)
		capacity += cap(in)
	}

	return Stats{
		Processed: p, Approved: e.approved.Load(), Reviewed: rev, Declined: dec,
		Rejected:   e.rejected.Load(),
		RatePerSec: e.lastRate.Load(),
		P50US:      lat.Quantile(0.50), P99US: lat.Quantile(0.99),
		QueueDepth: depth, QueueCap: capacity,
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
