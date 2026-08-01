package engine

import "sync/atomic"

// latencyBuckets are the upper bounds, in microseconds, of the scoring-latency
// histogram. Spread wide on purpose: scoring is single-digit microseconds when
// the windows are warm and hundreds when a shard is contended, and the
// interesting question is which of those you are in.
var latencyBuckets = [...]int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// histogram counts scoring latencies per bucket, lock-free.
//
// This exists alongside the sampled reservoir behind Stats.P50US/P99US, and
// the duplication is deliberate: a percentile computed inside one process
// cannot be aggregated with another process's — averaging the p99s of eight
// pods is meaningless. The reservoir gives a human watching one dashboard an
// exact live number; the histogram gives Prometheus raw buckets it can sum
// across every pod and take a real quantile from.
type histogram struct {
	// counts are per-bucket (not cumulative) so the hot path is one Add;
	// snapshot does the cumulating. The final entry is the +Inf overflow.
	counts [len(latencyBuckets) + 1]atomic.Int64
	sum    atomic.Int64
}

func (h *histogram) observe(us int64) {
	h.sum.Add(us)
	for i, b := range latencyBuckets {
		if us <= b {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[len(latencyBuckets)].Add(1)
}

// Histogram is a point-in-time view, shaped for the Prometheus exposition
// format: Counts is cumulative and one longer than Bounds, its final entry
// being the +Inf bucket.
type Histogram struct {
	Bounds []int64
	Counts []int64
	Sum    int64
	Count  int64
}

func (h *histogram) snapshot() Histogram {
	out := Histogram{
		Bounds: latencyBuckets[:],
		Counts: make([]int64, len(latencyBuckets)+1),
		Sum:    h.sum.Load(),
	}
	var running int64
	for i := range out.Counts {
		running += h.counts[i].Load()
		out.Counts[i] = running
	}
	out.Count = running
	return out
}

// Histogram returns the scoring-latency distribution for the metrics endpoint.
func (e *Engine) Histogram() Histogram { return e.lat.snapshot() }
