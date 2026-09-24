package engine

import "sync/atomic"

// latencyBuckets are the histogram's upper bounds in microseconds. They reach
// 1s because GC stalls under sustained load produced 1.4s scoring latencies;
// anything slower is reported as Overflow.
var latencyBuckets = [...]int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 100_000, 1_000_000}

// histogram counts scoring latencies per bucket, lock-free. It is the engine's
// only latency measurement: Prometheus gets the raw buckets (percentiles can't
// be aggregated across pods) and the dashboard's p50/p99 come from Quantile.
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

// Overflow is what Quantile returns for the +Inf bucket. It is not the largest
// bound, so "slower than 1s" can't be displayed as "1s".
const Overflow int64 = -1

// Quantile returns the upper bound of the bucket holding the q-th quantile
// (100000 means "in (10ms, 100ms]"), Overflow above the largest bucket, and 0
// before any observation. The bucket is the first whose cumulative count
// reaches rank q·N, as in Prometheus; "exceeds" would step one bucket too far
// when the rank lands exactly on an edge.
func (h Histogram) Quantile(q float64) int64 {
	if h.Count == 0 {
		return 0
	}
	rank := float64(h.Count) * q
	for i, c := range h.Counts {
		if float64(c) >= rank {
			if i == len(h.Bounds) {
				return Overflow
			}
			return h.Bounds[i]
		}
	}
	return Overflow
}

// Histogram returns the scoring-latency distribution for the metrics endpoint.
func (e *Engine) Histogram() Histogram { return e.lat.snapshot() }
