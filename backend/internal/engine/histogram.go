package engine

import "sync/atomic"

// latencyBuckets are the upper bounds, in microseconds, of the scoring-latency
// histogram. Spread wide on purpose: scoring is single-digit microseconds when
// the windows are warm and hundreds when a shard is contended, and the
// interesting question is which of those you are in.
var latencyBuckets = [...]int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// histogram counts scoring latencies per bucket, lock-free.
//
// It is the only latency measurement in the engine: Prometheus needs raw
// buckets (a per-pod percentile cannot be aggregated — averaging the p99s of
// eight pods is meaningless), and the dashboard's p50/p99 are read back out of
// those same buckets via Quantile. Bucket-quantized numbers on the dashboard
// are a fair price for having one mechanism instead of two.
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

// Quantile returns the upper bound of the bucket the q-th quantile falls in —
// the same "no worse than this" answer Prometheus' histogram_quantile gives,
// without interpolating. A quantile landing in the +Inf bucket reports the
// largest finite bound, which is then a floor: read it as "≥ 5000µs".
func (h Histogram) Quantile(q float64) int64 {
	if h.Count == 0 {
		return 0
	}
	last := h.Bounds[len(h.Bounds)-1]
	rank := int64(float64(h.Count) * q) // 0-indexed position of the quantile
	for i, c := range h.Counts {
		if c > rank {
			if i == len(h.Bounds) {
				return last
			}
			return h.Bounds[i]
		}
	}
	return last
}

// Histogram returns the scoring-latency distribution for the metrics endpoint.
func (e *Engine) Histogram() Histogram { return e.lat.snapshot() }
