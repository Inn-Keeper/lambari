package engine

import "sync/atomic"

// latencyBuckets are the upper bounds, in microseconds, of the scoring-latency
// histogram. Spread wide on purpose: scoring is single-digit microseconds when
// the windows are warm and hundreds when a shard is contended, and the
// interesting question is which of those you are in.
//
// The top of the range is 1s rather than 5ms because a quantile that overflows
// is reported as the largest finite bound (see Quantile) — a floor rendered as
// if it were a reading. The ceiling ramp produced GC assist waves of 1.4s, so
// millisecond-scale scoring latency is a thing that actually happens here, and
// pinning the dashboard at "5,000µs" through it would be a lie with three
// orders of magnitude in it.
var latencyBuckets = [...]int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 100_000, 1_000_000}

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

// Overflow is what Quantile returns when the quantile lands in the +Inf bucket.
// It is deliberately not the largest bound: a caller that renders a bound as if
// it were a reading turns "slower than a second" into "exactly one second", and
// the only way to stop that is to make the overflow impossible to mistake for a
// measurement.
const Overflow int64 = -1

// Quantile returns the upper bound of the bucket the q-th quantile falls in —
// the same "no worse than this" answer Prometheus' histogram_quantile gives,
// without interpolating. Every result is a bound, never an exact latency: a
// return of 100000 means "somewhere in (10ms, 100ms]". Callers that display it
// have to say so. Returns Overflow above the largest bucket, and 0 when nothing
// has been observed yet.
func (h Histogram) Quantile(q float64) int64 {
	if h.Count == 0 {
		return 0
	}
	rank := int64(float64(h.Count) * q) // 0-indexed position of the quantile
	for i, c := range h.Counts {
		if c > rank {
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
