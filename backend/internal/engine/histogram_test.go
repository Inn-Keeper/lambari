package engine

import "testing"

// A Prometheus histogram is only useful if its buckets are cumulative and its
// count agrees with the +Inf bucket — get that wrong and every quantile query
// built on it is silently wrong too.
func TestHistogramBucketsAreCumulative(t *testing.T) {
	var h histogram
	// one observation in a handful of distinct buckets, plus an overflow
	for _, us := range []int64{1, 7, 40, 300, 9000} {
		h.observe(us)
	}

	got := h.snapshot()
	if got.Count != 5 {
		t.Fatalf("count = %d, want 5", got.Count)
	}
	if got.Sum != 1+7+40+300+9000 {
		t.Fatalf("sum = %d, want %d", got.Sum, 1+7+40+300+9000)
	}
	if len(got.Counts) != len(got.Bounds)+1 {
		t.Fatalf("counts has %d entries, want bounds+1 = %d", len(got.Counts), len(got.Bounds)+1)
	}

	// cumulative: never decreasing, and the last (+Inf) equals the total
	for i := 1; i < len(got.Counts); i++ {
		if got.Counts[i] < got.Counts[i-1] {
			t.Fatalf("bucket %d (%d) < bucket %d (%d) — not cumulative",
				i, got.Counts[i], i-1, got.Counts[i-1])
		}
	}
	if last := got.Counts[len(got.Counts)-1]; last != got.Count {
		t.Fatalf("+Inf bucket = %d, want count %d", last, got.Count)
	}

	// le=1 holds exactly the 1µs observation; le=10 holds 1µs and 7µs
	if got.Bounds[0] != 1 || got.Counts[0] != 1 {
		t.Fatalf("le=%d bucket = %d, want 1", got.Bounds[0], got.Counts[0])
	}
	for i, b := range got.Bounds {
		if b == 10 && got.Counts[i] != 2 {
			t.Fatalf("le=10 bucket = %d, want 2 (1µs and 7µs)", got.Counts[i])
		}
		if b == 5000 && got.Counts[i] != 4 {
			t.Fatalf("le=5000 bucket = %d, want 4 (all but the 9000µs overflow)", got.Counts[i])
		}
	}
}

// The dashboard's p50/p99 are read out of these buckets, so a quantile has to
// land in the bucket that actually holds it — off by one bucket is a 2-5×
// error at this scale.
func TestQuantileLandsInTheRightBucket(t *testing.T) {
	var h histogram
	// 100 observations: 90 at 3µs (bucket le=5), 9 at 60µs (le=100),
	// 1 at 9000µs (+Inf). p50 sits in the first, p99 in the last.
	for i := 0; i < 90; i++ {
		h.observe(3)
	}
	for i := 0; i < 9; i++ {
		h.observe(60)
	}
	h.observe(9000)

	got := h.snapshot()
	if p50 := got.Quantile(0.50); p50 != 5 {
		t.Errorf("p50 = %dµs, want 5 (the le=5 bucket holds 90%% of the mass)", p50)
	}
	if p99 := got.Quantile(0.99); p99 != 5000 {
		t.Errorf("p99 = %dµs, want 5000 — the 99th observation overflowed, and the largest finite bound is the floor we report", p99)
	}

	// An empty histogram reports 0 rather than reading past the end.
	if p := (Histogram{Bounds: latencyBuckets[:]}).Quantile(0.99); p != 0 {
		t.Errorf("empty histogram p99 = %d, want 0", p)
	}
}

// Scoring must feed the histogram, or the exported metric is a lie.
func TestScoringRecordsLatency(t *testing.T) {
	e := New()
	e.Start()
	e.Submit(benignTx(1))
	waitProcessed(t, e, 1)
	e.Stop()

	if got := e.Histogram().Count; got != 1 {
		t.Fatalf("histogram count = %d after scoring 1 transaction, want 1", got)
	}
}
