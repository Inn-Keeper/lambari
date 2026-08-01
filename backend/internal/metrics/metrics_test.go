package metrics

import (
	"strings"
	"testing"

	"lambari/internal/engine"
)

func sampleStats() engine.Stats {
	return engine.Stats{
		Processed: 100, Approved: 90, Reviewed: 8, Declined: 2, Rejected: 17,
		RatePerSec: 5000, P50US: 3, P99US: 40,
		QueueDepth: 12, QueueCap: 16384, UptimeSec: 60,
		RuleFires: map[string]int64{"amount_high": 7, "geo_mismatch": 3},
	}
}

func sampleHist() engine.Histogram {
	return engine.Histogram{
		Bounds: []int64{1, 10},
		Counts: []int64{4, 9, 10}, // cumulative, last is +Inf
		Sum:    250,
		Count:  10,
	}
}

func render(t *testing.T, lag map[int32]int64) string {
	t.Helper()
	var sb strings.Builder
	if err := Write(&sb, sampleStats(), sampleHist(), lag); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return sb.String()
}

func mustContain(t *testing.T, out string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(out, want) {
			t.Errorf("exposition is missing %q\n--- got ---\n%s", want, out)
		}
	}
}

func TestWriteExposesEngineCounters(t *testing.T) {
	out := render(t, nil)
	mustContain(t, out,
		"# HELP lambari_transactions_scored_total",
		"# TYPE lambari_transactions_scored_total counter",
		"lambari_transactions_scored_total 100",
		`lambari_decisions_total{decision="approve"} 90`,
		`lambari_decisions_total{decision="review"} 8`,
		`lambari_decisions_total{decision="decline"} 2`,
		// shed load must be visible, or a saturated engine looks like an idle one
		"# TYPE lambari_submissions_rejected_total counter",
		"lambari_submissions_rejected_total 17",
		`lambari_rule_fires_total{rule="amount_high"} 7`,
		`lambari_rule_fires_total{rule="geo_mismatch"} 3`,
		"lambari_queue_depth 12",
		"lambari_queue_capacity 16384",
		"lambari_throughput_per_second 5000",
		"lambari_uptime_seconds 60",
	)
}

// The whole point of exporting buckets rather than a pre-computed p99 is that
// Prometheus can aggregate them — which only works if the wire format is right.
func TestWriteExposesHistogramBuckets(t *testing.T) {
	out := render(t, nil)
	mustContain(t, out,
		"# TYPE lambari_scoring_duration_microseconds histogram",
		`lambari_scoring_duration_microseconds_bucket{le="1"} 4`,
		`lambari_scoring_duration_microseconds_bucket{le="10"} 9`,
		`lambari_scoring_duration_microseconds_bucket{le="+Inf"} 10`,
		"lambari_scoring_duration_microseconds_sum 250",
		"lambari_scoring_duration_microseconds_count 10",
	)
}

// Inline mode has no consumer: reporting lag 0 would look like a healthy
// consumer that is keeping up, which is worse than reporting nothing.
func TestKafkaFamiliesOmittedWithoutAConsumer(t *testing.T) {
	out := render(t, nil)
	if strings.Contains(out, "lambari_kafka_consumer_lag") {
		t.Errorf("inline mode must not emit consumer lag:\n%s", out)
	}
}

func TestKafkaLagPerPartitionAndTotal(t *testing.T) {
	out := render(t, map[int32]int64{0: 120, 1: 5, 2: 0})
	mustContain(t, out,
		"# TYPE lambari_kafka_consumer_lag gauge",
		`lambari_kafka_consumer_lag{partition="0"} 120`,
		`lambari_kafka_consumer_lag{partition="1"} 5`,
		`lambari_kafka_consumer_lag{partition="2"} 0`,
		"lambari_kafka_consumer_lag_total 125",
	)
}

// A scrape is a serialization boundary: one unescaped quote and the parser
// silently reads garbage.
func TestLabelValuesAreEscaped(t *testing.T) {
	s := sampleStats()
	s.RuleFires = map[string]int64{`we"ird\rule` + "\n": 1}
	var sb strings.Builder
	if err := Write(&sb, s, sampleHist(), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustContain(t, sb.String(), `lambari_rule_fires_total{rule="we\"ird\\rule\n"} 1`)
}

// Prometheus tolerates reordering, humans diffing a scrape do not — and map
// iteration order in Go is deliberately random.
func TestOutputIsDeterministic(t *testing.T) {
	lag := map[int32]int64{3: 1, 1: 2, 2: 3}
	first := render(t, lag)
	for i := 0; i < 5; i++ {
		if got := render(t, lag); got != first {
			t.Fatal("exposition output changed between renders of identical input")
		}
	}
}
