// Package metrics renders engine and consumer state as Prometheus text
// exposition. Hand-written on purpose: the format is three kinds of line, and
// the client library would cost four transitive dependencies to produce the
// same bytes.
package metrics

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strings"

	"lambari/internal/engine"
)

const prefix = "lambari_"

// Write renders one scrape. lag is nil in inline mode, where there is no
// consumer — the Kafka families are then omitted entirely rather than
// reported as zero, which would read as a consumer that is keeping up.
func Write(w io.Writer, s engine.Stats, h engine.Histogram, lag map[int32]int64) error {
	b := bufio.NewWriter(w)

	counter(b, "transactions_scored_total", "Transactions scored since start.")
	value(b, "transactions_scored_total", "", s.Processed)

	counter(b, "decisions_total", "Verdicts by decision.")
	value(b, "decisions_total", label("decision", "approve"), s.Approved)
	value(b, "decisions_total", label("decision", "review"), s.Reviewed)
	value(b, "decisions_total", label("decision", "decline"), s.Declined)

	counter(b, "rule_fires_total", "Times each rule contributed points to a score.")
	for _, name := range sortedKeys(s.RuleFires) {
		value(b, "rule_fires_total", label("rule", name), s.RuleFires[name])
	}

	histogram(b, h)

	gauge(b, "queue_depth", "Transactions buffered in front of the worker pool.")
	value(b, "queue_depth", "", int64(s.QueueDepth))
	gauge(b, "queue_capacity", "Buffer size; depth reaching this is backpressure.")
	value(b, "queue_capacity", "", int64(s.QueueCap))
	gauge(b, "throughput_per_second", "Transactions scored in the last second.")
	value(b, "throughput_per_second", "", s.RatePerSec)
	gauge(b, "uptime_seconds", "Seconds since the engine started.")
	value(b, "uptime_seconds", "", s.UptimeSec)

	if lag != nil {
		writeLag(b, lag)
	}

	return b.Flush()
}

func writeLag(b *bufio.Writer, lag map[int32]int64) {
	parts := make([]int, 0, len(lag))
	for p := range lag {
		parts = append(parts, int(p))
	}
	sort.Ints(parts)

	gauge(b, "kafka_consumer_lag", "Records behind the partition head, per partition.")
	var total int64
	for _, p := range parts {
		n := lag[int32(p)]
		total += n
		value(b, "kafka_consumer_lag", label("partition", fmt.Sprint(p)), n)
	}
	// The number to autoscale on: one series, no aggregation required by the
	// thing reading it.
	gauge(b, "kafka_consumer_lag_total", "Records behind across all partitions.")
	value(b, "kafka_consumer_lag_total", "", total)
}

func histogram(b *bufio.Writer, h engine.Histogram) {
	const name = "scoring_duration_microseconds"
	fmt.Fprintf(b, "# HELP %s%s Time to score one transaction.\n", prefix, name)
	fmt.Fprintf(b, "# TYPE %s%s histogram\n", prefix, name)
	for i, bound := range h.Bounds {
		fmt.Fprintf(b, "%s%s_bucket{le=%q} %d\n", prefix, name, fmt.Sprint(bound), h.Counts[i])
	}
	// Counts is one longer than Bounds; the extra entry is +Inf.
	fmt.Fprintf(b, "%s%s_bucket{le=\"+Inf\"} %d\n", prefix, name, h.Counts[len(h.Counts)-1])
	fmt.Fprintf(b, "%s%s_sum %d\n", prefix, name, h.Sum)
	fmt.Fprintf(b, "%s%s_count %d\n", prefix, name, h.Count)
}

func counter(b *bufio.Writer, name, help string) { declare(b, name, help, "counter") }
func gauge(b *bufio.Writer, name, help string)   { declare(b, name, help, "gauge") }

func declare(b *bufio.Writer, name, help, typ string) {
	fmt.Fprintf(b, "# HELP %s%s %s\n", prefix, name, help)
	fmt.Fprintf(b, "# TYPE %s%s %s\n", prefix, name, typ)
}

func value(b *bufio.Writer, name, labels string, v int64) {
	fmt.Fprintf(b, "%s%s%s %d\n", prefix, name, labels, v)
}

func label(k, v string) string { return "{" + k + `="` + escape(v) + `"}` }

// escape applies the exposition format's label-value rules. Rule names are
// internal constants today, but a serialization boundary is the wrong place
// to rely on that staying true.
var escaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escape(s string) string { return escaper.Replace(s) }

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
