package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"lambari/internal/cases"
	"lambari/internal/engine"
)

func newTestServer() *Server {
	return NewServer(engine.New(), cases.NewMemStore(10), "inline")
}

func scrape(t *testing.T, s *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain (Prometheus rejects anything else)", ct)
	}
	return rec.Body.String()
}

func TestMetricsEndpointServesExposition(t *testing.T) {
	body := scrape(t, newTestServer())
	for _, want := range []string{
		"# TYPE lambari_transactions_scored_total counter",
		"# TYPE lambari_scoring_duration_microseconds histogram",
		"lambari_queue_capacity 16384",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q\n--- got ---\n%s", want, body)
		}
	}
}

// Inline mode has no consumer; Kafka mode wires one in. The endpoint must
// reflect that rather than always reporting a lag of zero.
func TestMetricsIncludesLagOnlyInKafkaMode(t *testing.T) {
	if body := scrape(t, newTestServer()); strings.Contains(body, "kafka_consumer_lag") {
		t.Errorf("inline mode leaked a consumer-lag series:\n%s", body)
	}

	s := newTestServer()
	s.SetLagSource(func() map[int32]int64 { return map[int32]int64{0: 42} })
	body := scrape(t, s)
	if !strings.Contains(body, `lambari_kafka_consumer_lag{partition="0"} 42`) {
		t.Errorf("kafka mode did not expose partition lag:\n%s", body)
	}
	if !strings.Contains(body, "lambari_kafka_consumer_lag_total 42") {
		t.Errorf("kafka mode did not expose total lag:\n%s", body)
	}
}
