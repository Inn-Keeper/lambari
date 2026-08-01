package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"lambari/internal/cases"
	"lambari/internal/engine"
	"lambari/internal/model"
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

// ---- ingest under saturation ---------------------------------------------

func tx(i int) model.Transaction {
	return model.Transaction{
		ID: fmt.Sprintf("tx_%d", i), CardBIN: "520082", CardHash: fmt.Sprintf("tok_%d", i),
		Amount: 10, Currency: "SEK", Country: "SE", IP: "10.0.0.1",
		MerchantID: "m_1", MCC: "5411", Timestamp: time.Now(),
	}
}

// post returns the status and the decoded {accepted, rejected} body.
func post(t *testing.T, s *Server, n int) (int, map[string]int, string) {
	t.Helper()
	batch := make([]model.Transaction, n)
	for i := range batch {
		batch[i] = tx(i)
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/api/transactions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	var got map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec.Code, got, rec.Header().Get("Retry-After")
}

// A full engine used to answer 200 with the drop count buried in the body,
// which is exactly how a caller misses it. Saturation is a server-capacity
// fact, so it gets a status code the client cannot ignore.
func TestIngestSheddingReturns503(t *testing.T) {
	eng := engine.New() // never started: nothing drains the buffer
	s := NewServer(eng, cases.NewMemStore(10), "inline")
	for eng.TrySubmit(tx(0)) { // fill it
	}

	code, body, retryAfter := post(t, s, 3)
	if code != 503 {
		t.Errorf("status = %d, want 503 when the whole batch was shed", code)
	}
	if retryAfter == "" {
		t.Error("no Retry-After header — the client is left guessing when to come back")
	}
	if body["accepted"] != 0 || body["rejected"] != 3 {
		t.Errorf("accepted=%d rejected=%d, want 0 and 3", body["accepted"], body["rejected"])
	}
	// The handler stops offering at the first refusal, so the engine only ever
	// sees one — but all three were shed and all three must be counted.
	if got := eng.Snapshot().Rejected; got != 3 {
		t.Errorf("metric counted %d shed, want 3 — the un-offered remainder was dropped from the count", got)
	}
}

// "accepted: N" has to mean "the first N", or the caller cannot tell which
// transactions to resend.
func TestIngestAcceptsAPrefixThenSheds(t *testing.T) {
	// Learn the buffer's capacity from a throwaway engine, then fill a fresh
	// one to two slots short of it. Neither is started, so nothing drains and
	// the arithmetic stays exact.
	probe := engine.New()
	capacity := 0
	for probe.TrySubmit(tx(0)) {
		capacity++
	}

	eng := engine.New()
	for i := 0; i < capacity-2; i++ {
		eng.TrySubmit(tx(i))
	}
	s := NewServer(eng, cases.NewMemStore(10), "inline")

	code, body, _ := post(t, s, 5)
	if code != 503 {
		t.Errorf("status = %d, want 503 — part of the batch was shed", code)
	}
	if body["accepted"] != 2 || body["rejected"] != 3 {
		t.Errorf("accepted=%d rejected=%d, want 2 and 3", body["accepted"], body["rejected"])
	}
	if got := eng.Snapshot().Rejected; got != 3 {
		t.Errorf("metric counted %d shed, want 3", got)
	}
}

func TestIngestReturns200WhenNothingIsShed(t *testing.T) {
	s := newTestServer()
	code, body, retryAfter := post(t, s, 5)
	if code != 200 {
		t.Errorf("status = %d, want 200", code)
	}
	if body["accepted"] != 5 || body["rejected"] != 0 {
		t.Errorf("accepted=%d rejected=%d, want 5 and 0", body["accepted"], body["rejected"])
	}
	if retryAfter != "" {
		t.Errorf("Retry-After = %q on a healthy request, want none", retryAfter)
	}
}
