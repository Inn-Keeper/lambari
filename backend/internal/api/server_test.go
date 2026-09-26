package api

import (
	"bytes"
	"context"
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
	// Each card has its own queue, so use one card throughout. Learn that
	// queue's capacity from a throwaway engine, then fill a fresh one to two
	// slots short of it. Neither is started, so nothing drains.
	probe := engine.New()
	capacity := 0
	for probe.TrySubmit(tx(0)) {
		capacity++
	}

	eng := engine.New()
	for i := 0; i < capacity-2; i++ {
		eng.TrySubmit(tx(0))
	}
	s := NewServer(eng, cases.NewMemStore(10), "inline")

	batch := make([]model.Transaction, 5)
	for i := range batch {
		batch[i] = tx(i)
		batch[i].CardHash = tx(0).CardHash
	}
	body0, _ := json.Marshal(batch)
	rec := postRaw(s, body0)
	var body map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	code := rec.Code
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

// ---- ingest validation ----------------------------------------------------

func postRaw(s *Server, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/api/transactions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// One bad transaction rejects the batch before any of it is scored, so the
// caller can fix and resend all of it without double-scoring a prefix.
func TestIngestRejectsInvalidBatchWithoutScoringAny(t *testing.T) {
	eng := engine.New() // never started: queue depth shows what was submitted
	s := NewServer(eng, cases.NewMemStore(10), "inline")

	noCard := tx(1)
	noCard.CardHash = ""
	future := tx(2)
	future.Timestamp = time.Now().Add(time.Hour)

	for name, bad := range map[string]model.Transaction{"no card_hash": noCard, "future timestamp": future} {
		body, _ := json.Marshal([]model.Transaction{tx(0), bad})
		rec := postRaw(s, body)
		if rec.Code != 400 {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `"index":1`) {
			t.Errorf("%s: body %q does not point at the bad transaction", name, rec.Body.String())
		}
	}
	if d := eng.Snapshot().QueueDepth; d != 0 {
		t.Errorf("%d transactions submitted from rejected batches, want 0", d)
	}
}

func TestIngestRejectsOversizedBody(t *testing.T) {
	body := append([]byte(`[{"id":"`), bytes.Repeat([]byte("x"), maxIngestBytes)...)
	if rec := postRaw(newTestServer(), body); rec.Code != 400 {
		t.Errorf("status = %d, want 400 for a body over the cap", rec.Code)
	}
}

// Shutdown used to panic with "send on closed channel": the simulator kept
// submitting after Engine.Stop closed the buffer. StopSimulator must wait
// until it has actually exited.
func TestStopSimulatorBeforeEngineStopDoesNotPanic(t *testing.T) {
	eng := engine.New()
	eng.Start()
	s := NewServer(eng, cases.NewMemStore(10), "inline")

	req := httptest.NewRequest("POST", "/api/simulate", strings.NewReader(`{"rate":100000}`))
	req.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), req)
	time.Sleep(50 * time.Millisecond) // let it submit a few batches

	s.StopSimulator()
	eng.Stop()
}

// A cross-origin form or text/plain POST needs no CORS preflight, so it would
// reach the handler. It must not be able to resolve a case.
func TestNonJSONPostIsRejected(t *testing.T) {
	store := cases.NewMemStore(10)
	store.Open(model.Verdict{TxID: "tx_1", Score: 50, Decision: model.Review})
	s := NewServer(engine.New(), store, "inline")

	req := httptest.NewRequest("POST", "/api/cases/tx_1/resolve",
		strings.NewReader(`{"resolution":"false_positive"}`))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Origin", "https://evil.example")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != 415 {
		t.Errorf("status = %d, want 415", rec.Code)
	}
	if open, _, _ := store.Counts(); open != 1 {
		t.Error("a text/plain POST resolved the case")
	}
}

// ---- public-deploy limits ---------------------------------------------------

func TestCORSOnlyForAllowedOrigins(t *testing.T) {
	s := newTestServer()
	s.SetLimits(Limits{SimMaxRate: 1000, AllowedOrigins: []string{"https://lambari-frontend.vercel.app"}})

	preflight := func(origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("OPTIONS", "/api/simulate", nil)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "POST")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := preflight("https://lambari-frontend.vercel.app"); rec.Code != 204 ||
		rec.Header().Get("Access-Control-Allow-Origin") != "https://lambari-frontend.vercel.app" {
		t.Errorf("allowed origin: status %d, ACAO %q", rec.Code, rec.Header().Get("Access-Control-Allow-Origin"))
	}
	if rec := preflight("https://evil.example"); rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("an origin outside the allowlist got CORS headers")
	}
}

func TestSimulatorRateIsCapped(t *testing.T) {
	s := newTestServer()
	s.SetLimits(Limits{SimMaxRate: 1000})
	req := httptest.NewRequest("POST", "/api/simulate", strings.NewReader(`{"rate":5000}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "1000") {
		t.Errorf("status %d, body %q; want 400 naming the 1000 cap", rec.Code, rec.Body.String())
	}
}

func TestSimulatorStopsItselfAfterMaxDuration(t *testing.T) {
	eng := engine.New()
	eng.Start()
	s := NewServer(eng, cases.NewMemStore(10), "inline")
	s.SetLimits(Limits{SimMaxRate: 1000, SimMaxDuration: 50 * time.Millisecond})

	req := httptest.NewRequest("POST", "/api/simulate", strings.NewReader(`{"rate":500}`))
	req.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), req)

	deadline := time.Now().Add(2 * time.Second)
	for s.simActive.Load() {
		if time.Now().After(deadline) {
			t.Fatal("simulator still running past its max duration")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.StopSimulator()
	eng.Stop()
}

func TestIngestCanBeDisabled(t *testing.T) {
	s := newTestServer()
	s.SetLimits(Limits{SimMaxRate: 1000, DisableIngest: true})
	body, _ := json.Marshal([]model.Transaction{tx(0)})
	if rec := postRaw(s, body); rec.Code != 403 {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

// An idle engine sends its state once, then goes quiet.
func TestStreamSendsNothingWhileIdle(t *testing.T) {
	eng := engine.New()
	eng.Start()
	defer eng.Stop()
	s := NewServer(eng, cases.NewMemStore(10), "inline")

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/api/stream", nil).WithContext(ctx))

	if n := strings.Count(rec.Body.String(), "data: "); n != 1 {
		t.Fatalf("idle stream sent %d frames in 1.5s, want 1", n)
	}
}
