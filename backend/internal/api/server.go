package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"lambari/internal/cases"
	"lambari/internal/engine"
	"lambari/internal/metrics"
	"lambari/internal/model"
)

// Server exposes the engine over HTTP: ingest, stats, an SSE live stream
// for the dashboard, and a built-in simulator so the PoC runs with zero
// external dependencies (Kafka mode plugs in alongside, not instead).
type Server struct {
	eng   *engine.Engine
	store cases.Store
	mux   *http.ServeMux

	simMu     sync.Mutex
	simStop   chan struct{}
	simRate   atomic.Int64
	simActive atomic.Bool
	mode      string // "inline" or "kafka"

	// lagSource is nil in inline mode, where there is no consumer to lag.
	lagSource func() map[int32]int64
}

// SetLagSource wires consumer lag into the metrics endpoint. Called only in
// Kafka mode, which keeps this package free of any Kafka import.
func (s *Server) SetLagSource(fn func() map[int32]int64) { s.lagSource = fn }

func NewServer(eng *engine.Engine, store cases.Store, mode string) *Server {
	s := &Server{eng: eng, store: store, mux: http.NewServeMux(), mode: mode}
	s.mux.HandleFunc("GET /api/health", s.health)
	s.mux.HandleFunc("GET /api/stats", s.stats)
	s.mux.HandleFunc("GET /api/stream", s.stream)
	s.mux.HandleFunc("POST /api/transactions", s.ingest)
	s.mux.HandleFunc("POST /api/simulate", s.simulate)
	s.mux.HandleFunc("GET /api/cases", s.listCases)
	s.mux.HandleFunc("POST /api/cases/{id}/resolve", s.resolveCase)
	// Scrape endpoints live at the root by convention, not under /api.
	s.mux.HandleFunc("GET /metrics", s.metrics)
	return s
}

func (s *Server) Handler() http.Handler { return withCORS(s.mux) }

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{"ok": true, "mode": s.mode})
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.eng.Snapshot())
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	var lag map[int32]int64
	if s.lagSource != nil {
		lag = s.lagSource()
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := metrics.Write(w, s.eng.Snapshot(), s.eng.Histogram(), lag); err != nil {
		// The response is already partly on the wire, so the status is spent —
		// but a scrape failing silently is how monitoring rots.
		slog.Error("write metrics", "err", err)
	}
}

// ingest accepts a JSON array of transactions — this is the IO path an
// external load generator (or any upstream service) hits.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	var txs []model.Transaction
	if err := json.NewDecoder(r.Body).Decode(&txs); err != nil {
		http.Error(w, `{"error":"invalid JSON body, expected array of transactions"}`, http.StatusBadRequest)
		return
	}
	// Accept a prefix and stop at the first refusal, so "accepted: N" means
	// "the first N — resend from there". Scattered acceptance would leave the
	// caller holding a count it cannot act on.
	accepted := 0
	for i := range txs {
		if txs[i].Timestamp.IsZero() {
			txs[i].Timestamp = time.Now()
		}
		if !s.eng.TrySubmit(txs[i]) {
			break
		}
		accepted++
	}

	rejected := len(txs) - accepted
	status := http.StatusOK
	if rejected > 0 {
		// Count the whole remainder, not just the one refusal we saw: the rest
		// of the batch was shed without ever being offered.
		s.eng.RecordRejected(rejected)
		// The engine is at capacity — a fact about this server, not about this
		// caller's rate, so 503 rather than 429. Returning 200 with the count
		// buried in the body is how a client misses it entirely.
		//
		// The retry contract is "resend from accepted", not "resend the batch".
		// Re-offering the accepted prefix re-scores it: velocity windows advance
		// twice for those cards, counters and rule fires double-count, and a
		// second verdict is published. Only case creation dedupes on tx id.
		// That is why the count is precise — the caller has to be too.
		w.Header().Set("Retry-After", "1")
		status = http.StatusServiceUnavailable
	}
	writeJSONStatus(w, status, map[string]any{"accepted": accepted, "rejected": rejected})
}

// simulate starts/stops the built-in generator. Body: {"rate": 5000} tx/sec, {"rate": 0} stops.
func (s *Server) simulate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Rate int64 `json:"rate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"expected {\"rate\": <tx per second>}"}`, http.StatusBadRequest)
		return
	}
	if req.Rate < 0 || req.Rate > 100_000 {
		http.Error(w, `{"error":"rate must be between 0 and 100000"}`, http.StatusBadRequest)
		return
	}

	s.simMu.Lock()
	defer s.simMu.Unlock()

	if s.simStop != nil {
		close(s.simStop)
		s.simStop = nil
		s.simActive.Store(false)
	}
	s.simRate.Store(req.Rate)
	if req.Rate > 0 {
		s.simStop = make(chan struct{})
		s.simActive.Store(true)
		go s.runSimulator(s.simStop)
	}
	writeJSON(w, map[string]any{"running": req.Rate > 0, "rate": req.Rate})
}

// runSimulator emits transactions in 20ms micro-batches, which keeps pacing
// smooth at high rates without a hot spin loop.
func (s *Server) runSimulator(stop <-chan struct{}) {
	gen := model.NewGenerator(time.Now().UnixNano())
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			batch := int(s.simRate.Load() / 50) // rate per 20ms slice
			if batch < 1 {
				batch = 1
			}
			for i := 0; i < batch; i++ {
				s.eng.Submit(gen.Next())
			}
		}
	}
}

// stream pushes a stats snapshot + recent verdicts every 400ms over SSE.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	tick := time.NewTicker(400 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			open, confirmed, falsePos := s.store.Counts()
			payload := map[string]any{
				"stats":  s.eng.Snapshot(),
				"recent": s.eng.Recent(),
				"cases": map[string]int64{
					"open": open, "confirmed_fraud": confirmed, "false_positive": falsePos,
				},
				"sim": map[string]any{
					"running": s.simActive.Load(),
					"rate":    s.simRate.Load(),
				},
			}
			b, err := json.Marshal(payload)
			if err != nil {
				slog.Error("marshal stream payload", "err", err)
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

// listCases returns the review queue, worst score first. ?status=resolved
// shows the labeled history instead.
func (s *Server) listCases(w http.ResponseWriter, r *http.Request) {
	status := cases.Open
	if r.URL.Query().Get("status") == string(cases.Resolved) {
		status = cases.Resolved
	}
	writeJSON(w, map[string]any{"cases": s.store.List(status, 50)})
}

// resolveCase records an analyst decision — the label that makes flagged
// traffic useful as ML training data later.
func (s *Server) resolveCase(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Resolution cases.Resolution `json:"resolution"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
		(req.Resolution != cases.ConfirmedFraud && req.Resolution != cases.FalsePositive) {
		http.Error(w, `{"error":"resolution must be confirmed_fraud or false_positive"}`, http.StatusBadRequest)
		return
	}
	c, err := s.store.Resolve(r.PathValue("id"), req.Resolution)
	if err != nil {
		http.Error(w, `{"error":"case not found or already resolved"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, c)
}

func writeJSON(w http.ResponseWriter, v any) {
	writeJSONStatus(w, http.StatusOK, v)
}

// writeJSONStatus sets the content type before the status line — headers set
// after WriteHeader are silently dropped, which would leave the body sniffed
// instead of typed.
func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}
