package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"slices"
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
	simDone   chan struct{} // closed when the running simulator has exited
	simRate   atomic.Int64
	simActive atomic.Bool
	mode      string // "inline" or "kafka"

	// lagSource is nil in inline mode, where there is no consumer to lag.
	lagSource func() map[int32]int64

	limits Limits
}

// Limits bound what an unauthenticated caller can do on a public deploy.
// The zero value of each field except SimMaxRate means "no limit".
type Limits struct {
	SimMaxRate     int64         // simulator ceiling, tx/s
	SimMaxDuration time.Duration // simulator stops itself after this long
	DisableIngest  bool          // POST /api/transactions answers 403
	// AllowedOrigins get CORS headers, e.g. the Vercel dashboard. Empty means
	// same-origin only (the Vite dev proxy).
	AllowedOrigins []string
}

// SetLimits replaces the defaults. Call before serving.
func (s *Server) SetLimits(l Limits) { s.limits = l }

// SetLagSource wires consumer lag into the metrics endpoint. Called only in
// Kafka mode, which keeps this package free of any Kafka import.
func (s *Server) SetLagSource(fn func() map[int32]int64) { s.lagSource = fn }

func NewServer(eng *engine.Engine, store cases.Store, mode string) *Server {
	s := &Server{eng: eng, store: store, mux: http.NewServeMux(), mode: mode,
		limits: Limits{SimMaxRate: 100_000}}
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

// Handler serves same-origin, plus any origin in Limits.AllowedOrigins. Other
// sites get no CORS headers, which stops them reading responses but not
// sending writes, so requireJSON guards the writes.
func (s *Server) Handler() http.Handler {
	return withCORS(s.limits.AllowedOrigins, requireJSON(s.mux))
}

// withCORS answers CORS for allowlisted origins only, including the preflight
// a JSON POST triggers.
func withCORS(allowed []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && slices.Contains(allowed, origin) {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions {
				h.Set("Access-Control-Allow-Methods", "GET, POST")
				h.Set("Access-Control-Allow-Headers", "Content-Type")
				h.Set("Access-Control-Max-Age", "3600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// requireJSON rejects POSTs that aren't application/json. A browser sends a
// cross-origin JSON POST only after a CORS preflight, which fails here; the
// forms and text/plain requests it sends without one get 415.
func requireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
				http.Error(w, `{"error":"Content-Type must be application/json"}`, http.StatusUnsupportedMediaType)
				return
			}
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
	if s.limits.DisableIngest {
		// It would bypass the simulator cap and grow memory without bound.
		http.Error(w, `{"error":"ingest is disabled on this deployment"}`, http.StatusForbidden)
		return
	}
	var txs []model.Transaction
	r.Body = http.MaxBytesReader(w, r.Body, maxIngestBytes)
	if err := json.NewDecoder(r.Body).Decode(&txs); err != nil {
		http.Error(w, `{"error":"invalid JSON body, expected array of transactions (max 8 MiB)"}`, http.StatusBadRequest)
		return
	}
	// Validate the whole batch before scoring any of it, so a 400 means
	// nothing was accepted and the caller can fix and resend all of it.
	now := time.Now()
	for i := range txs {
		if txs[i].Timestamp.IsZero() {
			txs[i].Timestamp = now
		}
		if err := txs[i].Validate(now); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "index": i})
			return
		}
	}
	// Accept a prefix and stop at the first refusal, so "accepted: N" means
	// "the first N — resend from there". Scattered acceptance would leave the
	// caller holding a count it cannot act on.
	accepted := 0
	for i := range txs {
		if !s.eng.TrySubmit(txs[i]) {
			break
		}
		accepted++
	}

	rejected := len(txs) - accepted
	status := http.StatusOK
	if rejected > 0 {
		// Count the whole unoffered remainder, not just the one refusal seen.
		s.eng.RecordRejected(rejected)
		// 503, not 429: the limit is this server's capacity, not the caller's
		// rate. Callers must resend from `accepted`; resending the whole batch
		// scores the prefix twice (README.md#backpressure-at-the-ingest-boundary).
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
	if req.Rate < 0 || req.Rate > s.limits.SimMaxRate {
		writeJSONStatus(w, http.StatusBadRequest, map[string]any{
			"error": fmt.Sprintf("rate must be between 0 and %d", s.limits.SimMaxRate)})
		return
	}

	s.simMu.Lock()
	defer s.simMu.Unlock()

	s.stopSimulatorLocked()
	s.simRate.Store(req.Rate)
	if req.Rate > 0 {
		s.simStop = make(chan struct{})
		s.simDone = make(chan struct{})
		s.simActive.Store(true)
		go s.runSimulator(s.simStop, s.simDone)
	}
	writeJSON(w, map[string]any{"running": req.Rate > 0, "rate": req.Rate})
}

// runSimulator emits transactions in 20ms micro-batches, which keeps pacing
// smooth at high rates without a hot spin loop.
func (s *Server) runSimulator(stop chan struct{}, done chan<- struct{}) {
	defer close(done)
	gen := model.NewGenerator(time.Now().UnixNano())
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var expire <-chan time.Time // nil: never fires
	if d := s.limits.SimMaxDuration; d > 0 {
		expire = time.After(d)
	}
	for {
		select {
		case <-stop:
			return
		case <-expire:
			expire = nil
			// Stopping takes the lock and waits for this goroutine, so it
			// can't run here. stopIf leaves a newer simulator alone.
			go s.stopIf(stop)
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

// StopSimulator stops the simulator and waits for it to exit. Call it before
// Engine.Stop: a Submit racing the engine's closed channel panics.
func (s *Server) StopSimulator() {
	s.simMu.Lock()
	defer s.simMu.Unlock()
	s.stopSimulatorLocked()
}

func (s *Server) stopIf(stop chan struct{}) {
	s.simMu.Lock()
	defer s.simMu.Unlock()
	if s.simStop == stop {
		s.stopSimulatorLocked()
	}
}

func (s *Server) stopSimulatorLocked() {
	if s.simStop == nil {
		return
	}
	close(s.simStop)
	<-s.simDone
	s.simStop, s.simDone = nil, nil
	s.simActive.Store(false)
}

// stream pushes a stats snapshot + recent verdicts over SSE, checked every
// 400ms but sent only when something changed: an idle engine (simulator off,
// no ingest) goes quiet instead of repeating the same frame forever.
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
	var last []byte
	lastWrite := time.Now()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			stats := s.eng.Snapshot()
			uptime := stats.UptimeSec
			stats.UptimeSec = 0 // ticks every second; not a change worth a frame
			b, err := json.Marshal(s.frame(stats))
			if err != nil {
				slog.Error("marshal stream payload", "err", err)
				continue
			}
			if bytes.Equal(b, last) {
				// An SSE comment: EventSource ignores it, but it keeps proxies
				// from closing a connection that has gone quiet.
				if time.Since(lastWrite) >= streamKeepAlive {
					fmt.Fprint(w, ": ping\n\n")
					flusher.Flush()
					lastWrite = time.Now()
				}
				continue
			}
			last = b
			stats.UptimeSec = uptime
			if b, err = json.Marshal(s.frame(stats)); err != nil {
				slog.Error("marshal stream payload", "err", err)
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
			lastWrite = time.Now()
		}
	}
}

// streamKeepAlive is how long an idle stream waits before sending a ping.
const streamKeepAlive = 15 * time.Second

// frame is one SSE payload.
func (s *Server) frame(stats engine.Stats) map[string]any {
	open, confirmed, falsePos := s.store.Counts()
	return map[string]any{
		"stats":  stats,
		"recent": s.eng.Recent(),
		"cases": map[string]int64{
			"open": open, "confirmed_fraud": confirmed, "false_positive": falsePos,
		},
		// The top of the review queue rides the stream, so the dashboard
		// never polls /api/cases.
		"queue": s.store.List(cases.Open, queueInStream),
		"sim": map[string]any{
			"running":  s.simActive.Load(),
			"rate":     s.simRate.Load(),
			"max_rate": s.limits.SimMaxRate,
		},
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

// queueInStream is how many open cases each SSE frame carries: what the
// dashboard shows.
const queueInStream = 8

// maxIngestBytes caps one ingest body. The load generator's 500-transaction
// batches are ~150 KiB, so this leaves wide headroom while stopping an
// unauthenticated caller from making the server decode an unbounded array.
const maxIngestBytes = 8 << 20

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
