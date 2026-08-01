package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"lambari/internal/model"
)

// A paced generator can never report more than you asked it for, so the
// unthrottled mode is the only one that can discover a ceiling.
func TestPlanUnthrottledWhenRateIsZero(t *testing.T) {
	p := planWork(0, 4, 250)
	if p.interval != 0 {
		t.Errorf("interval = %v, want 0 (unthrottled)", p.interval)
	}
	if p.perSend != 250 {
		t.Errorf("perSend = %d, want the batch size 250", p.perSend)
	}
}

func TestPlanSplitsRateAcrossWorkers(t *testing.T) {
	// 4000/s over 2 workers at 20 ticks/sec = 100 per worker per tick
	p := planWork(4000, 2, 250)
	if p.perSend != 100 {
		t.Errorf("perSend = %d, want 100", p.perSend)
	}
	if p.interval != 50*time.Millisecond {
		t.Errorf("interval = %v, want 50ms", p.interval)
	}
	// the whole point: workers × ticks × perSend reconstructs the target rate
	if got := 2 * ticksPerSec * p.perSend; got != 4000 {
		t.Errorf("plan delivers %d tx/s, want 4000", got)
	}
}

func TestPlanAlwaysSendsAtLeastOne(t *testing.T) {
	// 10/s over 4 workers rounds to zero per tick — which would send nothing
	if p := planWork(10, 4, 250); p.perSend != 1 {
		t.Errorf("perSend = %d, want 1 — a plan that sends nothing is a broken tool", p.perSend)
	}
}

func TestPlanGuardsAgainstZeroWorkers(t *testing.T) {
	if p := planWork(4000, 0, 250); p.perSend < 1 {
		t.Errorf("perSend = %d with 0 workers, want a usable plan", p.perSend)
	}
}

// The ingest endpoint sheds load when the engine's buffer fills — it returns
// 200 and reports how many it dropped. Counting those as delivered is how a
// load test reports throughput the system never achieved.
func TestWorkerCountsOnlyWhatTheServerKept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sent, failed, rejected atomic.Int64
	calls := 0
	send := func(batch []model.Transaction) (int, error) {
		calls++
		if calls == 3 {
			cancel() // stop after three sends
		}
		return 4, nil // the server keeps 6 of every 10 offered
	}

	runWorker(ctx, model.NewGenerator(1), plan{perSend: 10}, send,
		&sent, &failed, &rejected, func(error) {})

	if sent.Load() != 18 {
		t.Errorf("sent = %d, want 18 (3 sends × 6 kept)", sent.Load())
	}
	if rejected.Load() != 12 {
		t.Errorf("rejected = %d, want 12 (3 sends × 4 shed)", rejected.Load())
	}
	if failed.Load() != 0 {
		t.Errorf("failed = %d, want 0 — shed load is not a transport failure", failed.Load())
	}
}

func TestWorkerCountsTransportErrorsAsFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sent, failed, rejected atomic.Int64
	calls := 0
	send := func(batch []model.Transaction) (int, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return 0, errors.New("connection refused")
	}

	runWorker(ctx, model.NewGenerator(1), plan{perSend: 5}, send,
		&sent, &failed, &rejected, func(error) {})

	if sent.Load() != 0 {
		t.Errorf("sent = %d, want 0 — nothing was delivered", sent.Load())
	}
	if failed.Load() != 10 {
		t.Errorf("failed = %d, want 10 (2 sends × 5)", failed.Load())
	}
}

// Ingest answers 503 when the engine's buffer is full, with the shed count in
// the body. Treating that as a dead request would under-report throughput as
// badly as ignoring it over-reports it.
func TestHTTPSenderTreats503AsPartialAcceptance(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"accepted":6,"rejected":4}`)
	}))
	defer srv.Close()

	rejected, err := httpSender(srv.Client(), srv.URL)(make([]model.Transaction, 10))
	if err != nil {
		t.Fatalf("503 with a body is shed load, not an error: %v", err)
	}
	if rejected != 4 {
		t.Errorf("rejected = %d, want 4", rejected)
	}
}

func TestHTTPSenderReportsRealFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := httpSender(srv.Client(), srv.URL)(make([]model.Transaction, 3)); err == nil {
		t.Error("a 500 must surface as an error, not as silent success")
	}
}
