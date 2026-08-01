// loadgen floods the pipeline with synthetic traffic — the external half of
// the IO proof of concept. Two transports:
//
//	go run ./cmd/loadgen -rate 5000 -duration 30s                  # HTTP batches
//	go run ./cmd/loadgen -rate 5000 -kafka localhost:19092         # Kafka produce
//
// To find a ceiling rather than hold a rate, go unthrottled and add senders
// until throughput stops rising:
//
//	go run ./cmd/loadgen -rate 0 -workers 8 -kafka localhost:19092
//
// Every run reports the rate it actually achieved, not the one it was asked
// for. A generator that silently falls behind its own target is worse than no
// measurement at all, because it looks like a measurement.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"lambari/internal/kafka"
	"lambari/internal/model"
)

const ticksPerSec = 20

// plan is what one worker does each iteration. An interval of 0 means
// unthrottled: send as fast as the far end will accept.
type plan struct {
	perSend  int
	interval time.Duration
}

// planWork splits a total target rate across workers.
func planWork(rate, workers, batch int) plan {
	if workers < 1 {
		workers = 1
	}
	if rate <= 0 {
		return plan{perSend: batch}
	}
	per := rate / workers / ticksPerSec
	if per < 1 {
		// Below one transaction per tick the pacing is coarser than asked for
		// and the run will overshoot; the achieved-rate line reports the truth.
		per = 1
	}
	return plan{perSend: per, interval: time.Second / ticksPerSec}
}

func main() {
	rate := flag.Int("rate", 2000, "total transactions per second across all workers; 0 = unthrottled")
	workers := flag.Int("workers", 1, "concurrent senders; raise until throughput stops rising")
	batch := flag.Int("batch", 100, "transactions per send when unthrottled")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	target := flag.String("target", "http://localhost:8080", "API base URL (HTTP mode)")
	kafkaBrokers := flag.String("kafka", "", "comma-separated brokers; enables Kafka mode")
	flag.Parse()

	if *workers < 1 {
		*workers = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var sent, failed, rejected atomic.Int64
	var logOnce sync.Once
	fail := func(err error) {
		// Logging every failure at 20k/s drowns the terminal and slows the
		// generator enough to skew the number it exists to produce. Show the
		// first, count the rest, report the total.
		logOnce.Do(func() { slog.Error("send failed (further failures counted, not logged)", "err", err) })
	}

	// send reports how many of the batch the far end refused, separately from
	// a transport error: a 200 that quietly dropped half the batch is the
	// failure mode this tool exists to expose.
	var send func([]model.Transaction) (rejected int, err error)
	var asyncFailures func() int64

	if *kafkaBrokers != "" {
		producer, err := kafka.NewProducer([]string{*kafkaBrokers})
		if err != nil {
			slog.Error("kafka connect", "err", err)
			os.Exit(1)
		}
		defer producer.Close()
		asyncFailures = producer.Failures
		send = func(batch []model.Transaction) (int, error) {
			for _, tx := range batch {
				if err := producer.Send(ctx, tx); err != nil {
					return 0, err
				}
			}
			// Kafka accepts or errors asynchronously; nothing is "rejected"
			// synchronously, so failures arrive via producer.Failures().
			return 0, nil
		}
		fmt.Printf("producing to kafka %s · %s · %d workers\n", *kafkaBrokers, rateLabel(*rate), *workers)
	} else {
		client := &http.Client{
			Timeout: 5 * time.Second,
			// Go's default is 2 idle connections per host, so extra workers
			// would spend their time churning TCP rather than measuring the
			// server.
			Transport: &http.Transport{MaxIdleConnsPerHost: *workers, MaxConnsPerHost: *workers},
		}
		url := *target + "/api/transactions"
		send = func(batch []model.Transaction) (int, error) {
			b, err := json.Marshal(batch)
			if err != nil {
				return 0, err
			}
			resp, err := client.Post(url, "application/json", bytes.NewReader(b))
			if err != nil {
				return 0, err
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				return 0, fmt.Errorf("ingest returned %s", resp.Status)
			}
			// Ingest sheds load when the engine's buffer is full and says so in
			// the body while still returning 200. Ignoring that is how a load
			// test reports throughput the system never actually did.
			var res struct{ Accepted, Rejected int }
			if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
				return 0, fmt.Errorf("decode ingest response: %w", err)
			}
			return res.Rejected, nil
		}
		fmt.Printf("posting to %s · %s · %d workers\n", url, rateLabel(*rate), *workers)
	}

	p := planWork(*rate, *workers, *batch)
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			// Generator carries an unsynchronized rng and sequence counter, so
			// each worker gets its own rather than sharing one.
			runWorker(ctx, model.NewGenerator(seed), p, send, &sent, &failed, &rejected, fail)
		}(time.Now().UnixNano() + int64(w))
	}
	wg.Wait()

	report(time.Since(start), *rate, sent.Load(), failed.Load(), rejected.Load(), asyncFailures)
}

func runWorker(
	ctx context.Context,
	gen *model.Generator,
	p plan,
	send func([]model.Transaction) (int, error),
	sent, failed, rejected *atomic.Int64,
	fail func(error),
) {
	var tick <-chan time.Time
	if p.interval > 0 {
		t := time.NewTicker(p.interval)
		defer t.Stop()
		tick = t.C
	}

	// Reused across sends: both transports marshal before returning.
	buf := make([]model.Transaction, p.perSend)

	for {
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-tick:
			}
		} else if ctx.Err() != nil {
			return
		}

		for i := range buf {
			buf[i] = gen.Next()
		}
		shed, err := send(buf)
		if err != nil {
			failed.Add(int64(len(buf)))
			fail(err)
			continue
		}
		// Only what the far end kept counts as sent.
		sent.Add(int64(len(buf) - shed))
		rejected.Add(int64(shed))
	}
}

func report(elapsed time.Duration, rate int, sent, failed, rejected int64, asyncFailures func() int64) {
	secs := elapsed.Seconds()
	achieved := float64(sent) / secs

	fmt.Printf("accepted %d in %.1fs — %.0f tx/s achieved", sent, secs, achieved)
	if rate > 0 {
		fmt.Printf(" (requested %d, %.0f%%)", rate, achieved/float64(rate)*100)
	}
	if asyncFailures != nil {
		failed += asyncFailures()
	}
	if failed > 0 {
		fmt.Printf(" · %d failed", failed)
	}
	if rejected > 0 {
		fmt.Printf(" · %d shed by the server", rejected)
	}
	fmt.Println()

	// Shed load is the ceiling announcing itself: the engine's buffer filled
	// and the server started dropping rather than queueing.
	if rejected > 0 {
		pct := float64(rejected) / float64(sent+rejected) * 100
		fmt.Printf("note: server dropped %.1f%% of offered load — that is the saturation point\n", pct)
	}
	if rate > 0 && achieved < float64(rate)*0.95 {
		fmt.Println("note: requested rate not reached — add -workers, or the bottleneck is downstream")
	}
}

func rateLabel(rate int) string {
	if rate <= 0 {
		return "unthrottled"
	}
	return fmt.Sprintf("~%d tx/s", rate)
}
