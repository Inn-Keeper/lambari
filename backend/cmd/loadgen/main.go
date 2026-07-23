// loadgen floods the pipeline with synthetic traffic — the external half of
// the IO proof of concept. Two transports:
//
//	go run ./cmd/loadgen -rate 5000 -duration 30s                      # HTTP batches
//	go run ./cmd/loadgen -rate 5000 -kafka localhost:19092             # Kafka produce
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
	"time"

	"tripwire/internal/kafka"
	"tripwire/internal/model"
)

func main() {
	rate := flag.Int("rate", 2000, "transactions per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	target := flag.String("target", "http://localhost:8080", "API base URL (HTTP mode)")
	kafkaBrokers := flag.String("kafka", "", "comma-separated brokers; enables Kafka mode")
	flag.Parse()

	gen := model.NewGenerator(time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sent := 0
	start := time.Now()
	tick := time.NewTicker(50 * time.Millisecond) // 20 batches/sec
	defer tick.Stop()
	batchSize := *rate / 20
	if batchSize < 1 {
		batchSize = 1
	}

	var send func([]model.Transaction) error
	if *kafkaBrokers != "" {
		producer, err := kafka.NewProducer([]string{*kafkaBrokers})
		if err != nil {
			slog.Error("kafka connect", "err", err)
			os.Exit(1)
		}
		defer producer.Close()
		send = func(batch []model.Transaction) error {
			for _, tx := range batch {
				if err := producer.Send(ctx, tx); err != nil {
					return err
				}
			}
			return nil
		}
		fmt.Printf("producing to kafka %s at ~%d tx/s for %s\n", *kafkaBrokers, *rate, *duration)
	} else {
		client := &http.Client{Timeout: 5 * time.Second}
		url := *target + "/api/transactions"
		send = func(batch []model.Transaction) error {
			b, err := json.Marshal(batch)
			if err != nil {
				return err
			}
			resp, err := client.Post(url, "application/json", bytes.NewReader(b))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}
		fmt.Printf("posting to %s at ~%d tx/s for %s\n", url, *rate, *duration)
	}

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start).Seconds()
			fmt.Printf("done: %d transactions in %.1fs (%.0f tx/s effective)\n", sent, elapsed, float64(sent)/elapsed)
			return
		case <-tick.C:
			batch := make([]model.Transaction, batchSize)
			for i := range batch {
				batch[i] = gen.Next()
			}
			if err := send(batch); err != nil {
				slog.Error("send batch", "err", err)
				continue
			}
			sent += batchSize
		}
	}
}
