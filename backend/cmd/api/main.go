package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"lambari/internal/api"
	"lambari/internal/cases"
	"lambari/internal/engine"
	"lambari/internal/kafka"
	"lambari/internal/model"
)

func main() {
	addr := envOr("LAMBARI_ADDR", ":8080")
	brokers := os.Getenv("LAMBARI_KAFKA_BROKERS") // e.g. "localhost:19092"

	eng := engine.New()
	store := cases.NewMemStore(200)

	mode := "inline"
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var verdictPub *kafka.VerdictProducer
	var consumer *kafka.Consumer
	// consumerDone closes when Run has fully unwound — including the group
	// leave. Nothing below may run before that: stopping the engine first can
	// close the buffer under an in-flight SubmitBatch, and exiting first skips
	// LeaveGroup, which strands this member's partitions until the group's
	// session timeout expires (~45s of no one consuming them).
	consumerDone := make(chan struct{})
	if brokers != "" {
		mode = "kafka"
		seeds := strings.Split(brokers, ",")
		dlq, err := kafka.NewDLQProducer(seeds)
		if err != nil {
			slog.Error("dlq producer failed", "err", err)
			os.Exit(1)
		}
		defer dlq.Close()
		if verdictPub, err = kafka.NewVerdictProducer(seeds); err != nil {
			slog.Error("verdict producer failed", "err", err)
			os.Exit(1)
		}
		consumer, err = kafka.NewConsumer(seeds, eng, dlq, verdictPub)
		if err != nil {
			slog.Error("kafka connect failed", "err", err)
			os.Exit(1)
		}
		go func() {
			defer close(consumerDone)
			consumer.Run(ctx)
		}()
		slog.Info("kafka wired", "brokers", brokers, "in", kafka.Topic, "out", kafka.VerdictTopic, "dlq", kafka.DLQTopic)
	}

	// every flagged verdict opens a case; in kafka mode it's also published
	// downstream on the verdicts topic.
	eng.OnFlagged(func(v model.Verdict) {
		store.Open(v)
		if verdictPub != nil {
			verdictPub.Send(ctx, v)
		}
	})
	eng.Start()

	apiSrv := api.NewServer(eng, store, mode)
	if consumer != nil {
		apiSrv.SetLagSource(consumer.Lag)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("lambari api listening", "addr", addr, "mode", mode)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	if consumer != nil {
		select {
		case <-consumerDone:
		case <-time.After(10 * time.Second):
			slog.Error("consumer did not stop in time — leaving the group by session timeout")
		}
	}
	eng.Stop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
