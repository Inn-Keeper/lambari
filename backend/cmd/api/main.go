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

	"tripwire/internal/api"
	"tripwire/internal/cases"
	"tripwire/internal/engine"
	"tripwire/internal/kafka"
	"tripwire/internal/model"
)

func main() {
	addr := envOr("TRIPWIRE_ADDR", ":8080")
	brokers := os.Getenv("TRIPWIRE_KAFKA_BROKERS") // e.g. "localhost:19092"

	eng := engine.New()
	store := cases.NewMemStore(200)

	mode := "inline"
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var verdictPub *kafka.VerdictProducer
	if brokers != "" {
		mode = "kafka"
		consumer, err := kafka.NewConsumer(strings.Split(brokers, ","), eng)
		if err != nil {
			slog.Error("kafka connect failed", "err", err)
			os.Exit(1)
		}
		go consumer.Run(ctx)
		if verdictPub, err = kafka.NewVerdictProducer(strings.Split(brokers, ",")); err != nil {
			slog.Error("verdict producer failed", "err", err)
			os.Exit(1)
		}
		slog.Info("kafka wired", "brokers", brokers, "in", kafka.Topic, "out", kafka.VerdictTopic)
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

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewServer(eng, store, mode).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("tripwire api listening", "addr", addr, "mode", mode)
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
	eng.Stop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
