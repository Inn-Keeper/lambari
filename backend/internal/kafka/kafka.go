// Package kafka wires the engine to a Kafka-compatible broker (Kafka or
// Redpanda) using franz-go — the fastest, most actively maintained pure-Go
// client. The whole package is optional: if LAMBARI_KAFKA_BROKERS is unset,
// the API runs in inline mode and this code never executes.
package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"lambari/internal/engine"
	"lambari/internal/model"
)

const Topic = "transactions"

// Consumer pulls transaction batches from Kafka and feeds the engine.
type Consumer struct {
	client *kgo.Client
	eng    *engine.Engine
}

func NewConsumer(brokers []string, eng *engine.Engine) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("lambari-scoring"),
		kgo.ConsumeTopics(Topic),
		kgo.FetchMaxBytes(16<<20),
		kgo.DisableAutoCommit(), // commit after the engine accepts the batch
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, eng: eng}, nil
}

// Run polls until ctx is cancelled. Poll → decode → Submit applies natural
// backpressure: if the engine's buffer fills, Submit blocks, polling slows,
// and consumer lag becomes visible in Kafka — where you can alert on it.
func (c *Consumer) Run(ctx context.Context) {
	defer c.client.Close()
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("kafka fetch", "topic", e.Topic, "err", e.Err)
			}
			time.Sleep(time.Second)
			continue
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			var tx model.Transaction
			if err := json.Unmarshal(rec.Value, &tx); err != nil {
				slog.Warn("skipping malformed record", "err", err)
				return
			}
			c.eng.Submit(tx)
		})
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			slog.Error("kafka commit", "err", err)
		}
	}
}

// Producer publishes transactions — used by cmd/loadgen in Kafka mode.
type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(Topic),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(5*time.Millisecond), // batch aggressively for throughput
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Send(ctx context.Context, tx model.Transaction) error {
	b, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	// Key by card hash so one card's events stay ordered within a partition —
	// velocity rules depend on that ordering.
	p.client.Produce(ctx, &kgo.Record{Key: []byte(tx.CardHash), Value: b}, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Error("kafka produce", "err", err)
		}
	})
	return nil
}

func (p *Producer) Close() {
	p.client.Flush(context.Background())
	p.client.Close()
}

// ---- verdict publishing -----------------------------------------------

const VerdictTopic = "verdicts"

// VerdictProducer publishes scored outcomes for downstream consumers
// (notification services, data lake sinks, model-training pipelines).
type VerdictProducer struct {
	client *kgo.Client
}

func NewVerdictProducer(brokers []string) (*VerdictProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(VerdictTopic),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &VerdictProducer{client: client}, nil
}

func (p *VerdictProducer) Send(ctx context.Context, v model.Verdict) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshal verdict", "err", err)
		return
	}
	p.client.Produce(ctx, &kgo.Record{Key: []byte(v.TxID), Value: b}, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Error("publish verdict", "err", err)
		}
	})
}
