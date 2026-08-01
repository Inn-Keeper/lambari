// Package kafka wires the engine to a Kafka-compatible broker (Kafka or
// Redpanda) using franz-go — the fastest, most actively maintained pure-Go
// client. The whole package is optional: if LAMBARI_KAFKA_BROKERS is unset,
// the API runs in inline mode and this code never executes.
package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"lambari/internal/engine"
	"lambari/internal/model"
)

const (
	Topic    = "transactions"
	DLQTopic = "transactions.dlq"
)

// Consumer pulls transaction batches from Kafka and feeds the engine.
type Consumer struct {
	client *kgo.Client
	eng    *engine.Engine
	// dlq receives records that could not be decoded. Injected so the routing
	// can be tested without a broker.
	dlq func(ctx context.Context, rec *kgo.Record, cause error)
	// flush forces the async producers (verdicts, DLQ) to durable storage.
	// Called before every offset commit: committing first would mean a crash
	// silently loses whatever publishes were still buffered.
	flush func(ctx context.Context) error
	// lag is fed from every fetch and read by the metrics endpoint.
	lag lagTracker
}

// Lag reports how many records behind the head this consumer is, per
// partition. This is the signal to autoscale on: a saturated consumer can sit
// at low CPU while lag climbs, so CPU-based scaling would never react.
func (c *Consumer) Lag() map[int32]int64 { return c.lag.Snapshot() }

func NewConsumer(brokers []string, eng *engine.Engine, dlq *DLQProducer, verdicts *VerdictProducer) (*Consumer, error) {
	c := &Consumer{eng: eng, dlq: dlq.Send, flush: func(ctx context.Context) error {
		if err := dlq.Flush(ctx); err != nil {
			return err
		}
		return verdicts.Flush(ctx)
	}}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("lambari-scoring"),
		kgo.ConsumeTopics(Topic),
		kgo.FetchMaxBytes(16<<20),
		kgo.DisableAutoCommit(), // commit only once the engine has scored the batch
		// Last chance before partitions move to another member: franz-go blocks
		// the rebalance until the in-flight PollFetches has been handled, and
		// handleRecords only returns once scoring is done — so by the time this
		// runs, everything worth committing is genuinely complete.
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			if err := c.flush(ctx); err != nil {
				slog.Error("flush on partition revoke — skipping commit, records will replay", "err", err)
				return
			}
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				slog.Error("commit on partition revoke", "revoked", revoked, "err", err)
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}

// Run polls until ctx is cancelled. Poll → decode → score → commit, in that
// order. Committing after scoring rather than after queueing is what makes the
// pipeline at-least-once: a crash replays the batch instead of losing it.
// Backpressure still applies — if the engine's buffer fills, the batch blocks,
// polling slows, and consumer lag becomes visible in Kafka where you can alert
// on it.
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

		// The high watermark rides along on every fetch, so lag costs nothing
		// beyond reading it.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			last := int64(-1)
			if n := len(p.Records); n > 0 {
				last = p.Records[n-1].Offset
			}
			c.lag.Observe(p.Partition, p.HighWatermark, last)
		})

		recs := make([]*kgo.Record, 0, fetches.NumRecords())
		fetches.EachRecord(func(rec *kgo.Record) { recs = append(recs, rec) })
		c.handleRecords(ctx, recs)

		if err := c.flush(ctx); err != nil {
			// Not committing is the safe failure: these records redeliver on
			// restart instead of their verdicts vanishing.
			if ctx.Err() == nil {
				slog.Error("producer flush before commit — skipping commit", "err", err)
			}
			continue
		}
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			slog.Error("kafka commit", "err", err)
		}
	}
}

// handleRecords decodes a batch, routes what it cannot parse to the DLQ, and
// blocks until the engine has scored the rest. When it returns, every record in
// the batch has been either scored or made durable elsewhere — which is the
// precondition for committing the offsets.
func (c *Consumer) handleRecords(ctx context.Context, recs []*kgo.Record) {
	txs := make([]model.Transaction, 0, len(recs))
	for _, rec := range recs {
		var tx model.Transaction
		if err := json.Unmarshal(rec.Value, &tx); err != nil {
			// A record you cannot parse is a record you must not silently drop:
			// it is either a bug in a producer or an attack, and both are worth
			// keeping.
			slog.Error("undecodable record routed to dlq",
				"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			c.dlq(ctx, rec, err)
			continue
		}
		txs = append(txs, tx)
	}
	c.eng.SubmitBatch(txs)
}

// Producer publishes transactions — used by cmd/loadgen in Kafka mode.
type Producer struct {
	client *kgo.Client
	failed atomic.Int64
	logged sync.Once
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(Topic),
		kgo.AllowAutoTopicCreation(), // broker-side auto-create still gates this; a fresh dev broker just works
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
			// Produce failures are asynchronous, so a caller counting its own
			// successful Send calls would over-report. Count them here and let
			// the caller ask. Logging every one at load drowns the terminal and
			// slows the very thing being measured, so only the first is logged.
			p.failed.Add(1)
			p.logged.Do(func() {
				slog.Error("kafka produce (further failures counted, not logged)", "err", err)
			})
		}
	})
	return nil
}

// Failures reports asynchronous produce errors. A throughput number that
// ignores them is a lie.
func (p *Producer) Failures() int64 { return p.failed.Load() }

func (p *Producer) Close() {
	p.client.Flush(context.Background())
	p.client.Close()
}

// ---- dead letter queue -------------------------------------------------

// DLQProducer parks records the consumer could not decode, with enough headers
// to trace each one back to where it came from.
type DLQProducer struct {
	client *kgo.Client
}

func NewDLQProducer(brokers []string) (*DLQProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(DLQTopic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &DLQProducer{client: client}, nil
}

func (p *DLQProducer) Send(ctx context.Context, rec *kgo.Record, cause error) {
	p.client.Produce(ctx, &kgo.Record{
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: "cause", Value: []byte(cause.Error())},
			{Key: "origin-topic", Value: []byte(rec.Topic)},
			{Key: "origin-partition", Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			{Key: "origin-offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		},
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			// Nothing left to fall back on: say so loudly rather than lose it quietly.
			slog.Error("dlq publish failed — record lost",
				"origin_topic", rec.Topic, "origin_offset", rec.Offset, "err", err)
		}
	})
}

// Flush blocks until every buffered DLQ record is on the broker — the
// consumer calls this before committing offsets that cover those records.
func (p *DLQProducer) Flush(ctx context.Context) error { return p.client.Flush(ctx) }

func (p *DLQProducer) Close() {
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
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &VerdictProducer{client: client}, nil
}

// Flush blocks until every buffered verdict is on the broker — the consumer
// calls this before committing the offsets those verdicts came from.
func (p *VerdictProducer) Flush(ctx context.Context) error { return p.client.Flush(ctx) }

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
