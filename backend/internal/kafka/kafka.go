// Package kafka wires the engine to a Kafka-compatible broker (Kafka or
// Redpanda) using franz-go — the fastest, most actively maintained pure-Go
// client. The whole package is optional: if LAMBARI_KAFKA_BROKERS is unset,
// the API runs in inline mode and this code never executes.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
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
	Group    = "lambari-scoring"
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
	// commit marks every polled offset as done. Injected alongside flush so
	// the flush-before-commit ordering can be tested without a broker.
	commit func(ctx context.Context) error
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
		kgo.ConsumerGroup(Group),
		kgo.ConsumeTopics(Topic),
		kgo.FetchMaxBytes(16<<20),
		kgo.DisableAutoCommit(), // commit only once the engine has scored the batch
		// Without this, franz-go may run OnPartitionsRevoked concurrently with
		// processing, and its commit would claim a batch still being scored.
		// Blocked, a rebalance waits for AllowRebalance, which Run calls only
		// after the batch is scored, flushed and committed.
		kgo.BlockRebalanceOnPoll(),
		// Last chance before partitions move to another member (and on Close).
		// Thanks to BlockRebalanceOnPoll it runs between batches, never inside one.
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
	c.commit = client.CommitUncommittedOffsets
	return c, nil
}

// Run polls until ctx is cancelled: poll → decode → score → flush → commit.
// Committing after scoring makes it at-least-once; a full engine blocks the
// batch, which shows up as consumer lag. It returns an error only when a
// publish failed, and the caller should then exit so a restart replays.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			c.client.AllowRebalance()
			return nil
		}
		err := c.handleFetches(ctx, fetches)
		// Every poll must be paired with this (BlockRebalanceOnPoll); by now
		// the batch is committed or deliberately left uncommitted.
		c.client.AllowRebalance()
		if err != nil {
			return err
		}
	}
}

func (c *Consumer) handleFetches(ctx context.Context, fetches kgo.Fetches) error {
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			slog.Error("kafka fetch", "topic", e.Topic, "err", e.Err)
		}
		time.Sleep(time.Second)
		return nil
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
	return c.processBatch(ctx, recs)
}

// processBatch scores a batch, then commits it only if every verdict and DLQ
// record it produced actually reached the broker.
func (c *Consumer) processBatch(ctx context.Context, recs []*kgo.Record) error {
	c.handleRecords(ctx, recs)
	if err := c.flush(ctx); err != nil {
		if ctx.Err() != nil {
			// Shutting down mid-flush. Nothing is lost: the publishes carry a
			// context that shutdown doesn't cancel, and the revoke hook on
			// Close flushes and commits them, or skips the commit if one failed.
			return nil
		}
		return fmt.Errorf("publish failed, not committing: %w", err)
	}
	if err := c.commit(ctx); err != nil && ctx.Err() == nil {
		// A failed commit is safe to carry on from: the next one covers these
		// offsets, and until then a crash only replays them.
		slog.Error("kafka commit", "err", err)
	}
	return nil
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
			// WithoutCancel: a shutdown mid-batch must not fail a DLQ publish
			// the revoke hook is about to commit past.
			c.dlq(context.WithoutCancel(ctx), rec, err)
			continue
		}
		// Same rules as HTTP ingest. A record that would corrupt a velocity
		// window is parked with the undecodable ones.
		if err := tx.Validate(time.Now()); err != nil {
			slog.Error("invalid record routed to dlq",
				"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			c.dlq(context.WithoutCancel(ctx), rec, err)
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
	errs   publishErrors
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
			// Flush reports this, so the offset is never committed past it.
			p.errs.record()
			slog.Error("dlq publish failed",
				"origin_topic", rec.Topic, "origin_offset", rec.Offset, "err", err)
		}
	})
}

// Flush blocks until every buffered DLQ record is settled, and errors if any
// publish has ever failed — the consumer calls this before committing offsets
// that cover those records.
func (p *DLQProducer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return err
	}
	return p.errs.check("dlq")
}

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
	errs   publishErrors
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

// Flush blocks until every buffered verdict is settled, and errors if any
// publish has ever failed — the consumer calls this before committing the
// offsets those verdicts came from.
func (p *VerdictProducer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return err
	}
	return p.errs.check("verdict")
}

func (p *VerdictProducer) Send(ctx context.Context, v model.Verdict) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshal verdict", "err", err)
		return
	}
	p.client.Produce(ctx, &kgo.Record{Key: []byte(v.TxID), Value: b}, func(_ *kgo.Record, err error) {
		if err != nil {
			p.errs.record()
			slog.Error("publish verdict", "err", err)
		}
	})
}

// publishErrors records async publish failures, which kgo's Flush does not
// report. It never resets: any later commit would also cover the failed batch.
type publishErrors struct{ n atomic.Int64 }

func (e *publishErrors) record() { e.n.Add(1) }

func (e *publishErrors) check(what string) error {
	if n := e.n.Load(); n > 0 {
		return fmt.Errorf("%d %s publish(es) failed", n, what)
	}
	return nil
}
