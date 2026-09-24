package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"lambari/internal/engine"
	"lambari/internal/model"
)

// A record the engine cannot decode must land in the DLQ, not vanish into a log
// line — and it must not stop the rest of the batch from being scored.
// handleRecords is the whole commit-safety contract in one function: when it
// returns, everything decodable has been scored and everything else is durable.
func TestUndecodableRecordGoesToDLQNotSilentlyDropped(t *testing.T) {
	eng := engine.New()
	eng.Start()
	defer eng.Stop()

	var dead []*kgo.Record
	c := &Consumer{
		eng: eng,
		dlq: func(_ context.Context, rec *kgo.Record, cause error) {
			if cause == nil {
				t.Error("DLQ called without a cause")
			}
			dead = append(dead, rec)
		},
	}

	good, err := json.Marshal(model.Transaction{
		ID: "tx_1", CardBIN: "520082", CardHash: "tok_1",
		Amount: 49.90, Currency: "SEK", Country: "SE",
		IP: "10.0.0.1", MerchantID: "m_001", MCC: "5411",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	c.handleRecords(context.Background(), []*kgo.Record{
		{Topic: Topic, Partition: 3, Offset: 42, Value: []byte("{not json")},
		{Topic: Topic, Partition: 3, Offset: 43, Value: good},
	})

	if len(dead) != 1 {
		t.Fatalf("expected 1 record in the DLQ, got %d", len(dead))
	}
	if dead[0].Offset != 42 {
		t.Errorf("wrong record sent to DLQ: offset %d", dead[0].Offset)
	}
	// handleRecords blocks until scoring finishes, so this needs no polling —
	// which is exactly the property that makes committing after it safe.
	if got := eng.Snapshot().Processed; got != 1 {
		t.Fatalf("expected the 1 decodable record scored before handleRecords returned, got %d", got)
	}
}

// A publish that failed must stop the commit. kgo's Flush returns nil for
// failed records, so this is the only thing between a lost verdict and an
// offset that claims it was delivered.
func TestFailedPublishBlocksCommit(t *testing.T) {
	eng := engine.New()
	eng.Start()
	defer eng.Stop()

	committed := false
	c := &Consumer{
		eng:    eng,
		flush:  func(context.Context) error { return errors.New("1 verdict publish(es) failed") },
		commit: func(context.Context) error { committed = true; return nil },
	}

	if err := c.processBatch(context.Background(), nil); err == nil {
		t.Fatal("expected an error so the consumer stops")
	}
	if committed {
		t.Fatal("committed offsets past a failed publish")
	}
}

// Failures must stick: the next commit would cover the failed batch's offsets
// too, so a later clean batch cannot make committing safe again.
func TestPublishErrorsAreSticky(t *testing.T) {
	var e publishErrors
	if err := e.check("verdict"); err != nil {
		t.Fatalf("no failures yet, got %v", err)
	}
	e.record()
	for i := 0; i < 2; i++ {
		if e.check("verdict") == nil {
			t.Fatalf("check %d: failure forgotten", i)
		}
	}
}

// A record that decodes but would corrupt a velocity window (here: no card
// hash, so it would share one window with every other such record) is parked
// in the DLQ like an undecodable one.
func TestInvalidRecordGoesToDLQ(t *testing.T) {
	eng := engine.New()
	eng.Start()
	defer eng.Stop()

	var dead int
	c := &Consumer{eng: eng, dlq: func(context.Context, *kgo.Record, error) { dead++ }}
	bad, _ := json.Marshal(model.Transaction{ID: "tx_1", Timestamp: time.Now()})
	c.handleRecords(context.Background(), []*kgo.Record{{Topic: Topic, Value: bad}})

	if dead != 1 {
		t.Fatalf("DLQ got %d records, want 1", dead)
	}
	if got := eng.Snapshot().Processed; got != 0 {
		t.Fatalf("scored %d invalid records, want 0", got)
	}
}
