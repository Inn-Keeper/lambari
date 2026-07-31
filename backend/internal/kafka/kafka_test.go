package kafka

import (
	"context"
	"encoding/json"
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
