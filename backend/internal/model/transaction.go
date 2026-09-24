package model

import (
	"errors"
	"time"
)

// Transaction is a single payment event entering the pipeline.
type Transaction struct {
	ID         string    `json:"id"`
	CardBIN    string    `json:"card_bin"`  // first 6 digits, identifies issuer country
	CardHash   string    `json:"card_hash"` // tokenized PAN, used for velocity
	Amount     float64   `json:"amount"`
	Currency   string    `json:"currency"`
	Country    string    `json:"country"` // where the transaction originated
	IP         string    `json:"ip"`
	MerchantID string    `json:"merchant_id"`
	MCC        string    `json:"mcc"` // merchant category code
	Timestamp  time.Time `json:"timestamp"`
}

// BINCountry maps a card BIN to its issuing country. A toy table for the PoC;
// production would use a licensed BIN database.
var BINCountry = map[string]string{
	"411111": "US", "455673": "GB", "510510": "DE",
	"520082": "SE", "530127": "SE", "601100": "US",
	"356600": "JP", "627780": "BR", "506699": "NG",
}

// MaxClockSkew is how far into the future a timestamp may be. The velocity
// windows use the transaction's own time as "now", so one far-future
// timestamp would expire every real entry in that card's window.
const MaxClockSkew = 5 * time.Second

// Validate rejects transactions the rules cannot score safely: without an ID
// a verdict can't be traced or deduped, and without a card hash every such
// transaction would share one velocity window.
func (tx Transaction) Validate(now time.Time) error {
	switch {
	case tx.ID == "":
		return errors.New("id is required")
	case tx.CardHash == "":
		return errors.New("card_hash is required")
	case tx.Timestamp.IsZero():
		return errors.New("timestamp is required")
	case tx.Timestamp.After(now.Add(MaxClockSkew)):
		return errors.New("timestamp is in the future")
	}
	return nil
}

// Decision is the engine's ruling on a transaction.
type Decision string

const (
	Approve Decision = "approve"
	Review  Decision = "review"
	Decline Decision = "decline"
)

// Verdict is the scored outcome for one transaction.
type Verdict struct {
	TxID      string   `json:"tx_id"`
	CardBIN   string   `json:"card_bin"`
	Amount    float64  `json:"amount"`
	Currency  string   `json:"currency"`
	Country   string   `json:"country"`
	Score     int      `json:"score"`
	Flags     []string `json:"flags,omitempty"`
	Decision  Decision `json:"decision"`
	LatencyUS int64    `json:"latency_us"` // scoring latency, microseconds
	At        int64    `json:"at"`         // unix millis
}
