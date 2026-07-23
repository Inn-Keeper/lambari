package model

import "time"

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
