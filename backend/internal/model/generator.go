package model

import (
	"fmt"
	"math/rand"
	"time"
)

// Generator produces realistic-looking synthetic traffic with a controllable
// fraud rate: most transactions are benign, a slice exhibits card-testing,
// geo-mismatch, and high-amount patterns the rules are built to catch.
type Generator struct {
	rng     *rand.Rand
	seq     int64
	hotIPs  []string // IPs used by simulated fraud rings (card testing)
	hotCard string   // a card being hammered (velocity)
}

var (
	bins      = []string{"411111", "455673", "510510", "520082", "530127", "601100", "356600", "627780", "506699"}
	countries = map[string]string{"411111": "US", "455673": "GB", "510510": "DE", "520082": "SE", "530127": "SE", "601100": "US", "356600": "JP", "627780": "BR", "506699": "NG"}
	mccs      = []string{"5411", "5812", "5732", "4111", "5999", "7995", "6051", "4829"}
	geos      = []string{"SE", "US", "GB", "DE", "JP", "BR", "NG", "RU", "VN"}
)

func NewGenerator(seed int64) *Generator {
	rng := rand.New(rand.NewSource(seed))
	g := &Generator{rng: rng}
	for i := 0; i < 4; i++ {
		g.hotIPs = append(g.hotIPs, randomIP(rng))
	}
	g.hotCard = fmt.Sprintf("tok_%08x", rng.Uint32())
	return g
}

func (g *Generator) Next() Transaction {
	g.seq++
	r := g.rng.Float64()
	bin := bins[g.rng.Intn(len(bins))]

	tx := Transaction{
		ID:         fmt.Sprintf("tx_%d_%06x", g.seq, g.rng.Uint32()&0xFFFFFF),
		CardBIN:    bin,
		CardHash:   fmt.Sprintf("tok_%08x", g.rng.Uint32()),
		Amount:     roundCents(20 + g.rng.ExpFloat64()*120),
		Currency:   "SEK",
		Country:    countries[bin], // benign default: card used at home
		IP:         randomIP(g.rng),
		MerchantID: fmt.Sprintf("m_%03d", g.rng.Intn(400)),
		MCC:        mccs[g.rng.Intn(5)], // low-risk categories
		Timestamp:  time.Now(),
	}

	switch {
	case r < 0.03: // card-testing ring: hot IP, small amounts, many cards
		tx.IP = g.hotIPs[g.rng.Intn(len(g.hotIPs))]
		tx.Amount = roundCents(1 + g.rng.Float64()*9)
	case r < 0.05: // velocity attack: one card hammered
		tx.CardHash = g.hotCard
	case r < 0.08: // stolen card abroad: geo mismatch + larger amount
		tx.Country = geos[g.rng.Intn(len(geos))]
		tx.Amount = roundCents(400 + g.rng.ExpFloat64()*900)
	case r < 0.10: // high-risk merchant + big ticket
		tx.MCC = mccs[5+g.rng.Intn(3)]
		tx.Amount = roundCents(1200 + g.rng.ExpFloat64()*2500)
	}
	return tx
}

func randomIP(rng *rand.Rand) string {
	return fmt.Sprintf("%d.%d.%d.%d", 10+rng.Intn(200), rng.Intn(255), rng.Intn(255), 1+rng.Intn(254))
}

func roundCents(v float64) float64 {
	return float64(int(v*100)) / 100
}
