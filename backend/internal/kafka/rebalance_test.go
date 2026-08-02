package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"lambari/internal/model"
)

// TestRebalanceLosesVelocityState demonstrates the load-bearing limitation of
// this design: the sliding velocity windows live in the memory of whichever
// process owns the partition. Move the partition and the windows do not move
// with it.
//
// The experiment: two consumers in one group on a partitioned topic, a set of
// cards hammering hard enough to be scored card_velocity_extreme, then one
// consumer shuts down *cleanly* — no crash, the kind of thing a rolling deploy
// does several times a day. The survivor inherits those partitions with empty
// windows and scores the same cards, mid-attack, as if it had never seen them.
// Cards on partitions the survivor already owned keep their history, so the
// damage is partial and invisible in any aggregate metric.
//
// Every transaction here also trips amount_extreme, so a verdict is published
// either way — otherwise losing the velocity flag would turn a Decline into an
// Approve, which publishes nothing, and "no verdict" is indistinguishable from
// "not consumed yet".
//
// Skipped unless LAMBARI_KAFKA_BROKERS is set — run via `make rebalance`.
func TestRebalanceLosesVelocityState(t *testing.T) {
	brokers := os.Getenv("LAMBARI_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set LAMBARI_KAFKA_BROKERS to run (make rebalance)")
	}
	seeds := strings.Split(brokers, ",")

	const (
		partitions = 6  // enough that two members split them and neither owns everything
		hotCards   = 24 // spread over the partitions by key, so a kill lands on some of them
		warmRounds = 10 // ≥8 hits inside the window ⇒ card_velocity_extreme
		postRounds = 3  // <4 hits: a window that was truly lost cannot reach any velocity tier
		// The velocity window is 60s. If the run takes longer than this, warm
		// cards would age out on their own and a pass would prove nothing.
		maxElapsed = 45 * time.Second
	)

	ensurePartitions(t, seeds, partitions)

	runID := fmt.Sprintf("rb_%d", time.Now().UnixNano())
	txID := func(round, card int) string { return fmt.Sprintf("%s-r%02d-c%02d", runID, round, card) }

	// Watch verdicts from the end: this run's records are all produced after
	// the watcher exists, and starting at the end keeps old runs out.
	watcher, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(VerdictTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	verdicts := map[string]model.Verdict{} // TxID -> verdict (last one wins; duplicates are identical)
	collect := func(want int, deadline time.Duration) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()
		for len(verdicts) < want {
			fetches := watcher.PollFetches(ctx)
			if ctx.Err() != nil {
				t.Fatalf("only %d of %d verdicts arrived within %v — the survivor is not consuming",
					len(verdicts), want, deadline)
			}
			fetches.EachRecord(func(rec *kgo.Record) {
				var v model.Verdict
				if json.Unmarshal(rec.Value, &v) == nil && strings.HasPrefix(v.TxID, runID) {
					verdicts[v.TxID] = v
				}
			})
		}
	}

	prod, err := NewProducer(seeds)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	// One round = one transaction per hot card. Keyed by CardHash (see
	// Producer.Send), so a given card always lands on the same partition —
	// which is what makes per-partition velocity state coherent in the first
	// place, and exactly what makes it fragile when partitions move.
	round := func(n int) {
		t.Helper()
		for card := 0; card < hotCards; card++ {
			tx := model.Transaction{
				ID:       txID(n, card),
				CardBIN:  "520082",
				CardHash: fmt.Sprintf("%s-card%02d", runID, card),
				Amount:   9999, // amount_extreme ⇒ always flagged ⇒ always a verdict
				Currency: "SEK", Country: "SE",
				IP:         fmt.Sprintf("10.7.%d.%d", card/200, card%200),
				MerchantID: "m_rebal", MCC: "5411",
				Timestamp: time.Now(),
			}
			if err := prod.Send(context.Background(), tx); err != nil {
				t.Fatal(err)
			}
		}
		// No explicit flush: linger is 5ms, and collect() below will not return
		// until the verdicts are actually back, which is a stronger wait than
		// flushing the producer would be.
	}

	bin := buildAPIBinary(t)
	a := startAPIProcess(t, bin, brokers, ":18081")
	b := startAPIProcess(t, bin, brokers, ":18082")
	// Let the survivor leave the group on the way out. Cleanups run last-first,
	// so this beats the SIGKILL registered by startAPIProcess — a member killed
	// outright stays in the group until its session times out, and the next run
	// of this test would spend ~45s waiting for a ghost.
	t.Cleanup(func() { _ = b.Process.Signal(syscall.SIGTERM); _, _ = b.Process.Wait() })

	// cold reports which cards are not showing extreme velocity as of round n.
	cold := func(n int) []int {
		var out []int
		for card := 0; card < hotCards; card++ {
			if !hasFlag(verdicts[txID(n, card)], "card_velocity_extreme") {
				out = append(out, card)
			}
		}
		return out
	}

	// The second member joining the group is itself a rebalance — the very
	// effect under test — so it can wipe the windows partway through warm-up.
	// Warming up again is more honest than sleeping and hoping the group has
	// settled: if the group is stable, one pass is enough and the rest never
	// run; if it wasn't, the retry is exactly what a stable group looks like.
	sent, warmedAt := 0, time.Now()
	var stillCold []int
	for attempt := 1; attempt <= 3; attempt++ {
		warmedAt = time.Now()
		for i := 0; i < warmRounds; i++ {
			round(sent)
			sent++
		}
		collect(sent*hotCards, 2*time.Minute)
		if stillCold = cold(sent - 1); len(stillCold) == 0 {
			break
		}
		t.Logf("warm-up attempt %d: cards %v still cold (group rebalanced mid-warm-up) — retrying",
			attempt, stillCold)
	}

	// Control: the windows have to be warm before the rebalance, or the
	// "cold after" half of the experiment measures nothing.
	if len(stillCold) > 0 {
		t.Fatalf("cards %v never reached card_velocity_extreme — warm-up failed, so the "+
			"post-rebalance comparison would be meaningless", stillCold)
	}

	// SIGTERM, not SIGKILL: the point is that an ordinary, orderly shutdown —
	// a deploy — is enough to lose the state. A crash loses it too, just after
	// the group's session timeout rather than immediately.
	if err := a.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Process.Wait(); err != nil {
		t.Fatal(err)
	}

	firstPost := sent
	for n := 0; n < postRounds; n++ {
		round(sent)
		sent++
	}
	collect(sent*hotCards, 2*time.Minute)

	if elapsed := time.Since(warmedAt); elapsed > maxElapsed {
		t.Fatalf("run took %v, longer than the %v guard: warm cards could have aged out of the "+
			"60s velocity window on their own, so any observed loss is not attributable to the rebalance",
			elapsed, maxElapsed)
	}

	lost, kept := 0, 0
	for card := 0; card < hotCards; card++ {
		coldThroughout := true
		for n := firstPost; n < sent; n++ {
			if hasFlag(verdicts[txID(n, card)], "card_velocity_extreme") {
				coldThroughout = false
			}
		}
		if coldThroughout {
			lost++
		} else {
			kept++
		}
	}

	if lost == 0 {
		t.Fatalf("all %d cards kept their velocity history across the rebalance — expected the "+
			"partitions that moved to score cold on the survivor", hotCards)
	}
	t.Logf("velocity state after a clean rebalance: %d of %d cards lost their window "+
		"(partition moved, survivor started from empty), %d kept it (partition never moved). "+
		"Those %d cards were mid-attack and scored as first-time traffic.",
		lost, hotCards, kept, lost)
}

func hasFlag(v model.Verdict, flag string) bool {
	for _, f := range v.Flags {
		if f == flag {
			return true
		}
	}
	return false
}

// buildAPIBinary compiles cmd/api into the test's temp dir.
func buildAPIBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "api")
	build := exec.Command("go", "build", "-o", bin, "./cmd/api")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// startAPIProcess runs one API instance; every instance joins the same
// consumer group, which is what makes them members that can rebalance.
func startAPIProcess(t *testing.T, bin, brokers, addr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "LAMBARI_KAFKA_BROKERS="+brokers, "LAMBARI_ADDR="+addr)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// An orphan keeps its port and stays in the group, poisoning the next run.
	// Killing an already-dead process just errors — ignored.
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	return cmd
}

// ensurePartitions makes the transactions topic wide enough for two consumers
// to own a share each. A single-partition topic cannot rebalance meaningfully:
// one member would own everything and the other would idle.
func ensurePartitions(t *testing.T, seeds []string, want int32) {
	t.Helper()
	admin, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create it wide, or widen it if a previous run left it narrow. Exactly one
	// of these applies on any given run and the other returns an error saying
	// so, which is why neither result is inspected — the metadata check below
	// is the assertion that matters.
	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = Topic, want, 1
	create.Topics = append(create.Topics, ct)
	_, _ = create.RequestWith(ctx, admin)

	grow := kmsg.NewPtrCreatePartitionsRequest()
	gt := kmsg.NewCreatePartitionsRequestTopic()
	gt.Topic, gt.Count = Topic, want
	grow.Topics = append(grow.Topics, gt)
	_, _ = grow.RequestWith(ctx, admin)

	md := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(Topic)
	md.Topics = append(md.Topics, mt)
	resp, err := md.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("metadata for %s: %v", Topic, err)
	}
	if len(resp.Topics) == 0 {
		t.Fatalf("broker reports no topic %s", Topic)
	}
	if got := len(resp.Topics[0].Partitions); got < int(want) {
		t.Fatalf("topic %s has %d partitions, need %d — could not create or widen it", Topic, got, want)
	}
}
