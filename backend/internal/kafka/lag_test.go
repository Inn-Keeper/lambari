package kafka

import "testing"

// Consumer lag is the number to autoscale on, so its arithmetic is worth
// pinning: it comes from the high watermark every fetch already carries, not
// from an admin client.
func TestLagFromFetchedOffsets(t *testing.T) {
	var lt lagTracker
	// head is at offset 100 (next write lands there); we just read up to 89
	lt.Observe(0, 100, 89)
	if got := lt.Snapshot()[0]; got != 10 {
		t.Fatalf("lag = %d, want 10 (records 90..99 unread)", got)
	}
}

// A caught-up consumer stops receiving records. If lag froze at its last
// non-empty value, a healthy consumer would look permanently behind.
func TestEmptyFetchOnCaughtUpPartitionReportsZero(t *testing.T) {
	var lt lagTracker
	lt.Observe(0, 100, 99)
	lt.Observe(0, 100, -1) // empty fetch, head unchanged
	if got := lt.Snapshot()[0]; got != 0 {
		t.Fatalf("lag = %d after catching up, want 0", got)
	}
}

// Not knowing a partition's position is different from knowing it is zero;
// reporting 0 for an unread partition would hide a stalled assignment.
func TestUnreadPartitionIsAbsentNotZero(t *testing.T) {
	var lt lagTracker
	lt.Observe(7, 500, -1)
	if _, ok := lt.Snapshot()[7]; ok {
		t.Fatal("partition with no observed offset must be absent from the snapshot")
	}
}

// The watermark can trail the offset we just read across a metadata refresh.
// A negative lag would break any query built on the series.
func TestLagNeverGoesNegative(t *testing.T) {
	var lt lagTracker
	lt.Observe(0, 50, 60)
	if got := lt.Snapshot()[0]; got != 0 {
		t.Fatalf("lag = %d, want 0 — lag must not go negative", got)
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	var lt lagTracker
	lt.Observe(0, 100, 89)
	snap := lt.Snapshot()
	snap[0] = 9999
	if got := lt.Snapshot()[0]; got != 10 {
		t.Fatalf("mutating a snapshot changed the tracker: got %d, want 10", got)
	}
}
