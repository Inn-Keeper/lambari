package kafka

import "sync"

// lagTracker derives consumer lag from data every fetch already carries: the
// partition's high watermark and the offset of the last record we got. No
// admin client, no extra round-trips, no background goroutine — the poll loop
// is already talking to the broker about exactly this.
type lagTracker struct {
	mu    sync.Mutex
	parts map[int32]partitionLag
}

type partitionLag struct {
	lastOffset int64
	lag        int64
}

// Observe records one partition's slice of a fetch response. lastOffset is the
// offset of the final record returned, or -1 when the fetch came back empty —
// which is the normal state of a consumer that has caught up, so the stored
// position is reused rather than treated as unknown.
func (t *lagTracker) Observe(partition int32, highWatermark, lastOffset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	p, seen := t.parts[partition]
	if lastOffset < 0 && !seen {
		// Never read this partition: we know where the head is but not where
		// we are, and guessing zero would look like a healthy consumer.
		return
	}
	if lastOffset >= 0 {
		p.lastOffset = lastOffset
	}
	// The watermark can trail our own offset across a metadata refresh; clamp.
	if p.lag = highWatermark - p.lastOffset - 1; p.lag < 0 {
		p.lag = 0
	}
	if t.parts == nil {
		t.parts = make(map[int32]partitionLag)
	}
	t.parts[partition] = p
}

// Snapshot returns lag per partition. Partitions never read are absent.
func (t *lagTracker) Snapshot() map[int32]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[int32]int64, len(t.parts))
	for p, st := range t.parts {
		out[p] = st.lag
	}
	return out
}
