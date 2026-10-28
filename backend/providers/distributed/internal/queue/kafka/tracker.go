package kafka

import (
	"context"
	"sync"
)

// tracker computes, per partition, the highest offset that may be committed:
// the last offset in the run of registered messages that have all completed.
//
// Kafka logs can have offset holes (compacted topics, transaction markers), so
// contiguity is defined over the registered in-flight sequence in Begin order,
// never over offset+1.
type tracker struct {
	mu         sync.Mutex
	partitions map[int]*partitionTracker
	// pending counts registered messages not yet covered by a returned
	// watermark, summed over all partitions.
	pending int
	// changed is closed and reset to nil whenever pending may have dropped,
	// waking every blocked WaitBelow caller. The first waiter creates it.
	changed chan struct{}
}

// partitionTracker is the in-flight state of one partition.
type partitionTracker struct {
	epoch uint64
	// highest is the highest offset registered since the partition was first
	// seen. It stays set after the queue drains so a redelivery is detected.
	highest  int64
	queue    []*inflight
	byOffset map[int64]*inflight
}

// inflight is one registered message awaiting completion.
type inflight struct {
	offset int64
	done   bool
}

// token identifies one registered message. A Done carrying a token from an
// older epoch of its partition is ignored.
type token struct {
	partition int
	offset    int64
	epoch     uint64
}

func newTracker() *tracker {
	return &tracker{partitions: make(map[int]*partitionTracker)}
}

// Begin registers a fetched message. Offsets for a partition normally arrive
// in increasing order. If offset <= the highest offset already registered for
// the partition (the group rebalanced or the reader was reset and the broker
// redelivered from the committed position), the partition's state is dropped,
// its epoch is bumped, and the message starts the new epoch. Tokens from the
// old epoch become stale.
func (t *tracker) Begin(partition int, offset int64) token {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps := t.partitions[partition]
	switch {
	case ps == nil:
		ps = &partitionTracker{byOffset: make(map[int64]*inflight)}
		t.partitions[partition] = ps
	case offset <= ps.highest:
		// Redelivery: everything still registered under the old epoch is
		// abandoned, so it no longer counts as pending.
		t.pending -= len(ps.queue)
		ps.epoch++
		ps.queue = nil
		ps.byOffset = make(map[int64]*inflight)
		t.notifyLocked()
	}

	m := &inflight{offset: offset}
	ps.queue = append(ps.queue, m)
	ps.byOffset[offset] = m
	ps.highest = offset
	t.pending++
	return token{partition: partition, offset: offset, epoch: ps.epoch}
}

// Done marks a message complete. It returns (commitOffset, true) when this
// completion advanced the partition's watermark: commitOffset is the offset of
// the last message in the newly contiguous completed run, to be passed to the
// reader's CommitMessages. It returns (0, false) when the watermark did not
// move, the token is stale, or the message was already done.
func (t *tracker) Done(tk token) (commitOffset int64, advanced bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps := t.partitions[tk.partition]
	if ps == nil || ps.epoch != tk.epoch {
		return 0, false
	}
	m := ps.byOffset[tk.offset]
	if m == nil || m.done {
		// Either never registered, already done, or already covered by a
		// previously returned watermark.
		return 0, false
	}
	m.done = true

	n := 0
	for n < len(ps.queue) && ps.queue[n].done {
		commitOffset = ps.queue[n].offset
		delete(ps.byOffset, commitOffset)
		ps.queue[n] = nil
		n++
	}
	if n == 0 {
		return 0, false
	}
	ps.queue = ps.queue[n:]
	t.pending -= n
	t.notifyLocked()
	return commitOffset, true
}

// Pending is the number of registered messages not yet covered by a returned
// watermark (in-flight plus done-above-a-gap). The consumer uses it to bound
// how far fetching may run ahead of the committable position.
func (t *tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
}

// WaitBelow blocks until Pending() < limit or ctx ends (returns ctx.Err()).
// limit <= 0 means "no limit" and returns nil immediately.
func (t *tracker) WaitBelow(ctx context.Context, limit int) error {
	if limit <= 0 {
		return nil
	}
	for {
		t.mu.Lock()
		if t.pending < limit {
			t.mu.Unlock()
			return nil
		}
		if t.changed == nil {
			t.changed = make(chan struct{})
		}
		ch := t.changed
		t.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// notifyLocked wakes every WaitBelow caller. The caller must hold t.mu.
func (t *tracker) notifyLocked() {
	if t.changed != nil {
		close(t.changed)
		t.changed = nil
	}
}
