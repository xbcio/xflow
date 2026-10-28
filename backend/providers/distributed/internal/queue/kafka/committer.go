package kafka

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	defaultCommitRetry  = time.Second
	defaultFlushTimeout = 5 * time.Second
)

// commitFunc is Reader.CommitMessages.
type commitFunc func(ctx context.Context, msgs ...kafkago.Message) error

// committer serializes and coalesces the offset commits of one Reader.
//
// kafka-go's CommitMessages stores msg.Offset+1 as the partition's committed
// position without checking that it moves forward, so two workers committing
// 10 and 12 concurrently can leave the position at 11 after a crash. Routing
// every commit of a Reader through one committer goroutine keeps at most one
// commit in flight and keeps the committed offset of each partition monotonic.
type committer struct {
	topic        string
	commit       commitFunc
	onError      func(error)
	retryEvery   time.Duration
	flushTimeout time.Duration

	mu sync.Mutex
	// pending holds, per partition, the highest offset waiting to be committed.
	pending map[int]int64
	// inflight holds the batch currently being committed. Advance consults it
	// so a lower offset arriving mid-commit cannot be committed after a higher
	// one that is already on its way.
	inflight map[int]int64
	// committed holds, per partition, the last offset successfully committed.
	committed map[int]int64
	// wake has capacity 1; Advance pokes it without blocking.
	wake chan struct{}
}

// newCommitter: topic is stamped on every committed message. onError, if
// non-nil, is called (synchronously from the committer goroutine) with each
// commit failure; it must not block. retryEvery is how long to wait before
// retrying after a failed commit (<=0 means 1s). flushTimeout bounds the final
// flush when Run's context ends (<=0 means 5s).
func newCommitter(topic string, commit commitFunc, onError func(error), retryEvery, flushTimeout time.Duration) *committer {
	if retryEvery <= 0 {
		retryEvery = defaultCommitRetry
	}
	if flushTimeout <= 0 {
		flushTimeout = defaultFlushTimeout
	}
	return &committer{
		topic:        topic,
		commit:       commit,
		onError:      onError,
		retryEvery:   retryEvery,
		flushTimeout: flushTimeout,
		pending:      make(map[int]int64),
		committed:    make(map[int]int64),
		wake:         make(chan struct{}, 1),
	}
}

// Advance records that every message of partition up to and including offset
// is done (the value tracker.Done returned). It never blocks. A value not
// greater than what is already pending or already committed for that partition
// is dropped.
func (c *committer) Advance(partition int, offset int64) {
	c.mu.Lock()
	if cur, ok := c.committed[partition]; ok && offset <= cur {
		c.mu.Unlock()
		return
	}
	if cur, ok := c.inflight[partition]; ok && offset <= cur {
		c.mu.Unlock()
		return
	}
	if cur, ok := c.pending[partition]; ok && offset <= cur {
		c.mu.Unlock()
		return
	}
	c.pending[partition] = offset
	c.mu.Unlock()

	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Committed returns the last offset successfully committed for the partition.
func (c *committer) Committed(partition int) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	off, ok := c.committed[partition]
	return off, ok
}

// Run commits until ctx ends, then does one final bounded flush and returns.
// It must be called once, on its own goroutine.
func (c *committer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.flush(ctx)
			return
		case <-c.wake:
		}
		if !c.drain(ctx) {
			return
		}
	}
}

// drain commits pending offsets until none are left, retrying failed commits
// every retryEvery. It returns false when Run must return: the reader is
// closed, or ctx ended and the final flush has been done.
func (c *committer) drain(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			c.flush(ctx)
			return false
		}
		batch := c.take()
		if len(batch) == 0 {
			return true
		}
		err := c.commit(ctx, c.messages(batch)...)
		if err == nil {
			c.markCommitted(batch)
			return true
		}
		c.restore(batch)
		c.report(err)
		if errors.Is(err, io.ErrClosedPipe) {
			return false
		}
		if !c.sleep(ctx) {
			c.flush(ctx)
			return false
		}
	}
}

// sleep waits retryEvery and reports false if ctx ended first.
func (c *committer) sleep(ctx context.Context) bool {
	t := time.NewTimer(c.retryEvery)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// flush commits whatever is pending once, under a fresh deadline that outlives
// the cancelled run context. A failure is reported and not retried.
func (c *committer) flush(ctx context.Context) {
	batch := c.take()
	if len(batch) == 0 {
		return
	}
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.flushTimeout)
	defer cancel()
	if err := c.commit(fctx, c.messages(batch)...); err != nil {
		c.restore(batch)
		c.report(err)
		return
	}
	c.markCommitted(batch)
}

// take moves the pending offsets into the in-flight batch and returns it.
func (c *committer) take() map[int]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	batch := c.pending
	c.pending = make(map[int]int64)
	c.inflight = batch
	return batch
}

// restore puts a failed batch back into pending, keeping any larger offset
// that arrived while the commit was in flight.
func (c *committer) restore(batch map[int]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight = nil
	for p, off := range batch {
		if cur, ok := c.pending[p]; !ok || off > cur {
			c.pending[p] = off
		}
	}
}

// markCommitted records a successfully committed batch.
func (c *committer) markCommitted(batch map[int]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight = nil
	for p, off := range batch {
		if cur, ok := c.committed[p]; !ok || off > cur {
			c.committed[p] = off
		}
	}
}

// messages builds one message per partition, in ascending partition order.
func (c *committer) messages(batch map[int]int64) []kafkago.Message {
	msgs := make([]kafkago.Message, 0, len(batch))
	for _, p := range slices.Sorted(maps.Keys(batch)) {
		msgs = append(msgs, kafkago.Message{Topic: c.topic, Partition: p, Offset: batch[p]})
	}
	return msgs
}

func (c *committer) report(err error) {
	if c.onError != nil {
		c.onError(err)
	}
}
