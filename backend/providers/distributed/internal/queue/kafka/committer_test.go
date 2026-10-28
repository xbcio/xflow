package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	committerTestTopic = "t"
	// committerGuard only bounds how long a broken test may hang; no assertion
	// depends on it elapsing.
	committerGuard = 10 * time.Second
)

var errCommitBoom = errors.New("commit boom")

// ctxInfo is what a commit call observed about its context.
type ctxInfo struct {
	err         error
	hasDeadline bool
}

// fakeCommit is a commitFunc that records every call. fn, if non-nil, decides
// the result of call n (1-based) and runs without the lock held, so it may
// block. entered receives n when call n starts.
type fakeCommit struct {
	mu      sync.Mutex
	calls   [][]kafkago.Message
	infos   []ctxInfo
	fn      func(ctx context.Context, n int) error
	entered chan int
}

func newFakeCommit(fn func(ctx context.Context, n int) error) *fakeCommit {
	return &fakeCommit{fn: fn, entered: make(chan int, 1024)}
}

func (f *fakeCommit) commit(ctx context.Context, msgs ...kafkago.Message) error {
	_, hasDeadline := ctx.Deadline()
	f.mu.Lock()
	f.calls = append(f.calls, slices.Clone(msgs))
	f.infos = append(f.infos, ctxInfo{err: ctx.Err(), hasDeadline: hasDeadline})
	n := len(f.calls)
	f.mu.Unlock()
	select {
	case f.entered <- n:
	default:
	}
	if f.fn == nil {
		return nil
	}
	return f.fn(ctx, n)
}

// awaitCall blocks until the next commit call starts and returns its number.
func (f *fakeCommit) awaitCall(t *testing.T) int {
	t.Helper()
	select {
	case n := <-f.entered:
		return n
	case <-time.After(committerGuard):
		t.Fatal("timed out waiting for a commit call")
		return 0
	}
}

// snapshot returns every call so far, one string per call.
func (f *fakeCommit) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	for i, msgs := range f.calls {
		out[i] = describeMsgs(msgs)
	}
	return out
}

func (f *fakeCommit) info(n int) ctxInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.infos[n-1]
}

// describeMsgs renders messages as "topic/partition@offset,...".
func describeMsgs(msgs []kafkago.Message) string {
	parts := make([]string, len(msgs))
	for i, m := range msgs {
		parts[i] = fmt.Sprintf("%s/%d@%d", m.Topic, m.Partition, m.Offset)
	}
	return strings.Join(parts, ",")
}

func assertCalls(t *testing.T, f *fakeCommit, want ...string) {
	t.Helper()
	if got := f.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("commit calls = %q, want %q", got, want)
	}
}

// gate makes every call block until the test releases it with a result.
type gate struct {
	release chan error
}

func newGate() *gate { return &gate{release: make(chan error, 64)} }

func (g *gate) fn(ctx context.Context, n int) error { return <-g.release }

// startCommitter runs c.Run on its own goroutine and returns a channel that
// closes when Run returns.
func startCommitter(ctx context.Context, c *committer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(committerGuard):
		t.Fatal("timed out waiting for Run to return")
	}
}

// errorSink collects onError calls.
type errorSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errorSink) record(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
}

func (s *errorSink) all() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.errs)
}

// pendingOf copies the committer's pending map. Only call it when no Run is
// racing with the caller's expectations (Run stopped, or Run never started).
func pendingOf(c *committer) map[int]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.pending)
}

// waitCommitted blocks until Committed(partition) reports want.
func waitCommitted(t *testing.T, c *committer, partition int, want int64) {
	t.Helper()
	deadline := time.Now().Add(committerGuard)
	for time.Now().Before(deadline) {
		if off, ok := c.Committed(partition); ok && off == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	off, ok := c.Committed(partition)
	t.Fatalf("Committed(%d) = (%d, %v), want (%d, true)", partition, off, ok, want)
}

func TestCommitterCoalescesSamePartition(t *testing.T) {
	g := newGate()
	f := newFakeCommit(g.fn)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 1)
	if n := f.awaitCall(t); n != 1 {
		t.Fatalf("first call number = %d, want 1", n)
	}
	// Call 1 is blocked, so these pile up and must collapse into one commit.
	for _, off := range []int64{2, 5, 3, 4} {
		c.Advance(0, off)
	}
	g.release <- nil
	if n := f.awaitCall(t); n != 2 {
		t.Fatalf("second call number = %d, want 2", n)
	}
	g.release <- nil
	cancel()
	waitDone(t, done)

	assertCalls(t, f, "t/0@1", "t/0@5")
	if off, ok := c.Committed(0); !ok || off != 5 {
		t.Fatalf("Committed(0) = (%d, %v), want (5, true)", off, ok)
	}
}

func TestCommitterMultiPartitionAscending(t *testing.T) {
	g := newGate()
	f := newFakeCommit(g.fn)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(2, 1)
	f.awaitCall(t)
	// Inserted out of partition order on purpose.
	c.Advance(3, 30)
	c.Advance(1, 10)
	c.Advance(2, 20)
	c.Advance(0, 2)
	g.release <- nil
	f.awaitCall(t)
	g.release <- nil
	cancel()
	waitDone(t, done)

	assertCalls(t, f, "t/2@1", "t/0@2,t/1@10,t/2@20,t/3@30")
	for p, want := range map[int]int64{0: 2, 1: 10, 2: 20, 3: 30} {
		if off, ok := c.Committed(p); !ok || off != want {
			t.Fatalf("Committed(%d) = (%d, %v), want (%d, true)", p, off, ok, want)
		}
	}
}

func TestCommitterAdvanceNeverBlocks(t *testing.T) {
	f := newFakeCommit(nil)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)

	// Run is not started, so nothing drains wake; Advance must still return.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for off := range int64(10000) {
			c.Advance(int(off%3), off)
		}
	}()
	waitDone(t, done)

	got := pendingOf(c)
	want := map[int]int64{0: 9999, 1: 9997, 2: 9998}
	if !maps.Equal(got, want) {
		t.Fatalf("pending = %v, want %v", got, want)
	}
	if calls := f.snapshot(); len(calls) != 0 {
		t.Fatalf("commit called without Run: %q", calls)
	}
}

func TestCommitterMonotonicOutOfOrder(t *testing.T) {
	g := newGate()
	f := newFakeCommit(g.fn)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 12)
	f.awaitCall(t)
	// 12 is now in flight, so 10 and 11 must not be queued behind it.
	c.Advance(0, 10)
	c.Advance(0, 11)
	g.release <- nil
	waitCommitted(t, c, 0, 12)
	// Below the committed position.
	c.Advance(0, 10)
	c.Advance(0, 12)
	if got := pendingOf(c); len(got) != 0 {
		t.Fatalf("pending = %v, want empty", got)
	}
	c.Advance(0, 13)
	f.awaitCall(t)
	g.release <- nil
	waitCommitted(t, c, 0, 13)
	cancel()
	waitDone(t, done)

	assertCalls(t, f, "t/0@12", "t/0@13")
}

func TestCommitterDropsLowerThanPending(t *testing.T) {
	c := newCommitter(committerTestTopic, newFakeCommit(nil).commit, nil, time.Millisecond, time.Second)

	// Run is not started, so 12 stays pending and 10 must be dropped.
	c.Advance(0, 12)
	c.Advance(0, 10)
	c.Advance(0, 12)
	if got, want := pendingOf(c), map[int]int64{0: 12}; !maps.Equal(got, want) {
		t.Fatalf("pending = %v, want %v", got, want)
	}
}

func TestCommitterDropsAdvanceAtOrBelowCommitted(t *testing.T) {
	f := newFakeCommit(nil)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(1, 5)
	waitCommitted(t, c, 1, 5)
	c.Advance(1, 5)
	c.Advance(1, 3)
	c.Advance(1, 0)
	if got := pendingOf(c); len(got) != 0 {
		t.Fatalf("pending = %v, want empty", got)
	}
	cancel()
	waitDone(t, done)

	assertCalls(t, f, "t/1@5")
}

func TestCommitterCommitted(t *testing.T) {
	f := newFakeCommit(nil)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	if off, ok := c.Committed(0); ok || off != 0 {
		t.Fatalf("Committed(0) before any commit = (%d, %v), want (0, false)", off, ok)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(4, 0)
	waitCommitted(t, c, 4, 0)
	if off, ok := c.Committed(5); ok || off != 0 {
		t.Fatalf("Committed(5) = (%d, %v), want (0, false)", off, ok)
	}
	cancel()
	waitDone(t, done)
}

func TestCommitterDefaults(t *testing.T) {
	c := newCommitter(committerTestTopic, newFakeCommit(nil).commit, nil, 0, -time.Second)
	if c.retryEvery != time.Second {
		t.Fatalf("retryEvery = %v, want 1s", c.retryEvery)
	}
	if c.flushTimeout != 5*time.Second {
		t.Fatalf("flushTimeout = %v, want 5s", c.flushTimeout)
	}
}

func TestCommitterRetriesUntilSuccess(t *testing.T) {
	const failures = 3
	f := newFakeCommit(func(ctx context.Context, n int) error {
		if n <= failures {
			return errCommitBoom
		}
		return nil
	})
	var sink errorSink
	c := newCommitter(committerTestTopic, f.commit, sink.record, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 5)
	waitCommitted(t, c, 0, 5)
	cancel()
	waitDone(t, done)

	assertCalls(t, f, "t/0@5", "t/0@5", "t/0@5", "t/0@5")
	errs := sink.all()
	if len(errs) != failures {
		t.Fatalf("onError called %d times, want %d", len(errs), failures)
	}
	for _, err := range errs {
		if !errors.Is(err, errCommitBoom) {
			t.Fatalf("onError got %v, want %v", err, errCommitBoom)
		}
	}
}

func TestCommitterRetryKeepsNewerOffsets(t *testing.T) {
	g := newGate()
	f := newFakeCommit(g.fn)
	var sink errorSink
	c := newCommitter(committerTestTopic, f.commit, sink.record, time.Millisecond, time.Second)
	// Both are pending before Run starts, so the first call carries both.
	c.Advance(2, 7)
	c.Advance(0, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	f.awaitCall(t)
	// While call 1 is in flight: 9 and 2 are new, 7 and 4 are not above what is
	// in flight and must be dropped.
	c.Advance(0, 9)
	c.Advance(1, 2)
	c.Advance(2, 7)
	c.Advance(0, 4)
	g.release <- errCommitBoom
	f.awaitCall(t)
	g.release <- nil
	waitCommitted(t, c, 0, 9)
	cancel()
	waitDone(t, done)

	// The retry carries the newer 9 for partition 0, not the failed 5, and the
	// restored 7 for partition 2.
	assertCalls(t, f, "t/0@5,t/2@7", "t/0@9,t/1@2,t/2@7")
	if errs := sink.all(); len(errs) != 1 || !errors.Is(errs[0], errCommitBoom) {
		t.Fatalf("onError calls = %v, want exactly one %v", errs, errCommitBoom)
	}
	for p, want := range map[int]int64{0: 9, 1: 2, 2: 7} {
		if off, ok := c.Committed(p); !ok || off != want {
			t.Fatalf("Committed(%d) = (%d, %v), want (%d, true)", p, off, ok, want)
		}
	}
}

func TestCommitterNilOnErrorIsSafe(t *testing.T) {
	f := newFakeCommit(func(ctx context.Context, n int) error {
		if n == 1 {
			return errCommitBoom
		}
		return nil
	})
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 1)
	waitCommitted(t, c, 0, 1)
	cancel()
	waitDone(t, done)
}

func TestCommitterClosedReaderStopsRun(t *testing.T) {
	f := newFakeCommit(func(ctx context.Context, n int) error {
		if n == 1 {
			return errCommitBoom
		}
		return fmt.Errorf("commit offsets: %w", io.ErrClosedPipe)
	})
	var sink errorSink
	c := newCommitter(committerTestTopic, f.commit, sink.record, time.Millisecond, time.Second)
	// The context is never cancelled: Run must return because of the error.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 5)
	waitDone(t, done)

	assertCalls(t, f, "t/0@5", "t/0@5")
	errs := sink.all()
	if len(errs) != 2 || !errors.Is(errs[0], errCommitBoom) || !errors.Is(errs[1], io.ErrClosedPipe) {
		t.Fatalf("onError calls = %v, want [%v, ErrClosedPipe]", errs, errCommitBoom)
	}
	if off, ok := c.Committed(0); ok {
		t.Fatalf("Committed(0) = (%d, true), want nothing committed", off)
	}
	// Run is gone; Advance must still not block.
	c.Advance(0, 6)
}

func TestCommitterFinalFlushOnCancel(t *testing.T) {
	f := newFakeCommit(nil)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	// Cancelled before Run starts: whatever is pending must still be flushed.
	c.Advance(1, 8)
	c.Advance(0, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := startCommitter(ctx, c)
	waitDone(t, done)

	assertCalls(t, f, "t/0@3,t/1@8")
	// The flush must not inherit the cancelled run context, and must be bounded.
	if info := f.info(1); info.err != nil || !info.hasDeadline {
		t.Fatalf("flush ctx = %+v, want live context with a deadline", info)
	}
	for p, want := range map[int]int64{0: 3, 1: 8} {
		if off, ok := c.Committed(p); !ok || off != want {
			t.Fatalf("Committed(%d) = (%d, %v), want (%d, true)", p, off, ok, want)
		}
	}
}

func TestCommitterFinalFlushWithNothingPending(t *testing.T) {
	f := newFakeCommit(nil)
	c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := startCommitter(ctx, c)
	cancel()
	waitDone(t, done)
	assertCalls(t, f)
}

func TestCommitterFinalFlushTimesOut(t *testing.T) {
	f := newFakeCommit(func(ctx context.Context, n int) error {
		<-ctx.Done()
		return ctx.Err()
	})
	errs := make(chan error, 8)
	c := newCommitter(committerTestTopic, f.commit, func(err error) { errs <- err }, time.Millisecond, 20*time.Millisecond)
	c.Advance(0, 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := startCommitter(ctx, c)
	waitDone(t, done)

	assertCalls(t, f, "t/0@4")
	select {
	case err := <-errs:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("onError got %v, want DeadlineExceeded", err)
		}
	default:
		t.Fatal("onError was not called for the timed-out flush")
	}
	if off, ok := c.Committed(0); ok {
		t.Fatalf("Committed(0) = (%d, true), want nothing committed", off)
	}
}

func TestCommitterFinalFlushFailureIsNotRetried(t *testing.T) {
	f := newFakeCommit(func(ctx context.Context, n int) error { return errCommitBoom })
	var sink errorSink
	c := newCommitter(committerTestTopic, f.commit, sink.record, time.Millisecond, time.Second)
	c.Advance(0, 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := startCommitter(ctx, c)
	waitDone(t, done)

	assertCalls(t, f, "t/0@4")
	if errs := sink.all(); len(errs) != 1 || !errors.Is(errs[0], errCommitBoom) {
		t.Fatalf("onError calls = %v, want exactly one %v", errs, errCommitBoom)
	}
}

func TestCommitterCancelDuringRetryWaitFlushes(t *testing.T) {
	f := newFakeCommit(func(ctx context.Context, n int) error {
		if n == 1 {
			return errCommitBoom
		}
		return nil
	})
	errs := make(chan error, 8)
	// An hour-long retry wait: Run only returns promptly if cancel interrupts it.
	c := newCommitter(committerTestTopic, f.commit, func(err error) { errs <- err }, time.Hour, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 6)
	select {
	case <-errs:
	case <-time.After(committerGuard):
		t.Fatal("timed out waiting for the first commit failure")
	}
	c.Advance(0, 9)
	cancel()
	waitDone(t, done)

	assertCalls(t, f, "t/0@6", "t/0@9")
	if off, ok := c.Committed(0); !ok || off != 9 {
		t.Fatalf("Committed(0) = (%d, %v), want (9, true)", off, ok)
	}
}

func TestCommitterCancelDuringCommitFlushesPending(t *testing.T) {
	f := newFakeCommit(func(ctx context.Context, n int) error {
		if n == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	c := newCommitter(committerTestTopic, f.commit, nil, time.Hour, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	c.Advance(0, 6)
	f.awaitCall(t)
	cancel()
	waitDone(t, done)

	// The interrupted commit is put back and goes out in the final flush.
	assertCalls(t, f, "t/0@6", "t/0@6")
	if info := f.info(2); info.err != nil || !info.hasDeadline {
		t.Fatalf("flush ctx = %+v, want live context with a deadline", info)
	}
	if off, ok := c.Committed(0); !ok || off != 6 {
		t.Fatalf("Committed(0) = (%d, %v), want (6, true)", off, ok)
	}
}

func TestCommitterConcurrentStress(t *testing.T) {
	const (
		partitions = 8
		perPart    = 2000
		workers    = 16
	)
	var (
		mu       sync.Mutex
		seqs     = make(map[int][]int64)
		inFlight atomic.Int32
		overlap  atomic.Bool
	)
	commit := func(ctx context.Context, msgs ...kafkago.Message) error {
		if inFlight.Add(1) != 1 {
			overlap.Store(true)
		}
		defer inFlight.Add(-1)
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			seqs[m.Partition] = append(seqs[m.Partition], m.Offset)
		}
		return nil
	}
	c := newCommitter(committerTestTopic, commit, nil, time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startCommitter(ctx, c)

	// next hands out each partition's offsets in increasing order, but workers
	// call Advance after taking one, so arrival order is scrambled.
	var next [partitions]atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for range partitions * perPart / workers {
				p := rng.IntN(partitions)
				off := next[p].Add(1) - 1
				if off >= perPart {
					continue
				}
				c.Advance(p, off)
			}
		})
	}
	wg.Wait()
	// Top every partition up so each one's maximum is certain regardless of how
	// the random draws fell.
	for p := range partitions {
		c.Advance(p, perPart-1)
	}
	for p := range partitions {
		waitCommitted(t, c, p, perPart-1)
	}
	cancel()
	waitDone(t, done)

	if overlap.Load() {
		t.Fatal("two commits were in flight at the same time")
	}
	mu.Lock()
	defer mu.Unlock()
	for p := range partitions {
		seq := seqs[p]
		if len(seq) == 0 || seq[len(seq)-1] != perPart-1 {
			t.Fatalf("partition %d commits = %v, want last %d", p, seq, perPart-1)
		}
		for i := 1; i < len(seq); i++ {
			if seq[i] <= seq[i-1] {
				t.Fatalf("partition %d commits not strictly increasing at %d: %v", p, i, seq[i-1:i+1])
			}
		}
	}
}

func TestCommitterNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for range 20 {
		f := newFakeCommit(nil)
		c := newCommitter(committerTestTopic, f.commit, nil, time.Millisecond, time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		done := startCommitter(ctx, c)
		c.Advance(0, 1)
		waitCommitted(t, c, 0, 1)
		cancel()
		waitDone(t, done)
	}
	// Runs that end through a closed reader or a retry wait must also exit.
	for range 20 {
		f := newFakeCommit(func(ctx context.Context, n int) error { return io.ErrClosedPipe })
		c := newCommitter(committerTestTopic, f.commit, nil, time.Hour, time.Second)
		done := startCommitter(context.Background(), c)
		c.Advance(0, 1)
		waitDone(t, done)
	}
	for range 20 {
		f := newFakeCommit(func(ctx context.Context, n int) error { return errCommitBoom })
		errs := make(chan error, 4)
		c := newCommitter(committerTestTopic, f.commit, func(err error) { errs <- err }, time.Hour, time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		done := startCommitter(ctx, c)
		c.Advance(0, 1)
		<-errs
		cancel()
		waitDone(t, done)
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines: before=%d after=%d, want no leak", before, after)
	}
}
