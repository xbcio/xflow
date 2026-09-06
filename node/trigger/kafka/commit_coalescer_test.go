package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// fakeCommitter stands in for Reader.CommitMessages. It records what each
// merged call carried and can be made to block, which is how these tests create
// the "an RPC is in flight while other callers arrive" state that merging
// exists for.
//
// It observes ctx, unlike the pausingCommitter in offset_commit_metric_test.go,
// which sleeps and ignores it. That difference is the point of one of these
// tests: a coalescer that leaves its merged call on a context Close cannot
// cancel would hang, and a fake that never looks at ctx cannot tell.
type fakeCommitter struct {
	mu       sync.Mutex
	sizes    []int // messages carried by each merged call, in order
	calls    atomic.Int64
	ctxSeen  atomic.Int64 // calls that observed their ctx cancelled
	block    chan struct{}
	blockOne atomic.Bool // block only the first call
	err      error
}

func (f *fakeCommitter) commit(ctx context.Context, msgs ...kafkago.Message) error {
	n := f.calls.Add(1)
	f.mu.Lock()
	f.sizes = append(f.sizes, len(msgs))
	f.mu.Unlock()

	if f.block != nil && (!f.blockOne.Load() || n == 1) {
		select {
		case <-f.block:
		case <-ctx.Done():
			f.ctxSeen.Add(1)
			return ctx.Err()
		}
	}
	return f.err
}

func (f *fakeCommitter) callSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.sizes...)
}

func maxOf(xs []int) int {
	best := 0
	for _, x := range xs {
		if x > best {
			best = x
		}
	}
	return best
}

func msg(partition int, offset int64) kafkago.Message {
	return kafkago.Message{Topic: "t", Partition: partition, Offset: offset}
}

// TestCommitCoalescerMergesConcurrentCallers is the load-bearing test: callers
// that would have cost one RPC each must share.
//
// The assertions are deliberately robust rather than exact. Making them exact
// would require knowing that every caller has PARKED on the request channel
// before the in-flight call is released, and there is no observation point for
// that — only the instant before the send is visible, not the send itself. So
// the test asserts the properties that distinguish merging from not merging at
// all (fewer RPCs than callers, at least one RPC carrying several, nobody left
// without a result) and leaves the precise grouping to the scheduler.
func TestCommitCoalescerMergesConcurrentCallers(t *testing.T) {
	const callers = 8

	f := &fakeCommitter{block: make(chan struct{})}
	f.blockOne.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCommitCoalescer(ctx, f.commit)
	defer stopCoalescer(t, cancel, c)

	// First caller occupies the loop, so every later caller has to queue.
	inFlight := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(inFlight)
		if err := c.commitMessages(context.Background(), msg(0, 100)); err != nil {
			t.Errorf("first caller: %v", err)
		}
	}()
	<-inFlight
	waitFor(t, func() bool { return f.calls.Load() >= 1 }, "first commit to reach the committer")

	results := make([]error, callers-1)
	for i := 0; i < callers-1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = c.commitMessages(context.Background(), msg(i+1, int64(200+i)))
		}(i)
	}
	// No observation point for "all senders parked"; this gives them room. The
	// assertions below do not depend on all of them making it.
	time.Sleep(100 * time.Millisecond)
	close(f.block)
	awaitCallers(t, &wg, "merging arm")

	for i, err := range results {
		if err != nil {
			t.Errorf("caller %d: %v", i+1, err)
		}
	}

	sizes := f.callSizes()
	if len(sizes) >= callers {
		t.Errorf("no merging happened: %d callers produced %d commit calls %v; "+
			"each caller paid its own round trip, which is the behaviour this type replaces",
			callers, len(sizes), sizes)
	}
	if got := maxOf(sizes); got < 2 {
		t.Errorf("largest merged call carried %d message(s), want >= 2 (calls: %v). "+
			"A coalescer that never groups is a serial committer with extra hops", got, sizes)
	}
}

// TestCommitCoalescerIdleCallerIsNotDelayed pins the no-linger property. A
// batching design that waits for company would tax exactly this case, which is
// the common one when a single partition flushes alone.
func TestCommitCoalescerIdleCallerIsNotDelayed(t *testing.T) {
	f := &fakeCommitter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCommitCoalescer(ctx, f.commit)
	defer stopCoalescer(t, cancel, c)

	start := time.Now()
	if err := c.commitMessages(context.Background(), msg(3, 42)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	elapsed := time.Since(start)

	if sizes := f.callSizes(); len(sizes) != 1 || sizes[0] != 1 {
		t.Errorf("one caller with one message produced calls %v, want exactly [1]", sizes)
	}
	// Loose on purpose. The failure being excluded is a linger window, which
	// would be sized like a flush interval (hundreds of ms and up); a bound
	// tight enough to catch scheduler noise would flake on a loaded machine and
	// tell us nothing extra.
	if elapsed > 200*time.Millisecond {
		t.Errorf("an uncontended commit took %s: the loop appears to wait for company "+
			"before sending, which taxes the single-flusher case", elapsed)
	}
}

// TestCommitCoalescerFansErrorToEveryWaiter checks that merging does not lose
// an outcome. One RPC really does succeed or fail for all its partitions, so
// every caller in the batch must see the same error — a batch member that got
// nil would advance its offsets past work that was never committed.
func TestCommitCoalescerFansErrorToEveryWaiter(t *testing.T) {
	sentinel := errors.New("broker said no")
	f := &fakeCommitter{block: make(chan struct{}), err: sentinel}
	f.blockOne.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCommitCoalescer(ctx, f.commit)
	defer stopCoalescer(t, cancel, c)

	const callers = 6
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.commitMessages(context.Background(), msg(i, int64(i)))
		}(i)
	}
	waitFor(t, func() bool { return f.calls.Load() >= 1 }, "first commit to reach the committer")
	time.Sleep(50 * time.Millisecond)
	close(f.block)
	awaitCallers(t, &wg, "error fan-out arm")

	for i, err := range errs {
		if !errors.Is(err, sentinel) {
			t.Errorf("caller %d got %v, want the committer's error. A waiter that receives nil "+
				"from a failed batch would advance offsets past uncommitted work", i, err)
		}
	}
}

// TestCommitCoalescerToleratesAbandonedCaller covers the condition that forced
// the result channel to be buffered.
//
// A caller CAN walk away mid-commit — kafka-go's own CommitMessages has the
// same shape, enqueueing onto Reader.commits before it selects on ctx.Done, so
// this is a pre-existing condition rather than one merging introduces. What
// merging changes is the blast radius: an unbuffered result channel would wedge
// the loop on the departed caller and stall every other partition behind it.
func TestCommitCoalescerToleratesAbandonedCaller(t *testing.T) {
	f := &fakeCommitter{block: make(chan struct{})}
	f.blockOne.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCommitCoalescer(ctx, f.commit)
	defer stopCoalescer(t, cancel, c)

	abandonCtx, abandon := context.WithCancel(context.Background())
	abandoned := make(chan error, 1)
	go func() { abandoned <- c.commitMessages(abandonCtx, msg(0, 1)) }()
	waitFor(t, func() bool { return f.calls.Load() >= 1 }, "the abandoned caller's commit to start")

	abandon()
	select {
	case err := <-abandoned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoning caller got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abandoning caller did not return: its own ctx should release it " +
			"regardless of what the merged call is doing")
	}

	// The merged call is still in flight and now has no listener. Releasing it
	// must not wedge the loop.
	close(f.block)

	done := make(chan error, 1)
	go func() { done <- c.commitMessages(context.Background(), msg(1, 2)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("caller after an abandoned one: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the loop is stuck delivering to a caller that left; the result channel " +
			"must be buffered so an abandoned request cannot stall the others")
	}
}

// TestCommitCoalescerStopsWithoutTraffic is the leak test, and it covers the
// case that actually happens: Close almost always finds the queue empty, so the
// loop is parked on its outer select with nothing to wake it. Without ctx in
// that select the goroutine outlives every activation.
func TestCommitCoalescerStopsWithoutTraffic(t *testing.T) {
	f := &fakeCommitter{}
	ctx, cancel := context.WithCancel(context.Background())
	c := newCommitCoalescer(ctx, f.commit)

	cancel()
	stopped := make(chan struct{})
	go func() { c.wait(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("the loop did not exit after its context was cancelled: it leaks one " +
			"goroutine per trigger activation, and leaks in the quiet case")
	}

	// A caller arriving after the stop must be told so, not parked forever on a
	// channel nobody is reading.
	errCh := make(chan error, 1)
	go func() { errCh <- c.commitMessages(context.Background(), msg(0, 1)) }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Error("a commit submitted after the coalescer stopped reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a commit submitted after the coalescer stopped never returned")
	}
	if n := f.calls.Load(); n != 0 {
		t.Errorf("committer was called %d time(s) after the coalescer stopped, want 0", n)
	}
}

// waitFor polls until cond holds, so a test never depends on a fixed sleep for
// a condition that has an observable.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// stopCoalescer cancels the loop and waits for it, but only for a bounded time.
//
// The bound is not defensive padding; it was earned. A plain c.wait() in a
// deferred cleanup turns any wedged loop into a HANG rather than a failure, and
// it does so precisely when the test has already detected the problem: the
// assertion calls t.Fatal, t.Fatal runs the deferred cleanup, and the cleanup
// blocks on the same wedged loop the assertion just caught. A mutation that
// unbuffers the result channel reproduces this exactly — the test's own
// diagnosis becomes invisible behind a ten-minute timeout with no message.
func stopCoalescer(t *testing.T, cancel context.CancelFunc, c *commitCoalescer) {
	t.Helper()
	cancel()
	stopped := make(chan struct{})
	go func() { c.wait(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Errorf("coalescer loop did not exit within 3s of its context being cancelled; " +
			"it is wedged, most likely blocked delivering a result to a caller that left")
	}
}

// awaitCallers is wg.Wait with a deadline, for the same reason stopCoalescer is
// bounded, and it was earned by the same kind of mutation.
//
// Every test here has callers parked on their result channel. A defect that
// leaves one of them unanswered — fanning the outcome to only the first waiter,
// say — makes a bare wg.Wait block forever, and the test that would have named
// the defect instead reports nothing at all until the package timeout kills it.
// A stuck caller is itself the finding, so it has to be reportable.
func awaitCallers(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	returned := make(chan struct{})
	go func() { wg.Wait(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: at least one caller never returned from commitMessages. "+
			"Every request that reaches the loop must receive exactly one outcome", what)
	}
}
