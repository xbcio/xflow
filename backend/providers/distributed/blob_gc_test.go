package distributed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/blob"
)

// gcTestDeadline bounds every wait on the loop, so a broken loop fails a test
// instead of hanging the package.
const gcTestDeadline = 5 * time.Second

// gcHarness runs the real startBlobGC loop against a fake clock. The loop's
// Sleep announces on idle that it is waiting for its next tick and then blocks
// until the test fires it, so the test decides exactly when a tick happens and
// no real time passes.
type gcHarness struct {
	t       *testing.T
	idle    chan time.Duration
	release chan struct{}
	quit    chan struct{} // closed at test end so an abandoned loop still exits
	exited  chan struct{} // closed by the loop goroutine as its last act
	cancel  context.CancelFunc
	stop    func()
}

func newGCHarness(t *testing.T, run func(context.Context) (blob.CollectResult, error), opts blobGCOptions) *gcHarness {
	t.Helper()
	h := &gcHarness{
		t:       t,
		idle:    make(chan time.Duration),
		release: make(chan struct{}),
		quit:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
	opts.Sleep = h.sleep
	opts.onExit = func() { close(h.exited) }
	var ctx context.Context
	ctx, h.cancel = context.WithCancel(context.Background())
	h.stop = startBlobGC(ctx, run, opts)
	t.Cleanup(func() {
		close(h.quit)
		h.cancel()
		returned := make(chan struct{})
		go func() {
			h.stop()
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(gcTestDeadline):
			t.Error("blob gc loop did not exit at test cleanup")
		}
	})
	return h
}

// gcStopWithin calls stop and fails the test if it has not returned by the
// deadline, so a loop that never exits cannot hang the package from a test body.
func gcStopWithin(t *testing.T, stop func()) {
	t.Helper()
	returned := make(chan struct{})
	go func() {
		stop()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(gcTestDeadline):
		t.Fatal("stop did not return")
	}
}

// sleep is the loop's injected clock. It honours ctx like a real timer, but it
// only returns nil when the test fires a tick.
func (h *gcHarness) sleep(ctx context.Context, d time.Duration) error {
	select {
	case h.idle <- d:
	case <-ctx.Done():
		return ctx.Err()
	case <-h.quit:
		return context.Canceled
	}
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-h.quit:
		return context.Canceled
	}
}

// awaitIdle blocks until the loop is waiting for its next tick and returns how
// long it asked to wait.
func (h *gcHarness) awaitIdle() time.Duration {
	h.t.Helper()
	select {
	case d := <-h.idle:
		return d
	case <-h.exited:
		h.t.Fatal("blob gc loop exited; expected it to be waiting for the next tick")
	case <-time.After(gcTestDeadline):
		h.t.Fatal("blob gc loop did not get back to waiting for its next tick")
	}
	return 0
}

// fire releases the loop's pending wait, starting one tick.
func (h *gcHarness) fire() {
	h.t.Helper()
	select {
	case h.release <- struct{}{}:
	case <-h.exited:
		h.t.Fatal("blob gc loop exited; expected it to be waiting for a tick")
	case <-time.After(gcTestDeadline):
		h.t.Fatal("blob gc loop was not waiting for a tick")
	}
}

// tick runs one whole tick: it returns once the loop has finished the tick and
// is waiting again, so everything the tick did is visible to the test.
func (h *gcHarness) tick() {
	h.t.Helper()
	h.fire()
	h.awaitIdle()
}

// tickAndExpectExit runs one tick and requires the loop to end instead of
// waiting for another.
func (h *gcHarness) tickAndExpectExit() {
	h.t.Helper()
	h.fire()
	select {
	case <-h.exited:
	case <-h.idle:
		h.t.Fatal("blob gc loop went back to waiting; expected it to exit")
	case <-time.After(gcTestDeadline):
		h.t.Fatal("blob gc loop did not exit")
	}
}

func (h *gcHarness) hasExited() bool {
	select {
	case <-h.exited:
		return true
	default:
		return false
	}
}

// gcFakeRun is a scripted Collector.Run. fn answers call number n (1-based).
type gcFakeRun struct {
	calls atomic.Int32
	fn    func(ctx context.Context, n int) (blob.CollectResult, error)
}

func (r *gcFakeRun) run(ctx context.Context) (blob.CollectResult, error) {
	return r.fn(ctx, int(r.calls.Add(1)))
}

func (r *gcFakeRun) count() int { return int(r.calls.Load()) }

// gcAnswers returns a fake that answers from steps in order and then reports an
// empty pass, so a loop that wrongly keeps going stops getting full batches.
func gcAnswers(steps ...gcStep) *gcFakeRun {
	return &gcFakeRun{fn: func(_ context.Context, n int) (blob.CollectResult, error) {
		if n <= len(steps) {
			return steps[n-1].res, steps[n-1].err
		}
		return blob.CollectResult{}, nil
	}}
}

type gcStep struct {
	res blob.CollectResult
	err error
}

// gcFull is a pass that examined exactly batch candidates, so more may remain.
func gcFull(batch int) gcStep {
	return gcStep{res: blob.CollectResult{Candidates: batch, Deleted: batch}}
}

// gcObs records every OnGC call.
type gcObs struct {
	mu    sync.Mutex
	calls [][2]int
}

func (o *gcObs) OnGC(deleted, fenced int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, [2]int{deleted, fenced})
}

func (o *gcObs) got() [][2]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([][2]int(nil), o.calls...)
}

// gcRecLogger is an engine.Logger that keeps the Error and Info lines.
type gcRecLogger struct {
	mu     sync.Mutex
	errors []string
	infos  []string
}

func (l *gcRecLogger) add(dst *[]string, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*dst = append(*dst, strings.TrimSpace(msg+" "+fmt.Sprint(args...)))
}

func (l *gcRecLogger) Error(msg string, args ...any) { l.add(&l.errors, msg, args) }
func (l *gcRecLogger) Errorf(f string, a ...any)     { l.add(&l.errors, fmt.Sprintf(f, a...), nil) }
func (l *gcRecLogger) Info(msg string, args ...any)  { l.add(&l.infos, msg, args) }
func (l *gcRecLogger) Infof(f string, a ...any)      { l.add(&l.infos, fmt.Sprintf(f, a...), nil) }
func (l *gcRecLogger) Debug(string, ...any)          {}
func (l *gcRecLogger) Debugf(string, ...any)         {}
func (l *gcRecLogger) Warn(string, ...any)           {}
func (l *gcRecLogger) Warnf(string, ...any)          {}
func (l *gcRecLogger) Panic(msg string, _ ...any)    { panic(msg) }
func (l *gcRecLogger) Panicf(f string, a ...any)     { panic(fmt.Sprintf(f, a...)) }

func (l *gcRecLogger) errorLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.errors...)
}

func (l *gcRecLogger) infoLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.infos...)
}

// gcLoopGoroutines counts the live goroutines running the blob gc loop, by
// their stack, so unrelated goroutines from other tests do not matter.
func gcLoopGoroutines() int {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		return -1
	}
	return strings.Count(buf.String(), "distributed.gcLoop(")
}

// requireNoGCLoop fails unless every loop goroutine is gone shortly.
func requireNoGCLoop(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(gcTestDeadline)
	for gcLoopGoroutines() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("blob gc loop goroutines still alive: %d", gcLoopGoroutines())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGCLoopWaitsForFirstTick(t *testing.T) {
	run := gcAnswers()
	h := newGCHarness(t, run.run, blobGCOptions{})
	if d := h.awaitIdle(); d != 5*time.Minute {
		t.Fatalf("first wait = %v, want the default tick of 5m", d)
	}
	if got := run.count(); got != 0 {
		t.Fatalf("run called %d times before the first tick, want 0", got)
	}
	h.tick()
	if got := run.count(); got != 1 {
		t.Fatalf("run called %d times after one tick with an empty pass, want 1", got)
	}
}

func TestGCLoopChainsOnFullBatch(t *testing.T) {
	// Default Batch is 200: a pass with exactly 200 candidates chains, one with 199
	// does not.
	run := gcAnswers(gcFull(200), gcFull(200), gcStep{res: blob.CollectResult{Candidates: 199}}, gcFull(200))
	h := newGCHarness(t, run.run, blobGCOptions{})
	h.awaitIdle()
	h.tick()
	if got := run.count(); got != 3 {
		t.Fatalf("after tick 1 run called %d times, want 3 (full, full, short)", got)
	}
	h.tick()
	if got := run.count(); got != 5 {
		t.Fatalf("after tick 2 run called %d times, want 5 (full, then empty)", got)
	}
}

func TestGCLoopHonoursConfiguredBatch(t *testing.T) {
	run := gcAnswers(gcFull(3), gcStep{res: blob.CollectResult{Candidates: 2}})
	h := newGCHarness(t, run.run, blobGCOptions{Batch: 3, Tick: time.Second})
	if d := h.awaitIdle(); d != time.Second {
		t.Fatalf("wait = %v, want the configured tick of 1s", d)
	}
	h.tick()
	if got := run.count(); got != 2 {
		t.Fatalf("run called %d times, want 2 (3 candidates chain at Batch 3, 2 do not)", got)
	}
}

func TestGCLoopCapsBackToBackRuns(t *testing.T) {
	// Every pass reports a full batch until call 26. A loop without a cap would
	// chain all the way there; the cap must hold one tick to 10 passes.
	run := &gcFakeRun{fn: func(_ context.Context, n int) (blob.CollectResult, error) {
		if n > 25 {
			return blob.CollectResult{}, nil
		}
		return blob.CollectResult{Candidates: 200, Deleted: 200}, nil
	}}
	h := newGCHarness(t, run.run, blobGCOptions{})
	h.awaitIdle()
	h.tick()
	if got := run.count(); got != 10 {
		t.Fatalf("after tick 1 run called %d times, want the cap of 10", got)
	}
	h.tick()
	if got := run.count(); got != 20 {
		t.Fatalf("after tick 2 run called %d times, want 20 (a fresh cap per tick)", got)
	}
}

func TestGCLoopHonoursConfiguredCap(t *testing.T) {
	run := &gcFakeRun{fn: func(_ context.Context, n int) (blob.CollectResult, error) {
		if n > 9 {
			return blob.CollectResult{}, nil
		}
		return blob.CollectResult{Candidates: 200}, nil
	}}
	h := newGCHarness(t, run.run, blobGCOptions{MaxRunsPerTick: 3})
	h.awaitIdle()
	h.tick()
	if got := run.count(); got != 3 {
		t.Fatalf("run called %d times, want the configured cap of 3", got)
	}
}

func TestGCLoopSurvivesRunError(t *testing.T) {
	boom := errors.New("index unavailable")
	run := gcAnswers(gcStep{err: boom}, gcStep{}, gcStep{err: boom})
	logger := &gcRecLogger{}
	h := newGCHarness(t, run.run, blobGCOptions{Logger: logger})
	h.awaitIdle()
	h.tick() // fails
	h.tick() // recovers: the loop is still alive and runs again
	h.tick() // fails again
	if got := run.count(); got != 3 {
		t.Fatalf("run called %d times over 3 ticks, want 3", got)
	}
	if h.hasExited() {
		t.Fatal("loop exited after a run error")
	}
	if got := len(logger.errorLines()); got != 2 {
		t.Fatalf("logged %d errors %q, want 2", got, logger.errorLines())
	}
}

func TestGCLoopDoesNotChainAfterError(t *testing.T) {
	// A pass that fails after examining a full batch must not be followed at once
	// by another: the failed objects would just be listed and fail again.
	boom := errors.New("index unavailable")
	run := gcAnswers(gcStep{res: blob.CollectResult{Candidates: 200}, err: boom})
	h := newGCHarness(t, run.run, blobGCOptions{Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tick()
	if got := run.count(); got != 1 {
		t.Fatalf("run called %d times, want 1 (no chaining after an error)", got)
	}
}

func TestGCLoopExitsOnceOnDeleteUnsupported(t *testing.T) {
	// Collector.Run wraps the sentinel and joins it with the delete failures it
	// already saw; the loop must still recognise it.
	wrapped := errors.Join(
		fmt.Errorf("blob: delete ns/abc: %w", errors.New("disk full")),
		fmt.Errorf("blob: delete ns/def: %w", blob.ErrDeleteUnsupported),
	)
	run := gcAnswers(gcStep{res: blob.CollectResult{Candidates: 5}, err: wrapped})
	logger := &gcRecLogger{}
	h := newGCHarness(t, run.run, blobGCOptions{Logger: logger})
	h.awaitIdle()
	h.tickAndExpectExit()
	gcStopWithin(t, h.stop) // already over; must still return
	if got := run.count(); got != 1 {
		t.Fatalf("run called %d times, want 1", got)
	}
	if got := len(logger.errorLines()); got != 1 {
		t.Fatalf("logged %d error lines %q, want exactly 1", got, logger.errorLines())
	}
	requireNoGCLoop(t)
}

func TestGCLoopDeleteUnsupportedNeverRetries(t *testing.T) {
	run := gcAnswers(gcStep{err: blob.ErrDeleteUnsupported})
	h := newGCHarness(t, run.run, blobGCOptions{Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tickAndExpectExit()
	// With the loop gone nothing can answer a tick, so firing one must not run.
	select {
	case h.release <- struct{}{}:
		t.Fatal("an exited loop accepted a tick")
	case <-time.After(50 * time.Millisecond):
	}
	if got := run.count(); got != 1 {
		t.Fatalf("run called %d times, want 1", got)
	}
}

func TestGCLoopStopsOnCancel(t *testing.T) {
	// A pass is in flight and blocked until its ctx ends, like Collector.Run
	// parked in a slow Purge. Cancelling the loop's parent must reach the pass and
	// end the loop; if the loop ignored its ctx the pass would never return.
	inFlight := make(chan struct{})
	run := &gcFakeRun{fn: func(ctx context.Context, _ int) (blob.CollectResult, error) {
		close(inFlight)
		select {
		case <-ctx.Done():
			return blob.CollectResult{}, ctx.Err()
		case <-t.Context().Done(): // test is over: do not wedge the package
			return blob.CollectResult{}, nil
		}
	}}
	h := newGCHarness(t, run.run, blobGCOptions{Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.fire()
	select {
	case <-inFlight:
	case <-time.After(gcTestDeadline):
		t.Fatal("the pass never started")
	}
	h.cancel()
	select {
	case <-h.exited:
	case <-time.After(gcTestDeadline):
		t.Fatal("blob gc loop ignored its cancelled context")
	}
}

func TestGCLoopStopsOnCancelWhileWaiting(t *testing.T) {
	// No injected clock: the real timer must give way to ctx. The tick is an
	// hour, so only the cancellation can end the wait inside the deadline. On
	// failure the goroutine is left behind on purpose; stop would block on it.
	exited := make(chan struct{})
	run := gcAnswers()
	ctx, cancel := context.WithCancel(context.Background())
	stop := startBlobGC(ctx, run.run, blobGCOptions{Tick: time.Hour, onExit: func() { close(exited) }})
	cancel()
	select {
	case <-exited:
	case <-time.After(gcTestDeadline):
		t.Fatal("blob gc loop kept waiting for its tick after its context was cancelled")
	}
	gcStopWithin(t, stop)
	if got := run.count(); got != 0 {
		t.Fatalf("run called %d times, want 0", got)
	}
}

func TestGCLoopStopsBetweenChainedPasses(t *testing.T) {
	// Cancellation lands while a full batch is chaining: the pass in flight
	// finishes, but the loop must not start the next one.
	var h *gcHarness
	run := &gcFakeRun{fn: func(_ context.Context, n int) (blob.CollectResult, error) {
		if n == 2 {
			h.cancel()
		}
		return blob.CollectResult{Candidates: 200}, nil
	}}
	h = newGCHarness(t, run.run, blobGCOptions{})
	h.awaitIdle()
	h.fire()
	select {
	case <-h.exited:
	case <-time.After(gcTestDeadline):
		t.Fatal("blob gc loop kept chaining after its context was cancelled")
	}
	if got := run.count(); got != 2 {
		t.Fatalf("run called %d times, want 2 (cancelled during the second)", got)
	}
}

func TestGCLoopStopWaitsForTheLoopToExit(t *testing.T) {
	// The pass ignores cancellation until the test lets it finish. stop must not
	// return while it is still running, and must return once it has.
	sawCancel := make(chan struct{})
	finish := make(chan struct{})
	started := make(chan struct{})
	run := &gcFakeRun{fn: func(ctx context.Context, _ int) (blob.CollectResult, error) {
		close(started)
		select {
		case <-ctx.Done():
			close(sawCancel)
		case <-t.Context().Done(): // test is over: do not wedge the package
			return blob.CollectResult{}, nil
		}
		select {
		case <-finish:
		case <-t.Context().Done():
		}
		return blob.CollectResult{}, nil
	}}
	h := newGCHarness(t, run.run, blobGCOptions{})
	h.awaitIdle()
	h.fire()
	select {
	case <-started:
	case <-time.After(gcTestDeadline):
		t.Fatal("the pass never started")
	}
	stopped := make(chan struct{})
	go func() {
		h.stop()
		close(stopped)
	}()
	select {
	case <-sawCancel:
	case <-time.After(gcTestDeadline):
		t.Fatal("stop did not cancel the pass's context")
	}
	// The window only has to be long enough for a stop that does not wait to be
	// seen returning; a correct stop is held up by the pass for all of it.
	select {
	case <-stopped:
		t.Fatal("stop returned while the pass was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(finish)
	select {
	case <-stopped:
	case <-time.After(gcTestDeadline):
		t.Fatal("stop did not return after the pass finished")
	}
	if !h.hasExited() {
		t.Fatal("stop returned before the loop goroutine exited")
	}
}

func TestGCLoopStopIsRepeatable(t *testing.T) {
	run := gcAnswers()
	h := newGCHarness(t, run.run, blobGCOptions{})
	h.awaitIdle()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.stop()
		}()
	}
	allStopped := make(chan struct{})
	go func() {
		wg.Wait()
		close(allStopped)
	}()
	select {
	case <-allStopped:
	case <-time.After(gcTestDeadline):
		t.Fatal("concurrent stop calls did not all return")
	}
	gcStopWithin(t, h.stop)
	gcStopWithin(t, h.stop)
	if !h.hasExited() {
		t.Fatal("loop still running after stop returned")
	}
}

func TestGCLoopReportsDeletedAndFencedToObserver(t *testing.T) {
	obs := &gcObs{}
	run := gcAnswers(
		gcStep{res: blob.CollectResult{Candidates: 200, Deleted: 150, Fenced: 2, Skipped: 40, Failed: 8}},
		gcStep{res: blob.CollectResult{Candidates: 3, Deleted: 1, Fenced: 1, Skipped: 1}},
		gcStep{res: blob.CollectResult{Deleted: 4}, err: errors.New("boom")}, // counts are reported even when the pass errors
	)
	h := newGCHarness(t, run.run, blobGCOptions{Observer: obs, Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tick() // passes 1 and 2
	h.tick() // pass 3
	want := [][2]int{{150, 2}, {1, 1}, {4, 0}}
	got := obs.got()
	if len(got) != len(want) {
		t.Fatalf("observer calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("observer calls = %v, want %v", got, want)
		}
	}
}

func TestGCLoopNilObserverIsSafe(t *testing.T) {
	run := gcAnswers(gcFull(200), gcStep{res: blob.CollectResult{Candidates: 1, Deleted: 1, Fenced: 1}})
	h := newGCHarness(t, run.run, blobGCOptions{}) // Observer left nil
	h.awaitIdle()
	h.tick()
	if got := run.count(); got != 2 {
		t.Fatalf("run called %d times, want 2", got)
	}
}

func TestGCLoopLogsSkippedAndFailedButNotQuietPasses(t *testing.T) {
	logger := &gcRecLogger{}
	run := gcAnswers(
		gcStep{res: blob.CollectResult{Candidates: 5, Deleted: 5}},
		gcStep{res: blob.CollectResult{Candidates: 5, Skipped: 3, Failed: 2}},
	)
	h := newGCHarness(t, run.run, blobGCOptions{Logger: logger})
	h.awaitIdle()
	h.tick()
	if got := logger.infoLines(); len(got) != 0 {
		t.Fatalf("a pass with nothing skipped or failed logged %q", got)
	}
	h.tick()
	got := logger.infoLines()
	if len(got) != 1 || !strings.Contains(got[0], "skipped") || !strings.Contains(got[0], "3") ||
		!strings.Contains(got[0], "failed") || !strings.Contains(got[0], "2") {
		t.Fatalf("info lines = %q, want one line carrying skipped=3 failed=2", got)
	}
	if got := logger.errorLines(); len(got) != 0 {
		t.Fatalf("error lines = %q, want none", got)
	}
}

func TestGCLoopFallsBackToStdlibLogAndMasksCredentials(t *testing.T) {
	// The log package is process-global, so this test must not run in parallel.
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	leaky := errors.New("dial redis://user:hunter2@cache:6379 failed")
	run := gcAnswers(gcStep{err: leaky})
	h := newGCHarness(t, run.run, blobGCOptions{}) // no Logger
	h.awaitIdle()
	h.tick()
	gcStopWithin(t, h.stop) // the loop is gone, so buf is no longer written to
	out := buf.String()
	if !strings.Contains(out, "blob gc pass failed") {
		t.Fatalf("stdlib log = %q, want the failed-pass line", out)
	}
	if strings.Contains(out, "hunter2") {
		t.Fatalf("stdlib log leaked a credential: %q", out)
	}
}

func TestGCLoopStopLeavesNoGoroutine(t *testing.T) {
	base := gcLoopGoroutines()
	if base != 0 {
		t.Fatalf("%d blob gc loop goroutines alive before the test", base)
	}
	var stops []func()
	for range 3 {
		stops = append(stops, startBlobGC(context.Background(), gcAnswers().run, blobGCOptions{Tick: time.Hour}))
	}
	deadline := time.Now().Add(gcTestDeadline)
	for gcLoopGoroutines() != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("saw %d blob gc loop goroutines, want 3", gcLoopGoroutines())
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, stop := range stops {
		gcStopWithin(t, stop)
	}
	// stop waits for the goroutine, so the stack scan right after it must already
	// be clean: no polling here.
	if got := gcLoopGoroutines(); got != 0 {
		t.Fatalf("%d blob gc loop goroutines alive right after stop returned", got)
	}
}

func TestGCLoopExitsWhenParentContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := startBlobGC(ctx, gcAnswers().run, blobGCOptions{Tick: time.Hour})
	cancel()
	gcStopWithin(t, stop) // returns only because the loop noticed the parent ending
	if got := gcLoopGoroutines(); got != 0 {
		t.Fatalf("%d blob gc loop goroutines alive after the parent ended", got)
	}
}

func TestGCOptionsDefaults(t *testing.T) {
	o := blobGCOptions{}.withDefaults()
	if o.Tick != 5*time.Minute || o.Batch != 200 || o.MaxRunsPerTick != 10 || o.Sleep == nil {
		t.Fatalf("defaults = tick %v batch %d cap %d sleep-nil %v", o.Tick, o.Batch, o.MaxRunsPerTick, o.Sleep == nil)
	}
	set := blobGCOptions{Tick: time.Second, Batch: 7, MaxRunsPerTick: 2}.withDefaults()
	if set.Tick != time.Second || set.Batch != 7 || set.MaxRunsPerTick != 2 {
		t.Fatalf("explicit options overwritten: %+v", set)
	}
}
