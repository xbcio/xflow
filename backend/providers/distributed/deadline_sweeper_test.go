package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

const testSweepInterval = 5 * time.Millisecond

// completeRecorder counts the completion hook per execution, which is how a
// test sees both that an expiry happened and that it was announced.
type completeRecorder struct {
	engine.BaseHooks
	mu       sync.Mutex
	statuses map[types.ExecutionID][]types.ExecutionStatus
}

func (r *completeRecorder) OnExecutionComplete(_ context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.statuses == nil {
		r.statuses = make(map[types.ExecutionID][]types.ExecutionStatus)
	}
	r.statuses[id] = append(r.statuses[id], status)
}

func (r *completeRecorder) announced(id types.ExecutionID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.statuses[id])
}

// onlyTimeout reports whether every announcement for id was a timeout.
func (r *completeRecorder) onlyTimeout(id types.ExecutionID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, status := range r.statuses[id] {
		if status != types.ExecutionStatusTimeout {
			return false
		}
	}
	return true
}

func sweepTestGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "deadline-sweep",
		Nodes: []types.NodeDef{{Name: "park", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// newSweepBackend builds a backend over the given miniredis address whose
// deadline sweeper ticks every few milliseconds. It is not bound yet: a test
// seeds executions first, then calls bind.
func newSweepBackend(t *testing.T, addr string, hooks engine.Hooks, opts ...Option) (*Backend, *engine.Engine) {
	t.Helper()
	b, err := New(addr, nil, append([]Option{WithTransport(&stubTransport{})}, opts...)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	b.testHooks.deadlineSweepInterval = testSweepInterval
	var engOpts []engine.Option
	if hooks != nil {
		engOpts = append(engOpts, engine.WithHooks(hooks))
	}
	return b, engine.New(b.State(), b.Queue(), engOpts...)
}

// bindSweep binds b through the embedded StartBinding path and returns an
// idempotent stop that also runs on cleanup.
func bindSweep(t *testing.T, b *Backend, eng *engine.Engine) func() {
	t.Helper()
	stop, err := b.bindHandler(eng, func(context.Context, *engine.Task) error { return nil }, true)
	if err != nil {
		t.Fatalf("bindHandler() error = %v", err)
	}
	var once sync.Once
	run := func() { once.Do(stop) }
	t.Cleanup(run)
	return run
}

func seedExecution(t *testing.T, b *Backend, g *graph.Graph, id types.ExecutionID, status types.ExecutionStatus, deadline time.Time) {
	t.Helper()
	if err := b.State().CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: status, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution(%s) error = %v", id, err)
	}
}

// waitForTimeout polls until the execution is in timeout and its index entry is
// acknowledged (the sign that the whole expiry, cascade included, finished).
func waitForTimeout(t *testing.T, b *Backend, id types.ExecutionID, within time.Duration) {
	t.Helper()
	limit := time.Now().Add(within)
	for {
		snap, err := b.State().GetExecution(context.Background(), id)
		if err != nil || snap == nil {
			t.Fatalf("GetExecution(%s) = %+v, %v", id, snap, err)
		}
		if snap.Status == types.ExecutionStatusTimeout && deadlineAcked(t, b, id) {
			if snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("%s error = %q, want %q", id, snap.Error, engine.ExecutionDeadlineExceededError)
			}
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("%s is %q after %v, want timeout", id, snap.Status, within)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func deadlineAcked(t *testing.T, b *Backend, id types.ExecutionID) bool {
	t.Helper()
	due, err := b.state.ListDeadlineExceededExecutions(context.Background(), time.Now().UTC().Add(time.Hour), 1000)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions() error = %v", err)
	}
	for _, c := range due {
		if c.ExecutionID == id {
			return false
		}
	}
	return true
}

// TestEmbeddedBackendExpiresParkedExecutionsWithoutAControlPlane is the gap this
// sweeper closes: executions past settings.timeout in each state that holds no
// lease are driven to timeout by the backend alone, with no control plane and
// no runner anywhere.
func TestEmbeddedBackendExpiresParkedExecutionsWithoutAControlPlane(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	hooks := &completeRecorder{}
	b, eng := newSweepBackend(t, rdb.Options().Addr, hooks)
	g := sweepTestGraph(t)
	past := time.Now().UTC().Add(-time.Minute)

	states := map[types.ExecutionID]types.ExecutionStatus{
		"exec-running": types.ExecutionStatusRunning,
		"exec-queued":  types.ExecutionStatusQueued,
		"exec-paused":  types.ExecutionStatusPaused,
	}
	for id, status := range states {
		seedExecution(t, b, g, id, status, past)
	}
	seedExecution(t, b, g, "exec-future", types.ExecutionStatusRunning, time.Now().UTC().Add(time.Hour))
	seedExecution(t, b, g, "exec-no-deadline", types.ExecutionStatusRunning, time.Time{})
	bindSweep(t, b, eng)

	for id := range states {
		waitForTimeout(t, b, id, 10*time.Second)
	}
	for _, id := range []types.ExecutionID{"exec-future", "exec-no-deadline"} {
		snap, err := b.State().GetExecution(context.Background(), id)
		if err != nil || snap == nil || snap.Status != types.ExecutionStatusRunning {
			t.Fatalf("%s = %+v, %v, want it left running", id, snap, err)
		}
	}
	for id := range states {
		if got := hooks.announced(id); got != 1 {
			t.Fatalf("%s completion announced %d times, want exactly 1", id, got)
		}
	}
}

// stillRunning asserts the execution was left alone for a few sweep intervals.
func stillRunning(t *testing.T, b *Backend, id types.ExecutionID) {
	t.Helper()
	time.Sleep(20 * testSweepInterval)
	snap, err := b.State().GetExecution(context.Background(), id)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("%s = %+v, %v, want it left running: nothing may sweep here", id, snap, err)
	}
}

// TestEmbeddedBackendStopWaitsForTheDeadlineSweeper pins the lifecycle: stop
// returns only after the sweeper goroutine exited. The exit hook is slowed down
// so a stop that did not wait would return before it ran.
func TestEmbeddedBackendStopWaitsForTheDeadlineSweeper(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	b, eng := newSweepBackend(t, rdb.Options().Addr, nil)
	var exited atomic.Bool
	b.testHooks.onDeadlineSweeperDone = func() {
		time.Sleep(100 * time.Millisecond)
		exited.Store(true)
	}
	seedExecution(t, b, sweepTestGraph(t), "exec-before", types.ExecutionStatusRunning, time.Now().UTC().Add(-time.Minute))
	stop := bindSweep(t, b, eng)
	waitForTimeout(t, b, "exec-before", 10*time.Second)

	stop()
	if !exited.Load() {
		t.Fatal("stop returned while the deadline sweeper goroutine was still running")
	}
	stop() // idempotent
}

// TestDeadlineSweeperIsNotStartedWhereAnotherOwnerSweeps covers the three binds
// that must leave an overdue execution alone: the control-plane bind (its own
// leader-gated pass sweeps, once per cluster), a backend without a consumer
// (the same convention as the outbox dispatcher and timeout monitor), and
// transient mode (which has no lease timeout monitor either).
func TestDeadlineSweeperIsNotStartedWhereAnotherOwnerSweeps(t *testing.T) {
	handler := func(context.Context, *engine.Task) error { return nil }
	cases := []struct {
		name string
		opts []Option
		bind func(*Backend, *engine.Engine) (func(), error)
	}{
		{"control plane bind", nil, func(b *Backend, e *engine.Engine) (func(), error) {
			return b.BindTaskHandler(e, handler)
		}},
		{"consumer disabled", []Option{WithConsumer(false)}, func(b *Backend, e *engine.Engine) (func(), error) {
			return b.bindHandler(e, handler, true)
		}},
		{"transient mode", []Option{WithTransientMode(time.Hour, time.Hour)}, func(b *Backend, e *engine.Engine) (func(), error) {
			return b.bindHandler(e, handler, true)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rdb := newRedisStateTestClient(t)
			b, eng := newSweepBackend(t, rdb.Options().Addr, nil, tc.opts...)
			var started atomic.Bool
			b.testHooks.onDeadlineSweeperDone = func() { started.Store(true) }
			seedExecution(t, b, sweepTestGraph(t), "exec-overdue", types.ExecutionStatusRunning, time.Now().UTC().Add(-time.Minute))
			stop, err := tc.bind(b, eng)
			if err != nil {
				t.Fatalf("bind error = %v", err)
			}
			t.Cleanup(stop)
			stillRunning(t, b, "exec-overdue")
			stop()
			if started.Load() {
				t.Fatal("a deadline sweeper goroutine ran, want none")
			}
		})
	}
}

// brokenExpirer fails every execution whose ID starts with "broken-" and hands
// the rest to the real engine, which is how a permanently stuck execution at the
// head of the index is made.
type brokenExpirer struct{ eng *engine.Engine }

func (e brokenExpirer) ExpireExecution(ctx context.Context, id types.ExecutionID) (engine.ExpireOutcome, error) {
	if strings.HasPrefix(string(id), "broken-") {
		return "", errors.New("store unavailable")
	}
	return e.eng.ExpireExecution(ctx, id)
}

// TestEmbeddedBackendDeadlineSweepIsNotStarvedByFailingEntries is the
// starvation regression at the backend level: more permanently failing entries
// than one batch sit ahead of a fresh one in the index, and the fresh one must
// still be timed out.
func TestEmbeddedBackendDeadlineSweepIsNotStarvedByFailingEntries(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	b, eng := newSweepBackend(t, rdb.Options().Addr, nil)
	g := sweepTestGraph(t)
	old := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < deadlineSweepBatch+10; i++ {
		id := types.ExecutionID(fmt.Sprintf("broken-%03d", i))
		seedExecution(t, b, g, id, types.ExecutionStatusRunning, old.Add(time.Duration(i)*time.Millisecond))
	}
	seedExecution(t, b, g, "fresh", types.ExecutionStatusRunning, time.Now().UTC().Add(-time.Minute))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runDeadlineSweeper(ctx, brokenExpirer{eng: eng}, testSweepInterval)
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitForTimeout(t, b, "fresh", 10*time.Second)
	snap, err := b.State().GetExecution(context.Background(), "broken-000")
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("broken-000 = %+v, %v, want it left running", snap, err)
	}
}

// TestTwoEmbeddedBackendsSweepingOneRedisAreHarmless is the multi-instance
// check: every embedded process runs a sweeper against the same Redis, and each
// overdue execution must still end in timeout exactly once, with its index entry
// acknowledged and nothing left half-done.
func TestTwoEmbeddedBackendsSweepingOneRedisAreHarmless(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	addr := rdb.Options().Addr
	hooksA, hooksB := &completeRecorder{}, &completeRecorder{}
	a, engA := newSweepBackend(t, addr, hooksA)
	b, engB := newSweepBackend(t, addr, hooksB)
	g := sweepTestGraph(t)
	past := time.Now().UTC().Add(-time.Minute)
	const n = 30
	for i := 0; i < n; i++ {
		seedExecution(t, a, g, types.ExecutionID(fmt.Sprintf("exec-%02d", i)), types.ExecutionStatusRunning, past.Add(time.Duration(i)*time.Millisecond))
	}
	bindSweep(t, a, engA)
	bindSweep(t, b, engB)

	for i := 0; i < n; i++ {
		waitForTimeout(t, a, types.ExecutionID(fmt.Sprintf("exec-%02d", i)), 10*time.Second)
	}
	for i := 0; i < n; i++ {
		id := types.ExecutionID(fmt.Sprintf("exec-%02d", i))
		snap, err := a.State().GetExecution(context.Background(), id)
		if err != nil || snap == nil || snap.Status != types.ExecutionStatusTimeout {
			t.Fatalf("%s = %+v, %v, want timeout", id, snap, err)
		}
		// The narrow window in which both callers announce is accepted and
		// documented; what must hold is that it is announced at least once and
		// never as anything but timeout.
		if hooksA.announced(id)+hooksB.announced(id) < 1 {
			t.Fatalf("%s completion never announced", id)
		}
		if !hooksA.onlyTimeout(id) || !hooksB.onlyTimeout(id) {
			t.Fatalf("%s announced a status other than timeout", id)
		}
	}
}
