package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// deadlineExpiredCall is one observation the recorder received.
type deadlineExpiredCall struct {
	source string
	ns     namespace.Namespace
}

// deadlineExpiredRecorder keeps every deadline-expired observation so a test
// can assert both the count and what the sweep put in the context.
type deadlineExpiredRecorder struct {
	mu    sync.Mutex
	calls []deadlineExpiredCall
}

func (r *deadlineExpiredRecorder) OnExecutionDeadlineExpired(ctx context.Context, source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, deadlineExpiredCall{source: source, ns: namespace.FromContext(ctx)})
}

func (r *deadlineExpiredRecorder) snapshot() []deadlineExpiredCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]deadlineExpiredCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// runSweeperInBackground starts b's deadline sweep and returns a stop that is
// also registered with the test's cleanup.
func runSweeperInBackground(t *testing.T, b *Backend, expirer engine.ExecutionExpirer) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runDeadlineSweeper(ctx, expirer, 5*time.Millisecond)
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("runDeadlineSweeper did not return after its context was canceled")
		}
	}
	t.Cleanup(stop)
	return stop
}

// TestBackendDeadlineSweeperReportsExpiryToObserver is the in-process
// counterpart of the control plane's deadline observation: an execution past
// its settings.timeout is timed out by the backend alone, and the observer is
// told exactly once, as the embedded source, under the namespace the sweep ran
// it with.
func TestBackendDeadlineSweeperReportsExpiryToObserver(t *testing.T) {
	b := New(WithDeadlineExpiredObserver(&deadlineExpiredRecorder{}))
	recorder := b.deadlineExpiredObserver.(*deadlineExpiredRecorder)
	eng := engine.New(b.State(), b.Queue())
	id := types.ExecutionID("exec-observed")
	if err := b.state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Graph: nodeStateGraph(t), Status: types.ExecutionStatusRunning,
		Deadline: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("CreateExecution(%s) error = %v", id, err)
	}

	runSweeperInBackground(t, b, eng)
	// The index entry is acknowledged only after the cascade finished, so an
	// empty index is what "the expiry completed" looks like from the outside.
	waitForDeadlineIndexEmpty(t, b.state, 5*time.Second)

	calls := recorder.snapshot()
	if len(calls) != 1 {
		t.Fatalf("observer called %d times, want exactly 1: %+v", len(calls), calls)
	}
	if calls[0].source != engine.DeadlineExpiredSourceEmbedded {
		t.Fatalf("observer source = %q, want %q", calls[0].source, engine.DeadlineExpiredSourceEmbedded)
	}
	// The in-memory backend has no namespace dimension, so the sweep's own
	// context is the one the observer receives, and it names no tenant.
	if calls[0].ns != namespace.Default {
		t.Fatalf("observer namespace = %q, want %q", calls[0].ns, namespace.Default)
	}
}

// TestBackendDeadlineSweeperNilObserverStillSweeps pins the nil case: the
// option is optional, and a backend built with it must run exactly as before,
// expiring the execution without panicking.
func TestBackendDeadlineSweeperNilObserverStillSweeps(t *testing.T) {
	b := New(WithDeadlineExpiredObserver(nil))
	if b.deadlineExpiredObserver != nil {
		t.Fatalf("deadlineExpiredObserver = %#v, want nil", b.deadlineExpiredObserver)
	}
	eng := engine.New(b.State(), b.Queue())
	id := types.ExecutionID("exec-no-observer")
	if err := b.state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Graph: nodeStateGraph(t), Status: types.ExecutionStatusPaused,
		Deadline: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("CreateExecution(%s) error = %v", id, err)
	}

	runSweeperInBackground(t, b, eng)
	waitForDeadlineIndexEmpty(t, b.state, 5*time.Second)

	snap, err := b.state.GetExecution(context.Background(), id)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("execution = %+v, %v, want timeout", snap, err)
	}
}

// TestDeadlineExpiredObserverNilOptionKeepsConfiguredObserver pins the nil
// guard: a nil option must not disarm an observer another option installed.
func TestDeadlineExpiredObserverNilOptionKeepsConfiguredObserver(t *testing.T) {
	recorder := &deadlineExpiredRecorder{}
	b := New(WithDeadlineExpiredObserver(recorder), WithDeadlineExpiredObserver(nil))
	if b.deadlineExpiredObserver != recorder {
		t.Fatalf("deadlineExpiredObserver = %#v, want the sentinel to survive a nil call", b.deadlineExpiredObserver)
	}
}
