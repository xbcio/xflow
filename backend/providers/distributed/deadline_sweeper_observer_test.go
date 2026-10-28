package distributed

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
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

// seedExecutionInNamespace records an overdue execution the way a
// multi-namespace caller does, so the sweep's index entry names the tenant.
func seedExecutionInNamespace(t *testing.T, b *Backend, ns namespace.Namespace, g *graph.Graph, id types.ExecutionID, status types.ExecutionStatus, deadline time.Time) {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), ns)
	if err := b.State().CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: status, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution(%s) error = %v", id, err)
	}
}

// waitForObservedTimeout polls until the execution is in timeout under its own
// namespace and the observer has recorded a call. The observer runs after the
// timeout write, so a test that asserted on the status alone could read the
// count before the call landed.
func waitForObservedTimeout(t *testing.T, b *Backend, recorder *deadlineExpiredRecorder, ns namespace.Namespace, id types.ExecutionID, within time.Duration) {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), ns)
	limit := time.Now().Add(within)
	for {
		snap, err := b.State().GetExecution(ctx, id)
		if err != nil || snap == nil {
			t.Fatalf("GetExecution(%s) = %+v, %v", id, snap, err)
		}
		if snap.Status == types.ExecutionStatusTimeout && len(recorder.snapshot()) > 0 {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("%s is %q with %d observations after %v, want timeout and one observation",
				id, snap.Status, len(recorder.snapshot()), within)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestEmbeddedBackendDeadlineSweeperReportsExpiryToObserver is the Redis-backed
// counterpart of the control plane's deadline observation: the embedded
// backend times the execution out by itself, and the observer is told exactly
// once, as the embedded source, under the tenant the index entry names.
func TestEmbeddedBackendDeadlineSweeperReportsExpiryToObserver(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	recorder := &deadlineExpiredRecorder{}
	b, eng := newSweepBackend(t, rdb.Options().Addr, nil, WithDeadlineExpiredObserver(recorder))
	const tenant = namespace.Namespace("tenant-a")
	id := types.ExecutionID("exec-observed")
	seedExecutionInNamespace(t, b, tenant, sweepTestGraph(t), id, types.ExecutionStatusRunning, time.Now().UTC().Add(-time.Minute))

	bindSweep(t, b, eng)
	waitForObservedTimeout(t, b, recorder, tenant, id, 10*time.Second)
	// The index entry is acknowledged only after the cascade finished, so wait
	// for that before asserting the call count cannot grow.
	deadlineWaitAcked(t, b, id, 10*time.Second)

	calls := recorder.snapshot()
	if len(calls) != 1 {
		t.Fatalf("observer called %d times, want exactly 1: %+v", len(calls), calls)
	}
	if calls[0].source != engine.DeadlineExpiredSourceEmbedded {
		t.Fatalf("observer source = %q, want %q", calls[0].source, engine.DeadlineExpiredSourceEmbedded)
	}
	if calls[0].ns != tenant {
		t.Fatalf("observer namespace = %q, want %q: the observer must run under the execution's own tenant",
			calls[0].ns, tenant)
	}
}

// deadlineWaitAcked polls until the execution leaves the deadline index.
func deadlineWaitAcked(t *testing.T, b *Backend, id types.ExecutionID, within time.Duration) {
	t.Helper()
	limit := time.Now().Add(within)
	for {
		if deadlineAcked(t, b, id) {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("%s stayed in the deadline index for %v after its timeout", id, within)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestEmbeddedBackendDeadlineSweeperNilObserverStillSweeps pins the nil case:
// the option is optional, and a backend built with it must expire the
// execution exactly as before, without panicking.
func TestEmbeddedBackendDeadlineSweeperNilObserverStillSweeps(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	b, eng := newSweepBackend(t, rdb.Options().Addr, nil, WithDeadlineExpiredObserver(nil))
	if b.deadlineExpiredObserver != nil {
		t.Fatalf("deadlineExpiredObserver = %#v, want nil", b.deadlineExpiredObserver)
	}
	seedExecution(t, b, sweepTestGraph(t), "exec-no-observer", types.ExecutionStatusPaused, time.Now().UTC().Add(-time.Minute))

	bindSweep(t, b, eng)
	waitForTimeout(t, b, "exec-no-observer", 10*time.Second)
}

// TestDeadlineExpiredObserverNilOptionKeepsConfiguredObserver pins the nil
// guard: a nil option must not disarm an observer another option installed.
func TestDeadlineExpiredObserverNilOptionKeepsConfiguredObserver(t *testing.T) {
	recorder := &deadlineExpiredRecorder{}
	c := &config{}
	WithDeadlineExpiredObserver(recorder)(c)
	WithDeadlineExpiredObserver(nil)(c)
	if c.deadlineExpiredObserver != recorder {
		t.Fatalf("deadlineExpiredObserver = %#v, want the sentinel to survive a nil call", c.deadlineExpiredObserver)
	}
}
