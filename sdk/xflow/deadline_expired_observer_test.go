package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// deadlineExpiredCall is one observation the recorder received.
type deadlineExpiredCall struct {
	source string
	ns     namespace.Namespace
}

// deadlineExpiredObserverRecorder keeps every deadline-expired observation so
// a test can assert both the count and what the sweep put in the context.
type deadlineExpiredObserverRecorder struct {
	mu    sync.Mutex
	calls []deadlineExpiredCall
}

func (r *deadlineExpiredObserverRecorder) OnExecutionDeadlineExpired(ctx context.Context, source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, deadlineExpiredCall{source: source, ns: namespace.FromContext(ctx)})
}

func (r *deadlineExpiredObserverRecorder) snapshot() []deadlineExpiredCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]deadlineExpiredCall, len(r.calls))
	copy(out, r.calls)
	return out
}

const deadlineObserverTestBudget = 1500 * time.Millisecond

// submitParkedExecutionWithTimeout submits a one-wait-node workflow whose
// settings.timeout is budget, and returns once the node suspended (so the
// execution holds no lease and only the deadline sweep can end it). The caller
// must stop the engine through cleanup.
func submitParkedExecutionWithTimeout(t *testing.T, eng *Engine, name string, budget time.Duration) types.ExecutionID {
	t.Helper()
	ctx := context.Background()
	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	park := wf.Node("park", node.Wait("never-signalled"))
	wf.Connect(start, park)
	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition() error = %v", err)
	}
	def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: budget}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := eng.eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	waitForNodeStatus(t, waitCtx, eng, id, "park", types.NodeStatusSuspended)
	return id
}

// waitForExecutionTimeout pollers until the execution reached timeout.
func waitForExecutionTimeout(t *testing.T, eng *Engine, id types.ExecutionID) {
	t.Helper()
	waitForTransientCondition(t, 20*time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(context.Background(), id)
		return err == nil && snap != nil && snap.Status == types.ExecutionStatusTimeout
	})
}

// TestLocalReportsDeadlineExpiryToEmbeddedObserver is the NewLocal end-to-end
// check: a parked execution past settings.timeout is timed out by the embedded
// sweeper alone, and the observer the caller installed is told once, as the
// embedded source.
func TestLocalReportsDeadlineExpiryToEmbeddedObserver(t *testing.T) {
	recorder := &deadlineExpiredObserverRecorder{}
	eng, err := NewLocal(WithConcurrency(2), WithDeadlineExpiredObserver(recorder))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	t.Cleanup(eng.Stop)

	id := submitParkedExecutionWithTimeout(t, eng, "local_deadline_observer", deadlineObserverTestBudget)
	waitForExecutionTimeout(t, eng, id)
	// The observer is called after the timeout write; wait for it rather than
	// reading a count that may not have landed yet.
	waitForTransientCondition(t, 5*time.Second, func() bool { return len(recorder.snapshot()) > 0 })

	calls := recorder.snapshot()
	if len(calls) != 1 {
		t.Fatalf("observer called %d times, want exactly 1: %+v", len(calls), calls)
	}
	if calls[0].source != engine.DeadlineExpiredSourceEmbedded {
		t.Fatalf("observer source = %q, want %q", calls[0].source, engine.DeadlineExpiredSourceEmbedded)
	}
	if calls[0].ns != namespace.Default {
		t.Fatalf("observer namespace = %q, want %q", calls[0].ns, namespace.Default)
	}
}

// TestLocalDeadlineExpiryFeedsTheSweepMetrics wires the real metrics adapter
// instead of a fake, so it fails if the observer is not the one the caller's
// registry serves: the family must carry source=embedded for the expiry.
func TestLocalDeadlineExpiryFeedsTheSweepMetrics(t *testing.T) {
	m := metrics.New()
	eng, err := NewLocal(WithConcurrency(2), WithDeadlineExpiredObserver(metrics.NewSweepMetrics(m)))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	t.Cleanup(eng.Stop)

	id := submitParkedExecutionWithTimeout(t, eng, "local_deadline_metrics", deadlineObserverTestBudget)
	waitForExecutionTimeout(t, eng, id)

	const name = "xflow_execution_deadline_expired_total"
	// The label is selected literally, not through the engine constant: this is
	// the assertion that notices the published value drifting (a dashboard
	// selects source="embedded", not whatever the constant becomes).
	waitForTransientCondition(t, 5*time.Second, func() bool {
		var found bool
		forEachSample(t, m, name, func(labels map[string]string, _ float64) {
			if labels["source"] == "embedded" {
				found = true
			}
		})
		return found
	})

	total := 0.0
	forEachSample(t, m, name, func(labels map[string]string, v float64) {
		if labels["source"] == "embedded" {
			total += v
		}
	})
	if total != 1 {
		t.Fatalf("%s{source=%q} = %v, want 1: an embedded deployment's timeout must reach the caller's registry",
			name, "embedded", total)
	}
}
