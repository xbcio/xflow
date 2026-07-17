package xflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// newTransientClusterEngine builds a NewCluster engine backed by an in-process
// miniredis and configured for transient execution. The miniredis server and the
// engine are cleaned up automatically.
func newTransientClusterEngine(t *testing.T, opts ...Option) (*Engine, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	allOpts := append([]Option{
		WithExecutionMode(ExecutionModeTransient),
		WithTransientTTL(time.Minute),
		WithTransientCompletionTTL(30 * time.Second),
	}, opts...)

	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()}, allOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.Stop)
	return eng, mr
}

// TestNewLocalTransientReturnsRequiresCluster asserts that NewLocal rejects
// transient mode: transient execution is a cluster/Redis-only concept now that
// backend/transient is gone. Callers must use NewCluster.
func TestNewLocalTransientReturnsRequiresCluster(t *testing.T) {
	_, err := NewLocal(WithExecutionMode(ExecutionModeTransient))
	if !errors.Is(err, ErrTransientRequiresCluster) {
		t.Fatalf("NewLocal(transient) error = %v, want ErrTransientRequiresCluster", err)
	}
	// Default mode must still work.
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	eng.Stop()
}

// TestTransientClusterSuspendWorkflowFails is the cluster (miniredis) counterpart
// of the deleted TestNewLocalTransientFailsSuspendWorkflow. In transient mode
// suspend is disabled at the engine layer, so a wait node must fail the
// execution (not park) and never fire OnNodeSuspended.
func TestTransientClusterSuspendWorkflowFails(t *testing.T) {
	hooks := &transientHookRecorder{}
	eng, _ := newTransientClusterEngine(t, WithHooks(hooks), WithConcurrency(4))

	wf := Workflow("transient_cluster_suspend")
	start := wf.Node("start", node.Start())
	wait := wf.Node("wait", node.Wait("approval").OnError(types.OnErrorContinue))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	waitForTransientCondition(t, 5*time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(context.Background(), id)
		if err != nil || snap == nil {
			return false
		}
		return isTerminalStatus(snap.Status)
	})

	if hooks.nodeCompleteStatus("wait") != types.NodeStatusFailed {
		t.Fatalf("wait hook status = %s, want failed", hooks.nodeCompleteStatus("wait"))
	}
	if hooks.executionCompleteStatus(id) != types.ExecutionStatusFailed {
		t.Fatalf("execution hook status = %s, want failed", hooks.executionCompleteStatus(id))
	}
	if hooks.nodeSuspendedCount("wait") != 0 {
		t.Fatalf("OnNodeSuspended(wait) count = %d, want 0", hooks.nodeSuspendedCount("wait"))
	}
}

// TestTransientClusterControlAPIsReturnClearErrors is the cluster counterpart of
// the deleted TestTransientModeControlAPIsReturnClearErrors. The Inspect/Signal/
// RevokeSignal guards are SDK-layer and backend-agnostic, so an API-only
// (DisableConsumer) transient cluster is sufficient.
func TestTransientClusterControlAPIsReturnClearErrors(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr(), DisableConsumer: true},
		WithExecutionMode(ExecutionModeTransient),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	ctx := context.Background()
	id := types.ExecutionID("exec-transient")
	if _, err := eng.Inspect(ctx, id); !errors.Is(err, ErrTransientInspectionUnavailable) {
		t.Fatalf("Inspect() error = %v, want ErrTransientInspectionUnavailable", err)
	}
	if err := eng.Signal(ctx, id, "approval", nil); !errors.Is(err, ErrTransientSignalsUnsupported) {
		t.Fatalf("Signal() error = %v, want ErrTransientSignalsUnsupported", err)
	}
	if err := eng.RevokeSignal(ctx, id, "approval"); !errors.Is(err, ErrTransientSignalsUnsupported) {
		t.Fatalf("RevokeSignal() error = %v, want ErrTransientSignalsUnsupported", err)
	}
}

func waitForTransientCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("condition was not met before timeout")
		}
	}
}

type transientHookRecorder struct {
	engine.BaseHooks

	mu                  sync.Mutex
	nodeComplete        map[string]types.NodeStatus
	executionComplete   map[types.ExecutionID]types.ExecutionStatus
	nodeSuspendedCounts map[string]int
}

func (h *transientHookRecorder) OnNodeComplete(_ context.Context, _ types.ExecutionID, name string, status types.NodeStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.nodeComplete == nil {
		h.nodeComplete = make(map[string]types.NodeStatus)
	}
	h.nodeComplete[name] = status
}

func (h *transientHookRecorder) OnExecutionComplete(_ context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.executionComplete == nil {
		h.executionComplete = make(map[types.ExecutionID]types.ExecutionStatus)
	}
	h.executionComplete[id] = status
}

func (h *transientHookRecorder) OnNodeSuspended(_ context.Context, _ types.ExecutionID, name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.nodeSuspendedCounts == nil {
		h.nodeSuspendedCounts = make(map[string]int)
	}
	h.nodeSuspendedCounts[name]++
}

func (h *transientHookRecorder) nodeCompleteStatus(name string) types.NodeStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodeComplete[name]
}

func (h *transientHookRecorder) executionCompleteStatus(id types.ExecutionID) types.ExecutionStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.executionComplete[id]
}

func (h *transientHookRecorder) nodeSuspendedCount(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodeSuspendedCounts[name]
}
