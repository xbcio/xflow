package xflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestNewLocalTransientFailsSuspendWorkflow(t *testing.T) {
	hooks := &transientHookRecorder{}
	eng, err := NewLocal(
		WithExecutionMode(ExecutionModeTransient),
		WithHooks(hooks),
	)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("transient_local_suspend")
	start := wf.Node("start", node.Start())
	wait := wf.Node("wait", node.Wait("approval"))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
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

func TestNewLocalTransientExpiresActiveExecutionState(t *testing.T) {
	var calls int32
	action := node.Define("test.transient.local.slow", func(_ context.Context, input *types.Input) (*types.Output, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(150 * time.Millisecond)
		return &types.Output{Data: map[string]any{"seen": input.Data["value"]}}, nil
	})
	eng, err := NewLocal(
		WithExecutionMode(ExecutionModeTransient),
		WithTransientTTL(40*time.Millisecond),
		WithTransientCompletionTTL(time.Second),
		WithNodes(action),
	)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("transient_local_active_expiry")
	start := wf.Node("start", node.Start())
	run := wf.Node("run", action.New(nil))
	wf.Connect(start, run)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), map[string]any{"value": "ok"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	waitForTransientCondition(t, time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(context.Background(), id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		return snap == nil
	})

	time.Sleep(250 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("slow handler calls = %d, want 1; expired execution should not be retried", got)
	}
}

func TestNewLocalTransientExpiresCompletedExecutionState(t *testing.T) {
	action := node.Define("test.transient.local.action", func(_ context.Context, input *types.Input) (*types.Output, error) {
		return &types.Output{Data: map[string]any{"seen": input.Data["value"]}}, nil
	})
	eng, err := NewLocal(
		WithExecutionMode(ExecutionModeTransient),
		WithTransientCompletionTTL(25*time.Millisecond),
		WithNodes(action),
	)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("transient_local")
	start := wf.Node("start", node.Start())
	run := wf.Node("run", action.New(nil))
	wf.Connect(start, run)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), map[string]any{"value": "ok"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", result.Status)
	}
	if got := result.Output["run"].(map[string]any)["seen"]; got != "ok" {
		t.Fatalf("run.seen = %v, want ok", got)
	}

	waitForTransientCondition(t, time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(context.Background(), id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		return snap == nil
	})
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
