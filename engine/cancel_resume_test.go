package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// cancelResumeState decorates fakeState with one-shot faults at the two points
// an interrupted Cancel can strand an execution in canceling: reading the
// suspended set (before anything is reconciled) and the final canceled status
// write (after everything else is).
type cancelResumeState struct {
	*fakeState
	mu                sync.Mutex
	listSuspendedErr  error
	canceledStatusErr error
}

func (s *cancelResumeState) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
	s.mu.Lock()
	err := s.listSuspendedErr
	s.listSuspendedErr = nil
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.fakeState.ListSuspendedNodes(ctx, id)
}

func (s *cancelResumeState) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	if status == types.ExecutionStatusCanceled {
		s.mu.Lock()
		err := s.canceledStatusErr
		s.canceledStatusErr = nil
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return s.fakeState.UpdateExecutionStatus(ctx, id, status, errMsg)
}

func (s *cancelResumeState) failListSuspended(err error) {
	s.mu.Lock()
	s.listSuspendedErr = err
	s.mu.Unlock()
}

// CancelSuspendedNode mirrors the fenced compare-and-set both real backends
// implement. fakeState alone does not have it, so Cancel would otherwise take
// the GetNode/UpsertNode fallback, which cannot clear fakeState's suspended
// waiter index and would leave the fixture re-reporting a node that is already
// canceled.
func (s *cancelResumeState) CancelSuspendedNode(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + name
	ns := f.nodes[key]
	if ns == nil || ns.Status != types.NodeStatusSuspended {
		return false, nil
	}
	cp := *ns
	cp.Status = types.NodeStatusCanceled
	f.nodes[key] = &cp
	delete(f.suspended, key)
	return true, nil
}

func (s *cancelResumeState) failCanceledStatus(err error) {
	s.mu.Lock()
	s.canceledStatusErr = err
	s.mu.Unlock()
}

// newSuspendedCancelExecution builds one execution whose single node has
// suspended, so Cancel has real work in every step: a suspended node to cancel
// and a final status write to make.
func newSuspendedCancelExecution(t *testing.T, state *cancelResumeState) (*Engine, types.ExecutionID) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "cancel-resume",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "wait", Type: "test.suspend"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	state.InitInDegrees(id, g)
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("submission enqueued %d tasks, want 1", len(tasks))
	}
	executeTask(t, eng, tasks[0])

	suspended, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	if len(suspended) != 1 || suspended[0] != "wait" {
		t.Fatalf("suspended nodes = %v, want [wait]: the fixture must park real work before Cancel", suspended)
	}
	return eng, id
}

// TestCancelResumesAfterFinalStatusWriteFailure pins the resumability contract
// at the step where the damage is worst: every node-level effect of the cancel
// landed (the suspended node is canceled, the waiter is retired), but the
// terminal status write failed, so the execution sits in canceling holding its
// concurrency slot. A second Cancel must carry on from that point — not start
// over, not refuse — and reach canceled.
func TestCancelResumesAfterFinalStatusWriteFailure(t *testing.T) {
	ctx := context.Background()
	injected := errors.New("injected: terminal cancel status write failed")
	state := &cancelResumeState{fakeState: newFakeState()}
	eng, id := newSuspendedCancelExecution(t, state)

	state.failCanceledStatus(injected)
	if err := eng.Cancel(ctx, id); !errors.Is(err, injected) {
		t.Fatalf("Cancel() error = %v, want wrapped %v", err, injected)
	}

	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusCanceling {
		t.Fatalf("status after the failed final write = %q, want canceling", snap.Status)
	}
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status = %q, want canceled: the earlier steps must have landed before the failure", node.Status)
	}
	suspended, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	if len(suspended) != 0 {
		t.Fatalf("suspended nodes after the partial cancel = %v, want none", suspended)
	}

	// The interrupted cancel is exactly what the leader reaper would find; its
	// recovery action is this second call.
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("resumed Cancel() error = %v", err)
	}
	snap, err = state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() after resume = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("status after the resumed Cancel = %q, want canceled", snap.Status)
	}
	node, err = state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() after resume = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status after the resumed Cancel = %q, want canceled", node.Status)
	}
}

// TestCancelResumesAfterSuspendedListFailure pins resumability at the opposite
// end of Cancel: the first step that touches nodes failed before any of them
// was reconciled, so the execution is canceling with its suspended node still
// parked. The second call must pick up the suspended node and finish.
func TestCancelResumesAfterSuspendedListFailure(t *testing.T) {
	ctx := context.Background()
	injected := errors.New("injected: list suspended nodes failed")
	state := &cancelResumeState{fakeState: newFakeState()}
	eng, id := newSuspendedCancelExecution(t, state)

	state.failListSuspended(injected)
	if err := eng.Cancel(ctx, id); !errors.Is(err, injected) {
		t.Fatalf("Cancel() error = %v, want wrapped %v", err, injected)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusCanceling {
		t.Fatalf("status after the failed suspended read = %q, want canceling", snap.Status)
	}
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended: the failure happened before the node transition", node.Status)
	}

	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("resumed Cancel() error = %v", err)
	}
	snap, err = state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() after resume = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("status after the resumed Cancel = %q, want canceled", snap.Status)
	}
	node, err = state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() after resume = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status after the resumed Cancel = %q, want canceled", node.Status)
	}
}

// TestCancelIsSafeToRepeatWhileCanceling pins that re-entry itself is a no-op
// at the status level rather than an error: neither real backend rejects a
// second canceling write (the fenced CAS skips it), which is what lets the
// reaper call Cancel on an execution whose first attempt is still in flight.
func TestCancelIsSafeToRepeatWhileCanceling(t *testing.T) {
	ctx := context.Background()
	state := &cancelResumeState{fakeState: newFakeState()}
	eng, id := newSuspendedCancelExecution(t, state)

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("mark canceling: %v", err)
	}
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() on an already-canceling execution error = %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", snap.Status)
	}
}
