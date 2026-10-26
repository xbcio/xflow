package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// subWorkflowDeadlineGraph is a parent with settings.timeout whose sub-workflow
// node declares its own timeout (so the dispatch's wait-expiry branch is live)
// and every on_error route a failed node could take: a main target, and an
// error target for error_output.
func subWorkflowDeadlineGraph(t *testing.T, onError string, nodeTimeout time.Duration) *graph.Graph {
	t.Helper()
	return subWorkflowDeadlineGraphWithRetry(t, onError, nodeTimeout, nil)
}

// subWorkflowDeadlineGraphWithRetry is subWorkflowDeadlineGraph with retry
// settings on the sub-workflow node.
func subWorkflowDeadlineGraphWithRetry(t *testing.T, onError string, nodeTimeout time.Duration, retry *types.RetrySettings) *graph.Graph {
	t.Helper()
	def := subWorkflowParentDef()
	def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
	def.Nodes[0].Timeout = nodeTimeout
	def.Nodes[0].OnError = onError
	def.Nodes[0].Retry = retry
	def.Nodes = append(def.Nodes,
		types.NodeDef{Name: "next", Type: "test.echo"},
		types.NodeDef{Name: "onerr", Type: "test.echo"},
	)
	def.Connections = types.Connections{
		"sub": {
			"main":  {Targets: []types.Connection{{Node: "next", Input: "main"}}},
			"error": {Targets: []types.Connection{{Node: "onerr", Input: "main"}}},
		},
	}
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	return g
}

// redeliverSubWorkflowNode simulates the lease sweeper's reclaim of the parked
// node and hands the same intent to the engine again.
func redeliverSubWorkflowNode(t *testing.T, rig *expireSubWorkflowRig, g *graph.Graph) {
	t.Helper()
	handled, err := redeliverSubWorkflowNodeResult(t, rig, g)
	if err != nil || !handled {
		t.Fatalf("redeliver the sub-workflow node: handled=%v err=%v", handled, err)
	}
}

// redeliverSubWorkflowNodeResult is redeliverSubWorkflowNode for a test that
// asserts on the dispatch's own result instead of requiring it to succeed.
func redeliverSubWorkflowNodeResult(t *testing.T, rig *expireSubWorkflowRig, g *graph.Graph) (bool, error) {
	t.Helper()
	ctx := context.Background()
	node, err := rig.state.GetNode(ctx, rig.parentID, "sub")
	if err != nil || node == nil {
		t.Fatalf("read the sub node: node=%v err=%v", node, err)
	}
	if revoked, err := rig.state.RevokeLease(ctx, rig.parentID, "sub", node.LeaseToken); err != nil || !revoked {
		t.Fatalf("RevokeLease: revoked=%v err=%v", revoked, err)
	}
	idx, ok := g.NodeIndex("sub")
	if !ok {
		t.Fatal(`the graph has no node named "sub"`)
	}
	return rig.eng.handleSystemTask(ctx, &Task{
		ExecutionID:  rig.parentID,
		NodeName:     "sub",
		NodeIdx:      idx,
		UnitIdx:      g.UnitIndexForNode(idx),
		Type:         TaskTypeSubWorkflow,
		ActivationID: 1,
	}, true)
}

// TestSubWorkflowWaitEndingAtTheExecutionDeadlineExpiresTheExecution pins the
// tie rule for the dispatch's own wait expiry. The wait is parked clamped to the
// execution deadline, so a resumed dispatch that finds the wait over cannot tell
// the node's budget from the execution's by the wait deadline alone. When that
// deadline is the execution's, the execution has timed out: no on_error route
// (stop, continue, error_output) may absorb it, and the child is stopped by the
// execution's own cascade.
func TestSubWorkflowWaitEndingAtTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	for _, onError := range []string{"", string(types.OnErrorContinue), string(types.OnErrorOutput)} {
		name := onError
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			g := subWorkflowDeadlineGraph(t, onError, time.Minute)
			// The rig parks the node under an already-reached execution deadline,
			// so the stored wait end is the execution's.
			rig := newExpireSubWorkflowRigOf(t, g)
			if !rig.link.WaitDeadline.Equal(pastDeadline) {
				t.Fatalf("parked wait deadline = %s, want the execution deadline %s", rig.link.WaitDeadline, pastDeadline)
			}

			redeliverSubWorkflowNode(t, rig, g)

			parent, err := rig.state.GetExecution(ctx, rig.parentID)
			if err != nil || parent == nil {
				t.Fatalf("read the parent: parent=%v err=%v", parent, err)
			}
			if parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
				t.Fatalf("parent = %s %q, want timeout %q: the node's on_error must not absorb the execution's deadline",
					parent.Status, parent.Error, ExecutionDeadlineExceededError)
			}
			if node, err := rig.state.GetNode(ctx, rig.parentID, "sub"); err != nil || node == nil || node.Status == types.NodeStatusFailed {
				t.Fatalf("sub node = %+v (err %v), want it not failed through on_error", node, err)
			}
			child, err := rig.state.GetExecution(ctx, rig.link.ChildExecutionID)
			if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
				t.Fatalf("child = %+v (err %v), want canceled", child, err)
			}
			if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
				t.Fatalf("completion hooks = %v, want exactly [timeout]", got)
			}
		})
	}
}

// TestSubWorkflowWaitTimeoutBeforeTheExecutionDeadlineStillFollowsOnError is the
// other side of the tie rule (R3, same family): a wait that ran out of its node
// timeout while the execution deadline is still ahead is the node's failure, and
// it goes through the ordinary on_error pipeline. The execution is not expired.
func TestSubWorkflowWaitTimeoutBeforeTheExecutionDeadlineStillFollowsOnError(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowDeadlineGraph(t, "", time.Minute)
	rig := newExpireSubWorkflowRigOf(t, g)

	// The rig seeds an already-reached execution deadline; move it ahead and
	// drop the engine's memo of the old one. The parked wait end stays in the
	// past, so only the node's budget has run out.
	rig.state.mu.Lock()
	rig.state.executions[rig.parentID].Deadline = time.Now().Add(time.Hour)
	rig.state.mu.Unlock()
	rig.eng.EvictExecution(rig.parentID)

	redeliverSubWorkflowNode(t, rig, g)

	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil {
		t.Fatalf("read the parent: parent=%v err=%v", parent, err)
	}
	if parent.Status == types.ExecutionStatusTimeout {
		t.Fatalf("parent = %s %q, want the execution left to on_error: its deadline is still ahead", parent.Status, parent.Error)
	}
	node, err := rig.state.GetNode(ctx, rig.parentID, "sub")
	if err != nil || node == nil || node.Status != types.NodeStatusFailed {
		t.Fatalf("sub node = %+v (err %v), want failed through the node timeout", node, err)
	}
	child, err := rig.state.GetExecution(ctx, rig.link.ChildExecutionID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %+v (err %v), want canceled by the node-timeout path", child, err)
	}
}

// TestSubWorkflowFinishedChildBeatsTheExecutionDeadline pins the order of the two
// checks in the dispatch: a child that already finished is a result, and the
// result is committed before the execution deadline is even looked at. The
// guard sits after concludeTerminalChild, so a redelivery that finds the child
// success while the execution deadline is already reached still commits the
// parent node as a success. Moving the guard in front of the conclusion would
// time the execution out and cancel a child that had already done its work.
func TestSubWorkflowFinishedChildBeatsTheExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowDeadlineGraph(t, "", time.Minute)
	// Parked under the already-reached execution deadline, so the resumed wait
	// is over and the guard would fire were it reached.
	rig := newExpireSubWorkflowRigOf(t, g)
	childID := rig.link.ChildExecutionID
	if err := rig.state.UpdateExecutionStatus(ctx, childID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("finish the child: %v", err)
	}

	redeliverSubWorkflowNode(t, rig, g)

	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil {
		t.Fatalf("read the parent: parent=%v err=%v", parent, err)
	}
	if parent.Status != types.ExecutionStatusRunning {
		t.Fatalf("parent = %s %q, want running: a finished child is a result, not the execution's timeout", parent.Status, parent.Error)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusSuccess {
		t.Fatalf("sub node = %q, want success: the child's result is the node's", got)
	}
	child, err := rig.state.GetExecution(ctx, childID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusSuccess {
		t.Fatalf("child = %+v (err %v), want it left success, not canceled", child, err)
	}
	if got := rig.hooks.completions(); len(got) != 0 {
		t.Fatalf("completion hooks = %v, want none: the execution is not concluded", got)
	}
}

// timeoutWriteFailState fails the first write of the execution's timeout status
// and passes every other call through. It is the fault the guard's own step
// needs: the existing expiry fakes fail the acknowledgement and the cascade, but
// not the status write ExpireExecution makes before either.
type timeoutWriteFailState struct {
	*expireAckState
	mu  sync.Mutex
	err error
}

func (s *timeoutWriteFailState) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, reason string) error {
	if status == types.ExecutionStatusTimeout {
		s.mu.Lock()
		err := s.err
		s.err = nil
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return s.expireAckState.UpdateExecutionStatus(ctx, id, status, reason)
}

// TestSubWorkflowWaitGuardErrorIsReturned pins that an error inside the guard
// (here the execution's timeout write) comes back from the dispatch as it is. It
// must not be swallowed into the node-timeout path: that path commits the parked
// node through on_error, which would fail a node whose wait the execution's
// deadline may still own. The delivery is retried instead, and the retry finds
// the execution still running and settles it.
func TestSubWorkflowWaitGuardErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowDeadlineGraph(t, "", time.Minute)
	rig := newExpireSubWorkflowRigOf(t, g)
	injected := errors.New("injected: write the execution timeout failed")
	failing := &timeoutWriteFailState{expireAckState: &expireAckState{expireState: rig.state}, err: injected}
	rig.eng = New(failing, &fakeQueue{}, WithHooks(rig.hooks), WithSubWorkflowResolver(subWorkflowStubResolver(t)))

	handled, err := redeliverSubWorkflowNodeResult(t, rig, g)
	if !errors.Is(err, injected) {
		t.Fatalf("dispatch = (handled %v, %v), want the guard's error %v returned as it is", handled, err, injected)
	}

	if got := rig.nodeStatus(t); got != types.NodeStatusWaiting {
		t.Fatalf("sub node = %q, want waiting: a guard error must not fall into the on_error commit", got)
	}
	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil || parent.Status != types.ExecutionStatusRunning {
		t.Fatalf("parent = %+v (err %v), want running: the timeout write failed", parent, err)
	}
	child, err := rig.state.GetExecution(ctx, rig.link.ChildExecutionID)
	if err != nil || child == nil || child.Status == types.ExecutionStatusCanceled {
		t.Fatalf("child = %+v (err %v), want it left alone: nothing settled the wait", child, err)
	}

	// The fault was one-shot: the retried delivery settles the execution.
	redeliverSubWorkflowNode(t, rig, g)
	parent, err = rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil || parent.Status != types.ExecutionStatusTimeout {
		t.Fatalf("parent after the retry = %+v (err %v), want timeout", parent, err)
	}
}

// TestSubWorkflowWaitSettledByTheExecutionReturnsNilUnderRetry pins the return
// value of a handled guard on a node that retries. The execution's expiry has
// already finalized the parked node, so falling through to the node-timeout
// pipeline would ask the retry scheduler to reset a node that is no longer
// active and surface "retry state ... no longer active" as the dispatch's
// error. A handled guard must end the dispatch with nil, leave the execution
// timed out and schedule no retry.
func TestSubWorkflowWaitSettledByTheExecutionReturnsNilUnderRetry(t *testing.T) {
	ctx := context.Background()
	retry := &types.RetrySettings{MaxAttempts: 3, InitialInterval: 1}
	g := subWorkflowDeadlineGraphWithRetry(t, "", time.Minute, retry)
	rig := newExpireSubWorkflowRigOf(t, g)

	handled, err := redeliverSubWorkflowNodeResult(t, rig, g)
	if err != nil || !handled {
		t.Fatalf("dispatch = (handled %v, %v), want (true, nil): the execution settled the wait, there is no retry to refuse", handled, err)
	}

	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil || parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
		t.Fatalf("parent = %+v (err %v), want timeout %q", parent, err, ExecutionDeadlineExceededError)
	}
	if got := rig.nodeStatus(t); got == types.NodeStatusPending {
		t.Fatalf("sub node = %q, want it not reset for a retry", got)
	}
	entries, err := rig.state.ListOutbox(ctx, rig.parentID, time.Now().UTC().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("recorded %d retry intents, want none: the execution's timeout is not retried", len(entries))
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("completion hooks = %v, want exactly [timeout]", got)
	}
}
