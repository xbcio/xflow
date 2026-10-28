package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// chainResolver answers a sub-workflow reference by workflow name, which is what
// a three-level chain needs: the parent resolves "child" and the child resolves
// "grand", through the one resolver the engine was built with.
type chainResolver map[string]graph.SubWorkflowTarget

func (r chainResolver) ResolveSubWorkflow(_ context.Context, ref graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return r[ref.Name], nil
}

// expireChainRig is a parent whose sub-workflow node is parked on a child whose
// own sub-workflow node is parked on a grandchild, all three running, driven
// through the store and hooks of the expiry tests. The parent carries an
// already-reached execution deadline; the child and grandchild have none.
type expireChainRig struct {
	eng        *Engine
	state      *expireState
	log        *expireLog
	hooks      *expireHooks
	parentID   types.ExecutionID
	childID    types.ExecutionID
	grandchild types.ExecutionID
}

func newExpireChainRig(t *testing.T) *expireChainRig {
	t.Helper()
	ctx := context.Background()
	subDef := func(name, target string) *types.WorkflowDef {
		return &types.WorkflowDef{
			Name: name,
			Nodes: []types.NodeDef{{
				Name:       "sub",
				Type:       types.SubWorkflowNodeType,
				Parameters: map[string]any{"workflow": target},
			}},
		}
	}
	resolver := chainResolver{
		"grand": {Version: "v1", DefinitionHash: "sha256:grand", Graph: subWorkflowChildGraph(t)},
	}
	middle, err := graph.CompileWithResolver(ctx, subDef("middle", "grand"), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver(middle): %v", err)
	}
	resolver["child"] = graph.SubWorkflowTarget{Version: "v1", DefinitionHash: "sha256:child", Graph: middle}
	parent, err := graph.CompileWithResolver(ctx, subDef("parent", "child"), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver(parent): %v", err)
	}

	parentID := types.ExecutionID("exec-parent")
	log := &expireLog{track: parentID}
	hooks := &expireHooks{log: log}
	state := &expireState{subWorkflowLinkState: newSubWorkflowLinkState(), log: log}
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:       parentID,
		Graph:    parent,
		Status:   types.ExecutionStatusRunning,
		Deadline: pastDeadline,
	}); err != nil {
		t.Fatalf("seed parent execution: %v", err)
	}
	eng := New(&expireAckState{expireState: state}, &fakeQueue{},
		WithHooks(hooks), WithSubWorkflowResolver(resolver))
	rig := &expireChainRig{eng: eng, state: state, log: log, hooks: hooks, parentID: parentID}

	dispatch := func(execution types.ExecutionID, g *graph.Graph) types.ExecutionID {
		t.Helper()
		idx, ok := g.NodeIndex("sub")
		if !ok {
			t.Fatal(`the graph has no node named "sub"`)
		}
		handled, err := eng.handleSystemTask(ctx, &Task{
			ExecutionID:  execution,
			NodeName:     "sub",
			NodeIdx:      idx,
			UnitIdx:      g.UnitIndexForNode(idx),
			Type:         TaskTypeSubWorkflow,
			ActivationID: 1,
		}, true)
		if err != nil || !handled {
			t.Fatalf("dispatch the sub-workflow node of %q: handled=%v err=%v", execution, handled, err)
		}
		link, found, err := state.GetSubWorkflowLink(ctx, execution, "sub")
		if err != nil || !found {
			t.Fatalf("read the sub-workflow link of %q: found=%v err=%v", execution, found, err)
		}
		return link.ChildExecutionID
	}
	rig.childID = dispatch(parentID, parent)
	rig.grandchild = dispatch(rig.childID, middle)
	log.clear()
	return rig
}

func (r *expireChainRig) status(t *testing.T, id types.ExecutionID) *ExecutionSnapshot {
	t.Helper()
	snap, err := r.state.GetExecution(context.Background(), id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = (%v, %v)", id, snap, err)
	}
	return snap
}

// TestExpireExecutionCascadesThroughAThreeLevelChain pins the healthy chain: the
// parent's expiry cancels the child, and the child's own cancel cascades to the
// grandchild, so nothing is left running below the timed-out parent.
func TestExpireExecutionCascadesThroughAThreeLevelChain(t *testing.T) {
	rig := newExpireChainRig(t)

	got, err := rig.eng.ExpireExecution(context.Background(), rig.parentID)
	if err != nil || got != ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution() = (%q, %v), want (expired, nil)", got, err)
	}

	if parent := rig.status(t, rig.parentID); parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
		t.Fatalf("parent = %s %q, want timeout %q", parent.Status, parent.Error, ExecutionDeadlineExceededError)
	}
	if child := rig.status(t, rig.childID); child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %s, want canceled", child.Status)
	}
	if grand := rig.status(t, rig.grandchild); grand.Status != types.ExecutionStatusCanceled {
		t.Fatalf("grandchild = %s, want canceled: the child's cancel cascades", grand.Status)
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("parent completion hooks = %v, want exactly [timeout]", got)
	}
}

// TestExpireExecutionDoesNotRetryAFailedGrandchildCancel pins the present
// behaviour of a known gap (F1) rather than endorsing it. The child's Cancel
// treats its own child cascade as best-effort (cancel.go keeps only the log
// line), so a grandchild whose cancel fails is invisible to the parent's
// ExpireExecution: the call returns expired with no error, the deadline entry is
// acknowledged, and a later call is Resumed but never revisits the grandchild.
// The grandchild is left canceling, held there only by its own machinery (the
// canceling-execution reaper) and not by the parent's expiry.
//
// F1: the grandchild cancel is not retried by ExpireExecution; the gap was ruled
// out of scope for this round, so this test only pins the current state. If a
// later change closes it, only the "F1 as it stands" group below flips; the
// invariants group must keep holding under any reasonable fix.
func TestExpireExecutionDoesNotRetryAFailedGrandchildCancel(t *testing.T) {
	// Both groups read the same run, so a red group does not hide the other.
	run := runFailedGrandchildCancel(t)

	// Invariants: what any reasonable fix of F1 is expected to keep. The parent's
	// expiry is written and announced once, the child is stopped, and the
	// deadline entry is acknowledged exactly once.
	t.Run("invariants", func(t *testing.T) {
		if parent := run.parent; parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
			t.Fatalf("parent = %s %q, want timeout %q", parent.Status, parent.Error, ExecutionDeadlineExceededError)
		}
		if child := run.child; child.Status != types.ExecutionStatusCanceled {
			t.Fatalf("child = %s, want canceled", child.Status)
		}
		// The deadline entry is withdrawn if and only if the call succeeded. The
		// original pin said "acked exactly once"; that is this line plus the F1
		// group's firstErr == nil. Split this way, a fix that makes the first call
		// report the grandchild (an error, hence no ack until the replay) flips
		// only the F1 group, and nothing may ever ack a call that failed.
		wantAcks := 0
		if run.firstErr == nil {
			wantAcks = 1
		}
		if n := run.acksAfterFirst; n != wantAcks {
			t.Fatalf("acked %d times after a call that returned %v, want %d: the entry is withdrawn only once the call succeeded", n, run.firstErr, wantAcks)
		}
		if got := run.completions; len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
			t.Fatalf("parent completion hooks = %v, want exactly [timeout]", got)
		}
	})

	// F1 as it stands: the grandchild's failed cancel is invisible to the
	// parent's call (the child's Cancel keeps only the log line), so the call
	// returns expired with no error and the grandchild stays canceling, even
	// across the resumed call. Flip this group when F1 is fixed.
	t.Run("f1_as_it_stands", func(t *testing.T) {
		if run.firstErr != nil || run.firstOutcome != ExpireOutcomeExpired {
			t.Fatalf("ExpireExecution() = (%q, %v), want (expired, nil): the grandchild failure does not reach the parent", run.firstOutcome, run.firstErr)
		}
		if grand := run.grand; grand.Status != types.ExecutionStatusCanceling {
			t.Fatalf("grandchild = %s, want canceling: its cancel failed half way", grand.Status)
		}
		if run.secondErr != nil || run.secondOutcome != ExpireOutcomeResumed {
			t.Fatalf("second ExpireExecution() = (%q, %v), want (resumed, nil)", run.secondOutcome, run.secondErr)
		}
		if grand := run.grandAfterSecond; grand.Status != types.ExecutionStatusCanceling {
			t.Fatalf("grandchild after the resumed expiry = %s, want still canceling (F1: not retried)", grand.Status)
		}
	})
}

// failedGrandchildCancelRun is what the F1 scenario observed, recorded rather
// than asserted, so the two assertion groups of the test read one run and a red
// group cannot hide the other.
type failedGrandchildCancelRun struct {
	firstOutcome, secondOutcome ExpireOutcome
	firstErr, secondErr         error

	// Read after the first call.
	parent, child, grand *ExecutionSnapshot
	acksAfterFirst       int

	// Read after the second call.
	grandAfterSecond *ExecutionSnapshot
	completions      []types.ExecutionStatus
}

// runFailedGrandchildCancel makes the grandchild's own cancel fail half way,
// expires the parent twice and records what each call and the stores showed.
func runFailedGrandchildCancel(t *testing.T) failedGrandchildCancelRun {
	t.Helper()
	ctx := context.Background()
	rig := newExpireChainRig(t)
	rig.state.failListSuspendedFor(rig.grandchild, errors.New("injected: grandchild list suspended nodes failed"))

	var run failedGrandchildCancelRun
	run.firstOutcome, run.firstErr = rig.eng.ExpireExecution(ctx, rig.parentID)
	run.parent = rig.status(t, rig.parentID)
	run.child = rig.status(t, rig.childID)
	run.grand = rig.status(t, rig.grandchild)
	run.acksAfterFirst = rig.log.count("ack")

	run.secondOutcome, run.secondErr = rig.eng.ExpireExecution(ctx, rig.parentID)
	run.grandAfterSecond = rig.status(t, rig.grandchild)
	run.completions = rig.hooks.completions()
	return run
}
