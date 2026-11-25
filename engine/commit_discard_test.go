package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// commitDiscardRecorder records both commit classifications and discards, so a
// test can prove a discarded submission is classified as inactive and never
// leaks into the counter through another outcome.
type commitDiscardRecorder struct {
	outcomes []CommitOutcome
	discards []commitDiscardEvent
}

type commitDiscardEvent struct {
	kind    CommitDiscardKind
	outcome CommitOutcome
}

func (r *commitDiscardRecorder) OnCommitOutcome(_ context.Context, outcome CommitOutcome) {
	r.outcomes = append(r.outcomes, outcome)
}

func (r *commitDiscardRecorder) OnCommitDiscarded(_ context.Context, kind CommitDiscardKind, outcome CommitOutcome) {
	r.discards = append(r.discards, commitDiscardEvent{kind: kind, outcome: outcome})
}

// TestCommitDiscardObserver_NodeInactiveCountsOnce is the reclaimed-worker
// shape: the execution was canceled while the runner held the lease, and the
// late result is answered execution_inactive with a nil error. The commit
// semantics are unchanged; the discard is now observable, exactly once.
func TestCommitDiscardObserver_NodeInactiveCountsOnce(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	recorder := &commitDiscardRecorder{}
	eng := New(state, queue, WithCommitObserver(recorder))

	id, err := eng.Submit(ctx, compileCommitProbeGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = (%v, %v)", lease, err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "orphan reclaim"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	})
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v, want nil for an inactive execution", err)
	}
	if outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeExecutionInactive)
	}
	if len(recorder.outcomes) != 1 || recorder.outcomes[0] != CommitOutcomeExecutionInactive {
		t.Fatalf("commit outcomes = %v, want [%q]", recorder.outcomes, CommitOutcomeExecutionInactive)
	}
	if len(recorder.discards) != 1 {
		t.Fatalf("discards = %v, want exactly one", recorder.discards)
	}
	if got := recorder.discards[0]; got.kind != CommitDiscardKindNode || got.outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("discard = %+v, want kind=%q outcome=%q", got, CommitDiscardKindNode, CommitOutcomeExecutionInactive)
	}
}

// TestCommitDiscardObserver_MissingExecutionCountsOnce covers the other
// inactive exit: the graph is gone, so the entry point classifies the
// submission before any fenced commit runs.
func TestCommitDiscardObserver_MissingExecutionCountsOnce(t *testing.T) {
	recorder := &commitDiscardRecorder{}
	eng := New(newFakeState(), &fakeQueue{}, WithCommitObserver(recorder))

	lease := &TaskLease{Task: Task{ExecutionID: "gone-execution", NodeName: "start"}}
	outcome, err := eng.CommitTaskResultWithOutcome(context.Background(), lease, TaskResult{})
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v, want nil for a missing execution", err)
	}
	if outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeExecutionInactive)
	}
	if len(recorder.discards) != 1 || recorder.discards[0].kind != CommitDiscardKindNode {
		t.Fatalf("discards = %v, want one node discard", recorder.discards)
	}
}

// TestCommitDiscardObserver_AcceptedAndDuplicateAreNotCounted pins that an
// ordinary commit and the retry that finds the node already terminal keep
// their existing classifications and are not folded into the discard series.
func TestCommitDiscardObserver_AcceptedAndDuplicateAreNotCounted(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	recorder := &commitDiscardRecorder{}
	eng := New(state, queue, WithCommitObserver(recorder))

	id, err := eng.Submit(ctx, compileCommitProbeGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = (%v, %v)", lease, err)
	}

	result := TaskResult{Output: &types.Output{Data: map[string]any{"out": 1}}}
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, result)
	if err != nil || outcome != CommitOutcomeAccepted {
		t.Fatalf("first commit = (%q, %v), want (%q, nil)", outcome, err, CommitOutcomeAccepted)
	}
	outcome, err = eng.CommitTaskResultWithOutcome(ctx, lease, result)
	if err != nil || outcome != CommitOutcomeDuplicateTerminal {
		t.Fatalf("replayed commit = (%q, %v), want (%q, nil)", outcome, err, CommitOutcomeDuplicateTerminal)
	}
	if len(recorder.discards) != 0 {
		t.Fatalf("discards = %v, want none for accepted/duplicate commits", recorder.discards)
	}
}

// TestCommitDiscardObserver_StaleTokenIsNotCounted keeps the two "nothing
// applied" classifications apart: a fence refusal is not an inactive
// execution, and mixing them would corrupt the discard series.
func TestCommitDiscardObserver_StaleTokenIsNotCounted(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	recorder := &commitDiscardRecorder{}
	eng := New(state, queue, WithCommitObserver(recorder))

	id, err := eng.Submit(ctx, compileCommitProbeGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = (%v, %v)", lease, err)
	}
	lease.LeaseToken = "not-the-issued-token"

	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	})
	if err != ErrInvalidLeaseToken {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v, want ErrInvalidLeaseToken", err)
	}
	if outcome != CommitOutcomeStaleToken {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeStaleToken)
	}
	if len(recorder.discards) != 0 {
		t.Fatalf("discards = %v, want none for a stale-token commit", recorder.discards)
	}
}

// TestCommitDiscardObserver_GroupInactiveCountsOnce is the group entry point's
// discard. The group commit series deliberately excludes this classification,
// so this observer is its only trace.
func TestCommitDiscardObserver_GroupInactiveCountsOnce(t *testing.T) {
	ctx := context.Background()
	eng, g, execID := setupGroupLeaseTest(t)
	recorder := &commitDiscardRecorder{}
	eng.commitObserver = recorder
	state := eng.state.(*fakeGroupLeaseState)

	gm := g.Groups()[0]
	lease, _, err := eng.BuildGroupLease(ctx, &Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        TaskTypeGroupExec,
	})
	if err != nil || lease == nil {
		t.Fatalf("BuildGroupLease() = (%v, %v)", lease, err)
	}
	if err := state.UpdateExecutionStatus(ctx, execID, types.ExecutionStatusCanceled, "orphan reclaim"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{Outcome: GroupOutcomeSuccess})
	if err != nil {
		t.Fatalf("CommitGroupResult() error = %v, want nil for an inactive execution", err)
	}
	if outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeExecutionInactive)
	}
	if len(recorder.discards) != 1 {
		t.Fatalf("discards = %v, want exactly one", recorder.discards)
	}
	if got := recorder.discards[0]; got.kind != CommitDiscardKindGroup || got.outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("discard = %+v, want kind=%q outcome=%q", got, CommitDiscardKindGroup, CommitOutcomeExecutionInactive)
	}
}

// TestCommitDiscardObserver_NoObserverIsInert proves an engine built without a
// commit observer (and without a logger) keeps discarding silently rather
// than panicking.
func TestCommitDiscardObserver_NoObserverIsInert(t *testing.T) {
	eng := New(newFakeState(), &fakeQueue{})
	lease := &TaskLease{Task: Task{ExecutionID: "gone-execution", NodeName: "start"}}

	outcome, err := eng.CommitTaskResultWithOutcome(context.Background(), lease, TaskResult{})
	if err != nil || outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("commit = (%q, %v), want (%q, nil)", outcome, err, CommitOutcomeExecutionInactive)
	}
}
