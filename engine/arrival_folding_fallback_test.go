package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// arrivalIgnoringState is the third-party backend shape the capability gate
// exists for: a fully valid AtomicStateStore whose CommitNode accepts the
// request but ignores CommitNodeRequest.Arrivals.
//
// StateStore and AtomicStateStore are embedded as interfaces on purpose.
// Embedding *fakeState would promote its FoldsArrivalsIntoCommit marker and
// make the capability assertion succeed again — the exact false positive this
// test guards against. CommitNode and AdvanceNode are overridden to count and
// to prove which carrier the engine chose.
type arrivalIgnoringState struct {
	StateStore
	AtomicStateStore
	commits              int
	commitsWithAdvance   int
	commitsWithArrivals  int
	advanceCalls         int
	advancesWithArrivals int
}

func (s *arrivalIgnoringState) CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.commits++
	if req.AdvanceTask != nil {
		s.commitsWithAdvance++
	}
	if len(req.Arrivals) > 0 {
		s.commitsWithArrivals++
	}
	// This backend cannot fold: it drops the field instead of applying it.
	// Anything the engine sends here is lost, which is what makes the
	// carrier choice observable.
	req.Arrivals = nil
	return s.AtomicStateStore.CommitNode(ctx, req)
}

func (s *arrivalIgnoringState) AdvanceNode(ctx context.Context, req AdvanceNodeRequest) (AdvanceNodeResult, error) {
	s.advanceCalls++
	if len(req.Arrivals) > 0 {
		s.advancesWithArrivals++
	}
	return s.AtomicStateStore.AdvanceNode(ctx, req)
}

// TestNonFoldingBackendKeepsAdvanceTask is the reachability proof for the
// scheduling fallback. CommitNode belongs to AtomicStateStore, so a
// signature-only capability check reports true for every atomic backend — an
// external store that implements CommitNode but ignores Arrivals included. The
// engine would then send Arrivals, emit no AdvanceTask, and silently drop
// downstream scheduling. The explicit FoldsArrivalsIntoCommit marker keeps this
// store on the pre-fold carrier: the commit carries an advance intent,
// FlushOutbox consumes it through AdvanceNode, and the downstream exec task is
// still delivered.
func TestNonFoldingBackendKeepsAdvanceTask(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	state := &arrivalIgnoringState{StateStore: inner, AtomicStateStore: inner}
	queue := &fakeQueue{}
	eng := New(state, queue)

	if _, ok := any(state).(ArrivalFoldingCommitter); ok {
		t.Fatal("arrivalIgnoringState satisfies ArrivalFoldingCommitter; the explicit " +
			"FoldsArrivalsIntoCommit marker is not what gates the capability")
	}

	g := compileCommitProbeGraph(t)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	task := queue.Drain()[0]

	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult() error = %v", err)
	}

	// Downstream scheduling must survive the fallback. The exec task is written
	// by AdvanceNode and delivered by the commit's own flush, so an empty queue
	// here means the engine dropped the one carrier it emitted.
	delivered := queue.Drain()
	if len(delivered) != 1 || delivered[0].NodeName != "next" || delivered[0].Type != TaskTypeNodeExec {
		t.Fatalf("delivered after commit = %+v, want exactly the downstream exec task for next; "+
			"a non-folding backend must keep its advance carrier", delivered)
	}

	if state.commitsWithArrivals != 0 {
		t.Fatalf("CommitNode received Arrivals on %d of %d commits, want 0: the store does not "+
			"declare FoldsArrivalsIntoCommit, so folding is not available to it",
			state.commitsWithArrivals, state.commits)
	}
	if state.commitsWithAdvance != 1 {
		t.Fatalf("commits carrying an AdvanceTask = %d, want 1: the fallback emits the advance "+
			"intent in the terminal commit", state.commitsWithAdvance)
	}
	if state.advanceCalls != 1 || state.advancesWithArrivals == 0 {
		t.Fatalf("AdvanceNode calls = %d (with arrivals: %d), want 1 carrying the downstream "+
			"arrival: the fallback path must reach the store's advance transition",
			state.advanceCalls, state.advancesWithArrivals)
	}
}
