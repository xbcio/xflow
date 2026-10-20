package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// rttCountingState counts every StateStore call an engine path makes, one
// counted call per round trip. It embeds fakeState so the uninteresting methods
// (and the store's own semantics) are unchanged.
//
// The counters are deliberately per method rather than one total: a budget test
// that only sums cannot say WHICH round trip appeared when the sum grows, and
// the interesting regression here is specific — an AdvanceNode reappearing
// beside the fold, or a graph re-read after the cache was warmed.
type rttCountingState struct {
	*fakeState
	getExecution int
	getNode      int
	acquire      int
	commit       int
	list         int
	ack          int
	advance      int
	getOutput    int
	loadGraph    int
}

func (s *rttCountingState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.getExecution++
	return s.fakeState.GetExecution(ctx, id)
}

func (s *rttCountingState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	s.getNode++
	return s.fakeState.GetNode(ctx, id, name)
}

func (s *rttCountingState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.acquire++
	return s.fakeState.AcquireTaskLease(ctx, lease)
}

func (s *rttCountingState) CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.commit++
	return s.fakeState.CommitNode(ctx, req)
}

func (s *rttCountingState) ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]OutboxEntry, error) {
	s.list++
	return s.fakeState.ListOutbox(ctx, id, before, limit)
}

func (s *rttCountingState) AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error {
	s.ack++
	return s.fakeState.AckOutbox(ctx, id, entryID)
}

func (s *rttCountingState) AdvanceNode(ctx context.Context, req AdvanceNodeRequest) (AdvanceNodeResult, error) {
	s.advance++
	return s.fakeState.AdvanceNode(ctx, req)
}

func (s *rttCountingState) GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	s.getOutput++
	return s.fakeState.GetOutput(ctx, id, name)
}

func (s *rttCountingState) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.loadGraph++
	return s.fakeState.LoadGraph(ctx, id)
}

func (s *rttCountingState) count() int {
	return s.getExecution + s.getNode + s.acquire + s.commit + s.list + s.ack +
		s.advance + s.getOutput + s.loadGraph
}

func (s *rttCountingState) reset() {
	s.getExecution, s.getNode, s.acquire, s.commit = 0, 0, 0, 0
	s.list, s.ack, s.advance, s.getOutput, s.loadGraph = 0, 0, 0, 0, 0
}

// TestAcyclicCommitStaysWithinRTTBudget is the acceptance evidence for the
// arrival-folding switch: the whole runner-facing lifecycle of one acyclic node
// — lease the task, commit its result, flush the downstream intent — costs
// seven store round trips on this store, and the advance round trip is gone.
//
// Seven, measured, not "at most seven": the fixture is fixed and every call is
// named below, so an eighth call is a regression rather than noise. The window
// is BuildTaskLease through CommitTaskResult's internal FlushOutbox; enqueueing
// to the task queue is not a store call and is not counted. The graph cache is
// warm (Submit cached it), so LoadGraph must stay at zero.
//
// Breakdown, and why each is unavoidable on a store like this one:
//
//	GetExecution      2  one liveness read on the lease path, one input read
//	GetNode           1  the lease path's route check
//	AcquireTaskLease  1
//	CommitNode        1  the terminal write AND the downstream intent
//	ListOutbox        1  one claim; the folded intent is in the first batch
//	AckOutbox         1
//	AdvanceNode       0  the point of the change
//
// A backend that also implements ExecutionStatusReader,
// ExecutionInputContextReader and RouteCheckedLeaseAcquirer — both production
// backends do — folds the first four reads into two calls and comes out at five.
// This fake implements none of them, which is what makes seven here the count of
// ENGINE decisions rather than of backend capabilities.
func TestAcyclicCommitStaysWithinRTTBudget(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	state := &rttCountingState{fakeState: inner}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g := compileCommitProbeGraph(t)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	task := queue.Drain()[0]

	state.reset()
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult() error = %v", err)
	}

	// Teeth: the budget means nothing if the downstream intent was dropped to
	// hit it. The intent must have been delivered by the same commit's flush.
	delivered := queue.Drain()
	if len(delivered) != 1 || delivered[0].NodeName != "next" || delivered[0].Type != TaskTypeNodeExec {
		t.Fatalf("delivered after commit = %+v, want exactly the downstream exec task", delivered)
	}

	if state.advance != 0 {
		t.Fatalf("AdvanceNode calls = %d, want 0: the commit folds its arrivals", state.advance)
	}
	if state.loadGraph != 0 {
		t.Fatalf("LoadGraph calls = %d, want 0: the commit must use the warm graph cache", state.loadGraph)
	}
	if state.acquire != 1 || state.commit != 1 || state.list != 1 {
		t.Fatalf("acquire/commit/flush calls = %d/%d/%d, want 1/1/1 (one each)",
			state.acquire, state.commit, state.list)
	}
	if state.getExecution != 2 || state.getNode != 1 || state.ack != 1 || state.getOutput != 0 {
		t.Fatalf("read/ack breakdown = GetExecution:%d GetNode:%d AckOutbox:%d GetOutput:%d, "+
			"want 2/1/1/0", state.getExecution, state.getNode, state.ack, state.getOutput)
	}
	if got := state.count(); got != 7 {
		t.Fatalf("total engine-side store calls = %d, want exactly 7 "+
			"(2 GetExecution + 1 GetNode + 1 acquire + 1 commit + 1 claim + 1 ack)", got)
	}
}
