package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// recordingFailureObserver captures observations in delivery order.
type recordingFailureObserver struct {
	mu       sync.Mutex
	failures []ObservedNodeFailure
}

func (o *recordingFailureObserver) ObserveNodeFailure(_ types.ExecutionID, f ObservedNodeFailure) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, f)
}

func (o *recordingFailureObserver) observed(name string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, f := range o.failures {
		if f.NodeName == name {
			return true
		}
	}
	return false
}

// fatalCommitProbe records whether the store was asked to commit a fatal
// failure before the observer had been told about it. That ordering is the
// contract a group runtime depends on: it reads the failure from the observer
// after WaitDone reports the failed execution, and a commit that becomes
// visible first leaves the member unidentified.
type fatalCommitProbe struct {
	*fakeState
	observer *recordingFailureObserver
	violated bool
}

func (s *fatalCommitProbe) CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	if req.Fatal && req.Status == types.NodeStatusFailed && !s.observer.observed(req.NodeName) {
		s.violated = true
	}
	return s.fakeState.CommitNode(ctx, req)
}

// TestEngine_ObservesFatalFailureBeforeCommittingFailedExecution pins the
// delivery order: the observation must precede the store transition that can
// wake a waiter with the failed execution. Reversing them is a real race, not
// a theoretical one — the group runtime's FailedMember comes back empty when
// the waiter wins it.
func TestEngine_ObservesFatalFailureBeforeCommittingFailedExecution(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "failure-observer-order",
		Nodes: []types.NodeDef{{Name: "a", Type: "test.fail"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	observer := &recordingFailureObserver{}
	state := &fatalCommitProbe{fakeState: newFakeState(), observer: observer}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.fail": &failHandler{}}}
	eng := New(state, queue, WithNodeFailureObserver(observer))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })

	ctx := context.Background()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("tasks = %v, want one execution task", taskNames(tasks))
	}
	executeTask(t, eng, tasks[0])

	if state.violated {
		t.Fatal("the failed node was committed before ObserveNodeFailure ran; a waiter can read the failed execution with no observed member")
	}
	if !observer.observed("a") {
		t.Fatal("no failure was observed for the failed node")
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed", snap.Status)
	}
}
