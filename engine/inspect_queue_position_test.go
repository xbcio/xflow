package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/types"
)

// queuePositionState is a *fakeState that can also report where a queued
// execution stands in line, so the inspection path can be exercised without a
// backend that really queues.
//
// The positions are a map rather than a real line on purpose: what these tests
// pin is the engine's half of the contract — when it asks and what it does with
// the answer — and a fake that reimplemented a queue would be testing the fake.
type queuePositionState struct {
	*fakeState
	positions map[types.ExecutionID]int
	err       error
}

func (s *queuePositionState) ConcurrencyQueuePosition(_ context.Context, id types.ExecutionID) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	return s.positions[id], nil
}

// inspectQueuedExecution creates one execution in the given status and returns
// its ID. The graph is the smallest compilable definition: inspection asserts on
// the queue position, not on the nodes.
func inspectQueuedExecution(t *testing.T, state *fakeState, status types.ExecutionStatus) types.ExecutionID {
	t.Helper()
	g := compileConcurrencyGraph(t, "queue-inspect", "1", nil)
	id := types.ExecutionID("exec-" + string(status))
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID: id, Graph: g, Status: status,
	}); err != nil {
		t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
	return id
}

// TestInspectReportsTheQueuePosition pins the visibility half of queue mode: a
// caller looking at an execution the workflow parked can see where it stands,
// which is the only thing it can act on while it waits.
func TestInspectReportsTheQueuePosition(t *testing.T) {
	ctx := context.Background()
	state := &queuePositionState{
		fakeState: newFakeState(),
		positions: map[types.ExecutionID]int{},
	}
	eng := New(state, &fakeQueue{})

	queued := inspectQueuedExecution(t, state.fakeState, types.ExecutionStatusQueued)
	state.positions[queued] = 3

	detail, err := eng.Inspect(ctx, queued)
	if err != nil {
		t.Fatalf("Inspect(%q) error = %v", queued, err)
	}
	if detail.QueuePosition != 3 {
		t.Fatalf("QueuePosition = %d, want 3", detail.QueuePosition)
	}
}

// TestInspectAsksForAPositionOnlyWhenQueued keeps the capability from being
// consulted on every inspection: a running execution has no place in line, and
// asking would both cost a read and risk reporting a stale one. The fake answers
// a non-zero position for the running execution, so a lookup that happened
// anyway cannot pass unnoticed.
func TestInspectAsksForAPositionOnlyWhenQueued(t *testing.T) {
	ctx := context.Background()
	state := &queuePositionState{
		fakeState: newFakeState(),
		positions: map[types.ExecutionID]int{},
	}
	eng := New(state, &fakeQueue{})

	running := inspectQueuedExecution(t, state.fakeState, types.ExecutionStatusRunning)
	state.positions[running] = 7

	detail, err := eng.Inspect(ctx, running)
	if err != nil {
		t.Fatalf("Inspect(%q) error = %v", running, err)
	}
	if detail.QueuePosition != 0 {
		t.Fatalf("QueuePosition = %d for a running execution, want 0", detail.QueuePosition)
	}
}

// TestInspectSurvivesAPositionLookupFailure pins the failure policy: the
// position is an extra on top of the audit answer, so a store that cannot
// produce it must not turn an inspection into an error. A caller that cannot see
// the status is blind; a caller that cannot see the position has lost a detail.
func TestInspectSurvivesAPositionLookupFailure(t *testing.T) {
	ctx := context.Background()
	state := &queuePositionState{
		fakeState: newFakeState(),
		positions: map[types.ExecutionID]int{},
		err:       errors.New("redis unavailable"),
	}
	eng := New(state, &fakeQueue{})

	queued := inspectQueuedExecution(t, state.fakeState, types.ExecutionStatusQueued)

	detail, err := eng.Inspect(ctx, queued)
	if err != nil {
		t.Fatalf("Inspect(%q) error = %v, want the status to survive the missing position", queued, err)
	}
	if detail.Status != types.ExecutionStatusQueued {
		t.Fatalf("Status = %q, want %q", detail.Status, types.ExecutionStatusQueued)
	}
	if detail.QueuePosition != 0 {
		t.Fatalf("QueuePosition = %d after a failed lookup, want 0", detail.QueuePosition)
	}
}

// TestInspectReportsNoPositionWithoutTheCapability is the same policy one level
// up: most stores do not implement the inspector at all, and their inspections
// must read exactly as they did before it existed.
func TestInspectReportsNoPositionWithoutTheCapability(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(&plainStateStore{StateStore: state, AtomicStateStore: state}, &fakeQueue{})

	queued := inspectQueuedExecution(t, state, types.ExecutionStatusQueued)

	detail, err := eng.Inspect(ctx, queued)
	if err != nil {
		t.Fatalf("Inspect(%q) error = %v", queued, err)
	}
	if detail.QueuePosition != 0 {
		t.Fatalf("QueuePosition = %d without the capability, want 0", detail.QueuePosition)
	}
}
