package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// pauseGraph compiles the two-node graph the engine-level pause tests drive:
// one hop, so a commit on the first node produces real downstream work that a
// pause then has to hold.
func pauseGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "pause-engine",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestPauseHoldsDispatchUntilResume is the engine half of the hold.
//
// The engine keeps no dispatch state of its own — every delivery goes through a
// claim on the store — so what it must get right is narrower than it looks: a
// paused execution delivers nothing through FlushOutbox, INCLUDING the
// downstream work produced by a commit that was already in flight when the
// pause landed, and a resume both clears the hold and delivers what was held.
//
// The commit below is issued while the execution is paused, and the engine
// flushes the outbox from inside that commit. A pause implemented anywhere
// other than the store's claim would fail this: the flush would find the
// advance intent, handle it and enqueue the downstream node.
func TestPauseHoldsDispatchUntilResume(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, pauseGraph(t), map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	initial := queue.Drain()
	if len(initial) != 1 || initial[0].NodeName != "start" {
		t.Fatalf("queued after Submit() = %+v, want the start task", taskNames(initial))
	}

	if err := eng.Pause(ctx, id); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if got := snapshotStatus(t, state, id); got != types.ExecutionStatusPaused {
		t.Fatalf("status after Pause() = %q, want %q", got, types.ExecutionStatusPaused)
	}

	// A lease taken before the pause, committed after it: the work is in
	// flight, so the commit has to be accepted.
	lease, err := eng.BuildTaskLease(ctx, initial[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(while paused) error = %v", err)
	}
	if queued := queue.Drain(); len(queued) != 0 {
		t.Fatalf("queued while paused = %+v, want none: a paused execution delivers nothing, "+
			"including the work an in-flight commit appends", taskNames(queued))
	}

	if err := eng.Resume(ctx, id); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if got := snapshotStatus(t, state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after Resume() = %q, want %q", got, types.ExecutionStatusRunning)
	}
	resumed := queue.Drain()
	if len(resumed) != 1 || resumed[0].NodeName != "next" {
		t.Fatalf("queued after Resume() = %+v, want the downstream task: the resume has to "+
			"deliver what the pause held", taskNames(resumed))
	}
}

// TestPauseOnAStoreWithoutTheCapability pins the refusal a backend that cannot
// suspend anything gets. It is a verdict about the store, not about the
// execution, and it is checked before the execution is looked up: a store that
// cannot pause cannot pause any execution, so its answer must not depend on
// which id was asked about.
func TestPauseOnAStoreWithoutTheCapability(t *testing.T) {
	ctx := context.Background()
	eng := New(&plainStateStore{AtomicStateStore: newFakeState()}, &fakeQueue{})

	if err := eng.Pause(ctx, types.ExecutionID("exec-pause-unsupported")); !errors.Is(err, ErrPauseUnsupported) {
		t.Fatalf("Pause() error = %v, want %v", err, ErrPauseUnsupported)
	}
	if err := eng.Resume(ctx, types.ExecutionID("exec-pause-unsupported")); !errors.Is(err, ErrPauseUnsupported) {
		t.Fatalf("Resume() error = %v, want %v", err, ErrPauseUnsupported)
	}
}

// TestPauseOfAnUnrecordedExecutionIsNotFound separates the two refusals the
// store deliberately reports as one: an execution that was never there is a
// not-found, while one that exists but is queued or finished is unpausable.
// Collapsing them would make a typo'd id look like a live execution that cannot
// be paused.
func TestPauseOfAnUnrecordedExecutionIsNotFound(t *testing.T) {
	ctx := context.Background()
	eng := New(newFakeState(), &fakeQueue{})

	if err := eng.Pause(ctx, types.ExecutionID("exec-pause-not-recorded")); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("Pause() error = %v, want %v", err, ErrExecutionNotFound)
	}
	if err := eng.Resume(ctx, types.ExecutionID("exec-pause-not-recorded")); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("Resume() error = %v, want %v", err, ErrExecutionNotFound)
	}
}

// snapshotStatus reads the status through the store the engine holds.
func snapshotStatus(t *testing.T, state StateStore, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	snap, err := state.GetExecution(context.Background(), id)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", id, err)
	}
	if snap == nil {
		t.Fatalf("GetExecution(%q) returned no execution", id)
	}
	return snap.Status
}

// TestCancelAcceptsAPausedExecution pins the interaction that would otherwise
// deadlock: a paused execution keeps its workflow concurrency slot, so if
// Cancel refused it the slot could never be released — nothing else can finish
// an execution whose dispatch is held — and the workflow would sit at its limit
// for good.
//
// The engine's active check is "non-terminal", which a paused execution already
// satisfies. This is the test that keeps it that way: narrowing that check to
// running (the obvious-looking shape for "only a running execution can be
// cancelled") would compile, pass every other cancel test, and leak one slot
// per paused execution.
func TestCancelAcceptsAPausedExecution(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := compileLimitedGraph(t, 1)

	first, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	if err := eng.Pause(ctx, first); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if _, err := eng.Submit(ctx, g, nil); !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("Submit() beside a paused holder error = %v, want %v: a paused execution holds "+
			"its slot", err, ErrConcurrencyLimit)
	}

	if err := eng.Cancel(ctx, first); err != nil {
		t.Fatalf("Cancel(paused) error = %v", err)
	}
	if got := snapshotStatus(t, state, first); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after Cancel() = %q, want %q", got, types.ExecutionStatusCanceled)
	}
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() after canceling the paused execution error = %v, want the slot released",
			err)
	}
}
