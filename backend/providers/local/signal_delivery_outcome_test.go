package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestDeliverSignalResultOnTheDurablePath checks the outcome DeliverSignalResult
// reports when the store is the in-memory backend, which implements
// DurableSignalDeliverer. The engine package's own tests run over a fake store
// and so only cover the legacy two-step path; this is the same three-way split
// over the atomic consume-plus-outbox path.
func TestDeliverSignalResultOnTheDurablePath(t *testing.T) {
	goSpec := func() *types.SuspendSpec {
		return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}, Timeout: farAway}
	}
	newParked := func(t *testing.T, spec *types.SuspendSpec) (*deadlineRaceRig, types.ExecutionID) {
		t.Helper()
		ctx := context.Background()
		rig := newDeadlineRaceRig(t)
		g := raceGraphWithTimeout(t, "signal-outcome", farAway, 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		rig.parkWith(ctx, id, spec)
		return rig, id
	}
	require := func(t *testing.T, got engine.SignalDelivery, err error, want engine.SignalOutcome) {
		t.Helper()
		if err != nil || got.Outcome != want {
			t.Fatalf("DeliverSignalResult() = (%+v, %v), want outcome %q", got, err, want)
		}
	}

	t.Run("resumed when the signal wakes the parked node", func(t *testing.T) {
		ctx := context.Background()
		rig, id := newParked(t, goSpec())
		got, err := rig.eng.DeliverSignalResult(ctx, id, "go", map[string]any{"n": 1})
		require(t, got, err, engine.SignalOutcomeResumed)
		if queued := rig.queue.take(id); len(queued) != 1 {
			t.Fatalf("resumed signal queued %d tasks, want exactly the wakeup: %+v", len(queued), queued)
		}
	})

	t.Run("buffered when the node is not suspended yet", func(t *testing.T) {
		ctx := context.Background()
		rig := newDeadlineRaceRig(t)
		g := raceGraphWithTimeout(t, "signal-outcome", farAway, 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		got, err := rig.eng.DeliverSignalResult(ctx, id, "go", map[string]any{"n": 1})
		require(t, got, err, engine.SignalOutcomeBuffered)
	})

	t.Run("buffered while the quorum is incomplete", func(t *testing.T) {
		ctx := context.Background()
		rig, id := newParked(t, quorumSpec())
		got, err := rig.eng.DeliverSignalResult(ctx, id, "a", map[string]any{"n": 1})
		require(t, got, err, engine.SignalOutcomeBuffered)
		if queued := rig.queue.take(id); len(queued) != 0 {
			t.Fatalf("one signal of a quorum of 2 queued %d tasks: %+v", len(queued), queued)
		}
		got, err = rig.eng.DeliverSignalResult(ctx, id, "b", map[string]any{"n": 2})
		require(t, got, err, engine.SignalOutcomeResumed)
	})

	t.Run("inactive after the execution is canceled", func(t *testing.T) {
		ctx := context.Background()
		rig, id := newParked(t, goSpec())
		if err := rig.eng.Cancel(ctx, id); err != nil {
			t.Fatalf("Cancel() error = %v", err)
		}
		got, err := rig.eng.DeliverSignalResult(ctx, id, "go", map[string]any{"n": 1})
		require(t, got, err, engine.SignalOutcomeInactive)
		if queued := rig.queue.take(id); len(queued) != 0 {
			t.Fatalf("a signal to a canceled execution queued %d tasks: %+v", len(queued), queued)
		}
	})

	t.Run("inactive when a waiter is still parked on a finished execution", func(t *testing.T) {
		ctx := context.Background()
		rig, id := newParked(t, goSpec())
		// The status is written without the cascade, so the node is still parked on
		// a waiter the signal could wake and only the status says the run is over.
		if err := rig.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
			t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
		}
		if nodes := rig.listSuspended(ctx, id); len(nodes) != 1 {
			t.Fatalf("suspended nodes in the window = %v, want the node still parked", nodes)
		}
		got, err := rig.eng.DeliverSignalResult(ctx, id, "go", map[string]any{"n": 1})
		require(t, got, err, engine.SignalOutcomeInactive)
		if queued := rig.queue.take(id); len(queued) != 0 {
			t.Fatalf("a signal to a finished execution queued %d tasks: %+v", len(queued), queued)
		}
	})
}
