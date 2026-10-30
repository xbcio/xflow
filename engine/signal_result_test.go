package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// signalResultRig is one execution parked on a suspending node, over the
// engine package's fakeState. fakeState has no DurableSignalDeliverer, so these
// tests cover the legacy delivery path; the durable path is covered against the
// real in-memory store in backend/providers/local.
type signalResultRig struct {
	eng   *Engine
	state *fakeState
	queue *fakeQueue
	id    types.ExecutionID
}

// newSignalResultRig submits a one-node workflow whose node has the given
// handler type. park decides whether the node's task is executed, which parks
// it; leaving it queued models "the target node is not suspended yet".
func newSignalResultRig(t *testing.T, nodeType string, park bool) *signalResultRig {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "signal-result",
		Nodes: []types.NodeDef{{Name: "wait", Type: nodeType}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.suspend":      &fakeSuspendHandler{},
		"test.multi_signal": &fakeMultiSignalHandler{},
	}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	state.InitInDegrees(id, g)
	if park {
		tasks := queue.Drain()
		if len(tasks) != 1 {
			t.Fatalf("queued tasks = %v, want exactly the wait node", taskNames(tasks))
		}
		executeTask(t, eng, tasks[0])
		if tasks := queue.Drain(); len(tasks) != 0 {
			t.Fatalf("tasks after the park = %v, want none", taskNames(tasks))
		}
	}
	return &signalResultRig{eng: eng, state: state, queue: queue, id: id}
}

func (r *signalResultRig) deliver(t *testing.T, name string) SignalDelivery {
	t.Helper()
	got, err := r.eng.DeliverSignalResult(context.Background(), r.id, name, map[string]any{"by": "tester"})
	if err != nil {
		t.Fatalf("DeliverSignalResult(%q) error = %v", name, err)
	}
	return got
}

func TestDeliverSignalResult(t *testing.T) {
	t.Run("resumed when a node waits for the signal", func(t *testing.T) {
		r := newSignalResultRig(t, "test.suspend", true)
		got := r.deliver(t, "approval")
		if got.Outcome != SignalOutcomeResumed {
			t.Fatalf("DeliverSignalResult(approval).Outcome = %q, want %q", got.Outcome, SignalOutcomeResumed)
		}
		if tasks := r.queue.Drain(); len(tasks) != 1 || tasks[0].Type != TaskTypeNodeResume {
			t.Fatalf("tasks after a resumed signal = %v, want one resume", taskNames(tasks))
		}
	})

	t.Run("buffered when the node is not suspended yet", func(t *testing.T) {
		r := newSignalResultRig(t, "test.suspend", false)
		got := r.deliver(t, "approval")
		if got.Outcome != SignalOutcomeBuffered {
			t.Fatalf("DeliverSignalResult(approval).Outcome = %q, want %q", got.Outcome, SignalOutcomeBuffered)
		}
	})

	t.Run("buffered when no suspended node waits for the name", func(t *testing.T) {
		// The case that motivated the outcome: a signal whose name matches
		// nothing is stored, not rejected, and nothing marks it as unmatched.
		r := newSignalResultRig(t, "test.suspend", true)
		got := r.deliver(t, "not-the-approval-name")
		if got.Outcome != SignalOutcomeBuffered {
			t.Fatalf("DeliverSignalResult(unknown name).Outcome = %q, want %q", got.Outcome, SignalOutcomeBuffered)
		}
		if tasks := r.queue.Drain(); len(tasks) != 0 {
			t.Fatalf("tasks after a buffered signal = %v, want none", taskNames(tasks))
		}
		// The parked node is still parked, so the right name still wakes it.
		if got := r.deliver(t, "approval"); got.Outcome != SignalOutcomeResumed {
			t.Fatalf("DeliverSignalResult(approval).Outcome = %q, want %q", got.Outcome, SignalOutcomeResumed)
		}
	})

	t.Run("buffered while a quorum is incomplete", func(t *testing.T) {
		r := newSignalResultRig(t, "test.multi_signal", true)
		if got := r.deliver(t, "sec"); got.Outcome != SignalOutcomeBuffered {
			t.Fatalf("first signal of a quorum of 2: Outcome = %q, want %q", got.Outcome, SignalOutcomeBuffered)
		}
		if got := r.deliver(t, "app"); got.Outcome != SignalOutcomeResumed {
			t.Fatalf("second signal of a quorum of 2: Outcome = %q, want %q", got.Outcome, SignalOutcomeResumed)
		}
	})

	t.Run("inactive after the execution is canceled", func(t *testing.T) {
		r := newSignalResultRig(t, "test.suspend", true)
		if err := r.eng.Cancel(context.Background(), r.id); err != nil {
			t.Fatalf("Cancel() error = %v", err)
		}
		got := r.deliver(t, "approval")
		if got.Outcome != SignalOutcomeInactive {
			t.Fatalf("DeliverSignalResult(approval) after cancel: Outcome = %q, want %q", got.Outcome, SignalOutcomeInactive)
		}
		if tasks := r.queue.Drain(); len(tasks) != 0 {
			t.Fatalf("tasks after a signal to a canceled execution = %v, want none", taskNames(tasks))
		}
	})

	t.Run("inactive when the waiter is found on a finished execution", func(t *testing.T) {
		// The window between the terminal status write and the waiter cleanup:
		// the store still hands back the parked node, but the execution is over.
		r := newSignalResultRig(t, "test.suspend", true)
		if err := r.state.UpdateExecutionStatus(context.Background(), r.id, types.ExecutionStatusCanceled, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
		}
		got := r.deliver(t, "approval")
		if got.Outcome != SignalOutcomeInactive {
			t.Fatalf("DeliverSignalResult(approval) in the window: Outcome = %q, want %q", got.Outcome, SignalOutcomeInactive)
		}
		if tasks := r.queue.Drain(); len(tasks) != 0 {
			t.Fatalf("tasks after a signal to a finished execution = %v, want none", taskNames(tasks))
		}
	})

	t.Run("inactive for an execution that never existed", func(t *testing.T) {
		r := newSignalResultRig(t, "test.suspend", true)
		got, err := r.eng.DeliverSignalResult(context.Background(), "no-such-execution", "approval", nil)
		if err != nil {
			t.Fatalf("DeliverSignalResult(unknown execution) error = %v", err)
		}
		if got.Outcome != SignalOutcomeInactive {
			t.Fatalf("DeliverSignalResult(unknown execution).Outcome = %q, want %q", got.Outcome, SignalOutcomeInactive)
		}
	})
}

// TestDeliverSignalIsAThinWrapper pins that DeliverSignal still returns only an
// error and behaves exactly as before for every outcome.
func TestDeliverSignalIsAThinWrapper(t *testing.T) {
	r := newSignalResultRig(t, "test.suspend", true)
	ctx := context.Background()
	if err := r.eng.DeliverSignal(ctx, r.id, "other", nil); err != nil {
		t.Fatalf("DeliverSignal(buffered) error = %v", err)
	}
	if err := r.eng.DeliverSignal(ctx, r.id, "approval", nil); err != nil {
		t.Fatalf("DeliverSignal(resumed) error = %v", err)
	}
	if tasks := r.queue.Drain(); len(tasks) != 1 || tasks[0].Type != TaskTypeNodeResume {
		t.Fatalf("tasks = %v, want one resume", taskNames(tasks))
	}
}
