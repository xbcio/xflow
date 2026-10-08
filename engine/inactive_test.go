package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// terminalMarkerState is fakeState plus ExecutionTerminalReader, the optional
// capability a TTL-backed store implements to keep a terminal verdict readable
// after the status key has expired.
type terminalMarkerState struct {
	*fakeState
	terminals map[types.ExecutionID]types.ExecutionStatus
}

func newTerminalMarkerState() *terminalMarkerState {
	return &terminalMarkerState{
		fakeState: newFakeState(),
		terminals: make(map[types.ExecutionID]types.ExecutionStatus),
	}
}

func (s *terminalMarkerState) GetExecutionTerminalStatus(_ context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	status, ok := s.terminals[id]
	return status, ok, nil
}

// failingStatusState answers the narrow read with an error, the shape a Redis
// fault takes on the classification path.
type failingStatusState struct {
	*fakeState
	err error
}

func (s *failingStatusState) GetExecutionStatus(context.Context, types.ExecutionID) (types.ExecutionStatus, bool, error) {
	return "", false, s.err
}

// graphlessState keeps an execution's status record but never serves a graph,
// the torn state loadActiveGraph's cache-miss branch describes.
type graphlessState struct {
	*fakeState
}

func (s *graphlessState) LoadGraph(context.Context, types.ExecutionID) (*graph.Graph, error) {
	return nil, nil
}

func submitOneTaskEngine(t *testing.T, state StateStore) (*Engine, *fakeQueue, types.ExecutionID, *Task) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "inactive-classification",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
	eng := newTestEngine(t, state, queue, reg)
	id, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}
	return eng, queue, id, tasks[0]
}

// TestTaskRoutingClassifiesGoneExecution pins the loss verdict: when every
// trace of the execution is gone (the TTL-reclaim shape) TaskRouting must
// return a typed ExecutionInactiveError{Kind: gone} that still satisfies the
// sentinel contract every existing caller relies on.
func TestTaskRoutingClassifiesGoneExecution(t *testing.T) {
	ctx := context.Background()
	for _, cached := range []bool{true, false} {
		t.Run(map[bool]string{true: "graph cached", false: "graph evicted"}[cached], func(t *testing.T) {
			state := newFakeState()
			eng, _, id, task := submitOneTaskEngine(t, state)

			// Simulate the TTL reclaim: the execution's keys are gone.
			state.mu.Lock()
			delete(state.executions, id)
			state.mu.Unlock()
			if !cached {
				eng.EvictExecution(id)
			}

			_, err := eng.TaskRouting(ctx, task)
			if !errors.Is(err, ErrExecutionInactive) {
				t.Fatalf("TaskRouting() error = %v, want ErrExecutionInactive", err)
			}
			var inactive *ExecutionInactiveError
			if !errors.As(err, &inactive) {
				t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveError", err)
			}
			if inactive.Kind != ExecutionInactiveGone {
				t.Fatalf("Kind = %q, want %q", inactive.Kind, ExecutionInactiveGone)
			}
			if inactive.ExecutionID != id {
				t.Fatalf("ExecutionID = %q, want %q", inactive.ExecutionID, id)
			}
		})
	}
}

// TestTaskRoutingClassifiesTerminalExecution pins the benign verdict for every
// terminal status and both graph-cache states.
func TestTaskRoutingClassifiesTerminalExecution(t *testing.T) {
	ctx := context.Background()
	terminals := []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout,
	}
	for _, terminal := range terminals {
		for _, cached := range []bool{true, false} {
			t.Run(string(terminal)+"/"+map[bool]string{true: "cached", false: "evicted"}[cached], func(t *testing.T) {
				state := newFakeState()
				eng, _, id, task := submitOneTaskEngine(t, state)
				if err := state.UpdateExecutionStatus(ctx, id, terminal, "terminated by test"); err != nil {
					t.Fatalf("UpdateExecutionStatus() error = %v", err)
				}
				if !cached {
					eng.EvictExecution(id)
				}

				_, err := eng.TaskRouting(ctx, task)
				if !errors.Is(err, ErrExecutionInactive) {
					t.Fatalf("TaskRouting() error = %v, want ErrExecutionInactive", err)
				}
				var inactive *ExecutionInactiveError
				if !errors.As(err, &inactive) {
					t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveError", err)
				}
				if inactive.Kind != ExecutionInactiveTerminal {
					t.Fatalf("Kind = %q, want %q", inactive.Kind, ExecutionInactiveTerminal)
				}
				if inactive.Status != terminal {
					t.Fatalf("Status = %q, want %q", inactive.Status, terminal)
				}
			})
		}
	}
}

// TestTaskRoutingTerminalMarkerCoversExpiredStatus pins the tombstone path: a
// terminal execution whose status key has expired is still classified terminal
// (benign) because the marker outlives it. Without this, any backlog that
// outlives the completion TTL would report every late duplicate as lost work.
func TestTaskRoutingTerminalMarkerCoversExpiredStatus(t *testing.T) {
	ctx := context.Background()
	state := newTerminalMarkerState()
	eng, _, id, task := submitOneTaskEngine(t, state)

	// Terminal, status record expired (deleted), marker retained.
	state.mu.Lock()
	delete(state.executions, id)
	state.mu.Unlock()
	eng.EvictExecution(id)
	state.terminals[id] = types.ExecutionStatusCanceled

	_, err := eng.TaskRouting(ctx, task)
	var inactive *ExecutionInactiveError
	if !errors.As(err, &inactive) {
		t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveError", err)
	}
	if inactive.Kind != ExecutionInactiveTerminal {
		t.Fatalf("Kind = %q, want %q (a terminal marker must keep a late duplicate out of the gone bucket)",
			inactive.Kind, ExecutionInactiveTerminal)
	}
	if inactive.Status != types.ExecutionStatusCanceled {
		t.Fatalf("Status = %q, want canceled", inactive.Status)
	}
}

// TestTaskRoutingClassifiesTornState pins the surviving-status-but-no-graph
// case: the status says the execution is alive, but there is nothing to route
// against, so the verdict is gone (with the status attached for diagnosis).
func TestTaskRoutingClassifiesTornState(t *testing.T) {
	ctx := context.Background()
	state := &graphlessState{fakeState: newFakeState()}
	eng, _, id, task := submitOneTaskEngine(t, state)
	// Drop the graph cache so the cache-miss branch is the one under test:
	// there, a nil graph reports inactive without consulting the status.
	eng.EvictExecution(id)

	_, err := eng.TaskRouting(ctx, task)
	var inactive *ExecutionInactiveError
	if !errors.As(err, &inactive) {
		t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveError", err)
	}
	if inactive.Kind != ExecutionInactiveGone {
		t.Fatalf("Kind = %q, want %q", inactive.Kind, ExecutionInactiveGone)
	}
	if inactive.Status != types.ExecutionStatusRunning {
		t.Fatalf("Status = %q, want running (attached for diagnosis)", inactive.Status)
	}
}

// TestTaskRoutingKeepsNodeRouteStalenessUnclassified pins the boundary: a
// stale node route on a live execution is a duplicate/delayed delivery, not
// lost work, and must keep returning the bare sentinel so no delivery layer
// counts it as loss.
func TestTaskRoutingKeepsNodeRouteStalenessUnclassified(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng, _, id, task := submitOneTaskEngine(t, state)

	if err := state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusSuccess,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	eng.EvictExecution(id)

	_, err := eng.TaskRouting(ctx, task)
	if !errors.Is(err, ErrExecutionInactive) {
		t.Fatalf("TaskRouting() error = %v, want ErrExecutionInactive", err)
	}
	var inactive *ExecutionInactiveError
	if errors.As(err, &inactive) {
		t.Fatalf("stale node route was classified (%+v); duplicate/stale deliveries must never "+
			"be counted as lost work", inactive)
	}
}

// TestTaskRoutingClassifyReadErrorIsNotInactive pins the failure mode a Redis
// blip produces: the verdict must NOT be folded into the inactive sentinel (a
// caller that treats it as "execution is over" would drop live work), and it
// must be distinguishable so the dispatcher can count it as classify_error.
func TestTaskRoutingClassifyReadErrorIsNotInactive(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("redis: connection refused")
	state := &failingStatusState{fakeState: newFakeState(), err: readErr}
	eng := New(state, &fakeQueue{})

	_, err := eng.TaskRouting(ctx, &Task{ExecutionID: "exec-1", NodeName: "start", NodeIdx: 0})
	if errors.Is(err, ErrExecutionInactive) {
		t.Fatalf("TaskRouting() error = %v: a store read failure was reported as an inactive execution", err)
	}
	var classify *ExecutionInactiveClassifyError
	if !errors.As(err, &classify) {
		t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveClassifyError", err)
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("TaskRouting() error = %v, want it to wrap the underlying read error", err)
	}
}

// retentionState is fakeState plus the two optional readers a TTL-backed store
// implements: the terminal marker and the retention window that bounds how far
// a "no evidence" verdict can be trusted. unknown models the backend that
// cannot confirm the retention its writes used (ExecutionRetentionUnknown).
type retentionState struct {
	*fakeState
	window    time.Duration
	windowErr error
	unknown   bool
	terminal  map[types.ExecutionID]types.ExecutionStatus
}

func newRetentionState(window time.Duration) *retentionState {
	return &retentionState{
		fakeState: newFakeState(),
		window:    window,
		terminal:  make(map[types.ExecutionID]types.ExecutionStatus),
	}
}

func (s *retentionState) GetExecutionTerminalStatus(_ context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	status, ok := s.terminal[id]
	return status, ok, nil
}

func (s *retentionState) GetExecutionRetention(context.Context, types.ExecutionID) (time.Duration, error) {
	if s.unknown {
		return ExecutionRetentionUnknown, nil
	}
	return s.window, s.windowErr
}

// reclaimExecution deletes every trace of the execution and drops the engine's
// graph cache, the TTL-reclaim shape the classifier runs on.
func reclaimExecution(t *testing.T, state *retentionState, eng *Engine, id types.ExecutionID) {
	t.Helper()
	state.mu.Lock()
	delete(state.executions, id)
	state.mu.Unlock()
	eng.EvictExecution(id)
}

func routingInactiveKind(t *testing.T, eng *Engine, task *Task) ExecutionInactiveKind {
	t.Helper()
	_, err := eng.TaskRouting(context.Background(), task)
	if !errors.Is(err, ErrExecutionInactive) {
		t.Fatalf("TaskRouting() error = %v, want ErrExecutionInactive", err)
	}
	var inactive *ExecutionInactiveError
	if !errors.As(err, &inactive) {
		t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveError", err)
	}
	return inactive.Kind
}

// TestTaskRoutingBacklogBeyondRetentionIsUnattributed is the incident-scale
// case that motivated the classification split: with the terminal marker
// living exactly one active retention (and durable executions carrying no
// marker at all), a backlog longer than the retention makes a benign late
// duplicate of a long-finished execution indistinguishable from work that
// expired under its queue. A delivery whose wait is far beyond the window must
// therefore NOT be reported as gone: it is unattributed, the honest "cannot
// tell" bucket, and the counter/help say that.
func TestTaskRoutingBacklogBeyondRetentionIsUnattributed(t *testing.T) {
	const window = 10 * time.Minute
	state := newRetentionState(window)
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	// The backlog: deliverable two hours ago, >10x the marker's lifetime.
	task.DeliverableAt = time.Now().Add(-2 * time.Hour)

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: a wait of 2h against a %v retention window cannot be "+
			"reported as provable loss (that is the false positive this split removes)",
			kind, ExecutionInactiveUnattributed, window)
	}
}

// TestTaskRoutingWithinRetentionStaysGone pins the other half of the split: a
// stamped delivery that waited no longer than the window is still provable
// loss when no evidence exists, because a terminal transition during that wait
// would still be readable.
func TestTaskRoutingWithinRetentionStaysGone(t *testing.T) {
	state := newRetentionState(10 * time.Minute)
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	task.DeliverableAt = time.Now().Add(-time.Minute)

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveGone {
		t.Fatalf("Kind = %q, want %q: the wait is inside the evidence window, so absence "+
			"of a marker means the execution did not finish", kind, ExecutionInactiveGone)
	}
}

// TestTaskRoutingUnknownRetentionIsUnattributed pins the conservative half of
// the window contract: when the backend cannot confirm the retention its
// writes actually used (ExecutionRetentionUnknown), no age can be compared
// against a window that is not provable, so even a delivery that looks young
// lands in unattributed instead of a gone claim. Reporting the fallback here
// is exactly the over-report this signal removes: the fallback can name a
// longer TTL than the one the execution's terminal evidence lived by.
func TestTaskRoutingUnknownRetentionIsUnattributed(t *testing.T) {
	state := newRetentionState(10 * time.Minute)
	state.unknown = true
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	task.DeliverableAt = time.Now().Add(-time.Minute)

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: an unprovable retention window must not be "+
			"compared against, no matter how short the wait", kind, ExecutionInactiveUnattributed)
	}
}

// TestTaskRoutingDelayedIntentOutsideRetentionIsUnattributed is the R1
// false-positive case: a workflow suspended on a two-hour timer is cancelled
// at t0+5m (its marker, written with the 10m retention, is gone by t0+15m),
// and the delayed resume intent fires at t0+2h. Its DeliverableAt is the
// availability instant — measured from that, the delivery looks brand new —
// but its creation anchor is 12 windows old, so a benign drop must classify
// unattributed, not gone. This is the shape the documented
// rate(...{reason="execution_gone"}) > 0 page used to fire on.
func TestTaskRoutingDelayedIntentOutsideRetentionIsUnattributed(t *testing.T) {
	const window = 10 * time.Minute
	state := newRetentionState(window)
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	// The cancelled-timer delivery at the moment it fires: available now,
	// created when the timer was armed two hours earlier.
	task.DeliverableAt = time.Now()
	task.IntentCreatedAt = time.Now().Add(-2 * time.Hour)

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: a delayed intent's availability stamp is not its age, "+
			"and a benign cancelled timer must not be paged as provable loss",
			kind, ExecutionInactiveUnattributed)
	}
}

// TestTaskRoutingDelayedIntentInsideRetentionStaysGone pins the other
// direction of the R1 fix: measuring age from the intent's creation must not
// weaken the loss verdict for a delayed intent that really is young. A short
// timer whose execution expired under it (no status, no marker, created one
// minute ago against a ten-minute window) is still gone.
func TestTaskRoutingDelayedIntentInsideRetentionStaysGone(t *testing.T) {
	const window = 10 * time.Minute
	state := newRetentionState(window)
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	task.DeliverableAt = time.Now()
	task.IntentCreatedAt = time.Now().Add(-time.Minute)

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveGone {
		t.Fatalf("Kind = %q, want %q: the intent was created inside the evidence window, "+
			"so a terminal transition during its wait would still be readable",
			kind, ExecutionInactiveGone)
	}
}

// TestTaskRoutingUnstampedDeliveryOnRetentionBoundedStoreIsUnattributed pins
func TestTaskRoutingUnstampedDeliveryOnRetentionBoundedStoreIsUnattributed(t *testing.T) {
	state := newRetentionState(10 * time.Minute)
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	task.DeliverableAt = time.Time{}

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: without a deliverable stamp the wait cannot be "+
			"placed inside the retention window", kind, ExecutionInactiveUnattributed)
	}
}

// TestTaskRoutingTerminalMarkerBeatsWindow pins the ordering: when the marker
// exists the verdict is terminal regardless of how long the delivery waited,
// so the tri-state only ever applies to the no-evidence branch.
func TestTaskRoutingTerminalMarkerBeatsWindow(t *testing.T) {
	state := newRetentionState(10 * time.Minute)
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)
	state.terminal[id] = types.ExecutionStatusSuccess

	task.DeliverableAt = time.Now().Add(-2 * time.Hour)

	if kind := routingInactiveKind(t, eng, task); kind != ExecutionInactiveTerminal {
		t.Fatalf("Kind = %q, want %q: the retained marker is positive evidence, the window "+
			"only bounds the no-evidence branch", kind, ExecutionInactiveTerminal)
	}
}

// TestTaskRoutingRetentionReadErrorIsClassifyError pins that a failed read of
// the window is not silently treated as "no window" (which would restore the
// false-positive gone verdict) and not folded into the sentinel: it is a
// classification read failure, exactly like a failed marker read.
func TestTaskRoutingRetentionReadErrorIsClassifyError(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("redis: connection refused")
	state := newRetentionState(time.Minute)
	state.windowErr = readErr
	eng, _, id, task := submitOneTaskEngine(t, state)
	reclaimExecution(t, state, eng, id)

	_, err := eng.TaskRouting(ctx, task)
	if errors.Is(err, ErrExecutionInactive) {
		t.Fatalf("TaskRouting() error = %v: a window read failure was reported as an inactive execution", err)
	}
	var classify *ExecutionInactiveClassifyError
	if !errors.As(err, &classify) {
		t.Fatalf("TaskRouting() error = %v, want *ExecutionInactiveClassifyError", err)
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("TaskRouting() error = %v, want it to wrap the underlying read error", err)
	}
}

// TestHandleSystemTaskClassifiesInactiveExecution pins the system-task half of
// the classification: an advance or skip intent whose execution is inactive is
// a classified drop (handled=true so it is never routed, error carrying the
// kind so the delivery sink can count it), not the silent (true, nil) success
// it used to be.
func TestHandleSystemTaskClassifiesInactiveExecution(t *testing.T) {
	ctx := context.Background()
	for _, typ := range []TaskType{TaskTypeNodeAdvance, TaskTypeNodeSkip} {
		t.Run(fmt.Sprintf("task_type_%d", typ), func(t *testing.T) {
			state := newFakeState()
			eng, _, id, task := submitOneTaskEngine(t, state)
			state.mu.Lock()
			delete(state.executions, id)
			state.mu.Unlock()
			eng.EvictExecution(id)

			system := &Task{
				ExecutionID:  id,
				NodeName:     task.NodeName,
				NodeIdx:      task.NodeIdx,
				UnitIdx:      task.UnitIdx,
				Type:         typ,
				ActivationID: task.ActivationID,
			}
			handled, err := eng.HandleSystemTask(ctx, system)
			if !handled {
				t.Fatal("HandleSystemTask reported the task unhandled; the engine owns advance/skip and must never route it")
			}
			if err == nil {
				t.Fatal("HandleSystemTask returned (true, nil) for an inactive execution: the drop is still silent")
			}
			var inactive *ExecutionInactiveError
			if !errors.As(err, &inactive) {
				t.Fatalf("HandleSystemTask error = %v, want *ExecutionInactiveError", err)
			}
			if !errors.Is(err, ErrExecutionInactive) {
				t.Fatalf("HandleSystemTask error = %v, want it to carry ErrExecutionInactive", err)
			}
			if inactive.Kind != ExecutionInactiveGone {
				t.Fatalf("Kind = %q, want %q (the fake store is non-expiring, so absence is conclusive)",
					inactive.Kind, ExecutionInactiveGone)
			}
		})
	}
}
