package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// orphanObservation is one OnConcurrencyOrphanReclaimed call, kept small so the
// assertions read like the metric labels the observer produces.
type orphanObservation struct {
	kind   string
	reason string
}

type orphanObserverRecorder struct {
	mu     sync.Mutex
	events []orphanObservation
}

func (r *orphanObserverRecorder) OnConcurrencyOrphanReclaimed(_ context.Context, kind, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, orphanObservation{kind: kind, reason: reason})
}

func (r *orphanObserverRecorder) snapshot() []orphanObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]orphanObservation(nil), r.events...)
}

func orphanObserverGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit: 1,
			Mode:  types.ConcurrencyModeQueue,
		}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// TestMemoryOrphanObserverReportsSweepAndOperatorReset pins the reason/kinds the
// in-memory store publishes. The periodic sweep must report reason=sweep, and an
// include_live operator reset must report kind=running, reason=operator_reset —
// the distinction that keeps an operator-forced termination auditable as itself
// rather than as an automatic orphan repair.
func TestMemoryOrphanObserverReportsSweepAndOperatorReset(t *testing.T) {
	state := newMemoryState()
	obs := &orphanObserverRecorder{}
	state.orphanObserver = obs

	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	g := orphanObserverGraph(t, "orphan-observer-local")
	admission := engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     1,
		Mode:      types.ConcurrencyModeQueue,
	}
	submit := func(id types.ExecutionID) {
		t.Helper()
		if _, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		}, nil, admission); err != nil {
			t.Fatalf("submit %q error = %v", id, err)
		}
	}
	holder := engine.NewExecutionID()
	waiting := engine.NewExecutionID()
	submit(holder)
	submit(waiting)

	// Torn shape, aged past the grace so a sweep (not force) judges it.
	state.mu.Lock()
	if entry := state.executions[holder]; entry != nil {
		entry.snap.Graph = nil
		entry.snap.UpdatedAt = time.Now().UTC().Add(-time.Minute)
	}
	state.mu.Unlock()

	res, err := state.ReclaimConcurrencyOrphans(ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Workflow: g.Name(),
		Grace: time.Second, Limit: 8,
	})
	if err != nil {
		t.Fatalf("sweep error = %v", err)
	}
	if res.Reclaimed != 1 {
		t.Fatalf("sweep Reclaimed = %d, want 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	events := obs.snapshot()
	if len(events) != 1 || events[0] != (orphanObservation{kind: engine.OrphanKindRunning, reason: engine.OrphanTriggerSweep}) {
		t.Fatalf("sweep observations = %+v, want exactly [{running sweep}]", events)
	}
	// The freed slot must have gone to the queued member.
	snap, err := state.GetExecution(ctx, waiting)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("waiting execution = %+v (err %v), want promoted running", snap, err)
	}

	// Explicit operator reset with include_live converges the now-live waiter.
	res, err = state.ReclaimConcurrencyOrphans(ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Workflow: g.Name(),
		IncludeLive: true, Reason: engine.OrphanTriggerReset, Limit: 8,
	})
	if err != nil {
		t.Fatalf("operator reset error = %v", err)
	}
	if res.Reclaimed != 1 {
		t.Fatalf("operator reset Reclaimed = %d, want 1", res.Reclaimed)
	}
	events = obs.snapshot()
	if len(events) != 2 {
		t.Fatalf("observations after reset = %+v, want 2", events)
	}
	if got := events[1]; got != (orphanObservation{kind: engine.OrphanKindRunning, reason: engine.OrphanTriggerReset}) {
		t.Fatalf("reset observation = %+v, want {running operator_reset}", got)
	}
	snap, err = state.GetExecution(ctx, waiting)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("waiting execution after reset = %+v (err %v), want canceled", snap, err)
	}
}

// TestMemoryOrphanObserverReportsPromotionReclaim drives the promotion reclaim
// point: a queued member whose execution record is gone is reaped when the
// holder's terminal write promotes the line, and the observer reports
// reason=promotion with kind=missing.
func TestMemoryOrphanObserverReportsPromotionReclaim(t *testing.T) {
	state := newMemoryState()
	obs := &orphanObserverRecorder{}
	state.orphanObserver = obs

	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	g := orphanObserverGraph(t, "orphan-observer-promotion-local")
	admission := engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     1,
		Mode:      types.ConcurrencyModeQueue,
	}
	submit := func(id types.ExecutionID) {
		t.Helper()
		if _, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
			ID: id, Namespace: "default", Graph: g, Status: types.ExecutionStatusRunning,
		}, nil, admission); err != nil {
			t.Fatalf("submit %q error = %v", id, err)
		}
	}
	holder := engine.NewExecutionID()
	gone := engine.NewExecutionID()
	submit(holder)
	submit(gone)

	// Delete only the execution record; the queue membership stays, which is
	// the dangling shape promotion has to reap.
	state.mu.Lock()
	delete(state.executions, gone)
	state.mu.Unlock()

	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	events := obs.snapshot()
	if len(events) != 1 || events[0] != (orphanObservation{kind: engine.OrphanKindMissing, reason: engine.OrphanTriggerPromotion}) {
		t.Fatalf("promotion observations = %+v, want exactly [{missing promotion}]", events)
	}
}
