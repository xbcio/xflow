package rstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// orphanObservation is one OnConcurrencyOrphanReclaimed call.
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

func orphanForgetExecution(t *testing.T, ctx context.Context, rdb *redis.Client, id types.ExecutionID) {
	t.Helper()
	if err := rdb.Del(ctx,
		execKey(namespace.Default, id, "status"),
		execKey(namespace.Default, id, "graph"),
		execKey(namespace.Default, id, "updated_at_ms"),
		execKey(namespace.Default, id, "workflow_id"),
	).Err(); err != nil {
		t.Fatalf("forget execution %q: %v", id, err)
	}
}

// TestOrphanObserverReportsSweepAndOperatorReset pins the reason label the
// Redis store publishes for the two automatic/operator paths an operator has to
// tell apart: the periodic sweep reports sweep, and include_live reports
// operator_reset.
func TestOrphanObserverReportsSweepAndOperatorReset(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, _ := newOrphanStore(t)
	obs := &orphanObserverRecorder{}
	state.SetOrphanReclaimObserver(obs)

	g := orphanGraphLimit(t, "orphan-observer-redis", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	waiting, _ := orphanSubmit(t, ctx, state, g, 1)
	orphanDeleteGraph(t, ctx, rdb, holder)
	orphanAgeStatus(t, ctx, rdb, holder, time.Minute)

	res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Workflow: g.Name(),
		Grace: time.Second, Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("sweep Reclaimed = %d, want 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	events := obs.snapshot()
	if len(events) != 1 || events[0] != (orphanObservation{kind: engine.OrphanKindRunning, reason: engine.OrphanTriggerSweep}) {
		t.Fatalf("sweep observations = %+v, want exactly [{running sweep}]", events)
	}
	if got := orphanStatus(t, ctx, state, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("waiting status = %q, want promoted running", got)
	}

	res = orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Workflow: g.Name(),
		IncludeLive: true, Reason: engine.OrphanTriggerReset, Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("reset Reclaimed = %d, want 1", res.Reclaimed)
	}
	events = obs.snapshot()
	if len(events) != 2 || events[1] != (orphanObservation{kind: engine.OrphanKindRunning, reason: engine.OrphanTriggerReset}) {
		t.Fatalf("reset observations = %+v, want second entry {running operator_reset}", events)
	}
	if got := orphanStatus(t, ctx, state, waiting); got != types.ExecutionStatusCanceled {
		t.Fatalf("waiting status after reset = %q, want canceled", got)
	}
}

// TestOrphanObserverReportsQueuedAndMissingKinds covers the other two bounded
// kind values: a queued graph-lost member reclaimed by Cancel self-heal, and a
// dangling queue member whose execution record is already gone.
func TestOrphanObserverReportsQueuedAndMissingKinds(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, _ := newOrphanStore(t)
	obs := &orphanObserverRecorder{}
	state.SetOrphanReclaimObserver(obs)

	g := orphanGraphLimit(t, "orphan-observer-kinds", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	queued, _ := orphanSubmit(t, ctx, state, g, 1)
	orphanDeleteGraph(t, ctx, rdb, queued)
	res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: queued, Namespace: "default",
		Reason: engine.OrphanTriggerCancel, Limit: 1,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("queued reclaim Reclaimed = %d, want 1", res.Reclaimed)
	}
	events := obs.snapshot()
	if len(events) != 1 || events[0] != (orphanObservation{kind: engine.OrphanKindQueued, reason: engine.OrphanTriggerCancel}) {
		t.Fatalf("queued observation = %+v, want [{queued cancel}]", events)
	}
	if got := orphanStatus(t, ctx, state, holder); got != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want untouched running", got)
	}

	g2 := orphanGraphLimit(t, "orphan-observer-missing", 1)
	holder2, _ := orphanSubmit(t, ctx, state, g2, 1)
	gone, _ := orphanSubmit(t, ctx, state, g2, 1)
	orphanForgetExecution(t, ctx, rdb, gone)
	res = orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Workflow: g2.Name(),
		Reason: engine.OrphanTriggerSweep, Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("missing-member Reclaimed = %d, want 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	candidate := orphanCandidateFor(res, gone)
	if candidate == nil || candidate.Kind() != engine.OrphanKindMissing {
		t.Fatalf("candidate = %+v, want kind %q", candidate, engine.OrphanKindMissing)
	}
	events = obs.snapshot()
	if len(events) != 2 || events[1] != (orphanObservation{kind: engine.OrphanKindMissing, reason: engine.OrphanTriggerSweep}) {
		t.Fatalf("missing observation = %+v, want second entry {missing sweep}", events)
	}
	if got := orphanStatus(t, ctx, state, holder2); got != types.ExecutionStatusRunning {
		t.Fatalf("second holder status = %q, want untouched running", got)
	}
}

// TestOrphanObserverReportsPromotionReclaim drives the third automatic reclaim
// point: a queued member whose execution record is gone is reaped when the
// holder's terminal write promotes the line, and the observer must report
// reason=promotion (not sweep) with kind=missing.
func TestOrphanObserverReportsPromotionReclaim(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, _ := newOrphanStore(t)
	obs := &orphanObserverRecorder{}
	state.SetOrphanReclaimObserver(obs)

	g := orphanGraphLimit(t, "orphan-observer-promotion", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	gone, _ := orphanSubmit(t, ctx, state, g, 1)
	orphanForgetExecution(t, ctx, rdb, gone)

	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	events := obs.snapshot()
	if len(events) != 1 || events[0] != (orphanObservation{kind: engine.OrphanKindMissing, reason: engine.OrphanTriggerPromotion}) {
		t.Fatalf("promotion observations = %+v, want exactly [{missing promotion}]", events)
	}
}
