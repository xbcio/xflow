package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type leaseObserverRecorder struct {
	acquireResults []string
	scanCalls      int
	repairCalls    int
}

func (r *leaseObserverRecorder) OnLeaseAcquire(ctx context.Context, result string, _ time.Duration) {
	r.acquireResults = append(r.acquireResults, result)
}

func (r *leaseObserverRecorder) OnLeaseExpiryScan(_ context.Context, _ int, _ time.Duration, _ error) {
	r.scanCalls++
}

func (r *leaseObserverRecorder) OnLeaseRepair(_ context.Context, _ int, _ time.Duration, _ error) {
	r.repairCalls++
}

func TestRedisLeaseObserverReceivesLifecycleEvents(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	recorder := &leaseObserverRecorder{}
	state.leaseObserver = recorder
	ctx := context.Background()
	lease := &engine.TaskLease{
		LeaseID:    "lease-observer",
		LeaseToken: "token-observer",
		IssuedAt:   time.Now().Add(-time.Second).UTC(),
		TTL:        time.Millisecond,
		Task: engine.Task{
			ExecutionID: types.ExecutionID("lease-observer-execution"),
			NodeName:    "node",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	if len(recorder.acquireResults) != 1 || recorder.acquireResults[0] != "acquired" {
		t.Fatalf("lease acquire observations = %v, want [acquired]", recorder.acquireResults)
	}
	if _, err := state.ListExpiredLeases(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if recorder.scanCalls != 1 {
		t.Fatalf("lease expiry scan calls = %d, want 1", recorder.scanCalls)
	}
	if _, err := state.RepairLeaseIndex(ctx, 8); err != nil {
		t.Fatalf("RepairLeaseIndex() error = %v", err)
	}
	if recorder.repairCalls != 1 {
		t.Fatalf("lease repair calls = %d, want 1", recorder.repairCalls)
	}
}
