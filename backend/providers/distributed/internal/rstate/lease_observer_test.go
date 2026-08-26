package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// leaseObserverRecorder keeps every argument it is handed. The counts alone
// answer "was the observer called"; the values answer "was it told the truth",
// which is the only question the metrics on the other end care about --
// observability/metrics/asynq.go:73,85 turn candidates and reconciled into
// gauges and branch the result label on err.
type leaseObserverRecorder struct {
	acquireResults   []string
	scanCalls        int
	scanCandidates   []int
	scanErrs         []error
	repairCalls      int
	repairReconciled []int
	repairErrs       []error
}

func (r *leaseObserverRecorder) OnLeaseAcquire(ctx context.Context, result string, _ time.Duration) {
	r.acquireResults = append(r.acquireResults, result)
}

func (r *leaseObserverRecorder) OnLeaseExpiryScan(_ context.Context, candidates int, _ time.Duration, err error) {
	r.scanCalls++
	r.scanCandidates = append(r.scanCandidates, candidates)
	r.scanErrs = append(r.scanErrs, err)
}

func (r *leaseObserverRecorder) OnLeaseRepair(_ context.Context, reconciled int, _ time.Duration, err error) {
	r.repairCalls++
	r.repairReconciled = append(r.repairReconciled, reconciled)
	r.repairErrs = append(r.repairErrs, err)
}

func TestRedisLeaseObserverReceivesLifecycleEvents(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
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
	// The lease above was issued a second ago with a 1ms TTL, so this scan finds
	// exactly it.
	expired, err := state.ListExpiredLeases(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if recorder.scanCalls != 1 {
		t.Fatalf("lease expiry scan calls = %d, want 1", recorder.scanCalls)
	}
	if len(expired) != 1 {
		t.Fatalf("ListExpiredLeases() returned %d leases, want 1", len(expired))
	}
	if recorder.scanCandidates[0] != 1 {
		t.Errorf("observed candidates = %d, want 1: this number is published "+
			"verbatim as the lease_expiry_candidates gauge, and a wrong value "+
			"there is worse than a missing one -- an operator reading 0 while "+
			"leases pile up has no signal that anything is wrong",
			recorder.scanCandidates[0])
	}
	if recorder.scanErrs[0] != nil {
		t.Errorf("observed scan err = %v on a successful scan, want nil: the "+
			"metrics layer branches its result label on exactly this value",
			recorder.scanErrs[0])
	}

	reconciled, err := state.RepairLeaseIndex(ctx, 8)
	if err != nil {
		t.Fatalf("RepairLeaseIndex() error = %v", err)
	}
	if recorder.repairCalls != 1 {
		t.Fatalf("lease repair calls = %d, want 1", recorder.repairCalls)
	}
	if recorder.repairReconciled[0] != reconciled {
		t.Errorf("observed reconciled = %d, want %d (what RepairLeaseIndex "+
			"returned): the observation is emitted from a deferred closure over "+
			"the named return, so a mismatch means the deferred call was handed "+
			"some other quantity", recorder.repairReconciled[0], reconciled)
	}
	if recorder.repairErrs[0] != nil {
		t.Errorf("observed repair err = %v on a successful pass, want nil",
			recorder.repairErrs[0])
	}

	// The failure side. Nothing else in this package observes a failed scan, so
	// without this the err parameter could be dropped on the floor at
	// state_lease.go:148 and every failed expiry scan would be counted as
	// result="ok" -- the shape where the dashboard is green precisely because
	// the scan is broken.
	mr.Close()
	if _, err := state.ListExpiredLeases(ctx, time.Now().UTC()); err == nil {
		t.Fatal("ListExpiredLeases() against a closed server returned no error")
	}
	if recorder.scanCalls != 2 {
		t.Fatalf("lease expiry scan calls = %d after the failing scan, want 2",
			recorder.scanCalls)
	}
	if recorder.scanErrs[1] == nil {
		t.Error("observed scan err = nil on a failed scan: the metrics layer " +
			"labels this pass result=\"ok\", so a permanently broken expiry " +
			"scan reports as healthy")
	}
}
