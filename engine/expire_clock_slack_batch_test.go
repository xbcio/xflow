package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// The batch call site: a map batch the runner cut off at the execution deadline
// reports its timeout (the batch_timeout marker) from a clock that may be a few
// milliseconds ahead of the server's. A batch lease keeps TaskLease's
// ExecutionDeadline at zero and carries its bound in a payload the runner
// echoes, so the verdict rebuilds the bound from the parent's issue time that
// rides on the batch task, the map node's timeout and the stored deadline.

// placeBatchBudget fixes where the batches' own budget ends relative to the
// stored execution deadline. The map node's timeout is the engine's default;
// moving the parent's issue time that every batch task carries is the only way
// to place the node budget without sleeping. budgetEndsAt is the instant the
// node budget ends at: the batch bound is the earlier of that and the deadline.
func placeBatchBudget(t *testing.T, rig *batchGuardRig, budgetEndsAt time.Time) {
	t.Helper()
	timeout := rig.eng.resolveNodeTimeout(0)
	if timeout <= 0 {
		t.Fatalf("the map node has no timeout of its own (%v): the budget cannot be placed", timeout)
	}
	issuedAt := budgetEndsAt.Add(-timeout).UTC().Format(time.RFC3339Nano)
	for _, batch := range rig.batches {
		batch.Payload.Data["parent_issued_at"] = issuedAt
	}
}

// reportTimedOutBefore pins the stored deadline margin ahead of now (a negative
// margin is an arrival after it), places the node budget with budget, and then
// reports every batch with the first one timed out, returning the downstream
// nodes queued over the run. The verdict is only known at the barrier.
func reportTimedOutBefore(t *testing.T, rig *batchGuardRig, margin time.Duration, budget func(deadline time.Time) time.Time) []string {
	t.Helper()
	deadline := time.Now().Add(margin)
	setStoredDeadline(rig.state, rig.eng, rig.id, deadline)
	placeBatchBudget(t, rig, budget(deadline))
	return rig.reportAll(t, 0, timedOutBatch())
}

var (
	// batchClamped is a node budget far past the execution deadline: the
	// batch bound is the execution deadline itself.
	batchClamped = func(d time.Time) time.Time { return d.Add(time.Hour) }
	// batchOwnBudget is a node budget that ends 30ms before the execution's.
	batchOwnBudget = func(d time.Time) time.Time { return d.Add(-30 * time.Millisecond) }
)

// TestClampedBatchTimeoutArrivingEarlyExpiresTheExecution: the batch's body is
// cut off at the execution deadline and the timeout lands inside the slack.
// That is the execution's timeout whichever on_error the map node has.
func TestClampedBatchTimeoutArrivingEarlyExpiresTheExecution(t *testing.T) {
	for _, margin := range []time.Duration{20 * time.Millisecond, 3 * time.Millisecond, -time.Millisecond} {
		for _, onError := range []string{"", "continue", "error_output", "main_output"} {
			t.Run(margin.String()+"/"+guardName(onError), func(t *testing.T) {
				rig := newBatchGuardRig(t, onError, true, time.Now().Add(time.Hour))
				queued := reportTimedOutBefore(t, rig, margin, batchClamped)
				rig.requireExpired(t, queued)
			})
		}
	}
}

// TestClampedBatchTimeoutFarBeforeTheDeadlineStillAppliesOnError: the slack is
// a bound. A batch that timed out 200ms before the execution deadline cannot
// be clock drift, so the map node's on_error decides.
func TestClampedBatchTimeoutFarBeforeTheDeadlineStillAppliesOnError(t *testing.T) {
	for _, tc := range []struct {
		onError    string
		wantExec   types.ExecutionStatus
		wantNode   types.NodeStatus
		wantQueued string
	}{
		{"", types.ExecutionStatusFailed, types.NodeStatusFailed, ""},
		{"continue", types.ExecutionStatusRunning, types.NodeStatusContinued, "next"},
		{"error_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "onerr"},
	} {
		t.Run(guardName(tc.onError), func(t *testing.T) {
			rig := newBatchGuardRig(t, tc.onError, true, time.Now().Add(time.Hour))
			queued := reportTimedOutBefore(t, rig, 200*time.Millisecond, batchClamped)
			requireBatchOnError(t, rig, queued, tc.wantExec, tc.wantNode, tc.wantQueued)
		})
	}
}

// TestUnclampedBatchTimeoutKeepsTheExactComparison: the map node's own budget
// ended 30ms before the execution's, so the batch really did time out before
// its execution did. The 30ms is inside the slack, which is why only the
// rebuilt bound can tell the two apart.
func TestUnclampedBatchTimeoutKeepsTheExactComparison(t *testing.T) {
	for _, tc := range []struct {
		onError    string
		wantExec   types.ExecutionStatus
		wantNode   types.NodeStatus
		wantQueued string
	}{
		{"", types.ExecutionStatusFailed, types.NodeStatusFailed, ""},
		{"continue", types.ExecutionStatusRunning, types.NodeStatusContinued, "next"},
		{"error_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "onerr"},
	} {
		t.Run(guardName(tc.onError), func(t *testing.T) {
			rig := newBatchGuardRig(t, tc.onError, true, time.Now().Add(time.Hour))
			queued := reportTimedOutBefore(t, rig, 30*time.Millisecond, batchOwnBudget)
			requireBatchOnError(t, rig, queued, tc.wantExec, tc.wantNode, tc.wantQueued)
		})
	}
}

// TestUnclampedBatchTimeoutAfterTheDeadlineStillExpiresTheExecution: a batch
// with a shorter budget that reports once the execution deadline is reached is
// still caught by it.
func TestUnclampedBatchTimeoutAfterTheDeadlineStillExpiresTheExecution(t *testing.T) {
	rig := newBatchGuardRig(t, "continue", true, time.Now().Add(time.Hour))
	queued := reportTimedOutBefore(t, rig, -time.Millisecond, batchOwnBudget)
	rig.requireExpired(t, queued)
}

// TestBatchBoundIsRebuiltFromTheBatchTask: the bound the verdict judges by is
// the one BuildSubgraphLease issued, from the same arithmetic, including when
// the lease is rebuilt for a recovery. A rebuild must not hand the batch a
// fresh budget and must not move the bound the verdict compares against.
func TestBatchBoundIsRebuiltFromTheBatchTask(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		margin time.Duration
		budget func(deadline time.Time) time.Time
	}{
		{"clamped to the execution deadline", time.Hour, batchClamped},
		{"the map node budget is shorter", time.Hour, func(d time.Time) time.Time { return d.Add(-30 * time.Minute) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newBatchGuardRig(t, "continue", true, time.Now().Add(tc.margin))
			deadline := time.Now().Add(tc.margin)
			setStoredDeadline(rig.state, rig.eng, rig.id, deadline)
			placeBatchBudget(t, rig, tc.budget(deadline))

			g, err := rig.eng.loadGraph(ctx, rig.id)
			if err != nil || g == nil {
				t.Fatalf("loadGraph() = (%v, %v)", g, err)
			}
			want := tc.budget(deadline)
			if deadline.Before(want) {
				want = deadline
			}
			lease, payload, err := rig.eng.BuildSubgraphLease(ctx, rig.batches[0])
			if err != nil {
				t.Fatalf("BuildSubgraphLease() error = %v", err)
			}
			if !payload.Deadline.Equal(want) {
				t.Fatalf("issued batch deadline = %v, want %v", payload.Deadline, want)
			}
			rebuilt, err := rig.eng.timedOutBatchDeadline(ctx, lease, g, &lease.Task)
			if err != nil {
				t.Fatalf("timedOutBatchDeadline() error = %v", err)
			}
			if !rebuilt.Equal(payload.Deadline) {
				t.Fatalf("rebuilt batch deadline = %v, want the issued %v", rebuilt, payload.Deadline)
			}
			_, again, err := rig.eng.BuildSubgraphLease(ctx, rig.batches[0])
			if err != nil {
				t.Fatalf("BuildSubgraphLease() rebuild error = %v", err)
			}
			if !again.Deadline.Equal(payload.Deadline) {
				t.Fatalf("rebuilt lease deadline = %v, want the issued %v: a rebuild must not move the bound", again.Deadline, payload.Deadline)
			}
			if clamped := leaseClampedToExecutionDeadline(rebuilt, deadline); clamped != rebuilt.Equal(deadline) {
				t.Fatalf("clamped = %v for bound %v against deadline %v", clamped, rebuilt, deadline)
			}
		})
	}
}

// TestBatchBoundOfAnUnusableBatchIsZero: an expansion with no batch task has
// nothing that could have timed out, and a node index outside the graph has no
// map node to take the timeout from. Both leave the guard exact.
func TestBatchBoundOfAnUnusableBatchIsZero(t *testing.T) {
	ctx := context.Background()
	rig := newBatchGuardRig(t, "continue", true, time.Now().Add(time.Hour))
	g, err := rig.eng.loadGraph(ctx, rig.id)
	if err != nil || g == nil {
		t.Fatalf("loadGraph() = (%v, %v)", g, err)
	}
	lease, _, err := rig.eng.BuildSubgraphLease(ctx, rig.batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	if got, err := rig.eng.timedOutBatchDeadline(ctx, lease, g, nil); err != nil || !got.IsZero() {
		t.Fatalf("timedOutBatchDeadline(nil batch) = (%v, %v), want (zero, nil)", got, err)
	}
	for _, nodeIdx := range []int{-1, g.NodeCount()} {
		bad := *lease
		bad.Task.NodeIdx = nodeIdx
		if got, err := rig.eng.timedOutBatchDeadline(ctx, &bad, g, &lease.Task); err != nil || !got.IsZero() {
			t.Fatalf("timedOutBatchDeadline(node %d) = (%v, %v), want (zero, nil)", nodeIdx, got, err)
		}
	}
}
