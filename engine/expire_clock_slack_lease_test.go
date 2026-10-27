package engine

import (
	"context"
	"testing"
	"time"
)

// Every way the engine builds a lease says, through the lease's own deadline,
// whether the node was cut off at the execution deadline. The guard's slack
// rests on that reading, so each construction path that exists is pinned: a
// lease whose budget is the execution's reads as clamped, one with a shorter
// budget of its own does not, and a lease with no execution deadline to be
// clamped to never does. A renewal moves the store's deadline and never the
// lease the commit is judged by, so it has no verdict of its own and must leave
// this one alone.

// clampedWant is a lease deadline placement and what it must read as.
type clampedWant struct {
	name         string
	nodeRaw      time.Duration // the node's own timeout (negative: none)
	execDeadline time.Time     // the stored execution deadline (zero: none)
	want         bool
}

func clampedWants() []clampedWant {
	now := time.Now().UTC()
	return []clampedWant{
		{"an execution deadline inside the node budget", 45 * time.Second, now.Add(10 * time.Second), true},
		{"a node without a timeout of its own", -1, now.Add(10 * time.Second), true},
		{"an execution deadline that already passed", 45 * time.Second, now.Add(-time.Minute), true},
		{"a node budget inside the execution deadline", 45 * time.Second, now.Add(time.Hour), false},
		{"no execution deadline", 45 * time.Second, time.Time{}, false},
		{"neither deadline", -1, time.Time{}, false},
	}
}

func TestBuiltNodeLeaseReportsWhetherItIsClamped(t *testing.T) {
	ctx := context.Background()
	for _, tc := range clampedWants() {
		t.Run(tc.name, func(t *testing.T) {
			eng, _, task := clampRig(t, tc.nodeRaw, tc.execDeadline)
			lease, err := eng.BuildTaskLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildTaskLease() error = %v", err)
			}
			if got := leaseClampedToExecutionDeadline(lease.ExecutionDeadline, tc.execDeadline); got != tc.want {
				t.Fatalf("clamped = %v for lease deadline %v against execution deadline %v, want %v",
					got, lease.ExecutionDeadline, tc.execDeadline, tc.want)
			}
		})
	}
}

func TestRecoveredNodeLeaseReportsWhetherItIsClamped(t *testing.T) {
	ctx := context.Background()
	for _, tc := range clampedWants() {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, task := clampRig(t, tc.nodeRaw, tc.execDeadline)
			lease, err := eng.BuildTaskLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildTaskLease() error = %v", err)
			}
			// A fresh engine has no memo of the deadline, like the control plane
			// that took over after a crash.
			restarted := New(state, &fakeQueue{})
			recovered, err := restarted.RecoverTaskLease(ctx, &lease.Task)
			if err != nil {
				t.Fatalf("RecoverTaskLease() error = %v", err)
			}
			if got := leaseClampedToExecutionDeadline(recovered.ExecutionDeadline, tc.execDeadline); got != tc.want {
				t.Fatalf("clamped = %v for recovered deadline %v against execution deadline %v, want %v",
					got, recovered.ExecutionDeadline, tc.execDeadline, tc.want)
			}
			if !recovered.ExecutionDeadline.Equal(lease.ExecutionDeadline) {
				t.Fatalf("recovered deadline = %v, want the issued %v: recovery must replay the lease", recovered.ExecutionDeadline, lease.ExecutionDeadline)
			}
		})
	}
}

// TestRenewingANodeLeaseLeavesItsClampedReadingAlone: RenewTaskLease caps the
// deadline it asks the store for, but the lease a runner later commits with is
// the one it was issued, and renewing it must not move its deadline.
func TestRenewingANodeLeaseLeavesItsClampedReadingAlone(t *testing.T) {
	ctx := context.Background()
	for _, tc := range clampedWants() {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, task := clampRig(t, tc.nodeRaw, tc.execDeadline)
			lease, err := eng.BuildTaskLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildTaskLease() error = %v", err)
			}
			issued := lease.ExecutionDeadline
			eng.state = &deadlineRenewer{fakeState: state}
			if renewed, err := eng.RenewTaskLease(ctx, lease, time.Minute); err != nil || !renewed {
				t.Fatalf("RenewTaskLease() = (%v, %v)", renewed, err)
			}
			if !lease.ExecutionDeadline.Equal(issued) {
				t.Fatalf("lease deadline after renewal = %v, want the issued %v", lease.ExecutionDeadline, issued)
			}
			if got := leaseClampedToExecutionDeadline(lease.ExecutionDeadline, tc.execDeadline); got != tc.want {
				t.Fatalf("clamped = %v after renewal, want %v", got, tc.want)
			}
		})
	}
}

// groupClampedWant reads the group lease cases groupClampCases places, as the
// clamped verdict each must give.
func groupClampedWant(tc groupClampCase) bool {
	if !tc.bounded || tc.execDeadline.IsZero() {
		return false
	}
	if tc.groupTimeout == 0 {
		return true
	}
	// A group timeout is anchored at the lease's issue time, which is a few
	// microseconds after the cases were placed: only deadlines clearly on one
	// side of it are used, so the verdict does not depend on that gap.
	return tc.execDeadline.Before(time.Now().Add(tc.groupTimeout))
}

func TestBuiltGroupLeaseReportsWhetherItIsClamped(t *testing.T) {
	ctx := context.Background()
	for _, tc := range groupClampCases() {
		t.Run(tc.name, func(t *testing.T) {
			eng, _, task := groupClampRig(t, tc.bounded, tc.groupTimeout, tc.execDeadline)
			_, payload, err := eng.BuildGroupLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildGroupLease() error = %v", err)
			}
			want := groupClampedWant(tc)
			if got := leaseClampedToExecutionDeadline(payload.Deadline, tc.execDeadline); got != want {
				t.Fatalf("clamped = %v for payload deadline %v against execution deadline %v, want %v",
					got, payload.Deadline, tc.execDeadline, want)
			}
		})
	}
}

func TestRecoveredGroupLeaseReportsWhetherItIsClamped(t *testing.T) {
	ctx := context.Background()
	for _, tc := range groupClampCases() {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, task := groupClampRig(t, tc.bounded, tc.groupTimeout, tc.execDeadline)
			lease, _, err := eng.BuildGroupLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildGroupLease() error = %v", err)
			}
			state.issuedAt = lease.IssuedAt
			restarted := New(state, &fakeQueue{})
			_, payload, err := restarted.RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx)
			if err != nil {
				t.Fatalf("RecoverGroupLease() error = %v", err)
			}
			want := groupClampedWant(tc)
			if got := leaseClampedToExecutionDeadline(payload.Deadline, tc.execDeadline); got != want {
				t.Fatalf("clamped = %v for recovered deadline %v against execution deadline %v, want %v",
					got, payload.Deadline, tc.execDeadline, want)
			}
		})
	}
}

// TestRenewingAGroupLeaseLeavesItsClampedReadingAlone is the group counterpart:
// the bound a group commits against is rebuilt from the lease's issue time, which
// a renewal never touches.
func TestRenewingAGroupLeaseLeavesItsClampedReadingAlone(t *testing.T) {
	ctx := context.Background()
	eng, state, task := groupClampRig(t, true, 45*time.Second, time.Now().UTC().Add(10*time.Second))
	lease, payload, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease() error = %v", err)
	}
	g, err := eng.loadGraph(ctx, task.ExecutionID)
	if err != nil || g == nil {
		t.Fatalf("loadGraph() = (%v, %v)", g, err)
	}
	issued, execDeadline := lease.IssuedAt, groupExecution(t, state, task.ExecutionID).Deadline
	if renewed, err := eng.RenewGroupLease(ctx, lease, time.Minute); err != nil || !renewed {
		t.Fatalf("RenewGroupLease() = (%v, %v)", renewed, err)
	}
	if !lease.IssuedAt.Equal(issued) {
		t.Fatalf("lease issue time after renewal = %v, want the issued %v", lease.IssuedAt, issued)
	}
	got, err := eng.committedGroupDeadline(ctx, lease, g)
	if err != nil {
		t.Fatalf("committedGroupDeadline() error = %v", err)
	}
	if !got.Equal(payload.Deadline) || !leaseClampedToExecutionDeadline(got, execDeadline) {
		t.Fatalf("rebuilt deadline after renewal = %v, want the issued %v, clamped to %v", got, payload.Deadline, execDeadline)
	}
}

func TestBuiltBatchLeaseReportsWhetherItIsClamped(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		budget func(deadline time.Time) time.Time
		want   bool
	}{
		{"a map node budget past the execution deadline", batchClamped, true},
		{"a map node budget inside the execution deadline", func(d time.Time) time.Time { return d.Add(-30 * time.Minute) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := time.Now().Add(time.Hour)
			rig := newBatchGuardRig(t, "continue", true, deadline)
			placeBatchBudget(t, rig, tc.budget(deadline))
			_, payload, err := rig.eng.BuildSubgraphLease(ctx, rig.batches[0])
			if err != nil {
				t.Fatalf("BuildSubgraphLease() error = %v", err)
			}
			if got := leaseClampedToExecutionDeadline(payload.Deadline, deadline); got != tc.want {
				t.Fatalf("clamped = %v for batch deadline %v against execution deadline %v, want %v",
					got, payload.Deadline, deadline, tc.want)
			}
		})
	}
}
