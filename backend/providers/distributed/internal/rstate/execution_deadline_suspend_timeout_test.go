package rstate

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// signalWithTimeout is a node parked on the signal "go" that gives up waiting
// after wait. The park writes one outbox entry for the node's own Timeout and
// none for the signal, which has not arrived.
func signalWithTimeout(wait time.Duration) *types.SuspendSpec {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}, Timeout: wait}
}

// TestNodeTimeoutThatFiresBeforeTheDeadlineResumesTheNodeOnRedis parks a node on a
// signal that never comes, with a Timeout well inside the execution's budget.
// The node's own timeout entry is delivered, the node resumes with the timeout
// trigger and finishes, and the execution ends success: a node timeout is the
// node's business, not the execution's, so the execution is not timeout.
func TestNodeTimeoutThatFiresBeforeTheDeadlineResumesTheNodeOnRedis(t *testing.T) {
	ctx := context.Background()
	var (
		ran         bool
		nodeAfter   types.NodeStatus
		residue     []string
		waiterAfter int64
	)

	t.Run("invariants", func(t *testing.T) {
		rig := newDeadlineRaceRig(t)
		g := raceGraphWithTimeout(t, "deadline-suspend-node-timeout", time.Hour, 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		deadline := rig.snapshot(ctx, id).Deadline

		rig.parkWith(ctx, id, signalWithTimeout(timerWait))
		wakeup := rig.onlyQueuedWakeup(ctx, id, types.TimeoutFired)
		// Delivery only ever takes an entry that is due, so the wakeup being
		// queued while the deadline is still ahead means the node's timeout fired
		// first.
		if !time.Now().Before(deadline) {
			t.Fatalf("the node timeout was delivered at %v, want it before the deadline %v", time.Now(), deadline)
		}
		lease, err := rig.eng.BuildTaskLease(ctx, wakeup)
		if err != nil {
			t.Fatalf("BuildTaskLease(timeout resume) error = %v", err)
		}
		if got := lease.Task.Payload; got == nil || got.Triggered != types.TimeoutFired {
			t.Fatalf("resume lease carries %+v, want the timeout trigger", got)
		}
		commit, err := rig.eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"timed_out": true}},
		})
		if err != nil || commit != engine.CommitOutcomeAccepted {
			t.Fatalf("CommitTaskResultWithOutcome(resume) = (%q, %v), want accepted", commit, err)
		}

		snap := rig.snapshot(ctx, id)
		if snap.Status != types.ExecutionStatusSuccess || snap.Error != "" {
			t.Fatalf("execution = %q %q after the node timeout, want success", snap.Status, snap.Error)
		}
		node, err := rig.state.GetNode(ctx, id, "park")
		if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
			t.Fatalf("parked node = (%+v, %v), want success", node, err)
		}
		rig.requireAnnouncedOnly(0, id, types.ExecutionStatusSuccess)
		if left := rig.queue.take(id); len(left) != 0 {
			t.Fatalf("the finished execution left %d tasks queued: %+v", len(left), left)
		}
		if left := rig.outboxSize(ctx, id); left != 0 {
			t.Fatalf("the finished execution left %d outbox records", left)
		}
		rig.requireDrained(0, g.Name(), id)

		nodeAfter, residue, waiterAfter, ran = node.Status, rig.listSuspended(ctx, id), rig.existsWaiter(ctx, id), true
	})

	// suspended_residue_as_it_stands pins the same KNOWN, UNDECIDED defect as the
	// timer test: the node's Timeout wakeup reaches the node through the outbox,
	// nothing on that path removes the node from the suspended set, and the
	// signal wait it registered (waiter:go) is still there after the node ran to
	// success. It does no harm to the node: the cancel cascade skips a node that
	// is no longer suspended, and a late "go" that matches the stale waiter
	// queues a resume the engine then refuses.
	//
	// When the defect is fixed, this subtest turns red on purpose: change it to
	// require that the set is empty and the waiter key gone, and move those
	// assertions into invariants.
	t.Run("suspended_residue_as_it_stands", func(t *testing.T) {
		if !ran {
			t.Fatalf("the invariants subtest did not finish, so there is no run to read")
		}
		if nodeAfter != types.NodeStatusSuccess {
			t.Fatalf("node after the node timeout = %q, want success", nodeAfter)
		}
		if !slices.Contains(residue, "park") {
			t.Fatalf("suspended set = %v after the node timeout; the residue this subtest records is gone, "+
				"so assert the set is cleaned up instead", residue)
		}
		if waiterAfter != 1 {
			t.Fatalf("EXISTS waiter:go = %d after the node timeout; the residue this subtest records is gone, "+
				"so assert the waiter is cleaned up instead", waiterAfter)
		}
	})
}
