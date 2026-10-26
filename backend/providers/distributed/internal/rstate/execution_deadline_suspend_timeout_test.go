package rstate

import (
	"context"
	"errors"
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

// TestNodeTimeoutThatFiresAfterTheDeadlineIsRefusedOnceTheExecutionExpiredOnRedis parks
// a node on a signal with a Timeout in an execution whose deadline is already
// behind the clock, expires the execution, and only then lets the node's
// timeout wakeup reach a worker. The execution is over and its node is
// canceled, so the wakeup must be refused and must not run the node: a node
// timeout never outlives the execution that holds it.
//
// The two subtests differ in where the wakeup is when the expiry arrives, as in
// the timer test: still in the outbox with an availability that was not pulled
// forward to the deadline, or already delivered and sitting in the queue. The
// Redis store drops the outbox of a timed-out execution, so no late wakeup can
// be delivered from it afterwards.
func TestNodeTimeoutThatFiresAfterTheDeadlineIsRefusedOnceTheExecutionExpiredOnRedis(t *testing.T) {
	cases := []struct {
		name string
		wait time.Duration
	}{
		{name: "timeout_still_in_the_outbox", wait: farAway},
		{name: "timeout_already_queued", wait: timerWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rig := newDeadlineRaceRig(t)
			g := raceGraph(t, "deadline-suspend-node-timeout", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
			id := rig.submit(ctx, g)
			deadline := rig.snapshot(ctx, id).Deadline

			parkedAt := time.Now()
			rig.parkWith(ctx, id, signalWithTimeout(tc.wait))

			var wakeup engine.Task
			if tc.wait == timerWait {
				// Delivery never looks at the deadline, so a due timeout is queued
				// although the execution is already past it. The commit that parked
				// the node may have flushed it already, so read it from the queue.
				wakeup = *rig.onlyQueuedWakeup(ctx, id, types.TimeoutFired)
			} else {
				entry := rig.onlyEntry(ctx, id, types.TimeoutFired)
				// The wakeup is where the spec put it, the park time plus the
				// Timeout, and so behind the deadline: it was not pulled forward to
				// it. The store truncates to the millisecond, hence the allowance.
				if entry.AvailableAt.Before(parkedAt.Add(tc.wait - time.Millisecond)) {
					t.Fatalf("timeout entry is available at %v, want no earlier than %v after the park at %v (clamped to the deadline %v?)",
						entry.AvailableAt, tc.wait, parkedAt, deadline)
				}
				if !entry.AvailableAt.After(deadline) {
					t.Fatalf("timeout entry is available at %v, want it after the deadline %v", entry.AvailableAt, deadline)
				}
				wakeup = entry.Task
				if err := rig.eng.FlushOutbox(ctx, id); err != nil {
					t.Fatalf("FlushOutbox() error = %v", err)
				}
				// The queue ignores the delay it is handed, so a flush of an entry
				// that is not due would show up here as a queued task.
				if early := rig.queue.take(id); len(early) != 0 {
					t.Fatalf("a node timeout an hour away was delivered at once: %+v", early)
				}
			}

			outcome, err := rig.eng.ExpireExecution(ctx, id)
			if err != nil || outcome != engine.ExpireOutcomeExpired {
				t.Fatalf("ExpireExecution() = (%q, %v), want expired", outcome, err)
			}
			if snap := rig.snapshot(ctx, id); snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("execution = %q %q, want timeout for its deadline", snap.Status, snap.Error)
			}
			rig.requireNodesCanceled(ctx, 0, id, []string{"park"})
			if left := rig.outboxSize(ctx, id); left != 0 {
				t.Fatalf("a timed-out execution still holds %d outbox records", left)
			}

			// The node's timeout elapses and a worker is handed its wakeup.
			if _, err := rig.eng.BuildTaskLease(ctx, &wakeup); !errors.Is(err, engine.ErrExecutionInactive) {
				t.Fatalf("a node timeout wakeup after the execution timeout was leased: error = %v, want ErrExecutionInactive", err)
			}

			if snap := rig.snapshot(ctx, id); snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("execution = %q %q after the late wakeup, want it to stay timeout for its deadline", snap.Status, snap.Error)
			}
			rig.requireNodesCanceled(ctx, 0, id, []string{"park"})
			rig.requireAnnouncedOnly(0, id, types.ExecutionStatusTimeout)
			if left := rig.queue.take(id); len(left) != 0 {
				t.Fatalf("refusing the late wakeup left %d tasks queued: %+v", len(left), left)
			}
			if left := rig.outboxSize(ctx, id); left != 0 {
				t.Fatalf("refusing the late wakeup left %d outbox records", left)
			}
			rig.requireDrained(0, g.Name(), id)
		})
	}
}
