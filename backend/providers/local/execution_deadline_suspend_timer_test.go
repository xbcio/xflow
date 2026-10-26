package local

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// The tests in this file and its siblings cover the suspend variants of
// settings.timeout: a node parked on a timer, on a signal with its own Timeout,
// or on several signals. A suspend writes its wakeups to the outbox with an
// availability instant of park time plus the spec's duration, and the engine
// deliberately does not clamp that instant to the execution deadline (a wakeup
// that lands after the execution ended is refused when a worker presents it).
// The deadline races these wakeups, so each test fixes which one is first.
//
// The engine has no injectable clock, so the order is chosen with the two knobs
// the rig has. A graph whose settings.timeout is an hour keeps the deadline in
// the future; raceTimeout puts it behind the clock the moment Submit stamps it.
// A wakeup duration of a millisecond is due almost at once; an hour never is.
// The queue ignores the delay it is given, so "not delivered yet" is judged on
// the availability instant stored in the outbox entry, not on the queue.

// timerWait is a wakeup duration that is due a moment after the park, and
// farAway one that is not due within any test.
const (
	timerWait = time.Millisecond
	farAway   = time.Hour
)

// parkWith drives the execution's one queued task to a suspension described by
// spec, the way a handler's PrepareSuspend produces it.
func (r *deadlineRaceRig) parkWith(ctx context.Context, id types.ExecutionID, spec *types.SuspendSpec) {
	r.t.Helper()
	lease := r.lease(ctx, id)
	if err := r.eng.CommitTaskResult(ctx, lease, engine.TaskResult{Suspend: spec}); err != nil {
		r.t.Fatalf("CommitTaskResult(suspend %+v) error = %v", spec, err)
	}
	if got := r.snapshot(ctx, id).Status; got != types.ExecutionStatusRunning {
		r.t.Fatalf("execution after the park = %q, want running", got)
	}
}

// outboxEntries lists every delivery intent the store holds for the execution,
// due or not.
func (r *deadlineRaceRig) outboxEntries(ctx context.Context, id types.ExecutionID) []engine.OutboxEntry {
	r.t.Helper()
	entries, err := r.state.ListOutbox(ctx, id, time.Now().Add(10*365*24*time.Hour), 100)
	if err != nil {
		r.t.Fatalf("ListOutbox(%s) error = %v", id, err)
	}
	return entries
}

// onlyEntry returns the one delivery intent the execution holds and checks what
// it wakes the node with.
func (r *deadlineRaceRig) onlyEntry(ctx context.Context, id types.ExecutionID, want types.SignalTrigger) engine.OutboxEntry {
	r.t.Helper()
	entries := r.outboxEntries(ctx, id)
	if len(entries) != 1 {
		r.t.Fatalf("execution %s holds %d outbox entries after the park, want exactly 1: %+v", id, len(entries), entries)
	}
	entry := entries[0]
	if entry.Task.Type != engine.TaskTypeNodeResume || entry.Task.Payload == nil || entry.Task.Payload.Triggered != want {
		r.t.Fatalf("outbox entry wakes with %+v (type %v), want a node resume triggered by %v", entry.Task.Payload, entry.Task.Type, want)
	}
	return entry
}

// awaitQueued flushes the outbox until the execution has a task queued. A
// wakeup of timerWait is due a few milliseconds after the park, so the first
// flush may find nothing; the wait is bounded and only a broken delivery ever
// reaches the bound.
func (r *deadlineRaceRig) awaitQueued(ctx context.Context, id types.ExecutionID) []*engine.Task {
	r.t.Helper()
	giveUp := time.Now().Add(2 * time.Second)
	for {
		if err := r.eng.FlushOutbox(ctx, id); err != nil {
			r.t.Fatalf("FlushOutbox(%s) error = %v", id, err)
		}
		if tasks := r.queue.take(id); len(tasks) > 0 {
			return tasks
		}
		if time.Now().After(giveUp) {
			r.t.Fatalf("execution %s had nothing queued two seconds after a wakeup of %v", id, timerWait)
		}
		time.Sleep(time.Millisecond)
	}
}

// listSuspended reads the suspended index the way the cancel cascade does.
func (r *deadlineRaceRig) listSuspended(ctx context.Context, id types.ExecutionID) []string {
	r.t.Helper()
	nodes, err := r.state.ListSuspendedNodes(ctx, id)
	if err != nil {
		r.t.Fatalf("ListSuspendedNodes(%s) error = %v", id, err)
	}
	return nodes
}

// TestTimerThatFiresBeforeTheDeadlineResumesTheNode parks a node on a timer well
// inside the execution's budget. The timer entry is delivered, the node resumes
// and finishes, and the execution ends success with its deadline withdrawn and
// nothing left behind.
//
// The run is recorded once and read by both subtests, so the invariants and the
// known-residue observation always describe the same execution.
func TestTimerThatFiresBeforeTheDeadlineResumesTheNode(t *testing.T) {
	ctx := context.Background()
	var (
		ran       bool
		nodeAfter types.NodeStatus
		residue   []string
	)

	t.Run("invariants", func(t *testing.T) {
		rig := newDeadlineRaceRig(t)
		g := raceGraphWithTimeout(t, "deadline-suspend-timer", time.Hour, 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		deadline := rig.snapshot(ctx, id).Deadline
		if !deadline.After(time.Now()) {
			t.Fatalf("execution deadline = %v, want it still ahead", deadline)
		}

		rig.parkWith(ctx, id, &types.SuspendSpec{Mode: types.ModeTimer, Timer: timerWait})
		entry := rig.onlyEntry(ctx, id, types.TimerFired)
		if !entry.AvailableAt.Before(deadline) {
			t.Fatalf("timer entry is available at %v, want it before the deadline %v", entry.AvailableAt, deadline)
		}

		tasks := rig.awaitQueued(ctx, id)
		if len(tasks) != 1 || tasks[0].Type != engine.TaskTypeNodeResume {
			t.Fatalf("the timer queued %+v, want exactly one resume task", tasks)
		}
		lease, err := rig.eng.BuildTaskLease(ctx, tasks[0])
		if err != nil {
			t.Fatalf("BuildTaskLease(timer resume) error = %v", err)
		}
		if got := lease.Task.Payload; got == nil || got.Triggered != types.TimerFired {
			t.Fatalf("resume lease carries %+v, want the timer trigger", got)
		}
		commit, err := rig.eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"waited": true}},
		})
		if err != nil || commit != engine.CommitOutcomeAccepted {
			t.Fatalf("CommitTaskResultWithOutcome(resume) = (%q, %v), want accepted", commit, err)
		}

		snap := rig.snapshot(ctx, id)
		if snap.Status != types.ExecutionStatusSuccess || snap.Error != "" {
			t.Fatalf("execution = %q %q after the timer resume, want success", snap.Status, snap.Error)
		}
		node, err := rig.state.GetNode(ctx, id, "park")
		if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
			t.Fatalf("parked node = (%+v, %v), want success", node, err)
		}
		rig.requireAnnouncedOnly(0, id, types.ExecutionStatusSuccess)
		if left := rig.queue.take(id); len(left) != 0 {
			t.Fatalf("the finished execution left %d tasks queued: %+v", len(left), left)
		}
		if left := rig.outboxEntries(ctx, id); len(left) != 0 {
			t.Fatalf("the finished execution left %d outbox entries: %+v", len(left), left)
		}
		rig.requireDrained(0)

		nodeAfter, residue, ran = node.Status, rig.listSuspended(ctx, id), true
	})

	// suspended_residue_as_it_stands pins a KNOWN, UNDECIDED defect so that
	// fixing it is a visible, deliberate change. A timer wakeup reaches the node
	// through the outbox, and nothing on that path removes the node from the
	// suspended index the signal paths maintain, so the index keeps naming a node
	// that has already run to success. It does no harm to the node: the cancel
	// cascade skips a node that is no longer suspended, and a late signal that
	// matches the stale entry queues a resume the engine then refuses.
	//
	// When the defect is fixed, this subtest turns red on purpose: change it to
	// require that the index is empty and move that assertion into invariants.
	t.Run("suspended_residue_as_it_stands", func(t *testing.T) {
		if !ran {
			t.Fatalf("the invariants subtest did not finish, so there is no run to read")
		}
		if nodeAfter != types.NodeStatusSuccess {
			t.Fatalf("node after the timer resume = %q, want success", nodeAfter)
		}
		if !slices.Contains(residue, "park") {
			t.Fatalf("suspended index = %v after the timer resume; the residue this subtest records is gone, "+
				"so assert the index is cleaned up instead", residue)
		}
	})
}
