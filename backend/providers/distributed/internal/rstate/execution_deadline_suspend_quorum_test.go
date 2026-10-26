package rstate

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// quorumSpec parks a node on two signals of which both are required, with a
// Timeout far enough away that it never fires inside a test.
func quorumSpec() *types.SuspendSpec {
	return &types.SuspendSpec{
		Mode:    types.ModeMultiSignal,
		Signals: []string{"a", "b"},
		Quorum:  2,
		Timeout: farAway,
	}
}

// TestSecondSignalOfAQuorumAfterTheExecutionExpiredQueuesNothingOnRedis parks a node on
// two signals with quorum 2 and a Timeout, delivers the first, ends the
// execution by its deadline, and then delivers the second. Before the end the
// one signal is only collected: it must not resume the node, because the quorum
// is not met. After it the second signal would complete the quorum, and it must
// not resume a node of an execution that is over: it is a silent no-op that
// queues nothing and adds no delivery intent.
//
// Only the engine's behavior is asserted. What the store does with the unmatched
// signal it is handed afterwards (the Redis store keeps it under its signal key
// until the execution's TTL) is its own business and is not pinned here.
//
// The two subtests differ in how far the end has got. After the cascade there is
// no waiter left, and the late signal is harmless for that reason alone. In the
// window the timeout status is written but the cascade has not run, so the node
// is still in the suspended set. The window is made deterministic by writing the
// status without the cascade.
//
// On Redis two layers keep the second signal out in the window. The terminal
// status write already drops the waiter keys of a multi-signal node (its stored
// waiter spec names them), so the signal finds no waiter; and should it find
// one, the engine refuses to resume an execution that is over. Either layer
// alone is enough, so the window case only turns red when both are lifted.
func TestSecondSignalOfAQuorumAfterTheExecutionExpiredQueuesNothingOnRedis(t *testing.T) {
	cases := []struct {
		name string
		// end brings the execution to its deadline end. The window writes the
		// status directly, as the existing window test does, so no completion is
		// announced there and only the cascade case checks the announcement.
		end func(t *testing.T, ctx context.Context, rig *deadlineRaceRig, id types.ExecutionID)
	}{
		{
			name: "after_the_cascade",
			end: func(t *testing.T, ctx context.Context, rig *deadlineRaceRig, id types.ExecutionID) {
				outcome, err := rig.eng.ExpireExecution(ctx, id)
				if err != nil || outcome != engine.ExpireOutcomeExpired {
					t.Fatalf("ExpireExecution() = (%q, %v), want expired", outcome, err)
				}
				rig.requireNodesCanceled(ctx, 0, id, []string{"park"})
				rig.requireAnnouncedOnly(0, id, types.ExecutionStatusTimeout)
			},
		},
		{
			name: "in_the_window_after_the_timeout_write",
			end: func(t *testing.T, ctx context.Context, rig *deadlineRaceRig, id types.ExecutionID) {
				if err := rig.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
					t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
				}
				if nodes := rig.listSuspended(ctx, id); len(nodes) != 1 || nodes[0] != "park" {
					t.Fatalf("suspended nodes in the window = %v, want the node still parked", nodes)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rig := newDeadlineRaceRig(t)
			g := raceGraph(t, "deadline-suspend-quorum", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
			id := rig.submit(ctx, g)

			rig.parkWith(ctx, id, quorumSpec())
			// The Timeout is the only delivery intent a park on unmet signals creates.
			rig.onlyEntry(ctx, id, types.TimeoutFired)

			// One of two signals: collected, not delivered.
			if err := rig.eng.DeliverSignal(ctx, id, "a", map[string]any{"n": 1}); err != nil {
				t.Fatalf("DeliverSignal(a) error = %v", err)
			}
			if queued := rig.queue.take(id); len(queued) != 0 {
				t.Fatalf("one signal of a quorum of 2 queued %d tasks: %+v", len(queued), queued)
			}
			if got := rig.snapshot(ctx, id).Status; got != types.ExecutionStatusRunning {
				t.Fatalf("execution = %q after one signal of two, want running", got)
			}
			if nodes := rig.listSuspended(ctx, id); len(nodes) != 1 || nodes[0] != "park" {
				t.Fatalf("suspended nodes after one signal of two = %v, want the node still parked", nodes)
			}
			if left := rig.outboxEntries(ctx, id); len(left) != 1 {
				t.Fatalf("outbox holds %d entries after one signal of two, want only the Timeout: %+v", len(left), left)
			}

			tc.end(t, ctx, rig, id)

			before := rig.outboxSize(ctx, id)
			if err := rig.eng.DeliverSignal(ctx, id, "b", map[string]any{"n": 2}); err != nil {
				t.Fatalf("DeliverSignal(b) after the end error = %v, want a silent no-op", err)
			}
			if queued := rig.queue.take(id); len(queued) != 0 {
				t.Fatalf("the signal completing the quorum after the end queued %d tasks: %+v", len(queued), queued)
			}
			if after := rig.outboxSize(ctx, id); after != before {
				t.Fatalf("the signal completing the quorum after the end changed the outbox from %d to %d records", before, after)
			}
			if snap := rig.snapshot(ctx, id); snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("execution = %q %q after the late signal, want it to stay timeout for its deadline", snap.Status, snap.Error)
			}
			if node, err := rig.state.GetNode(ctx, id, "park"); err != nil || node == nil ||
				node.Status == types.NodeStatusSuccess || node.Status == types.NodeStatusRunning {
				t.Fatalf("parked node = (%+v, %v) after the late signal, want it neither resumed nor completed", node, err)
			}

			// Whatever the end had not yet done is repaired afterwards.
			if outcome, err := rig.eng.ExpireExecution(ctx, id); err != nil ||
				(outcome != engine.ExpireOutcomeExpired && outcome != engine.ExpireOutcomeResumed && outcome != engine.ExpireOutcomeAlreadyTerminal) {
				t.Fatalf("ExpireExecution() after the late signal = (%q, %v), want it to finish the end", outcome, err)
			}
			rig.requireNodesCanceled(ctx, 0, id, []string{"park"})
			rig.requireDrained(0, g.Name(), id)
		})
	}
}
