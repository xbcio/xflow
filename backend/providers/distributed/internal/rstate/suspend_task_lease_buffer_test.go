package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// suspendableLease acquires and claims a lease on node "wait" so a test can
// drive the suspend primitives directly. Each call needs its own lease ID.
func suspendableLease(t *testing.T, state *Store, id types.ExecutionID, round int) *engine.TaskLease {
	t.Helper()
	ctx := context.Background()
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID(fmt.Sprintf("lease-%s-%d", id, round)),
		LeaseToken: engine.LeaseToken(fmt.Sprintf("token-%s-%d", id, round)),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "wait",
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("round %d: AcquireTaskLease acquired=%v err=%v, want true/nil", round, acquired, err)
	}
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("round %d: GetNode node=%v err=%v", round, node, err)
	}
	lease.Attempt = node.Attempt
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("round %d: ClaimTaskLease claimed=%v err=%v, want true/nil", round, claimed, err)
	}
	return lease
}

// TestRedisSuspendTaskLeaseKeepsSignalsPastTheWait pins the non-outbox suspend
// primitive to the same rule as the durable one: a suspension consumes the
// stored signals its wait needs and no more. The script used to take every
// stored signal of the wait, return the first, and delete the rest, so a signal
// stored beside the one delivered vanished without reaching the node.
func TestRedisSuspendTaskLeaseKeepsSignalsPastTheWait(t *testing.T) {
	ctx := context.Background()
	deliver := func(t *testing.T, state *Store, id types.ExecutionID, names ...string) {
		t.Helper()
		for _, name := range names {
			if _, _, err := state.DeliverSignal(ctx, id, name, map[string]any{"by": name}); err != nil {
				t.Fatalf("DeliverSignal(%q): %v", name, err)
			}
		}
	}

	t.Run("single signal wait", func(t *testing.T) {
		state, _, _ := newTestRedisState(t)
		id := types.ExecutionID("suspend-lease-single")
		deliver(t, state, id, "first", "second")
		spec := &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"first", "second"}}

		payload, committed, err := state.SuspendTaskLease(ctx, suspendableLease(t, state, id, 1), nil, false, false, spec, "")
		if err != nil || !committed || payload == nil || payload.Name != "first" {
			t.Fatalf("first suspend payload=%+v committed=%v err=%v, want the first signal", payload, committed, err)
		}
		payload, committed, err = state.SuspendTaskLease(ctx, suspendableLease(t, state, id, 2), nil, false, false, spec, "first")
		if err != nil || !committed || payload == nil || payload.Name != "second" || payload.Data["by"] != "second" {
			t.Fatalf("second suspend payload=%+v committed=%v err=%v, want the signal the first suspend left stored", payload, committed, err)
		}
	})

	t.Run("multi signal wait", func(t *testing.T) {
		state, _, _ := newTestRedisState(t)
		id := types.ExecutionID("suspend-lease-multi")
		deliver(t, state, id, "one", "two", "three")
		spec := &types.SuspendSpec{Mode: types.ModeMultiSignal, Signals: []string{"one", "two", "three"}, Quorum: 2}

		payload, committed, err := state.SuspendTaskLease(ctx, suspendableLease(t, state, id, 1), nil, false, false, spec, "")
		if err != nil || !committed || payload == nil {
			t.Fatalf("suspend payload=%+v committed=%v err=%v, want a satisfied quorum", payload, committed, err)
		}
		if len(payload.All) != 2 || payload.All["one"] == nil || payload.All["two"] == nil {
			t.Fatalf("All = %v, want exactly the two signals the quorum needed", payload.All)
		}
		// The third signal must still be stored: delivering to the re-armed wait
		// completes the next quorum together with it.
		payload, committed, err = state.SuspendTaskLease(ctx, suspendableLease(t, state, id, 2), nil, false, false, spec, "one")
		if err != nil || !committed || payload != nil {
			t.Fatalf("re-suspend payload=%+v committed=%v err=%v, want parked: one stored signal is below the quorum", payload, committed, err)
		}
		node, signalName, err := state.DeliverSignal(ctx, id, "one", map[string]any{"by": "one"})
		if err != nil || node != "wait" || signalName == nil || len(signalName.All) != 2 || signalName.All["three"] == nil {
			t.Fatalf("DeliverSignal node=%q payload=%+v err=%v, want the stored third signal in the quorum", node, signalName, err)
		}
	})
}
