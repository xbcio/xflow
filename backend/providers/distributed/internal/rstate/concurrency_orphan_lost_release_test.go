package rstate

// The residual shape of a reclaim that converged in Lua but died before the
// Go-side slot release and projections.
//
// The production sequence in evaluateLiveOrphan is:
//
//	reclaimOrphanExecutionLua (status -> canceled, error, updated_at)  [Redis]
//	convergeLiveOrphan -> releaseWorkflowSlot                       [Redis]
//	convergeLiveOrphanEffects -> SQL projection, caches, event       [Go side]
//
// A crash between the first and second step leaves a terminal status key with
// the execution's slot member still charged and the line still behind it. This
// test builds exactly that by running the real Lua script and then stopping, and
// observes what the next maintenance pass repairs.
//
// It deliberately keeps the Lua injection shape instead of reusing the shared
// contract's TerminalResidueIsMirroredAfterReconcile: the contract writes the
// terminal status directly, which proves the repair; this test proves the
// reclaim script's own terminal write is the shape being repaired, including
// the force-mode variant a within-grace convergence produces.

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestReconcileProjectsTerminalResidueLostRelease builds the torn residual in
// two clock variants:
//
//   - past_grace: the sweep-mode Lua convergence applies normally;
//   - within_grace: the status clock is fresh (inside the grace) and only a
//     force-mode convergence — the shape of Cancel's self-heal or the operator
//     reset — can land, exactly as production would.
//
// In both variants the repair is the same: the next reconcile pass releases the
// leaked member, advances the line, and projects the terminal status the crash
// skipped onto the durable mirror.
func TestReconcileProjectsTerminalResidueLostRelease(t *testing.T) {
	cases := []struct {
		name     string
		clockAge time.Duration
		force    int64
	}{
		{name: "past_grace_sweep_convergence", clockAge: 10 * time.Minute, force: 0},
		{name: "within_grace_force_convergence", clockAge: 10 * time.Millisecond, force: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := namespace.WithNamespace(context.Background(), namespace.Default)
			state, rdb, db := newOrphanStore(t)
			g := orphanGraphLimit(t, "lost-release-"+tc.name, 1)

			holder, res := orphanSubmit(t, ctx, state, g, 1)
			if res.Queued {
				t.Fatalf("holder submission = %+v, want the slot", res)
			}
			waiting, res := orphanSubmit(t, ctx, state, g, 1)
			if !res.Queued || res.QueuePosition != 1 {
				t.Fatalf("waiting submission = %+v, want queued at 1", res)
			}

			// The torn shape the reclaim judges: live status, graph gone.
			orphanDeleteGraph(t, ctx, rdb, holder)
			orphanAgeStatus(t, ctx, rdb, holder, tc.clockAge)
			// No lease was ever taken, so no activity clock exists; the age
			// guard sees only the status clock, which is what the variants vary.

			// Step 1: the Lua half converges (the atomic decision and the
			// terminal write). After this call the test stops — this is the
			// crash.
			reply, err := reclaimOrphanExecutionLua.Run(ctx, rdb,
				[]string{
					execKey(namespace.Default, holder, "status"),
					execKey(namespace.Default, holder, "graph"),
					execKey(namespace.Default, holder, "updated_at_ms"),
					leaseExpiryZSetKey(namespace.Default, holder),
					suspendedNodesKey(namespace.Default, holder),
					execKey(namespace.Default, holder, "error"),
					activityAtKey(namespace.Default, holder),
				},
				time.Now().UTC().UnixMilli(), time.Minute.Milliseconds(), engine.OrphanReclaimReason,
				3600, tc.force, 0, 0,
			).Slice()
			if err != nil {
				t.Fatalf("reclaim Lua error = %v", err)
			}
			if got := luaReplyInt(reply, 0); got != 1 {
				t.Fatalf("reclaim Lua outcome = %d, want 1 (converged); reply=%v", got, reply)
			}

			// Residual assertions: the Lua half landed, the Go-side half did not.
			if got := orphanStatus(t, ctx, state, holder); got != types.ExecutionStatusCanceled {
				t.Fatalf("Redis status after Lua = %q, want canceled", got)
			}
			if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(holder) {
				t.Fatalf("slot members after Lua = %v, want the leaked holder [%q]", members, holder)
			}
			if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(waiting) {
				t.Fatalf("queue members after Lua = %v, want [%q]", members, waiting)
			}
			if got := orphanSQLStatus(t, ctx, db, holder); got != types.ExecutionStatusRunning {
				t.Fatalf("holder SQL before repair = %q, want running (the skipped projection is the residual)", got)
			}

			// An orphan sweep on its own does not repair this shape: a terminal
			// status is deliberately left to the terminal writer (and the slot
			// reconciler). Record that rather than assume it.
			orphanSweep := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 32, Grace: time.Minute,
			})
			if orphanSweep.Reclaimed != 0 {
				t.Fatalf("orphan sweep reclaimed = %d, want 0 (terminal status is not its shape)", orphanSweep.Reclaimed)
			}
			if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(holder) {
				t.Fatalf("slot members after orphan sweep = %v, want the leak still present", members)
			}

			// Step 2: one reconcile pass (what SweepConcurrencySlotsOnce runs).
			// The terminal-status branch has no age gate, so this is the only
			// pass required in both clock variants.
			swept, err := state.ReconcileConcurrencySlots(ctx, 64)
			if err != nil {
				t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
			}
			if swept != 1 {
				t.Fatalf("reconcile swept = %d, want 1 (the leaked terminal member)", swept)
			}
			if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(waiting) {
				t.Fatalf("slot members after reconcile = %v, want the promoted waiter [%q]: slot not re-released", members, waiting)
			}
			if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
				t.Fatalf("queue members after reconcile = %v, want empty: the line was not advanced", members)
			}
			if got := orphanStatus(t, ctx, state, waiting); got != types.ExecutionStatusRunning {
				t.Fatalf("waiting status after reconcile = %q, want running", got)
			}
			if got := orphanStatus(t, ctx, state, holder); got != types.ExecutionStatusCanceled {
				t.Fatalf("holder status after reconcile = %q, want canceled", got)
			}
			// The durable mirror of the converged execution must be repaired by
			// the same pass: Redis says canceled, and the skipped projection
			// must no longer be visible to SQL readers.
			if got := orphanSQLStatus(t, ctx, db, holder); got != types.ExecutionStatusCanceled {
				t.Fatalf("holder SQL after one reconcile pass = %q, want canceled: slot/queue self-heal, durable mirror does not", got)
			}
		})
	}
}
