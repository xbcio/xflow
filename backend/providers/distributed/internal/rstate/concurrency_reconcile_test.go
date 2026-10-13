package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func newMiniredisStore(t *testing.T) (*miniredis.Miniredis, *redis.Client, *Store) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return srv, rdb, New(rdb, nil, time.Minute)
}

func sweepGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

func sweepAdmission(g *graph.Graph, limit int) engine.ConcurrencyAdmission {
	return engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     limit,
		Mode:      types.ConcurrencyModeReject,
	}
}

// admitForSweep charges a slot through the real admission path and returns the
// execution's ID, so the sweep cases exercise the same state production writes.
func admitForSweep(t *testing.T, ctx context.Context, state *Store, g *graph.Graph, limit int) types.ExecutionID {
	t.Helper()
	id := engine.NewExecutionID()
	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	entries := []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}}
	if _, err := state.CreateExecutionWithConcurrency(ctx, snap, entries, sweepAdmission(g, limit)); err != nil {
		t.Fatalf("CreateExecutionWithConcurrency(%q) error = %v", id, err)
	}
	return id
}

// chargeSlotForSweep writes the state a charge leaves behind: the workflow tag
// registered first, exactly as admission writes it, then the slot itself. The
// order is the point — a charged slot is always already discoverable by the
// sweep, so nothing here can hide a leak from the mechanism that repairs it.
func chargeSlotForSweep(t *testing.T, ctx context.Context, rdb *redis.Client, state *Store, workflow string, member types.ExecutionID, limit int, score int64) {
	t.Helper()
	state.registerConcurrencyWorkflow(ctx, namespace.Default, workflowDigestTag(namespace.Default, workflow), 0)
	reply, err := workflowAdmitLua.Run(ctx, rdb, concurrencyAdmitKeys(namespace.Default, workflow),
		concurrencySlotMember(member), limit, score, 3600, 0, 0, 0, 0).Slice()
	if err != nil {
		t.Fatalf("charge slot for %q error = %v", member, err)
	}
	if got := luaReplyInt(reply, 0); got != concurrencyAdmitAdmitted {
		t.Fatalf("charge slot for %q code = %d, want %d (admitted)", member, got, concurrencyAdmitAdmitted)
	}
}

// TestSweepConcurrencySlotsRemovesLeakedCharge pins the leak the sweep exists
// for: a slot charged for an execution that never landed. Without the sweep the
// workflow runs one short of its limit forever, and nothing in the engine can
// see it — the member is a real execution ID, it just names nothing.
func TestSweepConcurrencySlotsRemovesLeakedCharge(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-leak")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	// Charge a slot by hand, exactly as admission would, and never create the
	// execution. The score is old so the sweep treats it as past the create
	// window rather than as a submission in flight.
	leaked := types.ExecutionID("leaked-execution")
	old := time.Now().UTC().Add(-10 * time.Minute).Unix()
	chargeSlotForSweep(t, ctx, rdb, state, g.Name(), leaked, 1, old)

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 1 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want 1", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 0 {
		t.Fatalf("slot ZSET size after sweep = %d (err %v), want 0 entries", n, err)
	}
	// The drained set and its index entry go together: an index that keeps
	// naming a deleted set sends every later sweep to a key that does not exist.
	if exists, err := rdb.Exists(ctx, slotKey).Result(); err != nil || exists != 0 {
		t.Fatalf("slot key after sweep exists = %d (err %v), want deleted", exists, err)
	}
	if n, err := rdb.SCard(ctx, concurrencyWorkflowIndexKey(namespace.Default)).Result(); err != nil || n != 0 {
		t.Fatalf("workflow index size after sweep = %d (err %v), want 0", n, err)
	}
}

// TestSweepConcurrencySlotsKeepsLiveHolder is the safety half: a slot whose
// execution is still running must survive the sweep. A sweep that removed live
// slots would raise the workflow's effective limit above the declared one.
func TestSweepConcurrencySlotsKeepsLiveHolder(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-live")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	id := admitForSweep(t, ctx, state, g, 1)

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 0 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want the live slot kept", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("slot ZSET size = %d (err %v), want the live holder", n, err)
	}
	if _, err := rdb.Get(ctx, concurrencyTagKey(namespace.Default, id)).Result(); err != nil {
		t.Fatalf("concurrency back-reference for %q error = %v, want it recorded", id, err)
	}
}

// TestSweepConcurrencySlotsRemovesLostRelease covers the other leak: a terminal
// execution whose release never landed. Unlike the phantom-charge case the
// execution record is real and says terminal, so the sweep does not have to
// wait out the create window to be sure.
//
// The state is built by hand rather than through UpdateExecutionStatus: the
// store's own terminal writer releases the slot, so driving the writer would
// test the writer instead of the sweep. What a lost release leaves behind is
// exactly this — a terminal status key and a slot member pointing at it.
func TestSweepConcurrencySlotsRemovesLostRelease(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-terminal")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	id := engine.NewExecutionID()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := rdb.Set(ctx, execKey(namespace.Default, id, "status"),
		string(types.ExecutionStatusCanceled), 0).Err(); err != nil {
		t.Fatalf("write terminal status error = %v", err)
	}
	chargeSlotForSweep(t, ctx, rdb, state, g.Name(), id, 1, time.Now().UTC().Unix())

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 1 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want the terminal member removed", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 0 {
		t.Fatalf("slot ZSET size = %d (err %v), want empty", n, err)
	}
}

// TestSweepConcurrencySlotsKeepsFreshPhantom pins the grace window: a member
// whose execution is not there YET is the normal state of a submission between
// charging its slot and writing the execution. Sweeping it would release live
// slots under load, which is a worse failure than the leak it repairs.
func TestSweepConcurrencySlotsKeepsFreshPhantom(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-grace")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	fresh := types.ExecutionID("fresh-execution")
	chargeSlotForSweep(t, ctx, rdb, state, g.Name(), fresh, 1, time.Now().UTC().Unix())

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 0 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want a within-grace member kept", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("slot ZSET size = %d (err %v), want the fresh member kept", n, err)
	}
}

// TestSweepConcurrencySlotsDropsTagsForMissingSets keeps the index honest. A
// tag is registered before the charge it announces, so a refused admission or a
// crash in between leaves a tag whose set was never created; nothing else in
// the store removes it, and an index that only grows sends every later sweep to
// a key that does not exist.
func TestSweepConcurrencySlotsDropsTagsForMissingSets(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-stale-tag")

	state.registerConcurrencyWorkflow(ctx, namespace.Default, workflowDigestTag(namespace.Default, g.Name()), 0)

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 0 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want 0 (there was no slot to release)", swept)
	}
	if n, err := rdb.SCard(ctx, concurrencyWorkflowIndexKey(namespace.Default)).Result(); err != nil || n != 0 {
		t.Fatalf("workflow index size after sweep = %d (err %v), want the stale tag dropped", n, err)
	}
}

// queueForSweep submits one QUEUE-mode execution through the real admission
// path and returns its ID, so the sweep cases exercise the state production
// writes rather than a hand-built imitation of it.
func queueForSweep(t *testing.T, ctx context.Context, state *Store, g *graph.Graph, limit int) types.ExecutionID {
	t.Helper()
	id := engine.NewExecutionID()
	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	entries := []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}}
	admission := sweepAdmission(g, limit)
	admission.Mode = types.ConcurrencyModeQueue
	res, err := state.CreateExecutionWithConcurrency(ctx, snap, entries, admission)
	if err != nil {
		t.Fatalf("queue submission for %q error = %v", id, err)
	}
	if !res.Queued {
		t.Fatalf("submission for %q = %+v, want it parked", id, res)
	}
	return id
}

// TestSweepConcurrencySlotsFinishesStrandedPromotion covers the promotion crash
// window: the release that freed a slot promoted the head of the line (that
// half is atomic with the release), and the process died before the promoted
// execution's own status and intents were written. The member holds a slot
// while reading queued, and removing it would strand the execution forever —
// nothing else would ever start it. The repair is to finish the promotion.
func TestSweepConcurrencySlotsFinishesStrandedPromotion(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-stranded")
	tag := workflowDigestTag(namespace.Default, g.Name())
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	holder := admitForSweep(t, ctx, state, g, 1)
	waiting := queueForSweep(t, ctx, state, g, 1)

	// The release half only: drive the script the release path runs, without
	// the Go-side promotion completion that follows it in production.
	if _, err := workflowReleaseLua.Run(ctx, rdb, concurrencyReleaseKeys(namespace.Default, tag),
		concurrencySlotMember(holder), time.Now().UTC().Unix(), 3600).Slice(); err != nil {
		t.Fatalf("release script error = %v", err)
	}
	if score, err := rdb.ZScore(ctx, slotKey, string(waiting)).Result(); err != nil {
		t.Fatalf("promoted member absent from the slot set: %v", err)
	} else if score == 0 {
		t.Fatal("promoted member scored 0, want a real admission score")
	}
	if got := queuedStatus(t, ctx, state, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("status after the release script = %q, want %q (the crash the sweep repairs)", got, types.ExecutionStatusQueued)
	}

	if _, err := state.ReconcileConcurrencySlots(ctx, 64); err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if got := queuedStatus(t, ctx, state, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("status after the sweep = %q, want the promotion finished", got)
	}
	if due, err := state.ListOutbox(ctx, waiting, time.Now().UTC(), 8); err != nil || len(due) != 1 {
		t.Fatalf("ready intents after the sweep = %d (err %v), want the parked root released", len(due), err)
	}
}

// TestSweepConcurrencySlotsFillsTheLineAfterRepair covers the other half of a
// repair: releasing a leaked slot frees capacity the line is owed. Promotions
// happen on release, and a sweep is not a release, so without the fill the
// workflow would get its limit back and still not start the waiting work.
func TestSweepConcurrencySlotsFillsTheLineAfterRepair(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-fill")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	// A leaked charge with no execution holds the only slot, and a real
	// submission waits behind it.
	old := time.Now().UTC().Add(-10 * time.Minute).Unix()
	chargeSlotForSweep(t, ctx, rdb, state, g.Name(), types.ExecutionID("leaked-holder"), 1, old)
	waiting := queueForSweep(t, ctx, state, g, 1)

	swept, err := state.ReconcileConcurrencySlots(ctx, 64)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 1 {
		t.Fatalf("ReconcileConcurrencySlots() swept = %d, want the leaked slot released", swept)
	}
	if got := queuedStatus(t, ctx, state, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("waiter status after the sweep = %q, want it promoted into the freed slot", got)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("slot ZSET size after the sweep = %d (err %v), want the promoted waiter", n, err)
	}
}

// queuedStatus reads one execution's lifecycle status.
func queuedStatus(t *testing.T, ctx context.Context, state *Store, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	status, found, err := state.GetExecutionStatus(ctx, id)
	if err != nil {
		t.Fatalf("GetExecutionStatus(%q) error = %v", id, err)
	}
	if !found {
		t.Fatalf("GetExecutionStatus(%q) found = false", id)
	}
	return status
}

// TestSweepConcurrencySlotsStopsAtLimit keeps the pass bounded: the sweeper
// calls it from a maintenance loop, and an unbounded sweep would turn one
// workflow's backlog into a stall for every namespace behind it.
func TestSweepConcurrencySlotsStopsAtLimit(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-sweep-limit")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())
	old := time.Now().UTC().Add(-10 * time.Minute).Unix()

	for i := 0; i < 5; i++ {
		id := types.ExecutionID("leaked-" + string(rune('a'+i)))
		chargeSlotForSweep(t, ctx, rdb, state, g.Name(), id, 8, old)
	}

	swept, err := state.ReconcileConcurrencySlots(ctx, 2)
	if err != nil {
		t.Fatalf("ReconcileConcurrencySlots() error = %v", err)
	}
	if swept != 2 {
		t.Fatalf("ReconcileConcurrencySlots(limit 2) swept = %d, want 2", swept)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 3 {
		t.Fatalf("slot ZSET size = %d (err %v), want 3 left for the next pass", n, err)
	}
}

// TestReleaseWorkflowSlotIsIdempotent pins the property every terminal writer
// depends on: calling the release twice, or on an execution that never held a
// slot, must release nothing and report no error.
func TestReleaseWorkflowSlotIsIdempotent(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-release-idempotent")
	slotKey := concurrencySlotKey(namespace.Default, g.Name())

	id := admitForSweep(t, ctx, state, g, 1)
	if err := state.releaseWorkflowSlot(ctx, id, namespace.Default, ""); err != nil {
		t.Fatalf("first release error = %v", err)
	}
	if err := state.releaseWorkflowSlot(ctx, id, namespace.Default, ""); err != nil {
		t.Fatalf("second release error = %v", err)
	}
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 0 {
		t.Fatalf("slot ZSET size = %d (err %v), want empty", n, err)
	}

	// An execution that never held a slot: no back-reference, nothing to free.
	unlimited := engine.NewExecutionID()
	if err := state.releaseWorkflowSlot(ctx, unlimited, namespace.Default, ""); err != nil {
		t.Fatalf("release for a slotless execution error = %v, want nil", err)
	}
}
