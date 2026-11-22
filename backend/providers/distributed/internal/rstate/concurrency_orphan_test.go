package rstate

// Orphan executions: the SQL audit row and the Redis status say running or
// queued while the engine can no longer route the execution, because the graph
// key that routing needs is gone. SAS reported this as a workflow stuck behind
// its own concurrency limit: execution 12 held the only slot, its xflow_nodes
// rows were empty, and Cancel could not converge it.
//
// These tests pin the repair. Cancel converges the torn shape, the periodic
// sweep converges it and hands the freed slot to the head of the line, a queued
// orphan is dropped rather than promoted, work that is visibly alive (a graph, a
// lease, a suspension) is never touched, and the SQL mirror is projected for
// every shape — including the one whose Redis status key is already gone, which
// the drift reconciler skips as not_found.

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// newOrphanStore builds the production read shape: Redis is the scheduling
// authority, memstore stands in for the SQL mirror the SAS deployment reads
// xflow_executions from, and both are wired through the same rstate store the
// server uses.
func newOrphanStore(t *testing.T) (*Store, *redis.Client, store.Store) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	db := memstore.New()
	return New(rdb, db, time.Minute), rdb, db
}

func orphanStatus(t *testing.T, ctx context.Context, state *Store, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	status, found, err := state.GetExecutionStatus(ctx, id)
	if err != nil {
		t.Fatalf("GetExecutionStatus(%q) error = %v", id, err)
	}
	if !found {
		t.Fatalf("execution %q has no status key", id)
	}
	return status
}

func orphanSQLStatus(t *testing.T, ctx context.Context, db store.Store, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("SQL mirror GetExecution(%q) error = %v", id, err)
	}
	return rec.Status
}

func orphanSlotMembers(t *testing.T, ctx context.Context, rdb *redis.Client, workflow string) []string {
	t.Helper()
	members, err := rdb.ZRange(ctx, concurrencySlotKey(namespace.Default, workflow), 0, -1).Result()
	if err != nil {
		t.Fatalf("read slot set for %q: %v", workflow, err)
	}
	return members
}

func orphanQueueMembers(t *testing.T, ctx context.Context, rdb *redis.Client, workflow string) []string {
	t.Helper()
	key := concurrencyQueueKeyForTag(namespace.Default, workflowDigestTag(namespace.Default, workflow))
	members, err := rdb.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatalf("read queue for %q: %v", workflow, err)
	}
	return members
}

func orphanDeleteGraph(t *testing.T, ctx context.Context, rdb *redis.Client, id types.ExecutionID) {
	t.Helper()
	if err := rdb.Del(ctx, execKey(namespace.Default, id, "graph")).Err(); err != nil {
		t.Fatalf("delete graph key for %q: %v", id, err)
	}
}

// orphanAgeStatus backdates the status transition clock — one half of the age
// the orphan judgement reads — so a sweep mode test does not have to wait out
// the grace. Activity is a separate clock; tests that exercise a working
// execution must age it with orphanAgeActivity too (or leave it recent on
// purpose).
func orphanAgeStatus(t *testing.T, ctx context.Context, rdb *redis.Client, id types.ExecutionID, age time.Duration) {
	t.Helper()
	updated := time.Now().Add(-age).UnixMilli()
	if err := rdb.Set(ctx, execKey(namespace.Default, id, "updated_at_ms"), strconv.FormatInt(updated, 10), 0).Err(); err != nil {
		t.Fatalf("age status clock for %q: %v", id, err)
	}
}

// orphanAgeActivity backdates the node activity clock — the other half of the
// age anchor. A lease acquisition rewrites it to "now", so a test that needs a
// genuinely stalled execution after leasing a node has to age this clock as
// well as the status clock.
func orphanAgeActivity(t *testing.T, ctx context.Context, rdb *redis.Client, id types.ExecutionID, age time.Duration) {
	t.Helper()
	activity := time.Now().Add(-age).UnixMilli()
	if err := rdb.Set(ctx, activityAtKey(namespace.Default, id), strconv.FormatInt(activity, 10), 0).Err(); err != nil {
		t.Fatalf("age activity clock for %q: %v", id, err)
	}
}

// orphanActivityAt reads the activity clock, failing when it is absent.
func orphanActivityAt(t *testing.T, ctx context.Context, rdb *redis.Client, id types.ExecutionID) int64 {
	t.Helper()
	raw, err := rdb.Get(ctx, activityAtKey(namespace.Default, id)).Int64()
	if err != nil {
		t.Fatalf("read activity clock for %q: %v", id, err)
	}
	return raw
}

func orphanGraphLimit(t *testing.T, name string, limit int) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit: limit,
			Mode:  types.ConcurrencyModeQueue,
		}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// orphanSubmit submits one execution through the real admission path and
// reports where it landed.
func orphanSubmit(t *testing.T, ctx context.Context, state *Store, g *graph.Graph, limit int) (types.ExecutionID, engine.ConcurrencyAdmissionResult) {
	t.Helper()
	id := engine.NewExecutionID()
	res, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec,
		},
	}}, engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     limit,
		Mode:      types.ConcurrencyModeQueue,
	})
	if err != nil {
		t.Fatalf("submission error = %v", err)
	}
	return id, res
}

func orphanReclaim(t *testing.T, ctx context.Context, state *Store, req engine.OrphanReclaimRequest) engine.OrphanReclaimResult {
	t.Helper()
	res, err := state.ReclaimConcurrencyOrphans(ctx, req)
	if err != nil {
		t.Fatalf("ReclaimConcurrencyOrphans(%+v) error = %v", req, err)
	}
	return res
}

func orphanCandidateFor(res engine.OrphanReclaimResult, id types.ExecutionID) *engine.OrphanReclaimCandidate {
	for i := range res.Candidates {
		if res.Candidates[i].ExecutionID == id {
			return &res.Candidates[i]
		}
	}
	return nil
}

func orphanError(t *testing.T, ctx context.Context, rdb *redis.Client, id types.ExecutionID) string {
	t.Helper()
	raw, err := rdb.Get(ctx, execKey(namespace.Default, id, "error")).Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("read error key for %q: %v", id, err)
	}
	return raw
}

// TestCancelConvergesGraphLostRunningExecution is the SAS shape at the level
// the caller saw it: the only reachable operation is Cancel, and Cancel used to
// answer ErrExecutionInactive without writing anything.
func TestCancelConvergesGraphLostRunningExecution(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	g := orphanGraphLimit(t, "orphan-cancel-running", 1)
	holder, res := orphanSubmit(t, ctx, state, g, 1)
	if res.Queued {
		t.Fatalf("first submission = %+v, want the slot", res)
	}
	waiting, res := orphanSubmit(t, ctx, state, g, 1)
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("second submission = %+v, want queued at 1", res)
	}

	orphanDeleteGraph(t, ctx, rdb, holder)
	// A fresh store instance models the restart this shape comes from: the store
	// that admitted the execution has its graph in an in-memory cache, and only a
	// process that has to re-read Redis sees the graph as gone.
	restarted := New(rdb, db, time.Minute)
	obs := &orphanObserverRecorder{}
	restarted.SetOrphanReclaimObserver(obs)
	eng := engine.New(restarted, nil)
	if err := eng.Cancel(ctx, holder); err != nil {
		t.Fatalf("Cancel(graph-lost %q) error = %v, want nil", holder, err)
	}
	// Cancel's self-heal is its own reclaim point: the observer must attribute
	// the convergence to reason=cancel, not to the periodic sweep.
	if events := obs.snapshot(); len(events) != 1 || events[0] != (orphanObservation{kind: engine.OrphanKindRunning, reason: engine.OrphanTriggerCancel}) {
		t.Fatalf("Cancel reclaim observations = %+v, want exactly [{running cancel}]", events)
	}

	if got := orphanStatus(t, ctx, restarted, holder); got != types.ExecutionStatusCanceled {
		t.Fatalf("Redis status = %q, want canceled", got)
	}
	if got := orphanError(t, ctx, rdb, holder); got != engine.OrphanReclaimReason {
		t.Fatalf("error key = %q, want %q", got, engine.OrphanReclaimReason)
	}
	if got := orphanSQLStatus(t, ctx, db, holder); got != types.ExecutionStatusCanceled {
		t.Fatalf("SQL status = %q, want canceled", got)
	}
	// The freed slot must go to the head of the line in the same operation.
	if got := orphanStatus(t, ctx, state, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("queued execution status = %q, want running (the slot was not refilled)", got)
	}
	if got := orphanSQLStatus(t, ctx, db, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("queued execution SQL status = %q, want running (promotion was not projected)", got)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(waiting) {
		t.Fatalf("slot members = %v, want [%q]", members, waiting)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
		t.Fatalf("queue members = %v, want empty", members)
	}
}

// TestCancelLeavesTerminalAndAbsentExecutionsInactive pins the other half of
// the Cancel contract: only the torn shape is converged. A terminal execution
// has nothing to do, and an execution with no evidence at all cannot be
// terminated by a cancel.
func TestCancelLeavesTerminalAndAbsentExecutionsInactive(t *testing.T) {
	t.Run("terminal", func(t *testing.T) {
		ctx := namespace.WithNamespace(context.Background(), namespace.Default)
		state, _, db := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-cancel-terminal", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
			t.Fatalf("terminal write error = %v", err)
		}
		eng := engine.New(state, nil)
		if err := eng.Cancel(ctx, id); !errors.Is(err, engine.ErrExecutionInactive) {
			t.Fatalf("Cancel(%q) error = %v, want ErrExecutionInactive", id, err)
		}
		if got := orphanStatus(t, ctx, state, id); got != types.ExecutionStatusCanceled {
			t.Fatalf("status after Cancel = %q, want canceled", got)
		}
		if got := orphanSQLStatus(t, ctx, db, id); got != types.ExecutionStatusCanceled {
			t.Fatalf("SQL status after Cancel = %q, want canceled", got)
		}
	})
	t.Run("absent", func(t *testing.T) {
		ctx := namespace.WithNamespace(context.Background(), namespace.Default)
		state, rdb, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-cancel-absent", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		// Retention lapsed: no status key at all, so there is no execution a
		// cancel could terminate. The concurrency member, if any, is the sweep's
		// shape, not Cancel's.
		if err := rdb.Del(ctx, execKey(namespace.Default, id, "status")).Err(); err != nil {
			t.Fatalf("delete status key: %v", err)
		}
		eng := engine.New(state, nil)
		if err := eng.Cancel(ctx, id); !errors.Is(err, engine.ErrExecutionInactive) {
			t.Fatalf("Cancel(absent %q) error = %v, want ErrExecutionInactive", id, err)
		}
		if _, found, err := state.GetExecutionStatus(ctx, id); err != nil || found {
			t.Fatalf("status after Cancel found=%v err=%v, want absent", found, err)
		}
	})
}

// TestSweepConvergesRunningOrphanAndResumesQueue is the periodic repair: the
// sweep mode judgement, the slot release, the refill, and both SQL projections.
func TestSweepConvergesRunningOrphanAndResumesQueue(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	g := orphanGraphLimit(t, "orphan-sweep-running", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	waiting, _ := orphanSubmit(t, ctx, state, g, 1)

	orphanDeleteGraph(t, ctx, rdb, holder)
	orphanAgeStatus(t, ctx, rdb, holder, 10*time.Minute)
	res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 32, Grace: time.Minute,
	})
	if res.Reclaimed < 1 {
		t.Fatalf("Reclaimed = %d, want at least 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	candidate := orphanCandidateFor(res, holder)
	if candidate == nil {
		t.Fatalf("no candidate for %q: %+v", holder, res.Candidates)
	}
	if candidate.Status != types.ExecutionStatusRunning || candidate.Evidence != engine.OrphanEvidenceGraphMissing {
		t.Fatalf("candidate = %+v, want running/%s", candidate, engine.OrphanEvidenceGraphMissing)
	}
	if got := orphanStatus(t, ctx, state, holder); got != types.ExecutionStatusCanceled {
		t.Fatalf("orphan status = %q, want canceled", got)
	}
	if got := orphanSQLStatus(t, ctx, db, holder); got != types.ExecutionStatusCanceled {
		t.Fatalf("orphan SQL status = %q, want canceled", got)
	}
	if got := orphanStatus(t, ctx, state, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("waiting status = %q, want running", got)
	}
	if got := orphanSQLStatus(t, ctx, db, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("waiting SQL status = %q, want running", got)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(waiting) {
		t.Fatalf("slot members = %v, want [%q]", members, waiting)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
		t.Fatalf("queue members = %v, want empty", members)
	}
}

// TestSweepDropsQueuedOrphanAndPromotesBehind pins that a graph-lost queued
// execution does not merely lose its graph: its place in line is removed, so it
// cannot be promoted into the slot it would waste.
func TestSweepDropsQueuedOrphanAndPromotesBehind(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	g := orphanGraphLimit(t, "orphan-sweep-queued", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	orphan, res := orphanSubmit(t, ctx, state, g, 1)
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("orphan submission = %+v, want queued at 1", res)
	}
	behind, res := orphanSubmit(t, ctx, state, g, 1)
	if !res.Queued || res.QueuePosition != 2 {
		t.Fatalf("behind submission = %+v, want queued at 2", res)
	}

	orphanDeleteGraph(t, ctx, rdb, orphan)
	orphanAgeStatus(t, ctx, rdb, orphan, 10*time.Minute)
	sweep := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 32, Grace: time.Minute,
	})
	if sweep.Reclaimed < 1 {
		t.Fatalf("Reclaimed = %d, want at least 1", sweep.Reclaimed)
	}
	if got := orphanStatus(t, ctx, state, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("orphan status = %q, want canceled", got)
	}
	if got := orphanSQLStatus(t, ctx, db, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("orphan SQL status = %q, want canceled", got)
	}
	if got := orphanStatus(t, ctx, state, holder); got != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want untouched running", got)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(behind) {
		t.Fatalf("queue members after sweep = %v, want [%q]", members, behind)
	}

	// The holder ends; the line must advance past the canceled member.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	if got := orphanStatus(t, ctx, state, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("orphan status after promotion = %q, want still canceled", got)
	}
	if got := orphanStatus(t, ctx, state, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("execution behind the orphan status = %q, want running: the canceled orphan held the line", got)
	}
	if got := orphanSQLStatus(t, ctx, db, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("execution behind the orphan SQL status = %q, want running", got)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(behind) {
		t.Fatalf("slot members = %v, want [%q]", members, behind)
	}
}

// TestSweepRefusesWorkThatIsVisiblyAlive covers every guard that outranks a
// missing graph: a graph that is present, an unexpired node lease, a suspended
// (signal or timer) node, a paused execution, and a status younger than the
// grace. Force mode bypasses the grace and nothing else, so it is used to prove
// each non-age guard independently.
func TestSweepRefusesWorkThatIsVisiblyAlive(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	t.Run("live graph", func(t *testing.T) {
		state, _, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-guard-graph", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		})
		if res.Reclaimed != 0 {
			t.Fatalf("Reclaimed = %d, want 0", res.Reclaimed)
		}
		if got := orphanStatus(t, ctx, state, id); got != types.ExecutionStatusRunning {
			t.Fatalf("status = %q, want running", got)
		}
	})

	t.Run("active lease", func(t *testing.T) {
		state, rdb, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-guard-lease", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		orphanDeleteGraph(t, ctx, rdb, id)
		if err := rdb.ZAdd(ctx, leaseExpiryZSetKey(namespace.Default, id), redis.Z{
			Score: float64(time.Now().Add(time.Minute).UnixMilli()), Member: "node/start",
		}).Err(); err != nil {
			t.Fatalf("seed lease expiry: %v", err)
		}
		req := engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		}
		if res := orphanReclaim(t, ctx, state, req); res.Reclaimed != 0 {
			t.Fatalf("Reclaimed = %d, want 0 while a lease is live", res.Reclaimed)
		}
		if got := orphanStatus(t, ctx, state, id); got != types.ExecutionStatusRunning {
			t.Fatalf("status = %q, want running", got)
		}
		if err := rdb.ZRem(ctx, leaseExpiryZSetKey(namespace.Default, id), "node/start").Err(); err != nil {
			t.Fatalf("remove lease: %v", err)
		}
		if res := orphanReclaim(t, ctx, state, req); res.Reclaimed != 1 {
			t.Fatalf("Reclaimed = %d, want 1 once the lease is gone", res.Reclaimed)
		}
	})

	t.Run("suspended timer wait", func(t *testing.T) {
		state, rdb, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-guard-suspended", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		orphanDeleteGraph(t, ctx, rdb, id)
		if err := rdb.SAdd(ctx, suspendedNodesKey(namespace.Default, id), "wait").Err(); err != nil {
			t.Fatalf("seed suspended node: %v", err)
		}
		req := engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		}
		if res := orphanReclaim(t, ctx, state, req); res.Reclaimed != 0 {
			t.Fatalf("Reclaimed = %d, want 0 while a node is suspended", res.Reclaimed)
		}
		if err := rdb.SRem(ctx, suspendedNodesKey(namespace.Default, id), "wait").Err(); err != nil {
			t.Fatalf("remove suspension: %v", err)
		}
		if res := orphanReclaim(t, ctx, state, req); res.Reclaimed != 1 {
			t.Fatalf("Reclaimed = %d, want 1 once the wait is gone", res.Reclaimed)
		}
	})

	t.Run("paused", func(t *testing.T) {
		state, rdb, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-guard-paused", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		orphanDeleteGraph(t, ctx, rdb, id)
		if err := rdb.Set(ctx, execKey(namespace.Default, id, "status"), string(types.ExecutionStatusPaused), 0).Err(); err != nil {
			t.Fatalf("pause execution: %v", err)
		}
		res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		})
		if res.Reclaimed != 0 {
			t.Fatalf("Reclaimed = %d, want 0 for a paused execution", res.Reclaimed)
		}
		if got := orphanStatus(t, ctx, state, id); got != types.ExecutionStatusPaused {
			t.Fatalf("status = %q, want paused", got)
		}
	})

	t.Run("within grace", func(t *testing.T) {
		state, rdb, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-guard-grace", 1)
		id, _ := orphanSubmit(t, ctx, state, g, 1)
		orphanDeleteGraph(t, ctx, rdb, id)
		orphanAgeStatus(t, ctx, rdb, id, 10*time.Millisecond)
		res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Hour,
		})
		if res.Reclaimed != 0 {
			t.Fatalf("Reclaimed = %d, want 0 inside the grace window", res.Reclaimed)
		}
		res = orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		})
		if res.Reclaimed != 1 {
			t.Fatalf("force Reclaimed = %d, want 1", res.Reclaimed)
		}
	})
}

// TestReclaimDropsMissingExecutionMemberAndProjectsSQL is the retention-lapsed
// shape: the concurrency member survives, every execution key is gone, and the
// SQL row is the only thing that can still be corrected. The drift reconciler
// skips exactly this row as not_found.
func TestReclaimDropsMissingExecutionMemberAndProjectsSQL(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	g := orphanGraphLimit(t, "orphan-missing-status", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	orphan, res := orphanSubmit(t, ctx, state, g, 1)
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("orphan submission = %+v, want queued at 1", res)
	}

	if err := rdb.Del(ctx,
		execKey(namespace.Default, orphan, "status"),
		execKey(namespace.Default, orphan, "graph"),
		execKey(namespace.Default, orphan, "updated_at_ms"),
		execKey(namespace.Default, orphan, "workflow_id"),
	).Err(); err != nil {
		t.Fatalf("delete execution keys: %v", err)
	}
	sweep := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Limit: 32,
	})
	if sweep.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1", sweep.Reclaimed)
	}
	candidate := orphanCandidateFor(sweep, orphan)
	if candidate == nil || candidate.Evidence != engine.OrphanEvidenceExecutionMissing {
		t.Fatalf("candidate = %+v, want evidence %q", candidate, engine.OrphanEvidenceExecutionMissing)
	}
	if _, found, err := state.GetExecutionStatus(ctx, orphan); err != nil || found {
		t.Fatalf("status found=%v err=%v, want no status invented", found, err)
	}
	if got := orphanSQLStatus(t, ctx, db, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("SQL status = %q, want canceled", got)
	}
	if got := orphanStatus(t, ctx, state, holder); got != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want untouched running", got)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
		t.Fatalf("queue members = %v, want empty", members)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(holder) {
		t.Fatalf("slot members = %v, want [%q]", members, holder)
	}
}

// TestPromotionValidatesGraphAndProjectsSQL exercises the promotion path on its
// own, without a sweep: a slot release promotes the head of the line, and that
// promotion must refuse a graph-lost execution instead of charging it the slot.
// The head's clocks are aged past the grace first: promotion now applies the
// same age gate as the sweep, so a freshly queued member that loses its graph
// keeps the slot until the grace elapses (see the contract's
// PromotionGraceLeavesHeadThenSweepConverges for that half).
func TestPromotionValidatesGraphAndProjectsSQL(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	g := orphanGraphLimit(t, "orphan-promotion", 1)
	holder, _ := orphanSubmit(t, ctx, state, g, 1)
	orphan, _ := orphanSubmit(t, ctx, state, g, 1)
	behind, _ := orphanSubmit(t, ctx, state, g, 1)
	orphanDeleteGraph(t, ctx, rdb, orphan)
	orphanAgeStatus(t, ctx, rdb, orphan, 10*time.Minute)

	// The holder finishes normally; the release promotes the graph-lost member.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	if got := orphanStatus(t, ctx, state, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("promoted orphan status = %q, want canceled", got)
	}
	if got := orphanSQLStatus(t, ctx, db, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("promoted orphan SQL status = %q, want canceled", got)
	}
	// The slot the promotion had handed it must be given to the next in line,
	// not held by an execution that can never start.
	if got := orphanStatus(t, ctx, state, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("execution behind the promoted orphan status = %q, want running", got)
	}
	if got := orphanSQLStatus(t, ctx, db, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("promotion SQL projection = %q, want running", got)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(behind) {
		t.Fatalf("slot members = %v, want [%q]", members, behind)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
		t.Fatalf("queue members = %v, want empty", members)
	}
}

// TestReclaimDryRunPreviewsWithoutWriting is the shape the reset API's preview
// will reuse: the same judgement, no mutation.
func TestReclaimDryRunPreviewsWithoutWriting(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	g := orphanGraphLimit(t, "orphan-dry-run", 1)
	id, _ := orphanSubmit(t, ctx, state, g, 1)
	orphanDeleteGraph(t, ctx, rdb, id)

	res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, DryRun: true, ExecutionID: id, Namespace: "default", Limit: 1,
	})
	if res.Reclaimed != 0 {
		t.Fatalf("dry-run Reclaimed = %d, want 0", res.Reclaimed)
	}
	candidate := orphanCandidateFor(res, id)
	if candidate == nil || candidate.Evidence != engine.OrphanEvidenceGraphMissing {
		t.Fatalf("dry-run candidate = %+v, want graph_missing", candidate)
	}
	if got := orphanStatus(t, ctx, state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("dry-run status = %q, want running", got)
	}
	if got := orphanSQLStatus(t, ctx, db, id); got != types.ExecutionStatusRunning {
		t.Fatalf("dry-run SQL status = %q, want running", got)
	}
}

// TestSweepConvergesSASShapeInOnePass reproduces the incident: two slots held
// and five executions queued, all seven with their graph keys lost. A single
// bounded pass must converge all seven, empty the concurrency state, and project
// every SQL row.
func TestSweepConvergesSASShapeInOnePass(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, rdb, db := newOrphanStore(t)
	const limit = 2
	g := orphanGraphLimit(t, "orphan-sas-shape", limit)
	ids := make([]types.ExecutionID, 0, limit+5)
	for i := 0; i < limit+5; i++ {
		id, res := orphanSubmit(t, ctx, state, g, limit)
		if wantQueued := i >= limit; res.Queued != wantQueued {
			t.Fatalf("submission %d = %+v, want queued=%v", i, res, wantQueued)
		}
		ids = append(ids, id)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != limit {
		t.Fatalf("slot members = %v, want %d", members, limit)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 5 {
		t.Fatalf("queue members = %v, want 5", members)
	}
	for _, id := range ids {
		orphanDeleteGraph(t, ctx, rdb, id)
		orphanAgeStatus(t, ctx, rdb, id, 10*time.Minute)
	}

	res := orphanReclaim(t, ctx, state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 64, Grace: time.Minute,
	})
	if res.Reclaimed != len(ids) {
		t.Fatalf("Reclaimed = %d, want %d (candidates %+v)", res.Reclaimed, len(ids), res.Candidates)
	}
	if len(res.Candidates) != len(ids) {
		t.Fatalf("Candidates = %d, want %d", len(res.Candidates), len(ids))
	}
	for _, id := range ids {
		if got := orphanStatus(t, ctx, state, id); got != types.ExecutionStatusCanceled {
			t.Fatalf("execution %q status = %q, want canceled", id, got)
		}
		if got := orphanSQLStatus(t, ctx, db, id); got != types.ExecutionStatusCanceled {
			t.Fatalf("execution %q SQL status = %q, want canceled", id, got)
		}
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
		t.Fatalf("slot members after one pass = %v, want empty", members)
	}
	if members := orphanQueueMembers(t, ctx, rdb, g.Name()); len(members) != 0 {
		t.Fatalf("queue members after one pass = %v, want empty", members)
	}

	// The repaired state must be usable: a fresh submission of the same
	// workflow takes the freed capacity and starts instead of queueing behind
	// members the sweep no longer knows about.
	fresh, freshRes := orphanSubmit(t, ctx, state, g, limit)
	if freshRes.Queued {
		t.Fatalf("post-sweep submission = %+v, want it admitted into the freed capacity", freshRes)
	}
	if got := orphanStatus(t, ctx, state, fresh); got != types.ExecutionStatusRunning {
		t.Fatalf("post-sweep execution status = %q, want running", got)
	}
	if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 || members[0] != string(fresh) {
		t.Fatalf("slot members after the post-sweep submission = %v, want [%q]", members, fresh)
	}
}

// TestReclaimRacesTerminalWriteWithoutDoubleRelease runs the two convergent
// writers against each other: whichever wins, exactly one slot may be handed
// out, and the loser must not free it a second time.
func TestReclaimRacesTerminalWriteWithoutDoubleRelease(t *testing.T) {
	for iter := 0; iter < 10; iter++ {
		ctx := namespace.WithNamespace(context.Background(), namespace.Default)
		state, rdb, _ := newOrphanStore(t)
		g := orphanGraphLimit(t, "orphan-race-"+strconv.Itoa(iter), 1)
		holding, _ := orphanSubmit(t, ctx, state, g, 1)
		first, _ := orphanSubmit(t, ctx, state, g, 1)
		second, _ := orphanSubmit(t, ctx, state, g, 1)
		orphanDeleteGraph(t, ctx, rdb, holding)

		var wg sync.WaitGroup
		var reclaimErr, terminalErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, reclaimErr = state.ReclaimConcurrencyOrphans(ctx, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimForce, ExecutionID: holding, Namespace: "default", Limit: 1,
			})
		}()
		go func() {
			defer wg.Done()
			terminalErr = state.UpdateExecutionStatus(ctx, holding, types.ExecutionStatusCanceled, "")
		}()
		wg.Wait()
		if reclaimErr != nil || terminalErr != nil {
			t.Fatalf("iter %d reclaimErr=%v terminalErr=%v", iter, reclaimErr, terminalErr)
		}
		if got := orphanStatus(t, ctx, state, holding); got != types.ExecutionStatusCanceled {
			t.Fatalf("iter %d holding status = %q, want canceled", iter, got)
		}
		running := 0
		for _, id := range []types.ExecutionID{first, second} {
			if orphanStatus(t, ctx, state, id) == types.ExecutionStatusRunning {
				running++
			}
		}
		if running != 1 {
			t.Fatalf("iter %d running waiters = %d, want exactly 1: the slot was released twice", iter, running)
		}
		if members := orphanSlotMembers(t, ctx, rdb, g.Name()); len(members) != 1 {
			t.Fatalf("iter %d slot members = %v, want exactly one", iter, members)
		}
	}
}
