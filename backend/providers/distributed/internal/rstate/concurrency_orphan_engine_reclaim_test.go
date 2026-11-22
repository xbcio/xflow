package rstate

// Engine-level reclaim tests. They answer the risk question the store-level
// tests cannot: when Redis loses only the graph key while the executing process
// still holds that graph in memory, does the repair path wait for actual
// evidence that work is over, and what happens to a worker that keeps running
// after the repair has converged its execution?

import (
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestEngineLeaseBlocksSweepAndCancelUntilExpiry is the core guard: a real
// lease issued by a real engine, a graph key lost from Redis under it, and a
// status clock old enough that only the lease can be protecting the execution.
func TestEngineLeaseBlocksSweepAndCancelUntilExpiry(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := orphanGraphLimit(t, "orphan-engine-lease", 1)
	id := rig.submit(g)
	task := rig.queue.takeFor(id)
	lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	rig.dropRunningGraph(id)
	orphanAgeStatus(t, rig.ctx, rig.rdb, id, 10*time.Minute)

	sweep := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	}
	if res := orphanReclaim(t, rig.ctx, rig.state, sweep); res.Reclaimed != 0 {
		t.Fatalf("sweep Reclaimed = %d while the lease is live, want 0", res.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded sweep = %q, want running", got)
	}
	if members := orphanSlotMembers(t, rig.ctx, rig.rdb, g.Name()); len(members) != 1 || members[0] != string(id) {
		t.Fatalf("slot members after guarded sweep = %v, want [%q]", members, id)
	}

	// Cancel's self-heal asks the same question through force mode. A Store
	// that never cached this execution's graph is required to reach it: the
	// same-process Store still has the graph in its own LRU, so loadActiveGraph
	// succeeds and a warm Cancel takes the ordinary path (asserted below).
	// Force still has to honour the live lease.
	cold := engine.New(New(rig.rdb, nil, time.Minute), &engOrphanQueue{})
	if err := cold.Cancel(rig.ctx, id); !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("cold-engine Cancel error = %v, want ErrExecutionInactive (lease must block the self-heal)", err)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded Cancel = %q, want running", got)
	}
	if got := orphanError(t, rig.ctx, rig.rdb, id); got != "" {
		t.Fatalf("error key after guarded Cancel = %q, want empty", got)
	}

	// Expire the lease the way the sweeper sees it: the index member's score
	// moves into the past. The node meta is deliberately left running; the
	// reclaim judgement reads the lease index only. The acquire stamped the
	// execution's activity clock, so that clock must be aged too: "the lease
	// expired" alone is not proof of idleness while the lease acquisition itself
	// is recent evidence of work.
	if err := rig.rdb.ZAdd(rig.ctx, leaseExpiryZSetKey(namespace.Default, id), redis.Z{
		Score: float64(time.Now().Add(-time.Minute).UnixMilli()), Member: leaseExpiryMember(id, task.NodeName),
	}).Err(); err != nil {
		t.Fatalf("expire lease index: %v", err)
	}
	orphanAgeActivity(t, rig.ctx, rig.rdb, id, 10*time.Minute)
	res := orphanReclaim(t, rig.ctx, rig.state, sweep)
	if res.Reclaimed != 1 {
		t.Fatalf("sweep Reclaimed = %d after the lease expired, want 1", res.Reclaimed)
	}
	candidate := orphanCandidateFor(res, id)
	if candidate == nil || candidate.Status != types.ExecutionStatusRunning || candidate.Evidence != engine.OrphanEvidenceGraphMissing {
		t.Fatalf("candidate = %+v, want running/%s", candidate, engine.OrphanEvidenceGraphMissing)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after reclaim = %q, want canceled", got)
	}

	// The worker still holds its lease and its cached graph. What does its
	// commit do now? Observed: the outcome is CommitOutcomeExecutionInactive and
	// the error is nil, so the result is DROPPED — no output row, no downstream
	// task, no resurrection — while the node snapshot is left running. The nil
	// error is deliberate in the engine contract (an inactive execution is a
	// handled late result, not a retryable failure), so a runner cannot tell
	// "result applied" from "result discarded" here.
	outcome, err := rig.eng.CommitTaskResultWithOutcome(rig.ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"late": true}},
	})
	if err != nil {
		t.Fatalf("late commit error = %v, want nil (inactive commits are dropped, not rejected)", err)
	}
	if outcome != engine.CommitOutcomeExecutionInactive {
		t.Fatalf("late commit outcome = %q, want %q", outcome, engine.CommitOutcomeExecutionInactive)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after late commit = %q, want canceled (must not be resurrected)", got)
	}
	out, err := rig.state.GetOutput(rig.ctx, id, task.NodeName)
	if err != nil {
		t.Fatalf("GetOutput after late commit error = %v", err)
	}
	if out != nil {
		t.Fatalf("output after late commit = %#v, want nil (the commit was dropped)", out)
	}
	if node, err := rig.state.GetNode(rig.ctx, id, task.NodeName); err != nil || node == nil || node.Status != types.NodeStatusRunning {
		t.Fatalf("node after late commit = %+v err=%v, want the abandoned running snapshot", node, err)
	}
	if rig.queue.pending() != 0 {
		t.Fatalf("queued tasks after late commit = %d, want 0: a dropped commit must schedule nothing", rig.queue.pending())
	}
}

// TestEngineWarmCancelUsesOrdinaryPath documents the other half of the cache
// risk for Cancel: when the same process still has the graph, Cancel never
// reaches the orphan self-heal at all. loadActiveGraph serves the cached graph,
// the execution is active, and the ordinary cancel path runs — so the terminal
// write is recorded with no orphan_reclaimed reason. This is correct for an
// explicit operator cancel; it is called out because it means a warm process
// cannot be used to demonstrate the self-heal, and because the durable reason
// is the only way to tell the two paths apart after the fact.
func TestEngineWarmCancelUsesOrdinaryPath(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := orphanGraphLimit(t, "orphan-engine-warm-cancel", 1)
	id := rig.submit(g)
	rig.dropRunningGraph(id)

	if err := rig.eng.Cancel(rig.ctx, id); err != nil {
		t.Fatalf("warm Cancel error = %v, want nil through the ordinary path", err)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after warm Cancel = %q, want canceled", got)
	}
	if got := orphanError(t, rig.ctx, rig.rdb, id); got != "" {
		t.Fatalf("error key after warm Cancel = %q; non-empty means the orphan self-heal ran, "+
			"but a warm cache makes it unreachable", got)
	}
	if members := orphanSlotMembers(t, rig.ctx, rig.rdb, g.Name()); len(members) != 0 {
		t.Fatalf("slot members after warm Cancel = %v, want empty", members)
	}
}

// TestEnginePromotionRefusesLeaseHeldGraphLostMember pins the promotion guard's
// ordering. A queued execution cannot normally hold a lease, so the lease is
// seeded directly here; what is being tested is that the promotion path checks
// the lease before converging a graph-lost member, because a promotion that
// reaped a running worker would be the same false positive as a sweep doing it.
// The age clocks are backdated first, so the grace cannot mask which guard
// refused the promotion.
func TestEnginePromotionRefusesLeaseHeldGraphLostMember(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := orphanGraphLimit(t, "orphan-engine-promotion", 1)
	holder := rig.submit(g)
	waiting := rig.submit(g)
	if members := orphanQueueMembers(t, rig.ctx, rig.rdb, g.Name()); len(members) != 1 || members[0] != string(waiting) {
		t.Fatalf("queue members = %v, want [%q]", members, waiting)
	}
	rig.dropGraph(waiting)
	orphanAgeStatus(t, rig.ctx, rig.rdb, waiting, 10*time.Minute)
	if err := rig.rdb.ZAdd(rig.ctx, leaseExpiryZSetKey(namespace.Default, waiting), redis.Z{
		Score: float64(time.Now().Add(time.Minute).UnixMilli()), Member: leaseExpiryMember(waiting, "start"),
	}).Err(); err != nil {
		t.Fatalf("seed lease index: %v", err)
	}

	// The holder concluding promotes the head of the line; the promotion must
	// validate the graph and then refuse on the live lease.
	if err := rig.state.UpdateExecutionStatus(rig.ctx, holder, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("promoted orphan status = %q, want queued (the live lease must block the promotion reclaim)", got)
	}
	if members := orphanSlotMembers(t, rig.ctx, rig.rdb, g.Name()); len(members) != 1 || members[0] != string(waiting) {
		t.Fatalf("slot members = %v, want [%q] still holding the slot", members, waiting)
	}

	// Once the lease is gone the next sweep re-evaluates the slot-holding member
	// (the refused promotion left it exactly in the shape the sweep classifies as
	// a half-finished promotion) and converges it.
	if err := rig.rdb.ZRem(rig.ctx, leaseExpiryZSetKey(namespace.Default, waiting), leaseExpiryMember(waiting, "start")).Err(); err != nil {
		t.Fatalf("drop forged lease: %v", err)
	}
	res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d after the lease was gone, want 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, waiting); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after reclaim = %q, want canceled", got)
	}
}

// TestEngineCancelSelfHealRespectsRecentActivity is the false-positive
// regression at the level Cancel actually runs: an execution whose graph key is
// gone, whose status clock is old, and whose last node commit is inside the
// grace. Cancel's self-heal is a force pass that skips the status clock, so
// before the activity clock existed it would terminate this execution while a
// worker was demonstrably making progress. It must answer ErrExecutionInactive
// instead; once the activity clock also ages past the grace the same sweep
// converges it.
func TestEngineCancelSelfHealRespectsRecentActivity(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := engOrphanTwoNodeGraph(t, "orphan-engine-activity-cancel")
	id := rig.submit(g)
	task := rig.queue.takeFor(id)
	if task == nil {
		t.Fatal("submitted execution has no queued root task")
	}
	lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	// The commit is the activity the old guard could not see: it leaves the
	// execution between leases with its status clock unmoved.
	if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"step": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(start) error = %v", err)
	}
	rig.dropRunningGraph(id)
	orphanAgeStatus(t, rig.ctx, rig.rdb, id, 10*time.Minute)

	// A restarted process cannot route the execution and asks for the self-heal.
	cold := engine.New(New(rig.rdb, nil, time.Minute), &engOrphanQueue{})
	if err := cold.Cancel(rig.ctx, id); !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("Cancel error = %v, want ErrExecutionInactive: recent activity must block the self-heal", err)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded Cancel = %q, want running", got)
	}
	if got := orphanError(t, rig.ctx, rig.rdb, id); got != "" {
		t.Fatalf("error key after guarded Cancel = %q, want empty", got)
	}

	// Once the activity is also past the grace the next sweep converges it: the
	// block above was the activity clock, not some other guard.
	orphanAgeActivity(t, rig.ctx, rig.rdb, id, 10*time.Minute)
	res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d after both clocks aged, want 1", res.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// TestEngineActivityClockRefreshOnLeaseRenewAndCommit pins the write points of
// the activity clock: acquiring a lease, renewing it, and an accepted node
// commit each stamp it. The key starts at a stale sentinel before every step, so
// a fresh value can only come from that step.
func TestEngineActivityClockRefreshOnLeaseRenewAndCommit(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := engOrphanTwoNodeGraph(t, "orphan-engine-activity-refresh")
	id := rig.submit(g)
	task := rig.queue.takeFor(id)
	if task == nil {
		t.Fatal("submitted execution has no queued root task")
	}
	freshAfter := func(step string, ms int64) {
		t.Helper()
		if ms <= time.Now().Add(-time.Minute).UnixMilli() {
			t.Fatalf("%s did not refresh activity_at_ms: %d is not recent", step, ms)
		}
	}
	stale := func() {
		t.Helper()
		if err := rig.rdb.Set(rig.ctx, activityAtKey(namespace.Default, id), 1, 0).Err(); err != nil {
			t.Fatalf("seed stale activity clock: %v", err)
		}
	}

	stale()
	lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	freshAfter("lease acquisition", orphanActivityAt(t, rig.ctx, rig.rdb, id))

	stale()
	renewed, err := rig.state.RenewTaskLease(rig.ctx, id, task.NodeName, lease.LeaseToken, time.Now().Add(time.Minute))
	if err != nil || !renewed {
		t.Fatalf("RenewTaskLease() renewed=%v err=%v, want true/nil", renewed, err)
	}
	freshAfter("lease renewal", orphanActivityAt(t, rig.ctx, rig.rdb, id))

	stale()
	if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"step": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(start) error = %v", err)
	}
	freshAfter("accepted commit", orphanActivityAt(t, rig.ctx, rig.rdb, id))
}

// TestEngineSweepPartialRedisLossThreeClasses is the partial-loss matrix: only
// the graph key is deleted, the status keys and concurrency slots survive, and
// one execution sits in each of the three guard classes. Exactly the third may
// be reclaimed.
//
// The age guard is anchored on the newer of the status transition clock and the
// node activity clock (lease acquisition/renewal, accepted commit). The first
// execution is saved by its live lease (and its activity clock is recent from
// the acquisition); the second by its status clock being inside the grace. The
// third has neither a lease nor any activity, so both clocks are old and it is
// reclaimed — even though this process still has its graph cached and its root
// task still queued. There is no cache-awareness: an execution with no live
// lease and no activity for the grace is reclaimable regardless of who can
// still route it.
func TestEngineSweepPartialRedisLossThreeClasses(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := orphanGraphLimit(t, "orphan-engine-partial-loss", 3)
	leased := rig.submit(g)
	withinGrace := rig.submit(g)
	pastGrace := rig.submit(g)
	task := rig.queue.takeFor(leased)
	if task == nil {
		t.Fatal("leased execution has no queued root task")
	}
	if _, err := rig.eng.BuildTaskLease(rig.ctx, task); err != nil {
		t.Fatalf("BuildTaskLease(leased) error = %v", err)
	}

	for _, id := range []types.ExecutionID{leased, withinGrace, pastGrace} {
		rig.dropGraph(id)
	}
	orphanAgeStatus(t, rig.ctx, rig.rdb, leased, 10*time.Minute)
	orphanAgeStatus(t, rig.ctx, rig.rdb, withinGrace, time.Minute)
	orphanAgeStatus(t, rig.ctx, rig.rdb, pastGrace, 10*time.Minute)

	const grace = 5 * time.Minute
	res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 16, Grace: grace,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want exactly 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	candidate := orphanCandidateFor(res, pastGrace)
	if candidate == nil {
		t.Fatalf("reclaimed set = %+v, want only %q", res.Candidates, pastGrace)
	}
	for _, id := range []types.ExecutionID{leased, withinGrace} {
		if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
			t.Fatalf("execution %q status = %q, want untouched running", id, got)
		}
	}
	if got := orphanStatus(t, rig.ctx, rig.state, pastGrace); got != types.ExecutionStatusCanceled {
		t.Fatalf("past-grace execution status = %q, want canceled", got)
	}
	// The slot set is the observable capacity: the two survivors still hold
	// theirs, the reclaimed execution's was released.
	members := orphanSlotMembers(t, rig.ctx, rig.rdb, g.Name())
	if len(members) != 2 {
		t.Fatalf("slot members after sweep = %v, want the two survivors", members)
	}
	for _, raw := range members {
		if raw == string(pastGrace) {
			t.Fatalf("slot members = %v, still holds the reclaimed execution's slot", members)
		}
	}
	// The three executions all had their STATUS keys preserved: this was the
	// partial-loss shape, not retention lapse. A missing status would have been
	// reclaimed through the other evidence path regardless of grace.
	for _, id := range []types.ExecutionID{leased, withinGrace, pastGrace} {
		if _, found, err := rig.state.GetExecutionStatus(rig.ctx, id); err != nil || !found {
			t.Fatalf("status key for %q found=%v err=%v, want preserved", id, found, err)
		}
	}
}
