package statestoretest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ConcurrencyOrphanBackend bundles a fresh store with the hooks the orphan
// contract needs to reproduce the torn shape without reaching into backend
// internals.
type ConcurrencyOrphanBackend struct {
	Store engine.StateStore
	// LoseGraph removes the persisted graph while leaving the status key (and
	// the concurrency membership) untouched: the torn shape.
	LoseGraph func(t *testing.T, id types.ExecutionID)
	// AgeStatus backdates the execution's status clock, which is one half of the
	// age the sweep judges. Without it a test can only exercise force mode.
	AgeStatus func(t *testing.T, id types.ExecutionID, age time.Duration)
	// RecordActivity stamps the execution's node activity clock at "now": the
	// shape of a node that was just leased or committed, even though no status
	// transition happened. It is the other half of the age the sweep judges.
	RecordActivity func(t *testing.T, id types.ExecutionID)
	// AgeActivity backdates the node activity clock. Together with AgeStatus it
	// models an execution that has genuinely been inactive for the full grace.
	AgeActivity func(t *testing.T, id types.ExecutionID, age time.Duration)
	// ForgetActivity removes the activity clock entirely: the shape of every
	// execution created before the clock existed. The age guard must then fall
	// back to the status clock, exactly as it did before the clock was added.
	ForgetActivity func(t *testing.T, id types.ExecutionID)
	// ForgetExecution removes the execution's own record while leaving every
	// concurrency key untouched: the retention-lapsed shape.
	ForgetExecution func(t *testing.T, id types.ExecutionID)
	// TerminalStatusWithoutRelease rewrites the execution's status to canceled
	// while leaving its concurrency member charged and its durable mirror
	// untouched. It is the shape a crash between the terminal write (in
	// rstate, the Lua half) and the Go-side release/projection leaves behind.
	TerminalStatusWithoutRelease func(t *testing.T, id types.ExecutionID)
	// ReconcileSlots runs one maintenance pass over the workflow slot sets.
	// rstate wires ReconcileConcurrencySlots; the in-memory backend wires the
	// sweep of ReclaimConcurrencyOrphans, which performs the same repair under
	// its state mutex. The observable outcomes are what the contract asserts;
	// the two entry points count differently.
	ReconcileSlots func(t *testing.T)
	// Mirror is the optional durable store the state mirrors statuses to. When
	// nil, SQL projection assertions are skipped.
	Mirror store.Store
}

// ConcurrencyOrphanStoreFactory returns a fresh backend for one sub-test. The
// factory must register cleanup for its resources.
type ConcurrencyOrphanStoreFactory func(t *testing.T) ConcurrencyOrphanBackend

// RunConcurrencyOrphanContract exercises engine.ConcurrencyOrphanReclaimer
// against a concrete backend. Both backends must pass it, so the in-memory and
// Redis judgements cannot drift: a running execution whose graph is gone is
// reclaimed, a queued one is dropped instead of being promoted, and the
// reclaim never double-releases a slot racing a normal terminal write.
func RunConcurrencyOrphanContract(t *testing.T, manifest *Manifest, newBackend ConcurrencyOrphanStoreFactory) {
	t.Helper()
	manifest.Report(t)
	manifest.RequireDeclared(t,
		CapabilityConcurrencyOrphanReclaimer,
		CapabilityConcurrencyLimitedStateStore,
		CapabilityDurableSignalDeliverer,
	)
	t.Run("RunningOrphanConvergesAndFreesTheSlot", func(t *testing.T) {
		orphanRunningConverges(t, newBackend(t))
	})
	t.Run("QueuedOrphanIsDroppedNotPromoted", func(t *testing.T) {
		orphanQueuedIsDropped(t, newBackend(t))
	})
	t.Run("LiveGraphIsNeverReclaimed", func(t *testing.T) {
		orphanLiveGraphIsUntouched(t, newBackend(t))
	})
	t.Run("WithinGraceIsLeftAlone", func(t *testing.T) {
		orphanWithinGraceIsLeftAlone(t, newBackend(t))
	})
	t.Run("RecentActivityBlocksSweepAndForce", func(t *testing.T) {
		orphanRecentActivityBlocksSweepAndForce(t, newBackend(t))
	})
	t.Run("BothClocksPastGraceIsReclaimed", func(t *testing.T) {
		orphanBothClocksPastGraceIsReclaimed(t, newBackend(t))
	})
	t.Run("GroupLeaseActivityBlocksSweepAndForce", func(t *testing.T) {
		orphanGroupLeaseActivityBlocksSweepAndForce(t, newBackend(t))
	})
	t.Run("GroupLeaseActivityAgedPastGraceIsReclaimed", func(t *testing.T) {
		orphanGroupLeaseActivityAgedPastGraceIsReclaimed(t, newBackend(t))
	})
	t.Run("GroupUnitExecutionWithoutActivityFallsBackToStatusClock", func(t *testing.T) {
		orphanGroupUnitExecutionUsesStatusClock(t, newBackend(t))
	})
	t.Run("LegacyExecutionWithoutActivityUsesStatusClock", func(t *testing.T) {
		orphanLegacyExecutionUsesStatusClock(t, newBackend(t))
	})
	t.Run("PromotionGraceLeavesHeadThenSweepConverges", func(t *testing.T) {
		orphanPromotionGraceThenSweep(t, newBackend(t))
	})
	t.Run("SecondReclaimIsANoOp", func(t *testing.T) {
		orphanSecondReclaimIsIdempotent(t, newBackend(t))
	})
	t.Run("MissingExecutionMemberIsDroppedAndMirrored", func(t *testing.T) {
		orphanMissingExecutionMember(t, newBackend(t))
	})
	t.Run("ReclaimRacesTerminalWriteWithoutDoubleRelease", func(t *testing.T) {
		orphanRacesTerminalWrite(t, newBackend(t))
	})
	t.Run("WorkflowFilterScopesThePass", func(t *testing.T) {
		orphanWorkflowFilterScopes(t, newBackend(t))
	})
	t.Run("IncludeLiveConvergesGraphPresentExecution", func(t *testing.T) {
		orphanIncludeLiveConvergesGraphPresent(t, newBackend(t))
	})
	t.Run("DryRunWritesNothing", func(t *testing.T) {
		orphanDryRunWritesNothing(t, newBackend(t))
	})
	t.Run("TerminalResidueIsMirroredAfterReconcile", func(t *testing.T) {
		orphanTerminalResidueIsMirrored(t, newBackend(t))
	})
	t.Run("RetryBackoffWithFreshActivityIsNotReclaimed", func(t *testing.T) {
		orphanRetryBackoffFreshActivityIsNotReclaimed(t, newBackend(t))
	})
	t.Run("RetryBackoffReclaimedOnlyAfterGracePastActivity", func(t *testing.T) {
		orphanRetryBackoffReclaimedAfterGrace(t, newBackend(t))
	})
	t.Run("SignalDeliveryRefreshesActivityClock", func(t *testing.T) {
		orphanSignalDeliveryRefreshesActivityClock(t, newBackend(t))
	})
	t.Run("TimerResumeClearsSuspensionAndConverges", func(t *testing.T) {
		orphanTimerResumeClearsSuspension(t, newBackend(t))
	})
}

func orphanGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	return concurrencyGraph(t, name)
}

func orphanAdmission(g *graph.Graph) engine.ConcurrencyAdmission {
	return engine.ConcurrencyAdmission{
		Namespace: "default",
		Workflow:  g.Name(),
		Limit:     1,
		Mode:      types.ConcurrencyModeQueue,
	}
}

func orphanReclaimer(t *testing.T, state engine.StateStore) engine.ConcurrencyOrphanReclaimer {
	t.Helper()
	reclaimer, ok := state.(engine.ConcurrencyOrphanReclaimer)
	if !ok {
		t.Fatalf("state store %T implements ConcurrencyLimitedStateStore but not ConcurrencyOrphanReclaimer", state)
	}
	return reclaimer
}

// orphanSubmit submits one execution under the QUEUE-mode admission and returns
// its ID.
func orphanSubmit(t *testing.T, state engine.StateStore, ctx context.Context, g *graph.Graph, admission engine.ConcurrencyAdmission) types.ExecutionID {
	t.Helper()
	limited := concurrencyCapability(t, state)
	snap, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission); err != nil {
		t.Fatalf("submission error = %v", err)
	}
	return snap.ID
}

func orphanReclaim(t *testing.T, state engine.StateStore, ctx context.Context, req engine.OrphanReclaimRequest) engine.OrphanReclaimResult {
	t.Helper()
	res, err := orphanReclaimer(t, state).ReclaimConcurrencyOrphans(ctx, req)
	if err != nil {
		t.Fatalf("ReclaimConcurrencyOrphans(%+v) error = %v", req, err)
	}
	return res
}

func orphanSQLStatus(t *testing.T, mirror store.Store, ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool) {
	t.Helper()
	if mirror == nil {
		return "", false
	}
	rec, err := mirror.GetExecution(ctx, id)
	if err != nil || rec == nil {
		return "", false
	}
	return rec.Status, true
}

func orphanRunningConverges(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-running")
	admission := orphanAdmission(g)
	holding := orphanSubmit(t, backend.Store, ctx, g, admission)
	waiting := orphanSubmit(t, backend.Store, ctx, g, admission)
	if got := executionStatus(t, backend.Store, ctx, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("second submission status = %q, want queued", got)
	}

	backend.LoseGraph(t, holding)
	backend.AgeStatus(t, holding, 10*time.Minute)
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 32, Grace: time.Minute,
	})
	if res.Reclaimed < 1 {
		t.Fatalf("Reclaimed = %d, want at least 1 (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	if got := executionStatus(t, backend.Store, ctx, holding); got != types.ExecutionStatusCanceled {
		t.Fatalf("holding execution status = %q, want canceled", got)
	}
	// The freed slot must go to the head of the line, which is exactly what a
	// graph-lost execution would otherwise block forever.
	if got := executionStatus(t, backend.Store, ctx, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("waiting execution status = %q, want running (slot was not freed or not refilled)", got)
	}
	if status, ok := orphanSQLStatus(t, backend.Mirror, ctx, holding); ok && status != types.ExecutionStatusCanceled {
		t.Fatalf("SQL status = %q, want canceled", status)
	}
	candidate := orphanCandidateFor(res, holding)
	if candidate == nil {
		t.Fatalf("no candidate reported for %q: %+v", holding, res.Candidates)
	}
	if candidate.Status != types.ExecutionStatusRunning {
		t.Fatalf("candidate status = %q, want running", candidate.Status)
	}
	if candidate.Evidence != engine.OrphanEvidenceGraphMissing {
		t.Fatalf("candidate evidence = %q, want %q", candidate.Evidence, engine.OrphanEvidenceGraphMissing)
	}
	if candidate.Kind() != engine.OrphanKindRunning {
		t.Fatalf("candidate kind = %q, want %q", candidate.Kind(), engine.OrphanKindRunning)
	}
}

func orphanQueuedIsDropped(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-queued")
	admission := orphanAdmission(g)
	holding := orphanSubmit(t, backend.Store, ctx, g, admission)
	orphan := orphanSubmit(t, backend.Store, ctx, g, admission)
	behind := orphanSubmit(t, backend.Store, ctx, g, admission)

	backend.LoseGraph(t, orphan)
	backend.AgeStatus(t, orphan, 10*time.Minute)
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 32, Grace: time.Minute,
	})
	if res.Reclaimed < 1 {
		t.Fatalf("Reclaimed = %d, want at least 1", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("queued orphan status = %q, want canceled", got)
	}
	// The metric kind of a queued reclaim is queued, not running: the status the
	// execution was reclaimed from is what the bounded label reports.
	candidate := orphanCandidateFor(res, orphan)
	if candidate == nil || candidate.Kind() != engine.OrphanKindQueued {
		t.Fatalf("queued orphan candidate = %+v, want kind %q", candidate, engine.OrphanKindQueued)
	}
	if got := executionStatus(t, backend.Store, ctx, holding); got != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want untouched running", got)
	}
	// The holder ends; the line must advance past the canceled member, not
	// promote it into the slot the release just freed.
	if err := backend.Store.UpdateExecutionStatus(ctx, holding, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	if got := executionStatus(t, backend.Store, ctx, orphan); got != types.ExecutionStatusCanceled {
		t.Fatalf("orphan status after promotion = %q, want still canceled", got)
	}
	if got := executionStatus(t, backend.Store, ctx, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("execution behind the orphan status = %q, want running: a canceled orphan held the line", got)
	}
}

func orphanLiveGraphIsUntouched(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-live")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
	})
	if res.Reclaimed != 0 {
		t.Fatalf("Reclaimed = %d, want 0 for an execution with a live graph", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status = %q, want running", got)
	}
}

func orphanWithinGraceIsLeftAlone(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-grace")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Millisecond)
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Hour,
	})
	if res.Reclaimed != 0 {
		t.Fatalf("Reclaimed = %d, want 0 inside the grace window", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status = %q, want running", got)
	}
	// Force mode is the explicit operator override: same shape, no grace.
	res = orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("force Reclaimed = %d, want 1", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after force = %q, want canceled", got)
	}
}

// orphanRecentActivityBlocksSweepAndForce pins the false-positive fix: the
// status clock may be hours old while the execution is demonstrably working —
// a node was leased, renewed, or committed inside the grace — and that activity
// must block both the periodic sweep and Cancel's force-mode self-heal.
func orphanRecentActivityBlocksSweepAndForce(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.RecordActivity == nil || backend.AgeActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no activity-clock hooks")
	}
	ctx := context.Background()
	g := orphanGraph(t, "orphan-activity")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	backend.RecordActivity(t, id)

	sweep := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	}
	if res := orphanReclaim(t, backend.Store, ctx, sweep); res.Reclaimed != 0 {
		t.Fatalf("sweep Reclaimed = %d, want 0: node activity is inside the grace", res.Reclaimed)
	}
	// Cancel's self-heal is a targeted force pass; force skips the status clock,
	// never direct evidence that a node is working.
	force := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		Reason: engine.OrphanTriggerCancel,
	}
	if res := orphanReclaim(t, backend.Store, ctx, force); res.Reclaimed != 0 {
		t.Fatalf("Cancel self-heal Reclaimed = %d, want 0: node activity is inside the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status = %q, want running", got)
	}

	// Age the activity clock too; the same request now converges. That proves the
	// block above was the activity clock and not some other guard.
	backend.AgeActivity(t, id, 10*time.Minute)
	if res := orphanReclaim(t, backend.Store, ctx, sweep); res.Reclaimed != 1 {
		t.Fatalf("sweep Reclaimed = %d, want 1 once both clocks are past the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanBothClocksPastGraceIsReclaimed is the genuinely stalled shape: the
// status clock AND the activity clock are both past the grace, so the age guard
// no longer protects the execution.
func orphanBothClocksPastGraceIsReclaimed(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.RecordActivity == nil || backend.AgeActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no activity-clock hooks")
	}
	ctx := context.Background()
	g := orphanGraph(t, "orphan-stalled")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	backend.RecordActivity(t, id)
	backend.AgeActivity(t, id, 10*time.Minute)

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1 for an execution whose newer clock is past the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanLegacyExecutionUsesStatusClock pins the compatibility rule: an
// execution created before the activity clock existed has no activity key, and
// the sweep must fall back to its status clock exactly as it did before.
func orphanLegacyExecutionUsesStatusClock(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-legacy")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	// No lease or commit happened, so there is no activity clock to record; the
	// status clock is the only age evidence, and it alone must be enough.
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1 through the updated_at fallback", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanGroupGraph compiles a two-unit workflow. The group contract needs the
// execution to survive an accepted group commit, so there must be a second unit
// to keep remaining_nodes above zero: after committing unit 0, one unit is
// still outstanding and the execution stays running with no group lease held —
// the between-two-leases shape.
func orphanGroupGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "end", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "end", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// orphanGroupLeaseCycle drives one real group-unit lease through acquire,
// renew and an accepted commit using only the store's public GroupStateStore
// surface. No backend-specific hook is needed, and the cycle leaves no
// unexpired lease behind: the accepted commit removes the unit from the lease
// index while the execution stays running on its second unit.
func orphanGroupLeaseCycle(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) {
	t.Helper()
	groups, ok := state.(engine.GroupStateStore)
	if !ok {
		t.Fatalf("state store %T implements ConcurrencyOrphanReclaimer but not GroupStateStore", state)
	}
	lease := &engine.GroupLease{
		LeaseID: "orphan-group-lease", LeaseToken: "orphan-group-token", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: 0, GroupName: "group", EntryNodeIdx: 0,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	acquired, err := groups.AcquireGroupLease(ctx, lease)
	if err != nil || !acquired {
		t.Fatalf("AcquireGroupLease(%q) acquired=%v err=%v, want true/nil", id, acquired, err)
	}
	renewed, err := groups.RenewGroupLease(ctx, id, 0, lease.LeaseToken, time.Now().UTC().Add(2*time.Minute))
	if err != nil || !renewed {
		t.Fatalf("RenewGroupLease(%q) renewed=%v err=%v, want true/nil", id, renewed, err)
	}
	committed, err := groups.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID:  id,
		GroupUnitIdx: 0,
		GroupID:      "group",
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Outcome:      engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("CommitGroup(%q) error = %v", id, err)
	}
	if !committed.Applied {
		t.Fatalf("CommitGroup(%q) = %+v, want an applied commit", id, committed)
	}
	if committed.ExecutionDone {
		t.Fatalf("CommitGroup(%q) finalized the execution; the fixture needs it between two leases", id)
	}
}

// orphanGroupActivityFixture submits a two-unit execution, tears its graph out,
// ages the status clock, and leaves one group lease cycle in its wake: recent
// group activity, no unexpired lease, and no status transition.
func orphanGroupActivityFixture(t *testing.T, backend ConcurrencyOrphanBackend, name string) (context.Context, types.ExecutionID) {
	t.Helper()
	ctx := context.Background()
	g := orphanGroupGraph(t, name)
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	orphanGroupLeaseCycle(t, backend.Store, ctx, id)
	return ctx, id
}

// orphanGroupLeaseActivityBlocksSweepAndForce is the group-unit counterpart of
// RecentActivityBlocksSweepAndForce: the status clock is very old and no lease
// is held, but a group unit was just acquired, renewed and committed. That
// activity must block both the periodic sweep and Cancel's force-mode
// self-heal, or a healthy group execution waiting for its next unit would be
// converged as an orphan.
func orphanGroupLeaseActivityBlocksSweepAndForce(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx, id := orphanGroupActivityFixture(t, backend, "orphan-group-activity")

	sweep := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	}
	if res := orphanReclaim(t, backend.Store, ctx, sweep); res.Reclaimed != 0 {
		t.Fatalf("sweep Reclaimed = %d, want 0: group activity is inside the grace", res.Reclaimed)
	}
	force := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		Reason: engine.OrphanTriggerCancel,
	}
	if res := orphanReclaim(t, backend.Store, ctx, force); res.Reclaimed != 0 {
		t.Fatalf("Cancel self-heal Reclaimed = %d, want 0: group activity is inside the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status = %q, want running", got)
	}
}

// orphanGroupLeaseActivityAgedPastGraceIsReclaimed is the genuinely stalled
// group shape: the status clock and the group activity clock are both past the
// grace, no lease is held, so the age guard no longer protects the execution.
func orphanGroupLeaseActivityAgedPastGraceIsReclaimed(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.AgeActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no activity-clock hooks")
	}
	ctx, id := orphanGroupActivityFixture(t, backend, "orphan-group-aged")
	backend.AgeActivity(t, id, 10*time.Minute)

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1 once group activity is past the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanGroupUnitExecutionUsesStatusClock pins the compatibility rule for group
// executions: an execution whose activity clock is absent (everything created
// before the clock existed) falls back to updated_at exactly as before, even
// when its group unit has been leased before. The group cycle is what leaves
// the unit keys behind; deleting the activity clock is what makes it legacy.
func orphanGroupUnitExecutionUsesStatusClock(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.ForgetActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no ForgetActivity hook")
	}
	ctx, id := orphanGroupActivityFixture(t, backend, "orphan-group-legacy")
	backend.ForgetActivity(t, id)

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1 through the updated_at fallback", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanPromotionGraceThenSweep covers the promotion path's age gate and the
// liveness of the queue behind it: a graph-lost head promoted inside the grace
// keeps the slot it was just handed (capacity may idle for up to one grace),
// and the next sweep — after the clocks pass the grace — converges it and
// promotes the execution behind it. The queue therefore cannot deadlock.
func orphanPromotionGraceThenSweep(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-promotion-grace")
	admission := orphanAdmission(g)
	holder := orphanSubmit(t, backend.Store, ctx, g, admission)
	head := orphanSubmit(t, backend.Store, ctx, g, admission)
	behind := orphanSubmit(t, backend.Store, ctx, g, admission)
	backend.LoseGraph(t, head)

	// The holder ends and the release promotes the line. The head is inside the
	// grace (no status transition or activity older than it), so the promotion
	// must refuse to converge it.
	if err := backend.Store.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	if got := executionStatus(t, backend.Store, ctx, head); got != types.ExecutionStatusQueued {
		t.Fatalf("head status = %q, want queued inside the grace", got)
	}
	if got := executionStatus(t, backend.Store, ctx, behind); got != types.ExecutionStatusQueued {
		t.Fatalf("execution behind the head status = %q, want queued", got)
	}

	// Past the grace the next sweep must converge the head and hand the capacity
	// to the member behind it, or the workflow would stay stuck forever.
	backend.AgeStatus(t, head, 10*time.Minute)
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 16, Grace: time.Minute,
	})
	if res.Reclaimed < 1 {
		t.Fatalf("Reclaimed = %d, want at least the graph-lost head", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, head); got != types.ExecutionStatusCanceled {
		t.Fatalf("head status after sweep = %q, want canceled", got)
	}
	if got := executionStatus(t, backend.Store, ctx, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("execution behind the head status = %q, want running: the line deadlocked", got)
	}
}

func orphanSecondReclaimIsIdempotent(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-idempotent")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	req := engine.OrphanReclaimRequest{Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1}
	if res := orphanReclaim(t, backend.Store, ctx, req); res.Reclaimed != 1 {
		t.Fatalf("first Reclaimed = %d, want 1", res.Reclaimed)
	}
	if res := orphanReclaim(t, backend.Store, ctx, req); res.Reclaimed != 0 {
		t.Fatalf("second Reclaimed = %d, want 0 (already terminal)", res.Reclaimed)
	}
}

func orphanRacesTerminalWrite(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-race")
	admission := orphanAdmission(g)
	holding := orphanSubmit(t, backend.Store, ctx, g, admission)
	first := orphanSubmit(t, backend.Store, ctx, g, admission)
	second := orphanSubmit(t, backend.Store, ctx, g, admission)
	backend.LoseGraph(t, holding)

	var wg sync.WaitGroup
	var reclaimErr, terminalErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, reclaimErr = orphanReclaimer(t, backend.Store).ReclaimConcurrencyOrphans(ctx, engine.OrphanReclaimRequest{
			Mode: engine.OrphanReclaimForce, ExecutionID: holding, Namespace: "default", Limit: 1,
		})
	}()
	go func() {
		defer wg.Done()
		terminalErr = backend.Store.UpdateExecutionStatus(ctx, holding, types.ExecutionStatusCanceled, "")
	}()
	wg.Wait()
	if reclaimErr != nil {
		t.Fatalf("reclaim error = %v", reclaimErr)
	}
	if terminalErr != nil {
		t.Fatalf("terminal write error = %v", terminalErr)
	}

	if got := executionStatus(t, backend.Store, ctx, holding); got != types.ExecutionStatusCanceled {
		t.Fatalf("holding status = %q, want canceled", got)
	}
	// Exactly one slot existed. If both the reclaim and the terminal write
	// released it, both waiters would be running and the limit would be broken
	// silently.
	running := 0
	statuses := make([]types.ExecutionStatus, 0, 2)
	for _, id := range []types.ExecutionID{first, second} {
		status := executionStatus(t, backend.Store, ctx, id)
		statuses = append(statuses, status)
		if status == types.ExecutionStatusRunning {
			running++
		}
	}
	if running != 1 {
		t.Fatalf("running waiters = %d, want exactly 1: the slot was released twice (statuses %q, %q)", running, statuses[0], statuses[1])
	}
}

// orphanMissingExecutionMember covers the retention-lapsed shape: the
// concurrency member survives but there is no execution left to converge. The
// member is removed and the durable mirror is written directly, because the
// ordinary drift repair skips the missing row as not_found. No status is
// invented: a bare status key without the rest of a creation would read as a
// live execution with no time budget.
func orphanMissingExecutionMember(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.ForgetExecution == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no ForgetExecution hook")
	}
	ctx := context.Background()
	g := orphanGraph(t, "orphan-missing")
	admission := orphanAdmission(g)
	holding := orphanSubmit(t, backend.Store, ctx, g, admission)
	gone := orphanSubmit(t, backend.Store, ctx, g, admission)
	if got := executionStatus(t, backend.Store, ctx, gone); got != types.ExecutionStatusQueued {
		t.Fatalf("gone submission status = %q, want queued", got)
	}
	backend.ForgetExecution(t, gone)

	// Force mode: the member's own age is the only clock this shape has, and the
	// admission hook does not expose it. The judgement being tested is the
	// missing execution, not the age guard.
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Limit: 32,
	})
	if res.Reclaimed < 1 {
		t.Fatalf("Reclaimed = %d, want at least 1", res.Reclaimed)
	}
	candidate := orphanCandidateFor(res, gone)
	if candidate == nil || candidate.Evidence != engine.OrphanEvidenceExecutionMissing {
		t.Fatalf("candidate = %+v, want evidence %q", candidate, engine.OrphanEvidenceExecutionMissing)
	}
	if candidate.Kind() != engine.OrphanKindMissing {
		t.Fatalf("candidate kind = %q, want %q", candidate.Kind(), engine.OrphanKindMissing)
	}
	// The holder keeps its slot; the dangling member is gone from the line.
	if got := executionStatus(t, backend.Store, ctx, holding); got != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want running", got)
	}
	// The SQL row is the SAS-visible symptom: it must not be left queued.
	if status, ok := orphanSQLStatus(t, backend.Mirror, ctx, gone); ok && status != types.ExecutionStatusCanceled {
		t.Fatalf("SQL status = %q, want canceled", status)
	}
	// Ending the holder must not promote the execution that no longer exists.
	if err := backend.Store.UpdateExecutionStatus(ctx, holding, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("cancel holder error = %v", err)
	}
	snap, err := backend.Store.GetExecution(ctx, gone)
	if err != nil || (snap != nil && snap.Status == types.ExecutionStatusRunning) {
		t.Fatalf("gone execution = %+v (err %v), want no running execution: a dangling member was promoted", snap, err)
	}
}

// orphanWorkflowFilterScopes proves a Workflow-scoped pass reads exactly that
// workflow's slot set and leaves every other workflow's state alone. This is
// the judgement the operator reset API exposes, so it must not depend on the
// store's namespace-wide workflow index being complete.
func orphanWorkflowFilterScopes(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	gA := orphanGraph(t, "orphan-filter-a")
	gB := orphanGraph(t, "orphan-filter-b")
	a := orphanSubmit(t, backend.Store, ctx, gA, orphanAdmission(gA))
	b := orphanSubmit(t, backend.Store, ctx, gB, orphanAdmission(gB))
	backend.LoseGraph(t, a)
	backend.LoseGraph(t, b)
	backend.AgeStatus(t, a, 10*time.Minute)
	backend.AgeStatus(t, b, 10*time.Minute)

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Workflow: gA.Name(), Limit: 32,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want exactly 1 for a workflow-scoped pass (candidates %+v)", res.Reclaimed, res.Candidates)
	}
	if got := executionStatus(t, backend.Store, ctx, a); got != types.ExecutionStatusCanceled {
		t.Fatalf("scoped workflow execution status = %q, want canceled", got)
	}
	if got := executionStatus(t, backend.Store, ctx, b); got != types.ExecutionStatusRunning {
		t.Fatalf("other workflow's execution status = %q, want untouched running", got)
	}
}

// orphanIncludeLiveConvergesGraphPresent covers the explicit operator-reset
// override: include_live lists a graph-present execution in a dry run without
// writing, and converges it when applied, terminalizing the execution and its
// durable mirror.
func orphanIncludeLiveConvergesGraphPresent(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-include-live")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))

	preview := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, DryRun: true, Namespace: "default",
		Workflow: g.Name(), IncludeLive: true, Limit: 8,
	})
	if preview.Reclaimed != 0 {
		t.Fatalf("preview Reclaimed = %d, want 0", preview.Reclaimed)
	}
	candidate := orphanCandidateFor(preview, id)
	if candidate == nil {
		t.Fatalf("include_live preview did not list the live execution: %+v", preview.Candidates)
	}
	if candidate.Evidence != engine.OrphanEvidenceGraphMissing {
		t.Fatalf("candidate evidence = %q, want %q", candidate.Evidence, engine.OrphanEvidenceGraphMissing)
	}
	if candidate.Kind() != engine.OrphanKindRunning {
		t.Fatalf("candidate kind = %q, want %q", candidate.Kind(), engine.OrphanKindRunning)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after preview = %q, want running: a dry run must not write", got)
	}

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default",
		Workflow: g.Name(), IncludeLive: true, Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("include_live Reclaimed = %d, want 1", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after include_live apply = %q, want canceled", got)
	}
	if status, ok := orphanSQLStatus(t, backend.Mirror, ctx, id); ok && status != types.ExecutionStatusCanceled {
		t.Fatalf("SQL status = %q, want canceled", status)
	}
}

// orphanDryRunWritesNothing pins the preview contract: the same request that
// lists a candidate leaves authoritative state byte-for-byte unchanged, and
// the identical request with DryRun cleared converges exactly that candidate.
func orphanDryRunWritesNothing(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	ctx := context.Background()
	g := orphanGraph(t, "orphan-dry-run")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)

	req := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, DryRun: true, Namespace: "default",
		Workflow: g.Name(), Limit: 8,
	}
	res := orphanReclaim(t, backend.Store, ctx, req)
	if res.Reclaimed != 0 {
		t.Fatalf("dry-run Reclaimed = %d, want 0", res.Reclaimed)
	}
	if orphanCandidateFor(res, id) == nil {
		t.Fatalf("dry run did not list the orphan: %+v", res.Candidates)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after dry run = %q, want running", got)
	}

	req.DryRun = false
	if res := orphanReclaim(t, backend.Store, ctx, req); res.Reclaimed != 1 {
		t.Fatalf("apply Reclaimed = %d, want 1 for the candidate the dry run listed", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status after apply = %q, want canceled", got)
	}
}

func orphanCandidateFor(res engine.OrphanReclaimResult, id types.ExecutionID) *engine.OrphanReclaimCandidate {
	for i := range res.Candidates {
		if res.Candidates[i].ExecutionID == id {
			return &res.Candidates[i]
		}
	}
	return nil
}

// orphanTerminalResidueIsMirrored pins the crash window between a terminal
// write and its Go-side side effects. The Lua half (or the in-memory terminal
// transition) landed, then the process died: authority says canceled, the
// concurrency member is still charged, the line is still behind it, and the
// durable mirror still reads running. One maintenance pass must repair all
// three — release the member, promote the head of the line, and project the
// terminal status the terminal write never mirrored. Repairing only the
// membership would leave the SQL row wrong until the drift worker finds it.
func orphanTerminalResidueIsMirrored(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.TerminalStatusWithoutRelease == nil || backend.ReconcileSlots == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no terminal-residue/reconcile hooks")
	}
	if backend.Mirror == nil {
		t.Fatalf("backend supplies no durable mirror; the terminal projection cannot be verified")
	}
	ctx := context.Background()
	g := orphanGraph(t, "orphan-terminal-residue")
	admission := orphanAdmission(g)
	holder := orphanSubmit(t, backend.Store, ctx, g, admission)
	waiting := orphanSubmit(t, backend.Store, ctx, g, admission)
	if got := executionStatus(t, backend.Store, ctx, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("waiting submission status = %q, want queued", got)
	}

	backend.TerminalStatusWithoutRelease(t, holder)
	if got := executionStatus(t, backend.Store, ctx, holder); got != types.ExecutionStatusCanceled {
		t.Fatalf("holder status after the terminal half = %q, want canceled", got)
	}
	// The fixture is only meaningful when the skipped projection is visible:
	// a mirror that already reads terminal would pass the final assertion
	// without the reconcile doing anything.
	before, ok := orphanSQLStatus(t, backend.Mirror, ctx, holder)
	if !ok {
		t.Fatalf("holder has no durable row after admission; the projection cannot be verified")
	}
	if before == types.ExecutionStatusCanceled {
		t.Fatalf("holder SQL before reconcile = %q, want the unprojected running row", before)
	}

	backend.ReconcileSlots(t)

	if got := executionStatus(t, backend.Store, ctx, holder); got != types.ExecutionStatusCanceled {
		t.Fatalf("holder status after reconcile = %q, want canceled", got)
	}
	if got := executionStatus(t, backend.Store, ctx, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("waiting status after reconcile = %q, want running: the freed slot was not handed to the line", got)
	}
	after, ok := orphanSQLStatus(t, backend.Mirror, ctx, holder)
	if !ok {
		t.Fatalf("holder durable row disappeared during reconcile")
	}
	if after != types.ExecutionStatusCanceled {
		t.Fatalf("holder SQL after reconcile = %q, want canceled: the terminal residue was not mirrored", after)
	}
}

// orphanResetForRetry performs one real retry reset through the store's public
// interfaces: a running node with a lease token, then the fenced
// ResetNodeForRetryWithOutbox the engine retry path calls. The reset removes
// the lease and must refresh the activity clock in the same transition; no
// renewal follows during the backoff.
func orphanResetForRetry(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) {
	t.Helper()
	const (
		node  = "start"
		token = engine.LeaseToken("orphan-retry-token")
	)
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:   id,
		Name:          node,
		Status:        types.NodeStatusRunning,
		LeaseID:       engine.LeaseID("orphan-retry-lease"),
		LeaseToken:    token,
		LeaseIssuedAt: time.Now().UTC().Add(-time.Minute),
		LeaseTTL:      time.Minute,
	}); err != nil {
		t.Fatalf("UpsertNode(%q/%q) error = %v", id, node, err)
	}
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatalf("state store %T implements ConcurrencyOrphanReclaimer but not AtomicStateStore", state)
	}
	scheduled, err := atomic.ResetNodeForRetryWithOutbox(ctx, id, node, token, engine.OutboxEntry{
		ID:   "retry/" + string(id) + "/" + node + "/0/1",
		Task: engine.Task{ExecutionID: id, NodeName: node, NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	})
	if err != nil || !scheduled {
		t.Fatalf("ResetNodeForRetryWithOutbox(%q/%q) scheduled=%v err=%v, want true/nil", id, node, scheduled, err)
	}
}

// orphanRetryBackoffFreshActivityIsNotReclaimed pins the false-positive fix
// for the retry window: the execution's graph is gone and its status clock is
// very old, but a retry reset just happened and removed the lease. Without the
// reset's activity stamp the execution looks idle and both the periodic sweep
// and Cancel's force-mode self-heal would converge a node that is waiting to
// be retried.
func orphanRetryBackoffFreshActivityIsNotReclaimed(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.LoseGraph == nil || backend.AgeStatus == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no graph/status hooks")
	}
	ctx := context.Background()
	g := orphanGraph(t, "orphan-retry-backoff")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	orphanResetForRetry(t, backend.Store, ctx, id)

	sweep := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	}
	if res := orphanReclaim(t, backend.Store, ctx, sweep); res.Reclaimed != 0 {
		t.Fatalf("sweep Reclaimed = %d, want 0: the retry reset's activity is inside the grace", res.Reclaimed)
	}
	force := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		Reason: engine.OrphanTriggerCancel,
	}
	if res := orphanReclaim(t, backend.Store, ctx, force); res.Reclaimed != 0 {
		t.Fatalf("Cancel self-heal Reclaimed = %d, want 0: the retry reset's activity is inside the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status = %q, want running", got)
	}
}

// orphanRetryBackoffReclaimedAfterGrace proves the block above was the
// activity clock and nothing else: age that clock past the grace and the same
// sweep converges the execution.
func orphanRetryBackoffReclaimedAfterGrace(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.LoseGraph == nil || backend.AgeStatus == nil || backend.AgeActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no activity-clock hooks")
	}
	ctx := context.Background()
	g := orphanGraph(t, "orphan-retry-backoff-aged")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	orphanResetForRetry(t, backend.Store, ctx, id)
	backend.AgeActivity(t, id, 10*time.Minute)

	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1 once the reset's activity is past the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanSignalDeliveryRefreshesActivityClock covers the window between a signal
// delivery and the resume lease acquisition. Delivery consumes the waiter,
// removes the suspension registration and records a durable resume intent;
// nothing holds a lease until that resume is acquired. The delivery must
// therefore refresh the execution's activity clock exactly as a lease
// acquisition does, or a graph-lost execution whose resume is already durable
// and pending looks idle to the sweep.
//
// The fixture uses only the public Store surface: the execution is admitted
// through the concurrency-limited create, a real lease is acquired and claimed,
// the node parks on a signal wait through DurableLeaseSuspender, and the
// delivery goes through DurableSignalDeliverer. Both age clocks are pushed past
// the grace before delivery, so the guarded half can only pass through the
// stamp the delivery itself writes; aging that stamp afterwards proves the
// block was the activity clock and nothing else.
func orphanSignalDeliveryRefreshesActivityClock(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.LoseGraph == nil || backend.AgeStatus == nil || backend.AgeActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no graph/activity hooks")
	}
	suspender, ok := backend.Store.(engine.DurableLeaseSuspender)
	if !ok {
		t.Fatalf("state store %T does not implement engine.DurableLeaseSuspender", backend.Store)
	}
	deliverer, ok := backend.Store.(engine.DurableSignalDeliverer)
	if !ok {
		t.Fatalf("state store %T does not implement engine.DurableSignalDeliverer", backend.Store)
	}

	ctx := context.Background()
	g := orphanGraph(t, "orphan-signal-activity")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	nodeIdx, ok := g.NodeIndex("start")
	if !ok {
		t.Fatal("graph has no start node")
	}
	unitIdx := g.UnitIndexForNode(nodeIdx)
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("orphan-signal-activity-lease"),
		LeaseToken: engine.LeaseToken("orphan-signal-activity-token"),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "start",
			NodeIdx:      nodeIdx,
			UnitIdx:      unitIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := backend.Store.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v, want true/nil", acquired, err)
	}
	node, err := backend.Store.GetNode(ctx, id, "start")
	if err != nil || node == nil {
		t.Fatalf("GetNode(start) node=%v err=%v, want the leased node", node, err)
	}
	lease.Attempt = node.Attempt
	if _, claimed, err := backend.Store.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease claimed=%v err=%v, want true/nil", claimed, err)
	}
	committed, err := suspender.SuspendTaskLeaseWithOutbox(ctx, lease, nil, false, false,
		&types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}}, "")
	if err != nil || !committed {
		t.Fatalf("SuspendTaskLeaseWithOutbox committed=%v err=%v, want true/nil", committed, err)
	}

	// The harshest clock: graph gone, status clock past the grace, and the
	// acquire's activity stamp aged past the grace too.
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	backend.AgeActivity(t, id, 10*time.Minute)

	target, err := deliverer.PeekResumeTarget(ctx, id, "go")
	if err != nil || target != "start" {
		t.Fatalf("PeekResumeTarget = (%q, %v), want (start, nil)", target, err)
	}
	resumeNode, _, committed, err := deliverer.DeliverSignalWithOutbox(ctx, id, "go",
		map[string]any{"ok": true},
		engine.ResumeIntent{NodeName: target, NodeIdx: nodeIdx, UnitIdx: unitIdx})
	if err != nil || !committed || resumeNode != "start" {
		t.Fatalf("DeliverSignalWithOutbox = (%q, committed=%v, err=%v), want (start, true, nil)", resumeNode, committed, err)
	}

	sweep := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	}
	if res := orphanReclaim(t, backend.Store, ctx, sweep); res.Reclaimed != 0 {
		t.Fatalf("sweep Reclaimed = %d, want 0: the delivery must refresh the activity clock", res.Reclaimed)
	}
	force := engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, ExecutionID: id, Namespace: "default", Limit: 1,
		Reason: engine.OrphanTriggerCancel,
	}
	if res := orphanReclaim(t, backend.Store, ctx, force); res.Reclaimed != 0 {
		t.Fatalf("Cancel self-heal Reclaimed = %d, want 0: the delivery must refresh the activity clock", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status = %q, want running", got)
	}

	// Age the delivery stamp; the same request now converges, which proves the
	// block above was the activity clock and nothing else.
	backend.AgeActivity(t, id, 10*time.Minute)
	if res := orphanReclaim(t, backend.Store, ctx, sweep); res.Reclaimed != 1 {
		t.Fatalf("sweep Reclaimed = %d, want 1 once the delivery stamp is past the grace", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// orphanTimerResumeClearsSuspension covers the timer wake path, which is the
// asymmetric half of the suspension bookkeeping: the park registers the node in
// the suspended set, but the wake is a delayed outbox entry that only reaches
// the node at its due time, through an ordinary acquire. The acquire is where
// the wait actually ends, so it must clear the member in the same transition
// that sets the node running — exactly what signal delivery does when it
// consumes a waiter and writes the resume intent.
//
// Without that, the member outlives the wait and the reclaim judge's
// suspended-node guard keeps sparing a graph-lost execution in which nothing is
// suspended: over-protection, so the orphan never heals. The fixture therefore
// asserts the member is gone after the resume acquire, then constructs the same
// torn shape the parked execution is protected from — graph lost, both clocks
// past the grace, no lease — and requires convergence. The lease is revoked
// rather than left to expire so the only possible blocker is the stale member.
//
// Mutation: removing the SREM from the acquire transition makes the member
// assertion and the final reclaim both red.
func orphanTimerResumeClearsSuspension(t *testing.T, backend ConcurrencyOrphanBackend) {
	t.Helper()
	if backend.LoseGraph == nil || backend.AgeStatus == nil || backend.AgeActivity == nil {
		t.Fatalf("backend declares ConcurrencyOrphanReclaimer but supplies no graph/status/activity hooks")
	}
	suspender, ok := backend.Store.(engine.DurableLeaseSuspender)
	if !ok {
		t.Fatalf("state store %T does not implement engine.DurableLeaseSuspender", backend.Store)
	}
	leaser, ok := backend.Store.(engine.OutboxLeaser)
	if !ok {
		t.Fatalf("state store %T does not implement engine.OutboxLeaser", backend.Store)
	}

	ctx := context.Background()
	g := orphanGraph(t, "orphan-timer-resume")
	id := orphanSubmit(t, backend.Store, ctx, g, orphanAdmission(g))
	nodeIdx, ok := g.NodeIndex("start")
	if !ok {
		t.Fatal("graph has no start node")
	}
	unitIdx := g.UnitIndexForNode(nodeIdx)
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("orphan-timer-resume-lease"),
		LeaseToken: engine.LeaseToken("orphan-timer-resume-token"),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "start",
			NodeIdx:      nodeIdx,
			UnitIdx:      unitIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := backend.Store.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v, want true/nil", acquired, err)
	}
	node, err := backend.Store.GetNode(ctx, id, "start")
	if err != nil || node == nil {
		t.Fatalf("GetNode(start) node=%v err=%v, want the leased node", node, err)
	}
	lease.Attempt = node.Attempt
	if _, claimed, err := backend.Store.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease claimed=%v err=%v, want true/nil", claimed, err)
	}
	committed, err := suspender.SuspendTaskLeaseWithOutbox(ctx, lease, nil, false, false,
		&types.SuspendSpec{Mode: types.ModeTimer, Timer: 10 * time.Millisecond}, "")
	if err != nil || !committed {
		t.Fatalf("SuspendTaskLeaseWithOutbox committed=%v err=%v, want true/nil", committed, err)
	}
	members, err := backend.Store.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes after the park: %v", err)
	}
	if !containsString(members, "start") {
		t.Fatalf("suspended nodes after the park = %v, want start: the fixture is not a timer wait", members)
	}

	// The timer entry is delayed; wait for it to become due and claim it the way
	// the outbox flush does.
	var resume *engine.Task
	for attempt := 0; attempt < 50 && resume == nil; attempt++ {
		entries, err := leaser.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
		if err != nil {
			t.Fatalf("LeaseOutbox: %v", err)
		}
		for i := range entries {
			if entries[i].Task.Type == engine.TaskTypeNodeResume && entries[i].Task.NodeName == "start" {
				task := entries[i].Task
				resume = &task
				break
			}
		}
		if resume == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if resume == nil {
		t.Fatal("the timer park wrote no due resume intent: the wake path under test never starts")
	}
	resumeLease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("orphan-timer-resume-wake-lease"),
		LeaseToken: engine.LeaseToken("orphan-timer-resume-wake-token"),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task:       *resume,
	}
	if _, acquired, err := backend.Store.AcquireTaskLease(ctx, resumeLease); err != nil || !acquired {
		t.Fatalf("resume AcquireTaskLease acquired=%v err=%v, want true/nil", acquired, err)
	}
	members, err = backend.Store.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes after the resume: %v", err)
	}
	if containsString(members, "start") {
		t.Fatal("the resume acquire left the node in the suspended set: nothing is suspended but the reclaim guard still sees it")
	}

	// Make the execution the torn shape a parked wait is protected from. The
	// lease is revoked so no unexpired lease can mask a stale-member failure.
	backend.LoseGraph(t, id)
	backend.AgeStatus(t, id, 10*time.Minute)
	backend.AgeActivity(t, id, 10*time.Minute)
	if revoked, err := backend.Store.RevokeLease(ctx, id, "start", resumeLease.LeaseToken); err != nil || !revoked {
		t.Fatalf("RevokeLease revoked=%v err=%v, want true/nil", revoked, err)
	}
	res := orphanReclaim(t, backend.Store, ctx, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, ExecutionID: id, Namespace: "default", Limit: 1, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1: the resumed execution has nothing suspended and is graph-lost", res.Reclaimed)
	}
	if got := executionStatus(t, backend.Store, ctx, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}
