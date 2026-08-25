package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// reclaimGroupGraph: group {ingest,analyze} + external store => two units, so the
// group unit is not the only work item and a reclaim that mixed up unit indices
// would show up.
func reclaimGroupGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "reclaim-group",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
		Connections: types.Connections{
			"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
			"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestReclaimExpiredGroupLeaseRedelivers is the end-to-end regression for a
// runner that dies holding a group lease. Before the fix, ReclaimLease sent the
// group lease down the node path: it fenced against the group's ENTRY NODE
// name, which never has node-level state, so the revoke returned false and the
// call reported reclaimed=false with no error. The sweeper reads that as
// "already handled" and never retries — while AcquireGroupLease refuses a unit
// that is still "running", so nothing else revives it either. The unit is
// stranded for the lifetime of the execution.
func TestReclaimExpiredGroupLeaseRedelivers(t *testing.T) {
	ctx := context.Background()
	backend := New(WithConcurrency(1))
	eng := engine.New(backend.State(), backend.Queue(), engine.WithDefaultLeaseTTL(time.Minute))

	g := reclaimGroupGraph(t)
	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	gm := g.Groups()[0]
	task := &engine.Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        engine.TaskTypeGroupExec,
	}
	lease, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}

	// The runner is now dead. Its lease is past its deadline, so the sweeper
	// finds it — a cutoff far in the future stands in for waiting out the TTL.
	expired, err := backend.State().ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	var target *engine.ExpiredLease
	for i := range expired {
		if expired[i].UnitIdx == gm.UnitIdx && expired[i].TaskType == engine.TaskTypeGroupExec {
			target = &expired[i]
			break
		}
	}
	if target == nil {
		t.Fatalf("sweeper did not report the expired group lease (got %+v)", expired)
	}
	if target.LeaseToken != lease.LeaseToken {
		t.Fatalf("expired lease token = %q, want the acquired %q", target.LeaseToken, lease.LeaseToken)
	}

	reclaimed, err := eng.ReclaimLease(ctx, *target)
	if err != nil {
		t.Fatalf("ReclaimLease: %v", err)
	}
	if !reclaimed {
		t.Fatal("ReclaimLease reported reclaimed=false with no error — the sweeper treats that " +
			"as already handled and never retries, so the unit stays running forever")
	}

	// Reclaim is only half done if the unit is re-acquirable but nothing is
	// queued: a group unit with no lease and no task is invisible to every later
	// sweep, because ListExpiredLeases only reports units that hold a lease.
	if _, _, err := eng.BuildGroupLease(ctx, task); err != nil {
		t.Fatalf("group unit still not acquirable after reclaim: %v", err)
	}
}

// TestReclaimExpiredGroupLeaseIsTokenFenced guards the other direction: a
// sweeper acting on a stale observation must not revoke the lease of the runner
// that legitimately holds the unit now.
func TestReclaimExpiredGroupLeaseIsTokenFenced(t *testing.T) {
	ctx := context.Background()
	backend := New(WithConcurrency(1))
	eng := engine.New(backend.State(), backend.Queue(), engine.WithDefaultLeaseTTL(time.Minute))

	g := reclaimGroupGraph(t)
	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	gm := g.Groups()[0]
	task := &engine.Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        engine.TaskTypeGroupExec,
	}
	if _, _, err := eng.BuildGroupLease(ctx, task); err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}

	stale := engine.ExpiredLease{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		LeaseID:     engine.LeaseID("lease-stale"),
		LeaseToken:  engine.LeaseToken("token-stale"),
		TaskType:    engine.TaskTypeGroupExec,
	}
	reclaimed, err := eng.ReclaimLease(ctx, stale)
	if err != nil {
		t.Fatalf("ReclaimLease with a stale token: %v", err)
	}
	if reclaimed {
		t.Fatal("a stale token reclaimed the live owner's group lease")
	}
	held, err := backend.State().(engine.GroupLeaseReader).GetGroupLease(ctx, execID, gm.UnitIdx)
	if err != nil {
		t.Fatalf("GetGroupLease: %v", err)
	}
	if held == nil {
		t.Fatal("the live group lease was cleared by a stale reclaim")
	}
}
