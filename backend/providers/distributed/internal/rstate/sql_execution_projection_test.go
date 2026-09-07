package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// The atomic commit path finalizes an execution INSIDE its Lua script: the
// script sets the exec status key and returns done=1. Every terminal transition
// that happens that way must also reach the SQL audit projection, because Redis
// execution keys expire (DefaultExecTTL = 24h) and the SQL executions row is
// then the only surviving record. Without the projection every normally
// completed execution is permanently "running" with an empty error_msg.
//
// The three Lua finalization points are covered here: node commit
// (state_commit.go), group commit (group_state.go) and entry admission
// (entry_admission.go).

func newStateWithAuditStore(t *testing.T) (*Store, store.Store) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	db := memstore.New()
	return New(rdb, db, time.Minute), db
}

func TestCommitLeasedNodeProjectsTerminalStatusToSQL(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "sql-projection-node",
		Nodes: []types.NodeDef{{Name: "only", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("sql-projection-node-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	idx, _ := g.NodeIndex("only")
	lease := &engine.TaskLease{
		LeaseID: "l1", LeaseToken: "t1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "only", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess, StoreOutput: true, Port: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
		t.Fatalf("commit result = %+v, want done+success", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after commit: %v", err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("SQL execution status = %q, want %q (Redis says %q)",
			rec.Status, types.ExecutionStatusSuccess, res.ExecutionStatus)
	}
}

func TestCommitLeasedNodeProjectsFailureReasonToSQL(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "sql-projection-node-fail",
		Nodes: []types.NodeDef{{Name: "only", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("sql-projection-node-fail-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	idx, _ := g.NodeIndex("only")
	lease := &engine.TaskLease{
		LeaseID: "l1", LeaseToken: "t1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	const reason = "node only failed"
	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "only", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusFailed, Error: reason, Fatal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
		t.Fatalf("commit result = %+v, want done+failed", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after commit: %v", err)
	}
	if rec.Status != types.ExecutionStatusFailed {
		t.Fatalf("SQL execution status = %q, want failed", rec.Status)
	}
	if rec.Error != reason {
		t.Fatalf("SQL execution error = %q, want %q", rec.Error, reason)
	}
}

// A cyclic execution that fails on the depth limit has NO failed node: the node
// that tripped the limit succeeded, and it was its downstream activation that
// was refused. CyclicFinalError is therefore the only carrier of the reason, and
// terminalExecutionError must prefer it over the (empty) node error.
//
// This test pins the SQL audit row specifically. The online read-back path for
// the same reason — GetExecution loading exec:<id>:error into
// ExecutionSnapshot.Error — is pinned by the shared state-store contract.
func TestCommitLeasedNodeProjectsCyclicFinalErrorToSQL(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "sql-projection-cyclic",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "finish", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "finish", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("sql-projection-cyclic-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	idx, _ := g.NodeIndex("finish")
	lease := &engine.TaskLease{
		LeaseID: "l1", LeaseToken: "t1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "finish", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	const reason = "max auto execution depth exceeded"
	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "finish", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		// The node itself SUCCEEDED and carries no error — this is the whole
		// point: without CyclicFinalError the failure would have no reason at all.
		Status: types.NodeStatusSuccess, StoreOutput: true, Port: "main",
		AllowCycles:       true,
		CyclicComplete:    true,
		CyclicFinalStatus: types.ExecutionStatusFailed,
		CyclicFinalError:  reason,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
		t.Fatalf("commit result = %+v, want done+failed", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after cyclic commit: %v", err)
	}
	if rec.Status != types.ExecutionStatusFailed {
		t.Fatalf("SQL execution status = %q, want failed", rec.Status)
	}
	if rec.Error != reason {
		t.Fatalf("SQL execution error = %q, want %q", rec.Error, reason)
	}
}

// TestCommitLeasedNodeProjectsCyclicErrorOverNodeErrorToSQL is the case the two
// tests above cannot distinguish between them.
//
// TestCommitLeasedNodeProjectsFailureReasonToSQL sets only req.Error;
// TestCommitLeasedNodeProjectsCyclicFinalErrorToSQL sets only
// req.CyclicFinalError. engine.TerminalExecutionError returns whichever of its
// two string arguments is non-empty, so with one of them always empty the order
// of the two arguments at state_commit.go:247-248 is unobservable -- swapping
// them leaves both tests green. Repo-wide, CyclicFinalError appears in no other
// test file, and TerminalExecutionError has no direct unit test at all.
//
// The shape below is reachable: a node failure routed to its own "error" port
// is non-fatal, so commit.go still sets req.Error to the node's message while
// the cyclic planner keeps going, and if the resulting activation trips
// MaxAutoDepth the scheduler sets a finalError of its own -- a different
// non-empty string on the same commit.
//
// The live Redis view is unaffected either way: commitNodeLua writes the
// correct field directly (the fatal branch takes ARGV[10], the CyclicComplete
// branch takes ARGV[21]). state_commit.go:247-248 is the ONLY place that
// re-derives the precedence, and it does so for the SQL audit row -- which,
// per the comment just above it, is the only surviving record once the Redis
// keys expire. A swap therefore leaves the live run correct and writes the
// wrong permanent failure reason into the audit trail.
func TestCommitLeasedNodeProjectsCyclicErrorOverNodeErrorToSQL(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "sql-projection-both-errors",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "finish", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "finish", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("sql-projection-both-errors-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	idx, _ := g.NodeIndex("finish")
	lease := &engine.TaskLease{
		LeaseID: "l1", LeaseToken: "t1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "finish", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	const nodeReason = "upstream service rejected the record"
	const cyclicReason = "max auto execution depth exceeded"
	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "finish", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess, StoreOutput: true, Port: "error",
		// Both are non-empty and different — the only configuration in which
		// the argument order is observable.
		Error:             nodeReason,
		AllowCycles:       true,
		CyclicComplete:    true,
		CyclicFinalStatus: types.ExecutionStatusFailed,
		CyclicFinalError:  cyclicReason,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
		t.Fatalf("commit result = %+v, want done+failed", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after commit: %v", err)
	}
	if rec.Error != cyclicReason {
		t.Fatalf("SQL execution error = %q, want the cyclic reason %q (not the "+
			"node's own %q): engine/types.go documents that a cyclic reason wins "+
			"because the cyclic terminal path can carry a reason no node holds",
			rec.Error, cyclicReason, nodeReason)
	}
}

// A non-terminal commit must NOT touch the SQL execution status: the row stays
// running until the execution actually finishes.
func TestCommitLeasedNodeLeavesSQLStatusAloneWhileRunning(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "sql-projection-node-partial",
		Nodes: []types.NodeDef{
			{Name: "first", Type: "test.echo"},
			{Name: "second", Type: "test.echo"},
		},
		Connections: types.Connections{
			"first": {"main": types.PortConnections{Targets: []types.Connection{{Node: "second", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("sql-projection-node-partial-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	idx, _ := g.NodeIndex("first")
	lease := &engine.TaskLease{
		LeaseID: "l1", LeaseToken: "t1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "first", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "first", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess, StoreOutput: true, Port: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExecutionDone {
		t.Fatalf("commit of the first of two nodes must not finish the execution: %+v", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after commit: %v", err)
	}
	if rec.Status != types.ExecutionStatusRunning {
		t.Fatalf("SQL execution status = %q, want running", rec.Status)
	}
}

func TestCommitGroupProjectsTerminalStatusToSQL(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "sql-projection-group",
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("sql-projection-group-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}
	unitIdx := g.Groups()[0].UnitIdx

	if ok, err := state.AcquireGroupLease(ctx, &engine.GroupLease{
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: unitIdx, GroupID: "tg",
		IssuedAt: time.Now(), TTL: time.Minute,
	}); err != nil || !ok {
		t.Fatalf("AcquireGroupLease ok=%v err=%v", ok, err)
	}

	const reason = "group tg failed"
	res, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID: id, GroupUnitIdx: unitIdx, GroupID: "tg",
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		Outcome: engine.GroupOutcomeFailed, Error: reason,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
		t.Fatalf("group commit result = %+v, want done+failed", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after group commit: %v", err)
	}
	if rec.Status != types.ExecutionStatusFailed {
		t.Fatalf("SQL execution status = %q, want failed", rec.Status)
	}
	if rec.Error != reason {
		t.Fatalf("SQL execution error = %q, want %q", rec.Error, reason)
	}
}

// SeedExecutionFromEntry creates the execution inside its Lua script — it never
// calls CreateExecution. Without an explicit projection the SQL executions table
// has no row at all for a trigger-group-seeded execution, so every execution
// admitted from a Kafka entry unit is invisible to the audit trail.
func TestSeedExecutionFromEntryProjectsExecutionToSQL(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "sql-projection-entry-seed",
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	resp, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: "sql-projection-entry-seed-key",
		WorkflowID:   "wf", WorkflowVersion: "v1",
		EntryUnitID: gm.Name, EntryUnitIdx: gm.UnitIdx, Graph: g,
		Outcome: engine.GroupOutcomeSuccess, Exits: exits,
		ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("admission state = %q, want accepted", resp.State)
	}

	rec, err := db.GetExecution(ctx, resp.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution after entry seed: %v", err)
	}
	// The whole entry unit is done, and it is the only unit that carries work;
	// the graph's remaining node (body) is a group member, so the execution
	// finishes inside the seed itself.
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("SQL execution status = %q, want success", rec.Status)
	}
}

// A duplicate admission must not create a second row or resurrect a stale
// status: the projection is idempotent like the admission it mirrors.
func TestSeedExecutionFromEntryProjectionIsIdempotent(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "sql-projection-entry-seed-dup",
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey: "sql-projection-entry-seed-dup-key",
		WorkflowID:   "wf", WorkflowVersion: "v1",
		EntryUnitID: gm.Name, EntryUnitIdx: gm.UnitIdx, Graph: g,
		Outcome: engine.GroupOutcomeSuccess, Exits: exits,
		ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	}
	first, err := state.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := state.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate {
		t.Fatal("second admission of the same key+hash must be duplicate")
	}
	if second.ExecutionID != first.ExecutionID {
		t.Fatalf("duplicate admission execution id = %q, want %q", second.ExecutionID, first.ExecutionID)
	}

	rec, err := db.GetExecution(ctx, first.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution after duplicate seed: %v", err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("SQL execution status after duplicate = %q, want success", rec.Status)
	}
}
