//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"github.com/xbcio/xflow/types"
)

// The atomic commit paths finalize an execution INSIDE their Lua script and
// project the terminal state to SQL themselves. Unit tests cover that against
// miniredis + memstore; this file runs the same two paths through the real
// backend assembly (distributed.New over Redis with a mysqlstore audit store)
// so the projection is exercised against the production storage drivers, not
// two in-memory doubles.
//
// Note the schema caveat: db/xflow_schema.sql declares workflow_def NOT NULL,
// but a database whose tables came from sqlstore.AutoMigrate has that column
// nullable. Column constraints therefore cannot be relied on here — the value
// is asserted explicitly instead.
func newProjectionEnv(t *testing.T) (*distributed.Backend, store.Store) {
	t.Helper()
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	db, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	b, err := distributed.New(addr, db, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	return b, db
}

func projectionGraph(t *testing.T, name string, grouped bool) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
	}
	if grouped {
		def.Groups = []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	return g
}

// TestSeedExecutionFromEntryProjectsToRealMySQL proves the entry-admission
// projection creates a row the production schema accepts. This path never calls
// CreateExecution — the execution is created inside seedExecutionFromEntryLua —
// so the projection is the only thing standing between a Kafka trigger-group
// execution and being absent from the audit trail entirely.
func TestSeedExecutionFromEntryProjectsToRealMySQL(t *testing.T) {
	b, db := newProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admitter, ok := b.State().(engine.EntryAdmissionStore)
	if !ok {
		t.Fatal("distributed state store does not implement EntryAdmissionStore")
	}

	stamp := time.Now().Format("150405.000")
	g := projectionGraph(t, "sql-projection-real-seed", true)
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	resp, err := admitter.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey("sql-projection-real-seed-" + stamp),
		WorkflowID:      "wf-projection-real",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		EntryUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
		TraceID:         "trace-projection-" + stamp,
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("admission state = %q, want accepted", resp.State)
	}

	// Read back through an independent connection: the row must be durable in
	// MySQL, not merely accepted by an in-process cache.
	db2, err := mysqlstore.New(requireMySQL(t))
	if err != nil {
		t.Fatalf("reopen mysqlstore.New: %v", err)
	}
	rec, err := db2.GetExecution(ctx, resp.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution(%q) after entry seed: %v", resp.ExecutionID, err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("SQL execution status = %q, want success", rec.Status)
	}
	if rec.WorkflowName != g.Name() {
		t.Fatalf("SQL workflow_name = %q, want %q", rec.WorkflowName, g.Name())
	}
	if rec.TraceID != "trace-projection-"+stamp {
		t.Fatalf("SQL trace_id = %q, want %q", rec.TraceID, "trace-projection-"+stamp)
	}
	// workflow_def is NOT NULL in db/xflow_schema.sql and a seed carries no
	// WorkflowDef, so the projection must supply valid JSON. Asserted on the
	// value rather than left to the constraint: a shared test database whose
	// tables were created by sqlstore.AutoMigrate has nullable columns, and
	// would accept a NULL that production rejects.
	if len(rec.WorkflowDef) == 0 {
		t.Fatalf("SQL workflow_def is empty; production schema declares it NOT NULL")
	}
	_ = db
}

// TestCommitLeasedNodeProjectsToRealMySQL proves the node-commit projection
// carries both the terminal status and the failure reason onto a row created by
// the normal CreateExecution path.
func TestCommitLeasedNodeProjectsToRealMySQL(t *testing.T) {
	b, db := newProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	state := b.State()
	committer, ok := state.(engine.LegacyNodeCommitter)
	if !ok {
		t.Fatal("distributed state store does not implement LegacyNodeCommitter")
	}

	g := projectionGraph(t, "sql-projection-real-node", false)
	id := types.ExecutionID("sql-projection-real-node-" + time.Now().Format("150405.000"))
	// workflow_def is NOT NULL: CreateExecution fills it from the context-carried
	// definition, which is how the production submit path supplies it.
	ctx = engine.WithWorkflowDef(ctx, &types.WorkflowDef{Name: g.Name()})
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	idx, _ := g.NodeIndex("entry")
	lease := &engine.TaskLease{
		LeaseID: "L-real", LeaseToken: "T-real",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "entry", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	// The Lua bumps a fresh node's attempt to 1; the commit fence compares it.
	lease.Attempt = 1

	const reason = "entry node failed"
	res, err := committer.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "entry", NodeIdx: idx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: lease.Attempt,
		Status: types.NodeStatusFailed, Error: reason, Fatal: true,
	})
	if err != nil {
		t.Fatalf("CommitLeasedNode: %v", err)
	}
	if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
		t.Fatalf("commit result = %+v, want done+failed", res)
	}

	rec, err := db.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution(%q) after node commit: %v", id, err)
	}
	if rec.Status != types.ExecutionStatusFailed {
		t.Fatalf("SQL execution status = %q, want failed", rec.Status)
	}
	if rec.Error != reason {
		t.Fatalf("SQL execution error = %q, want %q", rec.Error, reason)
	}
}
