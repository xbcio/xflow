//go:build integration

package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// transientProjectionGraph compiles the two-node chain as a TRANSIENT workflow:
// the mode a workflow declares when its payloads must not reach the SQL audit
// tables.
func transientProjectionGraph(t *testing.T, name string) *graph.Graph {
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
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
		Options: &types.WorkflowOptions{
			Transient:              true,
			TransientTTL:           10 * time.Minute,
			TransientCompletionTTL: 30 * time.Second,
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	if !g.Transient() {
		t.Fatal("compiled graph did not carry Transient: the option never " +
			"reached the graph, so nothing downstream can honour it")
	}
	return g
}

// TestTransientSubmitSkipsRealMySQLProjection is the baseline: on the submit
// path (CreateExecution), a transient workflow must leave no execution row.
//
// This is the path engine.Submit/Invoke take, and the one attachTransientHint
// feeds. If this fails, per-workflow transient is broken outright.
func TestTransientSubmitSkipsRealMySQLProjection(t *testing.T) {
	b, db := newProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	g := transientProjectionGraph(t, "transient-submit-real")
	id := types.ExecutionID("transient-submit-real-" + time.Now().Format("150405.000"))

	// Exactly what engine.Submit does: attach the hint derived from the graph,
	// then create. Setting the hint by hand would prove nothing about whether
	// the production path attaches it -- engine.attachTransientHint is the code
	// under test as much as the store is.
	ctx = engine.WithWorkflowDef(ctx, &types.WorkflowDef{Name: g.Name()})
	ctx = engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL: g.TransientTTL(), CompletionTTL: g.TransientCompletionTTL(),
	})

	state := b.State()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	if rec, err := db.GetExecution(ctx, id); err == nil && rec != nil {
		t.Fatalf("a transient execution was projected to SQL anyway "+
			"(status=%q): every node payload of this workflow will be written "+
			"to the audit tables, credentials included", rec.Status)
	}
}

// TestTransientEntrySeedSkipsRealMySQLProjection is the one that matters for
// the Kafka topology.
//
// A Kafka trigger group never calls CreateExecution: node/trigger/kafka/
// entryseed.go admits the batch through SeedExecutionFromEntry, which creates
// the execution inside its own Lua script. projectSeededExecution guards that
// path with isTransient() -- but isTransient resolves from the per-execution
// Redis marker, and markExecutionTransient is only ever called from
// CreateExecution. On the seed path there is no marker to find, so the guard
// resolves "not transient" and projects the row regardless of what the workflow
// declared.
//
// That is the whole exposure: the workflow carrying raw third-party traffic is
// precisely the one admitted through the seed path.
func TestTransientEntrySeedSkipsRealMySQLProjection(t *testing.T) {
	b, db := newProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	state := b.State()
	seeder, ok := state.(engine.EntryAdmissionStore)
	if !ok {
		t.Fatal("distributed state store does not implement EntryAdmissionStore")
	}

	g := transientProjectionGraph(t, "transient-seed-real")
	entryNodeIdx, ok := g.NodeIndex("entry")
	if !ok {
		t.Fatal("entry node not found in compiled graph")
	}
	entryIdx := g.UnitIndexForNode(entryNodeIdx)
	admission := engine.AdmissionKey("transient-seed-real-" + time.Now().Format("150405.000"))

	resp, err := seeder.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: admission,
		Graph:        g,
		EntryUnitIdx: entryIdx,
		Outcome:      engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("seed was not accepted: %+v", resp)
	}
	execID := resp.ExecutionID

	if rec, err := db.GetExecution(ctx, execID); err == nil && rec != nil {
		t.Fatalf("a transient workflow admitted through the entry-seed path was "+
			"projected to SQL (status=%q): the Kafka trigger topology never "+
			"calls CreateExecution, so no per-execution transient marker is "+
			"ever written and projectSeededExecution's isTransient() guard "+
			"always resolves false", rec.Status)
	}

	// An absent execution row does not imply absent payloads. The node rows are
	// projected by a different guard (state_commit.go's isTransient check), and
	// they are the ones carrying output -- which for the real workflow means the
	// undecoded request bodies, Authorization headers and session cookies of
	// third-party traffic. Assert on them separately, and scan the whole record
	// rather than checking for a named column: a credential that arrives under
	// an unexpected key is still a credential in the database.
	// Limit: 100, not the zero value -- see seedAndCommitNode for why a
	// zero-valued ListOptions makes this assertion vacuous.
	nodes, err := db.ListNodes(ctx, execID, store.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("ListNodes(%q): %v", execID, err)
	}
	if len(nodes) > 0 {
		t.Fatalf("a transient execution left %d node row(s) in SQL: node "+
			"output is exactly where third-party payloads live, so the audit "+
			"tables now hold the data the workflow declared ephemeral "+
			"(first row: node=%q output=%s)",
			len(nodes), nodes[0].NodeName, nodes[0].Output)
	}
}

// TestDurableEntrySeedStillProjects is the counter-case that gives the two
// assertions above their teeth.
//
// Both of them assert an ABSENCE, and an absence passes for the wrong reasons
// just as easily as the right ones: a seed that silently failed to admit, a
// query against the wrong execution ID, a projection that was never wired for
// this path at all. If a durable workflow admitted the same way also leaves no
// row, those tests prove nothing about transient.
func TestDurableEntrySeedStillProjects(t *testing.T) {
	b, db := newProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seeder, ok := b.State().(engine.EntryAdmissionStore)
	if !ok {
		t.Fatal("distributed state store does not implement EntryAdmissionStore")
	}

	// Same shape as the transient graph, minus the Transient option.
	g := projectionGraph(t, "durable-seed-real", true)
	entryNodeIdx, ok := g.NodeIndex("entry")
	if !ok {
		t.Fatal("entry node not found in compiled graph")
	}
	admission := engine.AdmissionKey("durable-seed-real-" + time.Now().Format("150405.000"))

	resp, err := seeder.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: admission,
		Graph:        g,
		EntryUnitIdx: g.UnitIndexForNode(entryNodeIdx),
		Outcome:      engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("seed was not accepted: %+v", resp)
	}

	if _, err := db.GetExecution(ctx, resp.ExecutionID); err != nil {
		t.Fatalf("a DURABLE workflow seeded through the same path left no SQL "+
			"row either, so the transient assertions above are vacuous -- they "+
			"would pass with the projection removed entirely: %v", err)
	}
}

// seedAndCommitNode admits a two-node workflow through the entry-seed path and
// then commits the downstream node with a payload, returning every node row the
// commit left in SQL.
//
// Both the transient case and its durable counter-case run through this, so the
// only difference between them is the workflow's Transient option -- which is
// what makes the absence assertion in the transient case mean something.
func seedAndCommitNode(t *testing.T, name, secret string, transient bool) []*store.NodeRecord {
	t.Helper()
	b, db := newProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	state := b.State()
	seeder, ok := state.(engine.EntryAdmissionStore)
	if !ok {
		t.Fatal("distributed state store does not implement EntryAdmissionStore")
	}
	committer, ok := state.(engine.LegacyNodeCommitter)
	if !ok {
		t.Fatal("distributed state store does not implement LegacyNodeCommitter")
	}

	// Only "entry" is in the group, so the seed does not complete the execution
	// and "body" is still leasable afterwards.
	def := &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry"}}},
	}
	if transient {
		def.Options = &types.WorkflowOptions{
			Transient: true, TransientTTL: 10 * time.Minute,
			TransientCompletionTTL: 30 * time.Second,
		}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	if g.Transient() != transient {
		t.Fatalf("compiled graph Transient() = %v, want %v", g.Transient(), transient)
	}

	entryNodeIdx, _ := g.NodeIndex("entry")
	admission := engine.AdmissionKey(name + "-" + time.Now().Format("150405.000"))
	resp, err := seeder.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey: admission,
		Graph:        g,
		EntryUnitIdx: g.UnitIndexForNode(entryNodeIdx),
		Outcome:      engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("seed was not accepted: %+v", resp)
	}
	execID := resp.ExecutionID

	bodyIdx, _ := g.NodeIndex("body")
	lease := &engine.TaskLease{
		LeaseID: engine.LeaseID("L-" + name), LeaseToken: engine.LeaseToken("T-" + name),
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: execID, NodeName: "body", NodeIdx: bodyIdx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	res, err := committer.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID: execID, NodeName: "body", NodeIdx: bodyIdx, ActivationID: 1,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess,
		Output: map[string]any{"headers": map[string]any{"authorization": secret}},
	})
	if err != nil {
		t.Fatalf("CommitLeasedNode: %v", err)
	}
	// Without this the transient case is a fake probe: the SQL projection runs
	// only under out.Applied (state_commit.go), so a commit the backend rejected
	// -- stale token, inactive execution, duplicate terminal -- writes no row for
	// a reason unrelated to transient, and the absence assertion passes with the
	// guard removed entirely.
	if !res.Applied {
		t.Fatalf("the node commit was not applied (outcome=%v), so no projection "+
			"was ever attempted", res.Outcome)
	}

	// An explicit Limit, not store.ListOptions{}. Normalized() documents limit=0
	// as "unbounded" and memstore implements it that way, but sqlstore hands the
	// zero straight to GORM's Limit(0), which emits a literal LIMIT 0 and returns
	// nothing. A zero-valued ListOptions here makes every row-absence assertion
	// in this file pass unconditionally -- which is exactly how the transient
	// node-commit case first appeared to pass against unfixed code.
	nodes, err := db.ListNodes(ctx, execID, store.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("ListNodes(%q): %v", execID, err)
	}
	return nodes
}

// TestTransientSeededNodeCommitLeavesNoPayload closes the gap the seed tests
// leave open.
//
// They assert that the admission itself writes no rows. But a Kafka batch
// execution does not end at admission: downstream nodes are leased and
// committed afterwards, through CommitLeasedNode, whose SQL projection is
// guarded by its own isTransient() call (state_commit.go). Node output is where
// the payload actually lives -- for the real workflow, the undecoded request
// bodies and Authorization headers of third-party traffic.
//
// This is the assertion that matters for the credential-disclosure constraint,
// and the one the marker fix has to hold up for: the guard reads the same
// per-execution marker the seed now writes.
func TestTransientSeededNodeCommitLeavesNoPayload(t *testing.T) {
	const secret = "Bearer super-secret-session-token"
	nodes := seedAndCommitNode(t, "transient-seed-commit", secret, true)

	// Scan the serialized record for the VALUE, not for a named column: the key
	// a credential arrives under is not fixed, and a payload nested one level
	// deeper is just as disclosed as one at the root.
	for _, n := range nodes {
		if bytes.Contains(n.Output, []byte(secret)) {
			t.Fatalf("node %q of a transient execution persisted its payload to "+
				"SQL: the credential is now in xflow_nodes.output. row = %s",
				n.NodeName, n.Output)
		}
	}
	if len(nodes) > 0 {
		t.Fatalf("a transient execution left %d node row(s) in SQL; even without "+
			"the probe value in them, the audit tables now hold node data the "+
			"workflow declared ephemeral (first: %q)", len(nodes), nodes[0].NodeName)
	}
}

// TestDurableSeededNodeCommitProjectsPayload gives the test above its teeth.
//
// The same seed-then-commit sequence on a DURABLE workflow must leave the node
// row, payload included. If it does not, the node projection is simply not
// reachable on this path and the transient assertion is vacuous -- it would
// pass with the transient guard deleted. That is not hypothetical: this test
// was written precisely because the transient case passed against the unfixed
// code, and only a durable counter-case can tell "the guard worked" apart from
// "nothing ever writes here".
func TestDurableSeededNodeCommitProjectsPayload(t *testing.T) {
	const secret = "Bearer durable-counter-case-token"
	nodes := seedAndCommitNode(t, "durable-seed-commit", secret, false)

	var found bool
	for _, n := range nodes {
		if n.NodeName == "body" && bytes.Contains(n.Output, []byte(secret)) {
			found = true
		}
	}
	if !found {
		t.Fatalf("a DURABLE seeded execution did not project its committed node "+
			"payload to SQL (%d row(s) found), so the transient assertion proves "+
			"nothing: no payload reaches xflow_nodes on this path either way",
			len(nodes))
	}
}
