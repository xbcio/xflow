package rstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// fakeNodeStore records the node rows a Store would project into SQL. It keeps
// the serialized Output because that column (xflow_nodes.output) is where a
// node's payload lands -- for a traffic-collection workflow, the raw request and
// response bodies, credentials included.
type fakeNodeStore struct {
	store.Store
	nodes []*store.NodeRecord
	// statuses records every UpdateExecutionStatus the Store attempted, keeping
	// errMsg because that string (xflow_executions.error) is the second place a
	// payload can land: engine/errorpolicy.go builds it from sysErr.Error()
	// verbatim, so whatever a node put in its error text arrives here intact.
	statuses []fakeStatusUpdate
}

// fakeStatusUpdate is one attempted projection of an execution's terminal state.
type fakeStatusUpdate struct {
	execID types.ExecutionID
	status types.ExecutionStatus
	errMsg string
}

func (f *fakeNodeStore) CreateExecution(_ context.Context, _ *store.ExecutionRecord) error {
	return nil
}

func (f *fakeNodeStore) UpsertNode(_ context.Context, rec *store.NodeRecord) error {
	f.nodes = append(f.nodes, rec)
	return nil
}

func (f *fakeNodeStore) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	f.statuses = append(f.statuses, fakeStatusUpdate{execID: id, status: status, errMsg: errMsg})
	return nil
}

// TestPerWorkflowTransient_SkipsNodeOutputProjection is the load-bearing
// assertion for per-workflow transient mode: a workflow that declared itself
// transient must leave NO node output in SQL.
//
// Skipping the audit mirror is not enough. The audit trail is a side record;
// xflow_nodes.output is the payload itself. A workflow carries raw traffic
// through its nodes precisely because the cleansing rules drop whole messages
// rather than redact fields -- a surviving message's body is byte-for-byte
// intact. If that row is written, the credentials the tagging rules exist to
// find are persisted in the platform database, which is the entire reason
// transient mode is a prerequisite for connecting real traffic.
//
// The durable execution in the same Store is the positive control: it proves
// the projection path is live and that a skip is a decision, not a no-op.
func TestPerWorkflowTransient_SkipsNodeOutputProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &fakeNodeStore{}
	state := New(rdb, fakeDB, time.Hour)
	// Global transient is OFF: the control plane hosts durable workflows too,
	// which is why the per-workflow switch has to exist at all.
	state.transient = false

	ctx := context.Background()

	transientID := types.ExecutionID("exec-transient-node")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID:     transientID,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution (transient): %v", err)
	}

	durableID := types.ExecutionID("exec-durable-node")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     durableID,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable): %v", err)
	}

	// The secret stands in for what a real payload carries through this node.
	const secret = "ghp_000000000000000000000000000000000000"
	for _, id := range []types.ExecutionID{transientID, durableID} {
		if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
			ExecutionID: id,
			Name:        "start",
			Status:      types.NodeStatusSuccess,
			Output:      map[string]any{"authorization": "Bearer " + secret},
		}); err != nil {
			t.Fatalf("UpsertNode (%s): %v", id, err)
		}
	}

	var transientRows, durableRows int
	for _, rec := range fakeDB.nodes {
		switch rec.ExecutionID {
		case transientID:
			transientRows++
		case durableID:
			durableRows++
		}
		// Scan the value, not the key: a key-absence check would pass while the
		// secret rode along inside a nested copy of the payload.
		if strings.Contains(string(rec.Output), secret) && rec.ExecutionID == transientID {
			t.Errorf("transient execution's node output was projected into SQL "+
				"carrying the payload verbatim: %s", rec.Output)
		}
	}

	if durableRows != 1 {
		t.Fatalf("durable node rows = %d, want 1 -- the positive control did not "+
			"reach the projection path, so the transient assertion proves nothing",
			durableRows)
	}
	if transientRows != 0 {
		t.Fatalf("transient node rows = %d, want 0 -- a per-workflow transient "+
			"execution persisted node output to SQL", transientRows)
	}
}

// TestPerWorkflowTransient_SkipsLeaseNodeProjection covers the lease path,
// which the test above does not reach.
//
// Four sites project into xflow_nodes (state_node, state_commit, state_suspend
// and this one), and each carries its own isTransient guard -- an invariant
// maintained by repetition, so a single omission disables it silently. This one
// was omitted: AcquireTaskLease gated only on `s.db != nil`.
//
// The row it writes carries no Output, but calling it harmless misses how the
// table works. xflow_nodes is unique on (execution_id, node_name), so the row
// created here is the row a later commit updates in place -- the commit-side
// guard cannot withdraw a record this side already opened. And lease_id,
// lease_token, attempt and the node name of an execution declared ephemeral
// outliving the Redis TTL is itself the opposite of what transient promises.
func TestPerWorkflowTransient_SkipsLeaseNodeProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &fakeNodeStore{}
	state := New(rdb, fakeDB, time.Hour)
	state.transient = false // global transient off; only the workflow declares it

	ctx := context.Background()

	transientID := types.ExecutionID("exec-transient-lease")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID: transientID, Status: types.ExecutionStatusRunning, Graph: tg,
	}); err != nil {
		t.Fatalf("CreateExecution (transient): %v", err)
	}

	durableID := types.ExecutionID("exec-durable-lease")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: durableID, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable): %v", err)
	}

	for i, id := range []types.ExecutionID{transientID, durableID} {
		lease := &engine.TaskLease{
			LeaseID:    engine.LeaseID("L" + string(rune('0'+i))),
			LeaseToken: engine.LeaseToken("T" + string(rune('0'+i))),
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task: engine.Task{
				ExecutionID: id, NodeName: "start", NodeIdx: 0,
				Type: engine.TaskTypeNodeExec, ActivationID: 1,
			},
		}
		_, acquired, err := state.AcquireTaskLease(ctx, lease)
		if err != nil || !acquired {
			t.Fatalf("AcquireTaskLease (%s): acquired=%v err=%v", id, acquired, err)
		}
	}

	var transientRows, durableRows int
	for _, rec := range fakeDB.nodes {
		switch rec.ExecutionID {
		case transientID:
			transientRows++
		case durableID:
			durableRows++
		}
	}
	if durableRows != 1 {
		t.Fatalf("durable lease rows = %d, want 1 -- the positive control never "+
			"reached the projection, so the transient assertion below would pass "+
			"even with the projection deleted entirely", durableRows)
	}
	if transientRows != 0 {
		t.Fatalf("transient lease rows = %d, want 0 -- AcquireTaskLease projected "+
			"a node row for an execution the workflow declared ephemeral, opening "+
			"the (execution_id, node_name) record that a later commit fills with "+
			"the node's payload", transientRows)
	}
}

// TestPerWorkflowTransient_SkipsExecutionStatusProjection covers the execution
// ERROR TEXT, which neither test above touches.
//
// The two above assert about xflow_nodes.output. But an execution's failure
// reason takes a different column and a different guard:
// xflow_executions.error, written by UpdateExecutionStatus and by
// projectExecutionStatus. Until this test existed, fakeNodeStore overrode only
// CreateExecution and UpsertNode, so a Store that projected the error text of a
// transient execution would have failed nothing here -- the call went straight
// through the embedded nil store.Store, which is to say it was never observed at
// all.
//
// That the column matters at all is the point the package doc used to get
// wrong. It asserted that this text "carries only the engine-generated reason
// string, never a node's output". Nothing enforces that:
// engine/errorpolicy.go does errMsg = sysErr.Error() on whatever the node
// returned, so a node doing fmt.Errorf("POST %s: %s", url, body) puts that body
// here -- and three built-in nodes were found doing exactly that (xflow.http's
// transport errors, since fixed; db_errors.go's raw MySQLError; grpc.go's
// st.Message()). Keeping payload out of Error() is a convention, not a
// mechanism; the !isTransient guard is the mechanism, and it is what this test
// pins.
//
// The durable execution is the positive control: without it, a Store that had
// dropped the projection entirely would look identical to one honouring
// transient.
func TestPerWorkflowTransient_SkipsExecutionStatusProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &fakeNodeStore{}
	state := New(rdb, fakeDB, time.Hour)
	state.transient = false // global transient off; only the workflow declares it

	ctx := context.Background()

	transientID := types.ExecutionID("exec-transient-status")
	tg := testTransientGraph()
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID: transientID, Status: types.ExecutionStatusRunning, Graph: tg,
	}); err != nil {
		t.Fatalf("CreateExecution (transient): %v", err)
	}

	durableID := types.ExecutionID("exec-durable-status")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: durableID, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable): %v", err)
	}

	// Shaped like an error a node builds by formatting an upstream response into
	// its message -- the case the old doc comment claimed could not happen.
	const secret = "ghp_111111111111111111111111111111111111"
	errText := `POST https://upstream/v1/send: 401 {"authorization":"Bearer ` + secret + `"}`

	for _, id := range []types.ExecutionID{transientID, durableID} {
		if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, errText); err != nil {
			t.Fatalf("UpdateExecutionStatus (%s): %v", id, err)
		}
	}

	var transientUpdates, durableUpdates int
	for _, u := range fakeDB.statuses {
		switch u.execID {
		case transientID:
			transientUpdates++
			// Assert on the value as well as the count. A future projection that
			// truncated or reshaped the text would still leak if the secret rode
			// along, and the count alone would not say so.
			if strings.Contains(u.errMsg, secret) {
				t.Errorf("a transient execution's error text reached SQL carrying "+
					"the payload verbatim: %s", u.errMsg)
			}
		case durableID:
			durableUpdates++
		}
	}

	if durableUpdates != 1 {
		t.Fatalf("durable status updates = %d, want 1 -- the positive control never "+
			"reached the projection, so the transient assertion below would pass "+
			"with the projection removed entirely", durableUpdates)
	}
	if transientUpdates != 0 {
		t.Fatalf("transient status updates = %d, want 0 -- an execution the workflow "+
			"declared ephemeral wrote its failure reason to xflow_executions.error, "+
			"and that reason is whatever string the failing node's error rendered to",
			transientUpdates)
	}
}

// transientGroupGraph compiles a two-node group workflow, transient or not.
// The group shape is required: the node-commit path nests its
// projectExecutionStatus call inside an outer !isTransient block, so it cannot
// tell that function's own guard apart from the outer one. The group path calls
// it unguarded (group_state.go), which makes that guard the only thing standing
// between a transient execution's error text and SQL.
func transientGroupGraph(t *testing.T, name string, transient bool) *graph.Graph {
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
	}
	if transient {
		def.Options = &types.WorkflowOptions{
			Transient: true, TransientTTL: 2 * time.Minute,
			TransientCompletionTTL: 30 * time.Second,
		}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile(%s): %v", name, err)
	}
	if g.Transient() != transient {
		t.Fatalf("compiled %s Transient() = %v, want %v", name, g.Transient(), transient)
	}
	return g
}

// TestPerWorkflowTransient_SkipsGroupCommitStatusProjection covers the route
// that the test above cannot reach, and that the Kafka topology actually takes.
//
// UpdateExecutionStatus is not how a trigger-group execution reaches its
// terminal state: commitGroupLua finalizes it inside the script and returns
// done=1, so group_state.go calls projectExecutionStatus directly and
// UpdateExecutionStatus is never entered. projectExecutionStatus carries its own
// isTransient check, and on this path nothing else does -- the node-commit path
// wraps its call in an outer !isTransient block, so deleting the inner guard
// leaves the node path (and any test driving it) green while opening this one.
// Four sites repeat this guard and one of them has already been found missing
// it; a per-site test is the only thing that notices.
//
// The error text is the group's own, passed through GroupCommitRequest.Error,
// which is where engine/errorpolicy.go's verbatim sysErr.Error() lands.
func TestPerWorkflowTransient_SkipsGroupCommitStatusProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	fakeDB := &fakeNodeStore{}
	state := New(rdb, fakeDB, time.Hour)
	state.transient = false

	ctx := context.Background()

	const secret = "ghp_222222222222222222222222222222222222"
	errText := `GET https://upstream/v1/fetch: 403 {"cookie":"SESSION=` + secret + `"}`

	commitGroup := func(id types.ExecutionID, transient bool) {
		t.Helper()
		g := transientGroupGraph(t, string(id), transient)
		cctx := ctx
		if transient {
			cctx = engine.WithExecutionTransient(ctx, engine.TransientHint{
				TTL: g.TransientTTL(), CompletionTTL: g.TransientCompletionTTL(),
			})
		}
		if err := state.CreateExecution(cctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution (%s): %v", id, err)
		}
		unitIdx := g.Groups()[0].UnitIdx
		leaseID := engine.LeaseID("L-" + string(id))
		leaseToken := engine.LeaseToken("T-" + string(id))
		if ok, err := state.AcquireGroupLease(ctx, &engine.GroupLease{
			LeaseID: leaseID, LeaseToken: leaseToken, Attempt: 1,
			ExecutionID: id, GroupUnitIdx: unitIdx, GroupID: "tg",
			IssuedAt: time.Now(), TTL: time.Minute,
		}); err != nil || !ok {
			t.Fatalf("AcquireGroupLease (%s): ok=%v err=%v", id, ok, err)
		}
		res, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
			ExecutionID: id, GroupUnitIdx: unitIdx, GroupID: "tg",
			LeaseID: leaseID, LeaseToken: leaseToken, Attempt: 1,
			Outcome: engine.GroupOutcomeFailed, Error: errText,
		})
		if err != nil {
			t.Fatalf("CommitGroup (%s): %v", id, err)
		}
		// Without this the assertion is a fake probe: a commit the script
		// rejected, or one that left the execution running, writes no row for a
		// reason unrelated to transient, and the absence below would then pass
		// against a Store with no guard at all.
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
			t.Fatalf("group commit (%s) = %+v, want done+failed -- the projection is "+
				"only attempted on a terminal transition", id, res)
		}
	}

	transientID := types.ExecutionID("exec-transient-group-status")
	durableID := types.ExecutionID("exec-durable-group-status")
	commitGroup(transientID, true)
	commitGroup(durableID, false)

	var transientUpdates, durableUpdates int
	for _, u := range fakeDB.statuses {
		switch u.execID {
		case transientID:
			transientUpdates++
			if strings.Contains(u.errMsg, secret) {
				t.Errorf("a transient execution's group-commit error text reached SQL "+
					"carrying the payload verbatim: %s", u.errMsg)
			}
		case durableID:
			durableUpdates++
			if !strings.Contains(u.errMsg, secret) {
				t.Errorf("the durable control projected an error that is NOT the group's "+
					"own text (%q). If the reason is rewritten somewhere between "+
					"GroupCommitRequest.Error and the audit row, the transient assertion "+
					"is scanning for a string that could never have arrived.", u.errMsg)
			}
		}
	}

	if durableUpdates != 1 {
		t.Fatalf("durable group-commit status updates = %d, want 1 -- the positive "+
			"control never reached projectExecutionStatus, so the transient "+
			"assertion below would pass with that projection deleted", durableUpdates)
	}
	if transientUpdates != 0 {
		t.Fatalf("transient group-commit status updates = %d, want 0 -- the atomic "+
			"group commit finalized an ephemeral execution straight into "+
			"xflow_executions.error, which carries whatever text the failing node's "+
			"error rendered to", transientUpdates)
	}
}
