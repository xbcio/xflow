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

const (
	probeActiveTTL     = 10 * time.Minute
	probeCompletionTTL = 30 * time.Second
)

func probeTransientGraph(t *testing.T, name string, def *types.WorkflowDef) *graph.Graph {
	t.Helper()
	def.Name = name
	def.Options = &types.WorkflowOptions{
		Transient:              true,
		TransientTTL:           probeActiveTTL,
		TransientCompletionTTL: probeCompletionTTL,
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile(%q) error = %v", name, err)
	}
	if g.TransientCompletionTTL() != probeCompletionTTL {
		t.Fatalf("graph lost its completion TTL: %v", g.TransientCompletionTTL())
	}
	return g
}

// assertCompletionTTL reports every key's TTL, failing when an existing key did
// not get the completion TTL. Absent keys are fine: the commit path is allowed
// not to have written them, and an EXPIRE on a missing key is a no-op by design.
func assertCompletionTTL(t *testing.T, rdb *redis.Client, id types.ExecutionID, keys map[string]string) {
	t.Helper()
	ctx := context.Background()
	for label, key := range keys {
		exists, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("EXISTS %s: %v", label, err)
		}
		if exists == 0 {
			t.Logf("  %-22s absent (nothing to shorten)", label)
			continue
		}
		ttl, err := rdb.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("TTL %s: %v", label, err)
		}
		t.Logf("  %-22s TTL=%v", label, ttl.Round(time.Second))
		if ttl <= 0 {
			t.Errorf("%s exists with no TTL (%v): a finished transient execution's keys must expire", label, ttl)
			continue
		}
		if ttl > probeCompletionTTL+5*time.Second {
			t.Errorf("%s TTL = %v, want <= %v: the completion TTL was never applied, so this key "+
				"keeps the %v active TTL and a finished execution holds memory for %v",
				label, ttl.Round(time.Second), probeCompletionTTL, probeActiveTTL,
				(probeActiveTTL / probeCompletionTTL))
		}
	}
}

func newProbeStore(t *testing.T) (*Store, *redis.Client, context.Context) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, nil, probeActiveTTL), rdb, context.Background()
}

// TestTransientCompletionTTLIsAppliedByCommitNode pins the completion TTL on the
// CommitNode terminal path.
//
// commitNodeLua finalizes the execution inside the script, so such a commit
// never reaches UpdateExecutionStatus — which is where the completion TTL was
// applied, and (before this test) its only call site. Every finished execution
// therefore kept its output at the full ACTIVE TTL: 20x the retention the
// workflow asked for, on the value class that dominates the keyspace.
func TestTransientCompletionTTLIsAppliedByCommitNode(t *testing.T) {
	state, rdb, ctx := newProbeStore(t)
	g := probeTransientGraph(t, "probe-commitnode-ttl", &types.WorkflowDef{
		Nodes: []types.NodeDef{{Name: "only", Kind: types.NodeKindTrigger}},
	})
	id := types.ExecutionID("probe-commitnode-ttl")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	lease := &engine.TaskLease{
		LeaseID: "L1", LeaseToken: "T1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v", claimed, err)
	}
	res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "only", Status: types.NodeStatusSuccess,
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		StoreOutput: true, Output: map[string]any{"k": "v"},
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	ns := namespace.FromContext(ctx)
	st, _ := rdb.Get(ctx, execKey(ns, id, "status")).Result()
	if !types.IsTerminalExecutionStatus(types.ExecutionStatus(st)) {
		t.Fatalf("CommitNode() outcome=%v left status %q, not terminal; this test needs a "+
			"terminal commit to exercise the completion hook", res.Outcome, st)
	}
	assertCompletionTTL(t, rdb, id, map[string]string{
		"output:only":     outputKey(ns, id, "only"),
		"exec:status":     execKey(ns, id, "status"),
		"exec:graph":      execKey(ns, id, "graph"),
		"remaining_nodes": remainingNodesKey(ns, id),
	})
}

// TestTransientCompletionTTLIsAppliedByCommitGroup does the same for the group
// path — the one every collection-pipeline execution takes, and the one whose
// boundary output is the largest value the store holds.
func TestTransientCompletionTTLIsAppliedByCommitGroup(t *testing.T) {
	state, rdb, ctx := newProbeStore(t)
	g := probeTransientGraph(t, "probe-commitgroup-ttl", &types.WorkflowDef{
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"ingest": {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
	})
	id := types.ExecutionID("probe-commitgroup-ttl")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	gu := g.Groups()[0].UnitIdx
	ok, err := state.AcquireGroupLease(ctx, &engine.GroupLease{
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		IssuedAt: time.Now(), TTL: time.Minute,
	})
	if err != nil || !ok {
		t.Fatalf("AcquireGroupLease() ok=%v err=%v", ok, err)
	}
	res, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		Outcome: engine.GroupOutcomeSuccess,
		Exits:   []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: map[string]any{"batch": "x"}}},
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	ns := namespace.FromContext(ctx)
	st, _ := rdb.Get(ctx, execKey(ns, id, "status")).Result()
	if !types.IsTerminalExecutionStatus(types.ExecutionStatus(st)) {
		t.Fatalf("CommitGroup() outcome=%v left status %q, not terminal; this test needs a "+
			"terminal commit to exercise the completion hook", res.Outcome, st)
	}
	assertCompletionTTL(t, rdb, id, map[string]string{
		"output:analyze":  outputKey(ns, id, "analyze"),
		"exec:status":     execKey(ns, id, "status"),
		"exec:graph":      execKey(ns, id, "graph"),
		"remaining_nodes": remainingNodesKey(ns, id),
	})
}

// TestTransientActiveTTLSurvivesANonTerminalCommit is the other half: shortening
// must be tied to completion, not to committing. A commit that leaves the
// execution running has to leave the active TTL intact, or a long-running
// execution's own output would expire underneath it.
func TestTransientActiveTTLSurvivesANonTerminalCommit(t *testing.T) {
	state, rdb, ctx := newProbeStore(t)
	// "store" sits OUTSIDE the group, so committing the group schedules it and
	// the execution stays running. A graph whose group covers every node would
	// terminalize here and the test would assert nothing about non-terminal
	// commits.
	g := probeTransientGraph(t, "probe-active-ttl", &types.WorkflowDef{
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
			"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
	})
	id := types.ExecutionID("probe-active-ttl")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	gu := g.Groups()[0].UnitIdx
	if ok, err := state.AcquireGroupLease(ctx, &engine.GroupLease{
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		IssuedAt: time.Now(), TTL: time.Minute,
	}); err != nil || !ok {
		t.Fatalf("AcquireGroupLease() ok=%v err=%v", ok, err)
	}
	if _, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		Outcome: engine.GroupOutcomeSuccess,
		Exits:   []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: map[string]any{"batch": "x"}}},
	}); err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	ns := namespace.FromContext(ctx)
	st, _ := rdb.Get(ctx, execKey(ns, id, "status")).Result()
	if types.IsTerminalExecutionStatus(types.ExecutionStatus(st)) {
		t.Fatalf("execution reached %q: this commit is supposed to leave it running", st)
	}
	ttl, err := rdb.TTL(ctx, outputKey(ns, id, "analyze")).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl < probeCompletionTTL*2 {
		t.Errorf("output TTL = %v after a NON-terminal commit, want the %v active TTL: "+
			"shortening ran without the execution finishing, so a long-running execution "+
			"would lose its own output", ttl.Round(time.Second), probeActiveTTL)
	}
}
