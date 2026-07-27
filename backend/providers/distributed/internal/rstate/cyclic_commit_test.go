package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestRedisCommitLeasedNodeCyclicPersistsDownstreamAtomically drives the Redis
// Lua branch added for #7: a cyclic node's terminal commit and its downstream
// delivery intent are persisted in one fenced transition, and a redelivered
// (duplicate-terminal) commit does not double-write the intent.
func TestRedisCommitLeasedNodeCyclicPersistsDownstreamAtomically(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()

	def := &types.WorkflowDef{
		Name:    "cyclic-redis",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("cyclic-redis-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}

	reviewIdx, _ := g.NodeIndex("review")
	lease := &engine.TaskLease{
		LeaseID:    "lease-review",
		LeaseToken: "token-review",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "review",
			NodeIdx:      reviewIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	downstream := engine.OutboxEntry{
		ID: fmt.Sprintf("cyclic/%s/start/2", id),
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "start",
			NodeIdx:      func() int { idx, _ := g.NodeIndex("start"); return idx }(),
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 2,
			AutoDepth:    1,
		},
	}
	req := engine.CommitNodeRequest{
		ExecutionID:  id,
		NodeName:     "review",
		NodeIdx:      reviewIdx,
		ActivationID: 1,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      1,
		Status:       types.NodeStatusSuccess,
		StoreOutput:  true,
		Port:         "reject",
		CyclicOutbox: []engine.OutboxEntry{downstream},
	}
	res, err := state.CommitLeasedNode(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("commit outcome = %v, want accepted", res.Outcome)
	}

	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Task.NodeName != "start" || entries[0].Task.ActivationID != 2 {
		t.Fatalf("cyclic downstream not persisted atomically: %+v", entries)
	}

	// Redelivered duplicate commit: terminal check fires before the outbox
	// write, so no second intent is created.
	res2, err := state.CommitLeasedNode(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Outcome != engine.CommitOutcomeDuplicateTerminal {
		t.Fatalf("duplicate commit outcome = %v, want duplicate_terminal", res2.Outcome)
	}
	entries2, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries2) != 1 {
		t.Fatalf("duplicate commit changed outbox: %+v", entries2)
	}
}

// TestRedisCommitLeasedNodeCyclicCompletionIsAtomic drives the Redis Lua branch
// that finalizes a cyclic execution status in the same fenced commit when the
// active branch has no downstream.
func TestRedisCommitLeasedNodeCyclicCompletionIsAtomic(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()

	def := &types.WorkflowDef{
		Name:    "cyclic-redis-complete",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "finish", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "finish", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("cyclic-redis-complete-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}

	finishIdx, _ := g.NodeIndex("finish")
	lease := &engine.TaskLease{
		LeaseID:    "lease-finish",
		LeaseToken: "token-finish",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "finish",
			NodeIdx:      finishIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1

	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
		ExecutionID:       id,
		NodeName:          "finish",
		NodeIdx:           finishIdx,
		ActivationID:      1,
		LeaseID:           lease.LeaseID,
		LeaseToken:        lease.LeaseToken,
		Attempt:           1,
		Status:            types.NodeStatusSuccess,
		StoreOutput:       true,
		Port:              "main",
		CyclicComplete:    true,
		CyclicFinalStatus: types.ExecutionStatusSuccess,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != engine.CommitOutcomeAccepted || !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
		t.Fatalf("commit result = %+v, want accepted+done+success", res)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", snap.Status)
	}
}
