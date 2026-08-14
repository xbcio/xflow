package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestCommitLeasedNodeDoesNotGuessTheGraphType pins the reason the commit path
// takes the graph type from the request instead of re-deriving it.
//
// The graph key carries the execution's TTL, and the in-memory cache does not
// survive a process restart. A long-lived cyclic execution (an approval loop
// parked on a signal for days is the motivating shape) can therefore reach a
// commit with the graph unavailable in both places. When the backend derived
// allowCycles from that lookup, an unavailable graph read as "acyclic":
//
//   - the cyclic downstream intents in CyclicOutbox were silently dropped,
//     because the Lua branch that persists them is gated on allowCycles==1;
//   - the acyclic branch ran instead and decremented the completion counter of
//     an execution that never had one, so a graph whose nodes commit repeatedly
//     could drive it negative.
//
// The failure is silent — no error, no log — and permanent: nothing re-derives
// a lost delivery intent. The request now carries the type, so this test drives
// a commit with the graph gone from BOTH the cache and Redis and still requires
// the cyclic intent to be persisted.
func TestCommitLeasedNodeDoesNotGuessTheGraphType(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()

	def := &types.WorkflowDef{
		Name:    "cyclic-graph-gone",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": types.PortConnections{Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("cyclic-graph-gone-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}

	reviewIdx, _ := g.NodeIndex("review")
	startIdx, _ := g.NodeIndex("start")
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

	// Make the graph unavailable the way a restart plus an expired key does:
	// drop the in-memory cache AND the Redis key. LoadGraph now returns
	// (nil, nil) — the shape the old code read as "acyclic".
	state.mu.Lock()
	delete(state.graphs, id)
	state.mu.Unlock()
	if err := rdb.Del(ctx, execKey(namespace.FromContext(ctx), id, "graph")).Err(); err != nil {
		t.Fatalf("delete graph key: %v", err)
	}
	if loaded, err := state.LoadGraph(ctx, id); err != nil || loaded != nil {
		t.Fatalf("LoadGraph() = %v, %v; want nil, nil so this test drives the graph-unavailable path", loaded, err)
	}

	downstreamID := fmt.Sprintf("cyclic/%s/start/2", id)
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
		AllowCycles:  true,
		CyclicOutbox: []engine.OutboxEntry{{
			ID: downstreamID,
			Task: engine.Task{
				ExecutionID:  id,
				NodeName:     "start",
				NodeIdx:      startIdx,
				Type:         engine.TaskTypeNodeExec,
				ActivationID: 2,
				AutoDepth:    1,
			},
		}},
	}
	res, err := state.CommitLeasedNode(ctx, req)
	if err != nil {
		t.Fatalf("CommitLeasedNode() error = %v", err)
	}
	if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
		t.Fatalf("CommitLeasedNode() = %+v, want an applied accepted commit", res)
	}
	if len(res.OutboxIDs) != 1 || res.OutboxIDs[0] != downstreamID {
		t.Fatalf("OutboxIDs = %v, want the cyclic downstream intent %q", res.OutboxIDs, downstreamID)
	}
	body, err := rdb.HGet(ctx, outboxBodyKey(namespace.FromContext(ctx), id), downstreamID).Result()
	if err != nil || body == "" {
		t.Fatalf("cyclic downstream intent %q was not persisted (err=%v); the commit was "+
			"classified as acyclic and the intent is lost permanently", downstreamID, err)
	}
	if res.ExecutionDone {
		t.Fatalf("CommitLeasedNode() finished the execution; a cyclic branch with downstream "+
			"work must stay running (result %+v)", res)
	}
}

// TestCommitNodeRejectsAContradictoryGraphType is the forcing function that
// keeps the new AllowCycles field from becoming a silent default. A bool that
// defaults to "acyclic" would reintroduce exactly the defect above the moment a
// caller forgot to set it, so the cyclic-only payload fields cross-check it.
func TestCommitNodeRejectsAContradictoryGraphType(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()

	cases := []struct {
		name string
		req  engine.CommitNodeRequest
	}{
		{
			name: "cyclic payload without the cyclic graph type",
			req: engine.CommitNodeRequest{
				ExecutionID:  "contradiction-1",
				NodeName:     "n",
				Status:       types.NodeStatusSuccess,
				CyclicOutbox: []engine.OutboxEntry{{ID: "x"}},
			},
		},
		{
			name: "cyclic completion without the cyclic graph type",
			req: engine.CommitNodeRequest{
				ExecutionID:       "contradiction-2",
				NodeName:          "n",
				Status:            types.NodeStatusSuccess,
				CyclicComplete:    true,
				CyclicFinalStatus: types.ExecutionStatusSuccess,
			},
		},
		{
			name: "acyclic fatal short-circuit on a cyclic graph",
			req: engine.CommitNodeRequest{
				ExecutionID: "contradiction-3",
				NodeName:    "n",
				Status:      types.NodeStatusFailed,
				AllowCycles: true,
				Fatal:       true,
			},
		},
		{
			name: "acyclic advance task on a cyclic graph",
			req: engine.CommitNodeRequest{
				ExecutionID: "contradiction-4",
				NodeName:    "n",
				Status:      types.NodeStatusSuccess,
				AllowCycles: true,
				AdvanceTask: &engine.Task{ExecutionID: "contradiction-4", NodeName: "n"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := state.CommitNode(ctx, tc.req); err == nil {
				t.Fatal("CommitNode() accepted a request whose graph type contradicts its payload")
			}
			if _, err := state.CommitLeasedNode(ctx, tc.req); err == nil {
				t.Fatal("CommitLeasedNode() accepted a request whose graph type contradicts its payload")
			}
		})
	}
}
