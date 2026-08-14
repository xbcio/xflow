package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestMemoryCommitNodeUsesTheRequestGraphType is the local mirror of
// rstate's TestCommitLeasedNodeDoesNotGuessTheGraphType. It pins the same
// contract on the in-memory backend: the commit's completion protocol is
// selected by the request, not by the snapshot the backend happens to hold.
//
// The local store used to read entry.snap.Graph.AllowCycles(). A snapshot
// created without a compiled graph — CreateExecutionWithOutbox accepts one, and
// callers that only need the outbox pass no Graph — made BOTH branches
// unreachable: no completion counting AND no cyclic outbox persistence. The
// commit reported Accepted with an empty OutboxIDs and the execution simply
// stopped advancing, with no error anywhere.
func TestMemoryCommitNodeUsesTheRequestGraphType(t *testing.T) {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "memory-cyclic",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": types.PortConnections{Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("memory-cyclic-1")
	// Deliberately no Graph on the snapshot: this is the shape that silenced
	// both branches when the backend derived the type from it.
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	reviewIdx, _ := g.NodeIndex("review")
	startIdx, _ := g.NodeIndex("start")
	lease := &engine.TaskLease{
		LeaseID:    "lease-review",
		LeaseToken: "token-review",
		IssuedAt:   time.Now().UTC(),
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

	const downstreamID = "cyclic/memory-cyclic-1/start/2"
	res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
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
	})
	if err != nil {
		t.Fatalf("CommitLeasedNode() error = %v", err)
	}
	if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
		t.Fatalf("CommitLeasedNode() = %+v, want an applied accepted commit", res)
	}
	if len(res.OutboxIDs) != 1 || res.OutboxIDs[0] != downstreamID {
		t.Fatalf("OutboxIDs = %v, want the cyclic downstream intent %q", res.OutboxIDs, downstreamID)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	if len(entries) != 1 || entries[0].ID != downstreamID {
		t.Fatalf("outbox = %+v, want the cyclic downstream intent %q persisted", entries, downstreamID)
	}
	if res.ExecutionDone {
		t.Fatalf("CommitLeasedNode() finished the execution; a cyclic branch with downstream "+
			"work must stay running (result %+v)", res)
	}
}

// TestMemoryCommitNodeRejectsAContradictoryGraphType keeps the local backend's
// Validate call from being dropped: without it, the field's zero value would
// silently re-select the acyclic protocol for a cyclic commit.
func TestMemoryCommitNodeRejectsAContradictoryGraphType(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:  "contradiction",
		NodeName:     "n",
		Status:       types.NodeStatusSuccess,
		CyclicOutbox: []engine.OutboxEntry{{ID: "x"}},
	}); err == nil {
		t.Fatal("CommitNode() accepted CyclicOutbox on an acyclic commit")
	}
	if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: "contradiction",
		NodeName:    "n",
		Status:      types.NodeStatusFailed,
		AllowCycles: true,
		Fatal:       true,
	}); err == nil {
		t.Fatal("CommitNode() accepted the acyclic Fatal signal on a cyclic commit")
	}
}
