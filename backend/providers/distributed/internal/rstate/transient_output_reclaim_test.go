package rstate

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestRedisCommitNodeReclaimsTransientOutputsOnlyAfterAcceptedFence(t *testing.T) {
	ctx := context.Background()

	t.Run("accepted", func(t *testing.T) {
		id := types.ExecutionID("redis-output-reclaim-accepted")
		state, _, lease := newRedisOutputReclaimFixture(t, ctx, id)

		result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID:        id,
			NodeName:           "start",
			NodeIdx:            0,
			LeaseID:            lease.LeaseID,
			LeaseToken:         lease.LeaseToken,
			Attempt:            lease.Attempt,
			Status:             types.NodeStatusSuccess,
			Output:             map[string]any{"consumer": "value"},
			StoreOutput:        true,
			ReclaimOutputNames: []string{"source"},
		})
		if err != nil {
			t.Fatalf("CommitNode() error = %v", err)
		}
		if result.Outcome != engine.CommitOutcomeAccepted || !result.Applied {
			t.Fatalf("CommitNode() = %+v, want accepted and applied", result)
		}

		assertRedisOutputAbsent(t, state, ctx, id, "source")
		assertRedisOutput(t, state, ctx, id, "start", map[string]any{"consumer": "value"})
	})

	t.Run("stale", func(t *testing.T) {
		id := types.ExecutionID("redis-output-reclaim-stale")
		state, sourceOutput, lease := newRedisOutputReclaimFixture(t, ctx, id)

		result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID:        id,
			NodeName:           "start",
			NodeIdx:            0,
			LeaseID:            lease.LeaseID,
			LeaseToken:         "stale-token",
			Attempt:            lease.Attempt,
			Status:             types.NodeStatusSuccess,
			StoreOutput:        true,
			ReclaimOutputNames: []string{"source"},
		})
		if err != nil {
			t.Fatalf("CommitNode() error = %v", err)
		}
		if result.Outcome != engine.CommitOutcomeStaleToken || result.Applied {
			t.Fatalf("CommitNode() = %+v, want stale and unapplied", result)
		}

		assertRedisOutput(t, state, ctx, id, "source", sourceOutput)
	})

	t.Run("duplicate", func(t *testing.T) {
		id := types.ExecutionID("redis-output-reclaim-duplicate")
		state, sourceOutput, lease := newRedisOutputReclaimFixture(t, ctx, id)
		request := engine.CommitNodeRequest{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			LeaseID:     lease.LeaseID,
			LeaseToken:  lease.LeaseToken,
			Attempt:     lease.Attempt,
			Status:      types.NodeStatusSuccess,
			StoreOutput: true,
		}
		if result, err := state.CommitNode(ctx, request); err != nil || !result.Applied {
			t.Fatalf("initial CommitNode() = %+v, %v; want accepted and applied", result, err)
		}

		request.ReclaimOutputNames = []string{"source"}
		result, err := state.CommitNode(ctx, request)
		if err != nil {
			t.Fatalf("duplicate CommitNode() error = %v", err)
		}
		if result.Outcome != engine.CommitOutcomeDuplicateTerminal || result.Applied {
			t.Fatalf("duplicate CommitNode() = %+v, want duplicate and unapplied", result)
		}

		assertRedisOutput(t, state, ctx, id, "source", sourceOutput)
	})

	t.Run("inactive", func(t *testing.T) {
		id := types.ExecutionID("redis-output-reclaim-inactive")
		state, sourceOutput, lease := newRedisOutputReclaimFixture(t, ctx, id)
		ns := namespace.FromContext(ctx)
		if err := state.rdb.Del(ctx, execKey(ns, id, "status")).Err(); err != nil {
			t.Fatalf("delete execution status: %v", err)
		}

		result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID:        id,
			NodeName:           "start",
			NodeIdx:            0,
			LeaseID:            lease.LeaseID,
			LeaseToken:         lease.LeaseToken,
			Attempt:            lease.Attempt,
			Status:             types.NodeStatusSuccess,
			StoreOutput:        true,
			ReclaimOutputNames: []string{"source"},
		})
		if err != nil {
			t.Fatalf("CommitNode() error = %v", err)
		}
		if result.Outcome != engine.CommitOutcomeExecutionInactive || result.Applied {
			t.Fatalf("CommitNode() = %+v, want inactive and unapplied", result)
		}

		assertRedisOutput(t, state, ctx, id, "source", sourceOutput)
	})
}

func TestRedisCommitNodeRejectsInvalidTransientOutputReclaim(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name        string
		status      types.NodeStatus
		allowCycles bool
		fatal       bool
		system      bool
	}{
		{name: "non-success", status: types.NodeStatusFailed},
		{name: "cyclic", status: types.NodeStatusSuccess, allowCycles: true},
		{name: "fatal", status: types.NodeStatusSuccess, fatal: true},
		{name: "system", status: types.NodeStatusSuccess, system: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id := types.ExecutionID("redis-output-reclaim-invalid-" + tt.name)
			state, sourceOutput, lease := newRedisOutputReclaimFixture(t, ctx, id)

			if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
				ExecutionID:        id,
				NodeName:           "start",
				NodeIdx:            0,
				LeaseID:            lease.LeaseID,
				LeaseToken:         lease.LeaseToken,
				Attempt:            lease.Attempt,
				Status:             tt.status,
				StoreOutput:        true,
				System:             tt.system,
				Fatal:              tt.fatal,
				AllowCycles:        tt.allowCycles,
				ReclaimOutputNames: []string{"source"},
			}); err == nil {
				t.Fatal("CommitNode() error = nil, want invalid reclaim request error")
			}

			assertRedisOutput(t, state, ctx, id, "source", sourceOutput)
		})
	}
}

func TestRedisCommitNodeKeepsCyclicAndTrailingArgumentsAligned(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("redis-output-reclaim-cyclic-tail")
	state, _, _ := newTestRedisState(t)
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name:    "redis-output-reclaim-cyclic-tail",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "source", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"source": {"main": {Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": {Targets: []types.Connection{{Node: "source", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: compiled, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.PutOutput(ctx, id, "source", map[string]any{"source": "value"}); err != nil {
		t.Fatalf("PutOutput(source) error = %v", err)
	}

	reviewIdx, _ := compiled.NodeIndex("review")
	sourceIdx, _ := compiled.NodeIndex("source")
	lease := &engine.TaskLease{
		LeaseID: "review-lease", LeaseToken: "review-token", IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{
			ExecutionID: id, NodeName: "review", NodeIdx: reviewIdx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1

	details := map[string]any{"source": "reclaim-tail-test"}
	entry := engine.OutboxEntry{
		ID: "cyclic/redis-output-reclaim-cyclic-tail/source/2",
		Task: engine.Task{
			ExecutionID: id, NodeName: "source", NodeIdx: sourceIdx,
			Type: engine.TaskTypeNodeExec, ActivationID: 2,
		},
	}
	result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:   id,
		NodeName:      "review",
		NodeIdx:       reviewIdx,
		ActivationID:  lease.Task.ActivationID,
		LeaseID:       lease.LeaseID,
		LeaseToken:    lease.LeaseToken,
		Attempt:       lease.Attempt,
		Status:        types.NodeStatusSuccess,
		Output:        map[string]any{"review": "private"},
		StoreOutput:   true,
		PrivateOutput: true,
		ErrorDetails:  details,
		AllowCycles:   true,
		CyclicOutbox:  []engine.OutboxEntry{entry},
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if result.Outcome != engine.CommitOutcomeAccepted || !result.Applied {
		t.Fatalf("CommitNode() = %+v, want accepted and applied", result)
	}

	assertRedisOutput(t, state, ctx, id, "source", map[string]any{"source": "value"})
	assertRedisOutput(t, state, ctx, id, "review", map[string]any{"review": "private"})
	node, err := state.GetNode(ctx, id, "review")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || !node.PrivateOutput || node.Output != nil {
		t.Fatalf("GetNode() = %+v, want a private snapshot without output", node)
	}
	if !reflect.DeepEqual(node.ErrorDetails, details) {
		t.Fatalf("GetNode() error details = %#v, want %#v", node.ErrorDetails, details)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Minute), 2)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	if len(entries) != 1 || entries[0].ID != entry.ID {
		t.Fatalf("ListOutbox() = %+v, want cyclic entry %q", entries, entry.ID)
	}
}

func newRedisOutputReclaimFixture(t *testing.T, ctx context.Context, id types.ExecutionID) (*Store, map[string]any, *engine.TaskLease) {
	t.Helper()
	state, _, _ := newTestRedisState(t)
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	sourceOutput := map[string]any{"source": "value"}
	if err := state.PutOutput(ctx, id, "source", sourceOutput); err != nil {
		t.Fatalf("PutOutput(source) error = %v", err)
	}
	lease := &engine.TaskLease{
		LeaseID: "consumer-lease", LeaseToken: "consumer-token", IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	return state, sourceOutput, lease
}

func assertRedisOutput(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, name string, want map[string]any) {
	t.Helper()
	got, err := state.GetOutput(ctx, id, name)
	if err != nil {
		t.Fatalf("GetOutput(%q) error = %v", name, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetOutput(%q) = %#v, want %#v", name, got, want)
	}
}

func assertRedisOutputAbsent(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, name string) {
	t.Helper()
	got, err := state.GetOutput(ctx, id, name)
	if err != nil {
		t.Fatalf("GetOutput(%q) error = %v", name, err)
	}
	if got != nil {
		t.Fatalf("GetOutput(%q) = %#v, want reclaimed output", name, got)
	}
}
