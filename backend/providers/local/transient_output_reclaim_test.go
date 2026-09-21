package local

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestMemoryCommitNodeReclaimsTransientOutputsAfterAcceptedConsumerCommit(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("transient-output-reclaim-accepted")
	state, consumerIdx, sourceOutput := newTransientOutputReclaimState(t, ctx, id)
	unrelatedOutput := map[string]any{"keep": true}
	if err := state.PutOutput(ctx, id, "unrelated", unrelatedOutput); err != nil {
		t.Fatalf("PutOutput(unrelated) error = %v", err)
	}

	lease := transientOutputReclaimLease(id, "consumer", consumerIdx, "consumer-lease", "consumer-token")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(consumer) acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	consumerOutput := map[string]any{"consumed": true}
	result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:        id,
		NodeName:           "consumer",
		NodeIdx:            consumerIdx,
		LeaseID:            lease.LeaseID,
		LeaseToken:         lease.LeaseToken,
		Attempt:            lease.Attempt,
		Status:             types.NodeStatusSuccess,
		Output:             consumerOutput,
		StoreOutput:        true,
		ReclaimOutputNames: []string{"source"},
	})
	if err != nil {
		t.Fatalf("CommitNode(consumer) error = %v", err)
	}
	if result.Outcome != engine.CommitOutcomeAccepted || !result.Applied {
		t.Fatalf("CommitNode(consumer) = %+v, want accepted and applied", result)
	}

	outputs := state.GetAllOutputs(id)
	if got, found := outputs["source"]; found {
		t.Fatalf("source output = %#v, want reclaimed", got)
	}
	if got := outputs["consumer"]; !reflect.DeepEqual(got, consumerOutput) {
		t.Fatalf("consumer output = %#v, want %#v", got, consumerOutput)
	}
	if got := outputs["unrelated"]; !reflect.DeepEqual(got, unrelatedOutput) {
		t.Fatalf("unrelated output = %#v, want %#v; source was %#v", got, unrelatedOutput, sourceOutput)
	}
}

func TestMemoryCommitNodeDoesNotReclaimTransientOutputsOnStaleCommit(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("transient-output-reclaim-stale")
	state, consumerIdx, sourceOutput := newTransientOutputReclaimState(t, ctx, id)

	lease := transientOutputReclaimLease(id, "consumer", consumerIdx, "current-lease", "current-token")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(consumer) acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:        id,
		NodeName:           "consumer",
		NodeIdx:            consumerIdx,
		LeaseID:            "stale-lease",
		LeaseToken:         "stale-token",
		Attempt:            lease.Attempt,
		Status:             types.NodeStatusSuccess,
		StoreOutput:        true,
		ReclaimOutputNames: []string{"source"},
	})
	if err != nil {
		t.Fatalf("CommitNode(stale consumer) error = %v", err)
	}
	if result.Outcome != engine.CommitOutcomeStaleToken || result.Applied {
		t.Fatalf("CommitNode(stale consumer) = %+v, want stale token and not applied", result)
	}

	if got := state.GetAllOutputs(id)["source"]; !reflect.DeepEqual(got, sourceOutput) {
		t.Fatalf("source output after stale commit = %#v, want %#v", got, sourceOutput)
	}
}

func TestMemoryCommitNodeRejectsInvalidTransientOutputReclaim(t *testing.T) {
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
			id := types.ExecutionID("transient-output-reclaim-invalid-" + tt.name)
			state, consumerIdx, sourceOutput := newTransientOutputReclaimState(t, ctx, id)
			lease := transientOutputReclaimLease(id, "consumer", consumerIdx, "consumer-lease", "consumer-token")
			if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
				t.Fatalf("AcquireTaskLease(consumer) acquired=%v err=%v, want true/nil", acquired, err)
			}
			lease.Attempt = 1

			if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
				ExecutionID:        id,
				NodeName:           "consumer",
				NodeIdx:            consumerIdx,
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

			if got := state.GetAllOutputs(id)["source"]; !reflect.DeepEqual(got, sourceOutput) {
				t.Fatalf("source output after invalid reclaim request = %#v, want %#v", got, sourceOutput)
			}
		})
	}
}

func newTransientOutputReclaimState(t *testing.T, ctx context.Context, id types.ExecutionID) (*memoryState, int, map[string]any) {
	t.Helper()
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name: "transient-output-reclaim",
		Nodes: []types.NodeDef{
			{Name: "source", Type: "test.source"},
			{Name: "consumer", Type: "test.consumer"},
		},
		Connections: types.Connections{
			"source": {"main": {Targets: []types.Connection{{Node: "consumer", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newMemoryState()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: compiled, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	sourceIdx, _ := compiled.NodeIndex("source")
	consumerIdx, _ := compiled.NodeIndex("consumer")
	sourceLease := transientOutputReclaimLease(id, "source", sourceIdx, "source-lease", "source-token")
	if _, acquired, err := state.AcquireTaskLease(ctx, sourceLease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(source) acquired=%v err=%v, want true/nil", acquired, err)
	}
	sourceLease.Attempt = 1
	sourceOutput := map[string]any{"source": "value"}
	result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "source",
		NodeIdx:     sourceIdx,
		LeaseID:     sourceLease.LeaseID,
		LeaseToken:  sourceLease.LeaseToken,
		Attempt:     sourceLease.Attempt,
		Status:      types.NodeStatusSuccess,
		Output:      sourceOutput,
		StoreOutput: true,
	})
	if err != nil {
		t.Fatalf("CommitNode(source) error = %v", err)
	}
	if result.Outcome != engine.CommitOutcomeAccepted || !result.Applied {
		t.Fatalf("CommitNode(source) = %+v, want accepted and applied", result)
	}
	return state, consumerIdx, sourceOutput
}

func transientOutputReclaimLease(id types.ExecutionID, name string, nodeIdx int, leaseID engine.LeaseID, token engine.LeaseToken) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID: leaseID, LeaseToken: token, IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: name, NodeIdx: nodeIdx, Type: engine.TaskTypeNodeExec},
	}
}
