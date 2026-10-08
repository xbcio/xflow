package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// mapBatchTask is batch idx of the expansion generation leaseID, shaped like
// engine.expansionBatchTask builds it.
func mapBatchTask(leaseID string, idx int) engine.Task {
	return engine.Task{
		ExecutionID: "exec-1", NodeName: []string{"m/_batch/0", "m/_batch/1"}[idx], NodeIdx: 0,
		Type: engine.TaskTypeNodeBatch, ActivationID: 1,
		Payload: &types.SignalPayload{Data: map[string]any{
			"_batch_exec": true, "parent_lease_id": leaseID, "parent_lease_token": "tok-" + leaseID,
			"batch_index": idx, "parent_node": "m",
		}},
	}
}

func batchAssignment(task engine.Task) Assignment {
	return Assignment{AssignmentID: BuildAssignmentID(&task), Task: task, Routing: engine.TaskRouting{NodeType: "xflow.function"}}
}

func TestBuildAssignmentIDScopesBatchToParentGeneration(t *testing.T) {
	g1, g2 := mapBatchTask("L1", 0), mapBatchTask("L2", 0)
	if a1, a2 := BuildAssignmentID(&g1), BuildAssignmentID(&g2); a1 == a2 {
		t.Fatalf("batch 0 of two generations share assignment ID %q", a1)
	}
	again := mapBatchTask("L1", 0)
	if BuildAssignmentID(&g1) != BuildAssignmentID(&again) {
		t.Fatal("same generation batch must keep a stable assignment ID")
	}
}

// A batch a SIGKILLed runner left leased must not swallow the same-numbered
// batch of the next expansion generation as an enqueue duplicate.
func TestRedisRunnerDirectoryDeadGenerationBatchDoesNotSwallowNextGeneration(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	dead := registerRedisDirectoryRunner(t, ctx, dir, "runner-dead", 2)

	g1 := mapBatchTask("L1", 0)
	mustEnqueueRedisDirectoryAssignment(t, ctx, dir, batchAssignment(g1))
	claim := claimRedisDirectoryAssignment(t, ctx, dir, dead, 2)
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "L1", LeaseToken: "tok-L1", Task: g1}); err != nil {
		t.Fatal(err)
	}

	enqueued, err := dir.EnqueueAssignment(ctx, batchAssignment(mapBatchTask("L2", 0)))
	if err != nil || !enqueued {
		t.Fatalf("next-generation batch enqueue = %v, %v; want enqueued", enqueued, err)
	}
}

func TestMemoryRunnerDirectoryDeadGenerationBatchDoesNotSwallowNextGeneration(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	dead := registerMemoryBatchRunner(t, ctx, dir, "runner-dead")

	g1 := mapBatchTask("L1", 0)
	if ok, err := dir.EnqueueAssignment(ctx, batchAssignment(g1)); err != nil || !ok {
		t.Fatalf("gen1 enqueue = %v, %v", ok, err)
	}
	claim := claimMemoryBatch(t, ctx, dir, dead)
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "L1", LeaseToken: "tok-L1", Task: g1}); err != nil {
		t.Fatal(err)
	}

	enqueued, err := dir.EnqueueAssignment(ctx, batchAssignment(mapBatchTask("L2", 0)))
	if err != nil || !enqueued {
		t.Fatalf("next-generation batch enqueue = %v, %v; want enqueued", enqueued, err)
	}
}

func registerMemoryBatchRunner(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, runnerID string) RunnerSession {
	t.Helper()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     4,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func claimMemoryBatch(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, session RunnerSession) Claim {
	t.Helper()
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 4,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}}, Now: time.Unix(11, 0),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner ok=%v err=%v", ok, err)
	}
	return claim
}

// The sweeper rebuilds the assignment ID from the expired lease; a batch's ID
// depends on its task type, so the rebuild must keep it.
func TestTaskFromExpiredLeaseKeepsBatchAssignmentID(t *testing.T) {
	task := mapBatchTask("L1", 0)
	expired := engine.ExpiredLease{
		ExecutionID: task.ExecutionID, NodeName: task.NodeName, NodeIdx: task.NodeIdx,
		ActivationID: task.ActivationID, AutoDepth: task.AutoDepth,
		TaskType: task.Type, Payload: task.Payload,
	}
	if got, want := BuildAssignmentID(taskFromExpiredLease(&expired)), BuildAssignmentID(&task); got != want {
		t.Fatalf("assignment ID from expired lease = %q, want %q", got, want)
	}
}
