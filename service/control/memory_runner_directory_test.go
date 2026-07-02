package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestMemoryRunnerDirectoryEnqueueClaimFinalizeAndRelease(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if session.SessionID == "" {
		t.Fatalf("SessionID is empty")
	}

	assignment := Assignment{
		AssignmentID: "exec-1/node-a/activation-1",
		Task: engine.Task{
			ExecutionID:  "exec-1",
			NodeName:     "node-a",
			NodeIdx:      0,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Routing: engine.TaskRouting{NodeType: "xflow.function"},
	}
	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatalf("first EnqueueAssignment() enqueued=false, want true")
	}
	enqueued, err = dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("duplicate EnqueueAssignment() error = %v", err)
	}
	if enqueued {
		t.Fatalf("duplicate EnqueueAssignment() enqueued=true, want false")
	}

	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Now:          time.Unix(11, 0),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if claim.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("claim assignment = %+v, want %+v", claim.Assignment, assignment)
	}

	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "lease-1"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("ReleaseLeased() error = %v", err)
	}
}

func TestMemoryRunnerDirectorySessionFencesStaleRequests(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	first, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	second, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("second Register() error = %v", err)
	}
	if first.SessionID == second.SessionID {
		t.Fatalf("sessions should differ, both %q", first.SessionID)
	}
	if err := dir.Heartbeat(context.Background(), HeartbeatRequest{
		RunnerID:  "runner-1",
		SessionID: first.SessionID,
		Capacity:  1,
		InFlight:  0,
	}); !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("stale Heartbeat() error = %v, want ErrRunnerSessionStale", err)
	}
}
