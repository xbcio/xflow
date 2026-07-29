package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestLabelRegistration_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	labels := map[string]string{"region": "us-east-1", "pool": "gpu"}
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Labels:       labels,
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	snap, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found after register")
	}
	if snap.Labels["region"] != "us-east-1" {
		t.Errorf("Labels[region] = %q, want us-east-1", snap.Labels["region"])
	}
	if snap.Labels["pool"] != "gpu" {
		t.Errorf("Labels[pool] = %q, want gpu", snap.Labels["pool"])
	}

	_ = session
}

func TestLabelPollRefresh_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Labels:       map[string]string{"region": "us-east-1"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Poll with updated labels.
	_, _, _ = dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Labels:       map[string]string{"region": "eu-west-1", "tier": "premium"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Now:          time.Unix(11, 0),
	})

	snap, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found")
	}
	if snap.Labels["region"] != "eu-west-1" {
		t.Errorf("Labels[region] = %q after poll refresh, want eu-west-1", snap.Labels["region"])
	}
	if snap.Labels["tier"] != "premium" {
		t.Errorf("Labels[tier] = %q after poll refresh, want premium", snap.Labels["tier"])
	}
	// Old key removed (replaced entirely).
	if _, exists := snap.Labels["pool"]; exists {
		t.Error("old label 'pool' should not exist after full replacement")
	}
}

func TestLabelNilOnPollPreservesExisting_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Labels:       map[string]string{"region": "us-east-1"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Poll with nil labels — should NOT clear.
	_, _, _ = dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Labels:       nil,
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Now:          time.Unix(11, 0),
	})

	snap, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found")
	}
	if snap.Labels["region"] != "us-east-1" {
		t.Errorf("Labels[region] = %q, want us-east-1 (nil poll should preserve)", snap.Labels["region"])
	}
}

func TestLabelSessionFence_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	session1, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Labels:       map[string]string{"v": "1"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Re-register (new session) with new labels.
	_, err = dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     3,
		Labels:       map[string]string{"v": "2"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(20, 0),
	})
	if err != nil {
		t.Fatalf("Re-register: %v", err)
	}

	// Old session poll should be fenced.
	_, _, err = dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session1.SessionID,
		Capacity:     2,
		Labels:       map[string]string{"v": "stale"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Now:          time.Unix(21, 0),
	})
	if err == nil {
		t.Fatal("expected stale session error")
	}

	// Labels should reflect re-registration, not stale poll.
	snap, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found")
	}
	if snap.Labels["v"] != "2" {
		t.Errorf("Labels[v] = %q, want 2 (stale session must not update)", snap.Labels["v"])
	}
}

// TestLabelSelectorClaim_MemoryDirectory verifies that ClaimForRunner enforces
// RunnerSelector.MatchLabels in the dispatch hot path: a task requiring labels
// the runner lacks must NOT be claimed, and a matching runner claims it.
func TestLabelSelectorClaim_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Labels:       map[string]string{"cloud": "aws"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Task requires cloud=tencent; runner is cloud=aws → must not claim.
	mismatch := Assignment{
		AssignmentID: "exec-1/node-a/activation-1",
		Task: engine.Task{
			ExecutionID:  "exec-1",
			NodeName:     "node-a",
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Routing: engine.TaskRouting{
			NodeType:       "http.request",
			RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}},
		},
	}
	if _, err := dir.EnqueueAssignment(ctx, mismatch); err != nil {
		t.Fatalf("EnqueueAssignment(mismatch): %v", err)
	}

	_, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Now:          time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner(mismatch): %v", err)
	}
	if ok {
		t.Fatal("ClaimForRunner claimed a task whose RunnerSelector labels do not match the runner")
	}

	// Task requires cloud=aws → runner matches → must claim.
	match := Assignment{
		AssignmentID: "exec-1/node-b/activation-1",
		Task: engine.Task{
			ExecutionID:  "exec-1",
			NodeName:     "node-b",
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Routing: engine.TaskRouting{
			NodeType:       "http.request",
			RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "aws"}},
		},
	}
	if _, err := dir.EnqueueAssignment(ctx, match); err != nil {
		t.Fatalf("EnqueueAssignment(match): %v", err)
	}

	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Now:          time.Unix(12, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner(match): %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner did not claim a task whose RunnerSelector labels match the runner")
	}
	if claim.Assignment.AssignmentID != match.AssignmentID {
		t.Fatalf("claimed %q, want %q", claim.Assignment.AssignmentID, match.AssignmentID)
	}
}
