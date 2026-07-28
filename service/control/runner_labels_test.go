package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
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
