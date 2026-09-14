package control

import (
	"context"
	"reflect"
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

func TestPollMetadataDoesNotRefreshRegistration_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Labels:       map[string]string{"region": "us-east-1"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request", "xflow.script"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Poll metadata is compatibility-only. It must not alter the registration
	// snapshot even when its values would describe another workload.
	_, _, _ = dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     99,
		Labels:       map[string]string{"region": "eu-west-1", "tier": "premium"},
		Capabilities: []protocol.Capability{{NodeType: "xflow.script"}},
		Now:          time.Unix(11, 0),
	})

	snap, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found")
	}
	if snap.Labels["region"] != "us-east-1" {
		t.Errorf("Labels[region] = %q after poll, want registered us-east-1", snap.Labels["region"])
	}
	if _, exists := snap.Labels["tier"]; exists {
		t.Error("poll-only label tier was persisted into the registration snapshot")
	}
	if len(snap.Capabilities) != 1 || snap.Capabilities[0].NodeType != "http.request" {
		t.Errorf("Capabilities after poll = %v, want only registered http.request", snap.Capabilities)
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

// TestMemoryRunnerDirectoryRunnerReturnsIndependentLabels protects the
// registration snapshot from in-process callers of Runner. Poll routing uses
// that snapshot, so returning its map directly would make it mutable outside
// the directory even after poll input itself became untrusted.
func TestMemoryRunnerDirectoryRunnerReturnsIndependentLabels(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	_, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Labels:       map[string]string{"workload": "local"},
		Capabilities: []protocol.Capability{{NodeType: "xflow.sas.sink"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.sas.sink"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	snapshot, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found")
	}
	snapshot.Labels["workload"] = "sas-runner"
	snapshot.Labels["injected"] = "true"

	after, ok := dir.Runner(ctx, "runner-1")
	if !ok {
		t.Fatal("runner not found after mutation")
	}
	if got := after.Labels["workload"]; got != "local" {
		t.Errorf("registered workload after caller mutation = %q, want local", got)
	}
	if _, exists := after.Labels["injected"]; exists {
		t.Error("caller-mutated label leaked into registration snapshot")
	}
}

// TestPollMetadataCannotEscapeRegistrationSnapshot_MemoryDirectory models the
// SAS topology: a local sink runner may run xflow.group, but it must not claim
// collection work merely by reporting the standalone workload's labels or
// capabilities in a Poll request.
func TestPollMetadataCannotEscapeRegistrationSnapshot_MemoryDirectory(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	registeredCapabilities := []protocol.Capability{
		{NodeType: "xflow.sas.sink"},
		{NodeType: "xflow.group", Features: []string{engine.FeatureGroupExecV1}},
	}
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "local-runner",
		Capacity:     1,
		Labels:       map[string]string{"workload": "local"},
		Capabilities: registeredCapabilities,
		Policy: RunnerPolicy{AllowedNodeTypes: []string{
			"xflow.group",
			"xflow.sas.sink",
		}},
		Now: time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	wrongWorkload := Assignment{
		AssignmentID: "exec-1/collection-workload/activation-1",
		Routing: engine.TaskRouting{
			NodeType: "xflow.group",
			Requirements: []engine.CapabilityRequirement{{
				NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1,
			}},
			RunnerSelector: &types.RunnerSelector{
				Mode:        types.RunnerSelectorModeRequired,
				MatchLabels: map[string]string{"workload": "sas-runner"},
			},
		},
	}
	wrongCapability := Assignment{
		AssignmentID: "exec-1/collection-capability/activation-1",
		Routing: engine.TaskRouting{
			NodeType: "xflow.group",
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
				{NodeType: "xflow.map"},
			},
			RunnerSelector: &types.RunnerSelector{
				Mode:        types.RunnerSelectorModeRequired,
				MatchLabels: map[string]string{"workload": "local"},
			},
		},
	}
	localSink := Assignment{
		AssignmentID: "exec-1/local-sink/activation-1",
		Routing: engine.TaskRouting{
			NodeType: "xflow.sas.sink",
			Requirements: []engine.CapabilityRequirement{{
				NodeType: "xflow.sas.sink",
			}},
			RunnerSelector: &types.RunnerSelector{
				Mode:        types.RunnerSelectorModeRequired,
				MatchLabels: map[string]string{"workload": "local"},
			},
		},
	}
	for _, assignment := range []Assignment{wrongWorkload, wrongCapability, localSink} {
		mustEnqueueAssignment(t, ctx, dir, assignment)
	}

	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		// These are intentionally a forged standalone-runner Poll payload.
		Capacity: 99,
		Labels: map[string]string{
			"workload": "sas-runner",
		},
		Capabilities: []protocol.Capability{
			{NodeType: "xflow.sas.sink"},
			{NodeType: "xflow.group", Features: []string{engine.FeatureGroupExecV1}},
			{NodeType: "xflow.map"},
			{NodeType: "xflow.script"},
		},
		Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	if !ok {
		t.Fatal("local runner did not claim its eligible sink assignment")
	}
	if claim.Assignment.AssignmentID != localSink.AssignmentID {
		t.Fatalf("forged poll claimed %q, want only local sink %q", claim.Assignment.AssignmentID, localSink.AssignmentID)
	}

	snapshot, ok := dir.Runner(ctx, session.RunnerID)
	if !ok {
		t.Fatal("runner not found after poll")
	}
	if got := snapshot.Labels["workload"]; got != "local" {
		t.Errorf("registered workload after forged poll = %q, want local", got)
	}
	if !reflect.DeepEqual(snapshot.Capabilities, registeredCapabilities) {
		t.Errorf("registered capabilities after forged poll = %#v, want %#v", snapshot.Capabilities, registeredCapabilities)
	}
}
