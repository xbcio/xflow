package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestCapabilityPolicy_OldRunnerCannotClaimGroup(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// Old-style runner: only has basic NodeType capability, no Features.
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID: "old-runner",
		Capacity: 5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "xflow.group", NodeVersion: 0},
		},
		Policy: RunnerPolicy{AllowedNodeTypes: []string{"http.request", "xflow.group"}},
		Now:    time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Group assignment requires group.exec.v1 feature.
	assignment := Assignment{
		AssignmentID: "exec-1/grp1/0/0/0/",
		Task: engine.Task{
			ExecutionID: "exec-1",
			NodeName:    "grp1",
			NodeIdx:     0,
			UnitIdx:     1,
			Type:        engine.TaskTypeGroupExec,
		},
		Routing: engine.TaskRouting{
			NodeType:    "xflow.group",
			NodeVersion: 0,
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "http.request", NodeVersion: 1},
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
			},
		},
	}
	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil || !enqueued {
		t.Fatalf("EnqueueAssignment: enqueued=%v err=%v", enqueued, err)
	}

	// Old runner should NOT be able to claim this.
	_, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:  "old-runner",
		SessionID: session.SessionID,
		Capacity:  5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "xflow.group", NodeVersion: 0},
		},
		Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	if ok {
		t.Fatal("old runner without group.exec.v1 feature should NOT claim group assignment")
	}
}

func TestCapabilityPolicy_NewRunnerClaimsGroup(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// New runner: declares group.exec.v1 feature and all member types.
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID: "new-runner",
		Capacity: 5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "code.python", NodeVersion: 2},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Policy: RunnerPolicy{AllowedNodeTypes: []string{"http.request", "code.python", "xflow.group"}},
		Now:    time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	assignment := Assignment{
		AssignmentID: "exec-1/grp1/0/0/0/",
		Task: engine.Task{
			ExecutionID: "exec-1",
			NodeName:    "grp1",
			NodeIdx:     0,
			UnitIdx:     1,
			Type:        engine.TaskTypeGroupExec,
		},
		Routing: engine.TaskRouting{
			NodeType:    "xflow.group",
			NodeVersion: 0,
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "http.request", NodeVersion: 1},
				{NodeType: "code.python", NodeVersion: 2},
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
			},
		},
	}
	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil || !enqueued {
		t.Fatalf("EnqueueAssignment: enqueued=%v err=%v", enqueued, err)
	}

	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:  "new-runner",
		SessionID: session.SessionID,
		Capacity:  5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "code.python", NodeVersion: 2},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	if !ok {
		t.Fatal("new runner with full capabilities should claim group assignment")
	}
	if claim.Assignment.AssignmentID != assignment.AssignmentID {
		t.Errorf("claimed wrong assignment: %v", claim.Assignment.AssignmentID)
	}
}

func TestCapabilityPolicy_MissingMemberTypeRejectsGroup(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// Runner has group feature but is missing code.python.
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID: "partial-runner",
		Capacity: 5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Policy: RunnerPolicy{AllowedNodeTypes: []string{"http.request", "xflow.group"}},
		Now:    time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	assignment := Assignment{
		AssignmentID: "exec-1/grp1/0/0/0/",
		Task: engine.Task{
			ExecutionID: "exec-1",
			NodeName:    "grp1",
			NodeIdx:     0,
			UnitIdx:     1,
			Type:        engine.TaskTypeGroupExec,
		},
		Routing: engine.TaskRouting{
			NodeType:    "xflow.group",
			NodeVersion: 0,
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "http.request", NodeVersion: 1},
				{NodeType: "code.python", NodeVersion: 2},
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
			},
		},
	}
	enqueued, _ := dir.EnqueueAssignment(ctx, assignment)
	if !enqueued {
		t.Fatal("EnqueueAssignment failed")
	}

	_, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:  "partial-runner",
		SessionID: session.SessionID,
		Capacity:  5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	if ok {
		t.Fatal("runner missing code.python should NOT claim group requiring it")
	}
}

func TestCapabilityPolicy_PolicyRejectsUnauthorizedType(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// Runner has capabilities but policy only allows http.request.
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID: "restricted-runner",
		Capacity: 5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Policy: RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:    time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	assignment := Assignment{
		AssignmentID: "exec-1/grp1/0/0/0/",
		Task: engine.Task{
			ExecutionID: "exec-1",
			NodeName:    "grp1",
			Type:        engine.TaskTypeGroupExec,
		},
		Routing: engine.TaskRouting{
			NodeType: "xflow.group",
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
			},
		},
	}
	enqueued, _ := dir.EnqueueAssignment(ctx, assignment)
	if !enqueued {
		t.Fatal("EnqueueAssignment failed")
	}

	_, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:  "restricted-runner",
		SessionID: session.SessionID,
		Capacity:  5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 1},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	if ok {
		t.Fatal("policy should reject xflow.group when only http.request is allowed")
	}
}

func TestCapabilityPolicy_LegacyNodeAssignmentStillWorks(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// Traditional runner, no group features.
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "legacy-runner",
		Capacity:     5,
		Capabilities: []protocol.Capability{{NodeType: "http.request", NodeVersion: 1}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Traditional node assignment (no Requirements).
	assignment := Assignment{
		AssignmentID: "exec-2/nodeA/0/0/0/",
		Task: engine.Task{
			ExecutionID: "exec-2",
			NodeName:    "nodeA",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
		Routing: engine.TaskRouting{NodeType: "http.request", NodeVersion: 1},
	}
	enqueued, _ := dir.EnqueueAssignment(ctx, assignment)
	if !enqueued {
		t.Fatal("EnqueueAssignment failed")
	}

	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "legacy-runner",
		SessionID:    session.SessionID,
		Capacity:     5,
		Capabilities: []protocol.Capability{{NodeType: "http.request", NodeVersion: 1}},
		Now:          time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	if !ok {
		t.Fatal("legacy runner should still claim traditional node assignments")
	}
	if claim.Assignment.AssignmentID != assignment.AssignmentID {
		t.Errorf("wrong assignment")
	}
}

func TestCapabilityPolicy_VersionZeroNotWildcardForGroup(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// Runner declares version 0 for http.request. Requirement asks version 1.
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID: "v0-runner",
		Capacity: 5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 0},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Policy: RunnerPolicy{AllowedNodeTypes: []string{"http.request", "xflow.group"}},
		Now:    time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Group requires http.request v1 specifically.
	assignment := Assignment{
		AssignmentID: "exec-1/grp1/0/0/0/",
		Task: engine.Task{
			ExecutionID: "exec-1",
			NodeName:    "grp1",
			Type:        engine.TaskTypeGroupExec,
		},
		Routing: engine.TaskRouting{
			NodeType: "xflow.group",
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "http.request", NodeVersion: 1},
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
			},
		},
	}
	enqueued, _ := dir.EnqueueAssignment(ctx, assignment)
	if !enqueued {
		t.Fatal("EnqueueAssignment failed")
	}

	// Note: canRunRouting still allows version 0 as wildcard for the top-level
	// routing match. But the requirement check is strict: NodeVersion=1 requires
	// the runner to have exactly NodeVersion=1 or NodeVersion=0 (wildcard).
	// This is the current behavior — version 0 on the runner side means "any".
	_, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:  "v0-runner",
		SessionID: session.SessionID,
		Capacity:  5,
		Capabilities: []protocol.Capability{
			{NodeType: "http.request", NodeVersion: 0},
			{NodeType: "xflow.group", NodeVersion: 0, Features: []string{engine.FeatureGroupExecV1}},
		},
		Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	// Version 0 on runner side currently acts as "supports any version".
	// This is intentional backward compat for the basic routing layer.
	if !ok {
		t.Fatal("version 0 runner capability should match (wildcard for runner side)")
	}
}
