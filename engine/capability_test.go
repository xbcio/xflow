package engine

import (
	"testing"

	"github.com/xbcio/xflow/engine/graph"
)

func TestNormalizeRequirements_Dedup(t *testing.T) {
	reqs := []CapabilityRequirement{
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "db.query", NodeVersion: 2},
	}
	normalized := NormalizeRequirements(reqs)
	if len(normalized) != 2 {
		t.Fatalf("len = %d, want 2", len(normalized))
	}
}

func TestNormalizeRequirements_DeterministicOrder(t *testing.T) {
	reqs1 := []CapabilityRequirement{
		{NodeType: "db.query", NodeVersion: 2},
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "code.python", NodeVersion: 3, Runtime: "python3.11"},
	}
	reqs2 := []CapabilityRequirement{
		{NodeType: "code.python", NodeVersion: 3, Runtime: "python3.11"},
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "db.query", NodeVersion: 2},
	}

	n1 := NormalizeRequirements(reqs1)
	n2 := NormalizeRequirements(reqs2)

	if len(n1) != len(n2) {
		t.Fatalf("lengths differ: %d vs %d", len(n1), len(n2))
	}
	for i := range n1 {
		if n1[i].NodeType != n2[i].NodeType || n1[i].NodeVersion != n2[i].NodeVersion || n1[i].Runtime != n2[i].Runtime {
			t.Errorf("index %d differs: %+v vs %+v", i, n1[i], n2[i])
		}
	}

	// Verify sorted order.
	for i := 1; i < len(n1); i++ {
		if n1[i].NodeType < n1[i-1].NodeType {
			t.Errorf("not sorted: %q comes after %q", n1[i].NodeType, n1[i-1].NodeType)
		}
	}
}

func TestNormalizeRequirements_MergesCredentials(t *testing.T) {
	reqs := []CapabilityRequirement{
		{NodeType: "http.request", NodeVersion: 1, Credentials: []string{"api_key"}},
		{NodeType: "http.request", NodeVersion: 1, Credentials: []string{"oauth_token", "api_key"}},
	}
	normalized := NormalizeRequirements(reqs)
	if len(normalized) != 1 {
		t.Fatalf("len = %d, want 1 (deduped)", len(normalized))
	}
	if len(normalized[0].Credentials) != 2 {
		t.Fatalf("credentials = %v, want 2 items", normalized[0].Credentials)
	}
	// Should be sorted.
	if normalized[0].Credentials[0] != "api_key" || normalized[0].Credentials[1] != "oauth_token" {
		t.Errorf("credentials = %v, want [api_key, oauth_token]", normalized[0].Credentials)
	}
}

func TestNormalizeRequirements_Nil(t *testing.T) {
	result := NormalizeRequirements(nil)
	if result != nil {
		t.Errorf("nil input should produce nil output, got %v", result)
	}
}

func TestNormalizeRequirements_Empty(t *testing.T) {
	result := NormalizeRequirements([]CapabilityRequirement{})
	if result != nil {
		t.Errorf("empty input should produce nil output, got %v", result)
	}
}

func TestRequirementsFromGraphPackage(t *testing.T) {
	graphReqs := []graph.Requirement{
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "code.python", NodeVersion: 2, Runtime: "python3.11"},
		{NodeType: "db.query", NodeVersion: 1, Resource: "resource_pool.sql", Credentials: []string{"db_cred"}},
	}

	reqs := RequirementsFromGraphPackage(graphReqs)

	// Should have 3 member reqs + 1 feature req = 4 total.
	if len(reqs) != 4 {
		t.Fatalf("len = %d, want 4", len(reqs))
	}

	// Must contain the group feature requirement.
	hasFeature := false
	for _, r := range reqs {
		if r.Feature == FeatureGroupExecV1 {
			hasFeature = true
		}
	}
	if !hasFeature {
		t.Error("missing FeatureGroupExecV1 requirement")
	}

	// Check resource and credentials are preserved.
	hasResource := false
	for _, r := range reqs {
		if r.Resource == "resource_pool.sql" && len(r.Credentials) == 1 && r.Credentials[0] == "db_cred" {
			hasResource = true
		}
	}
	if !hasResource {
		t.Error("resource/credentials not preserved from graph requirements")
	}
}

func TestGroupRouteIdentity_Fields(t *testing.T) {
	id := GroupRouteIdentity{
		GroupID:      "exec-123/grp1",
		UnitIdx:      3,
		PackageHash:  "pkg-sha256:v1:abc123",
		ActivationID: 0,
	}
	if id.GroupID == "" || id.PackageHash == "" {
		t.Error("identity fields should not be empty")
	}
	if id.UnitIdx != 3 {
		t.Errorf("UnitIdx = %d, want 3", id.UnitIdx)
	}
}

func TestTaskRouting_NodeTask(t *testing.T) {
	routing := TaskRouting{
		NodeType:    "http.request",
		NodeVersion: 1,
		Requirements: []CapabilityRequirement{
			{NodeType: "http.request", NodeVersion: 1},
		},
	}
	if len(routing.Requirements) != 1 {
		t.Fatalf("requirements = %d, want 1", len(routing.Requirements))
	}
	if routing.Requirements[0].NodeType != "http.request" {
		t.Errorf("requirement type = %q, want http.request", routing.Requirements[0].NodeType)
	}
}

func TestTaskRouting_GroupTask(t *testing.T) {
	routing := TaskRouting{
		NodeType: "xflow.group",
		Requirements: []CapabilityRequirement{
			{NodeType: "code.python", NodeVersion: 2, Runtime: "python3.11"},
			{NodeType: "http.request", NodeVersion: 1},
			{Feature: FeatureGroupExecV1},
		},
	}
	if routing.NodeType != "xflow.group" {
		t.Errorf("group routing NodeType = %q, want xflow.group", routing.NodeType)
	}
	if len(routing.Requirements) != 3 {
		t.Fatalf("requirements = %d, want 3", len(routing.Requirements))
	}
}

func TestNormalizeRequirements_VersionZeroNotCollapsed(t *testing.T) {
	// Version 0 and version 1 are NOT the same — version 0 must not be treated
	// as a wildcard in normalized requirements.
	reqs := []CapabilityRequirement{
		{NodeType: "http.request", NodeVersion: 0},
		{NodeType: "http.request", NodeVersion: 1},
	}
	normalized := NormalizeRequirements(reqs)
	if len(normalized) != 2 {
		t.Fatalf("len = %d, want 2 (version 0 != version 1)", len(normalized))
	}
}
