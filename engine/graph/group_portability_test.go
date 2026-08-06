package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestPortability_ExternalNodeReference(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "ext-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"url": `${{ $nodes['D'].json.result }}`,
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for external node reference")
	}
	if !strings.Contains(err.Error(), "non-portable") {
		t.Errorf("error = %v, want 'non-portable'", err)
	}
	if !strings.Contains(err.Error(), "D") {
		t.Errorf("error = %v, want mention of referenced node 'D'", err)
	}
}

func TestPortability_IntraGroupReferenceOK(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "intra-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"url": `${{ $nodes['B'].json.result }}`,
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile should succeed for intra-group reference: %v", err)
	}
}

func TestPortability_LocalNodeRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "local-node",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.local", Version: 1},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for xflow.local member")
	}
	if !strings.Contains(err.Error(), "non-portable") {
		t.Errorf("error = %v, want 'non-portable'", err)
	}
}

func TestPortability_ClosureNodeRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "closure-node",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.closure", Version: 1},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for xflow.closure member")
	}
	if !strings.Contains(err.Error(), "non-portable") {
		t.Errorf("error = %v, want 'non-portable'", err)
	}
}

func TestPortability_AllowCyclesWithGroupsRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "cyclic-groups",
		Options: &types.WorkflowOptions{
			AllowCycles: true,
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start", Version: 1},
			{Name: "A", Type: "http.request", Version: 1},
			{Name: "B", Type: "http.request", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "A", Input: "main"}}}},
			"A":     {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for AllowCycles + groups")
	}
	if !strings.Contains(err.Error(), "cyclic") {
		t.Errorf("error = %v, want mention of 'cyclic'", err)
	}
}

func TestPortability_ReservedGroupNameRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "reserved-name",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "xflow.group_evil", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for reserved group name")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("error = %v, want 'reserved'", err)
	}
}

func TestPortability_DoubleUnderscoreGroupNameRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "reserved-name2",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "__internal", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for __ prefix group name")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("error = %v, want 'reserved'", err)
	}
}

func TestPortability_SecretLiteralRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "secret-literal",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"headers": map[string]any{
					"Authorization": "Bearer sk-live-abc123def456ghi789jkl012mno",
				},
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for secret literal in parameters")
	}
	if !strings.Contains(err.Error(), "secret") {
		t.Errorf("error = %v, want mention of 'secret'", err)
	}
}

func TestPortability_SecretLiteralFalsePositive(t *testing.T) {
	// "skill_level_abc..." should NOT be flagged (only sk_live_ pattern).
	def := &types.WorkflowDef{
		Name: "false-positive",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"data": "skill_level_advanced",
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile should succeed (false positive guard): %v", err)
	}
}

func TestPortability_AWSKeyRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "aws-key",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"aws_key": "AKIAIOSFODNN7EXAMPLE",
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for AWS key in parameters")
	}
	if !strings.Contains(err.Error(), "secret") {
		t.Errorf("error = %v, want mention of 'secret'", err)
	}
}

func TestPortability_CredentialReferenceOK(t *testing.T) {
	// Using {{ $credentials.my_api_key }} is fine — it's a reference, not a literal.
	def := &types.WorkflowDef{
		Name: "cred-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"headers": map[string]any{
					"Authorization": "Bearer {{ $credentials.my_api_key }}",
				},
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile should succeed for credential reference: %v", err)
	}
}

func TestPortability_NestedParameterExternalRef(t *testing.T) {
	// Reference in nested array/map structure.
	def := &types.WorkflowDef{
		Name: "nested-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{
				"body": map[string]any{
					"items": []any{
						map[string]any{"value": `${{ $nodes["external"].json.id }}`},
					},
				},
			}},
			{Name: "B", Type: "http.request", Version: 1},
			{Name: "external", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "external", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to fail for nested external reference")
	}
	if !strings.Contains(err.Error(), "non-portable") {
		t.Errorf("error = %v, want 'non-portable'", err)
	}
	if !strings.Contains(err.Error(), "external") {
		t.Errorf("error = %v, want mention of 'external'", err)
	}
}
