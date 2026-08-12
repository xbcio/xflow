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
	// {{ $credentials.my_api_key }} in xflow.http headers now compiles
	// successfully: the boundary evaluation layer (execution/params.go) evaluates
	// every non-exempt parameter at runtime, so the template is not shipped
	// verbatim. However, exprx.BuildExprEnv does NOT inject a $credentials root
	// (only xflow.script injects it via extra env); at runtime this form fails
	// with "unknown name $credentials" -- a loud failure, not a silent one.
	//
	// The correct channel for credentials remains the "authentication" parameter's
	// declarative reference (input.Credential in action/http.go:212). This test
	// only asserts compile-time acceptance; it does not test runtime behavior.
	def := &types.WorkflowDef{
		Name: "cred-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.http", Version: 1, Parameters: map[string]any{
				"headers": map[string]any{
					"Authorization": "Bearer {{ $credentials.my_api_key }}",
				},
			}},
			{Name: "B", Type: "xflow.http", Version: 1},
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
		t.Fatalf("a template in xflow.http headers must compile -- the boundary "+
			"evaluation layer evaluates it at runtime: %v", err)
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

// TestPortability_GroupMemberMapBodyInternalRefOK guards the false rejection
// that validatePortability's whole-tree walk produced: a map node in a group
// whose body member references another BODY member was reported as referencing
// an "external node", because extractNodeRefs walked into parameters.body and
// attributed the inner graph's names to the outer map node.
//
// Measured before the fix:
//
//	group "grp": non-portable member "m": references external node "a" via $nodes
//
// while the byte-identical map compiles fine when it is not in a group -- the
// two paths disagreed about the same definition, which is the tell.
//
// The body is not left unchecked by skipping it here: ProjectNodeBodyPackage
// runs validatePortability again over the body's OWN compiled graph
// (node_body_package.go), so a body member referencing a name that is not a
// body member is still rejected -- see the sibling test below.
func TestPortability_GroupMemberMapBodyInternalRefOK(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "group-map-body",
		Options: &types.WorkflowOptions{ExperimentalNodeGroup: true},
		Groups:  []types.GroupDef{{Name: "grp", Members: []string{"m", "tail"}}},
		Nodes: []types.NodeDef{
			{Name: "src", Type: "test.echo"},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "a", "type": "test.echo"},
							map[string]any{"name": "bb", "type": "test.echo", "parameters": map[string]any{
								"x": `${{ $nodes['a'].v }}`,
							}},
						},
						"connections": map[string]any{
							"a": map[string]any{"main": map[string]any{"targets": []any{
								map[string]any{"node": "bb", "input": "main"}}}},
						},
					},
				},
			}},
			{Name: "tail", Type: "test.echo"},
		},
		Connections: types.Connections{
			"src": {"main": {Targets: []types.Connection{{Node: "m", Input: "main"}}}},
			"m":   {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
		},
	}
	if _, err := Compile(def); err != nil {
		t.Fatalf("a body member referencing another body member must compile: %v", err)
	}
}

// TestPortability_GroupMemberMapBodyExternalRefStillRejected is the other half
// of the pair: skipping the body in the GROUP walk must not make a body that
// reaches outside its own scope compile silently. The body's own portability
// pass catches it, so the rejection survives -- only its wording changes (it is
// now stamped by the body path, naming the body member, rather than by the
// group path naming the map node).
func TestPortability_GroupMemberMapBodyExternalRefStillRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "group-map-body-ext",
		Options: &types.WorkflowOptions{ExperimentalNodeGroup: true},
		Groups:  []types.GroupDef{{Name: "grp", Members: []string{"m", "tail"}}},
		Nodes: []types.NodeDef{
			{Name: "src", Type: "test.echo"},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "a", "type": "test.echo", "parameters": map[string]any{
								"x": `${{ $nodes['src'].v }}`,
							}},
						},
					},
				},
			}},
			{Name: "tail", Type: "test.echo"},
		},
		Connections: types.Connections{
			"src": {"main": {Targets: []types.Connection{{Node: "m", Input: "main"}}}},
			"m":   {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("a body member referencing a node outside the body must still be rejected")
	}
	if !strings.Contains(err.Error(), "non-portable") || !strings.Contains(err.Error(), "src") {
		t.Errorf("error = %v, want 'non-portable' naming 'src'", err)
	}
}
