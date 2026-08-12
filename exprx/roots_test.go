package exprx

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestExecutionRootAvailable is the regression test for the missing
// $execution root. DSL-SPECIFICATION.md §4.2 lists $execution.id among the
// available variables, but BuildExprEnv never populated it, so any expression
// using it failed at the parameter boundary with "unknown name $execution".
func TestExecutionRootAvailable(t *testing.T) {
	input := &types.Input{ExecutionID: "exec-abc"}
	env := BuildExprEnv(input, nil)
	got, err := EvalExpr("$execution.id", env, false)
	if err != nil {
		t.Fatalf("$execution.id is advertised by DSL-SPECIFICATION.md §4.2: %v", err)
	}
	if got != "exec-abc" {
		t.Fatalf("$execution.id = %#v, want %q", got, "exec-abc")
	}
}

// TestWorkflowRootAvailable is the same claim for $workflow. The spec's
// variable table gives name and version as its two examples.
func TestWorkflowRootAvailable(t *testing.T) {
	input := &types.Input{WorkflowName: "order-flow", WorkflowVersion: "v2"}
	env := BuildExprEnv(input, nil)
	name, err := EvalExpr("$workflow.name", env, false)
	if err != nil {
		t.Fatalf("$workflow.name: %v", err)
	}
	if name != "order-flow" {
		t.Fatalf("$workflow.name = %#v, want %q", name, "order-flow")
	}
	version, err := EvalExpr("$workflow.version", env, false)
	if err != nil {
		t.Fatalf("$workflow.version: %v", err)
	}
	if version != "v2" {
		t.Fatalf("$workflow.version = %#v, want %q", version, "v2")
	}
}

// TestExecutionAndWorkflowRootsAlwaysPresent pins that the roots exist even
// when the underlying fields are empty.
//
// An absent root is a compile error ("unknown name $execution") that fails the
// whole node; a present root with an empty string lets `$execution.id ?? 'x'`
// and comparisons behave predictably. Since these roots are unconditional --
// unlike $nodes, which is nil when the node declares no references -- an
// expression that compiles in one execution must compile in all of them.
func TestExecutionAndWorkflowRootsAlwaysPresent(t *testing.T) {
	env := BuildExprEnv(&types.Input{}, nil)
	for _, code := range []string{"$execution.id", "$workflow.name", "$workflow.version"} {
		got, err := EvalExpr(code, env, false)
		if err != nil {
			t.Fatalf("%s must resolve even on an empty Input, or an expression that "+
				"compiles in one execution would fail in another: %v", code, err)
		}
		if got != "" {
			t.Fatalf("%s = %#v, want the empty string", code, got)
		}
	}
}

// TestExecutionRootOverridableByExtra documents that the roots follow the same
// precedence rule as every other root: BuildExprEnv's extra map wins. The map
// adapter relies on this to re-scope roots for a body's per-item environment.
func TestExecutionRootOverridableByExtra(t *testing.T) {
	input := &types.Input{ExecutionID: "outer"}
	env := BuildExprEnv(input, map[string]any{
		"$execution": map[string]any{"id": "inner"},
	})
	got, err := EvalExpr("$execution.id", env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != "inner" {
		t.Fatalf("$execution.id = %#v, want %q -- extra must override", got, "inner")
	}
}

// TestWorkflowRootHasNoIDField is a negative test with a reason.
//
// The spec's database example used $workflow.id, but types.WorkflowDef.ID is
// an instance identifier that no production code ever writes:
// sdk/xflow/workflow_identity.go excludes it from runtimeHash precisely
// because it is "a runtime instance pointer, not part of the workflow
// definition". Building $workflow.id would put a field that is always the
// empty string into the DSL. The spec entry is removed instead; this test
// stops it from being added back without that decision being revisited.
func TestWorkflowRootHasNoIDField(t *testing.T) {
	env := BuildExprEnv(&types.Input{WorkflowName: "n"}, nil)
	wf, ok := env["$workflow"].(map[string]any)
	if !ok {
		t.Fatalf("$workflow is %T, want map[string]any", env["$workflow"])
	}
	if _, exists := wf["id"]; exists {
		t.Fatal("$workflow.id was added; WorkflowDef.ID has no production writer " +
			"and is excluded from workflow identity by design (see " +
			"sdk/xflow/workflow_identity.go). Revisit that decision before adding it")
	}
}
