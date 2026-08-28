package script

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestDeclaredNodeFailsClosedWhenSupplyNeverArrives is §9.3 probe (3). Moving
// registration from activation time to execution time removed the fail-closed
// guarantee that lived in service/runner/trigger_activation_handler.go:291,
// whose comment states the stake: "a module that cannot be compiled or
// registered would otherwise host traffic with no rules and no diagnostic".
//
// The assertion is not merely "an error came back" -- it is that Execute
// returned NO OUTPUT. A node that errors but still emits is exactly the leak:
// for SAS that means the credential a clean rule exists to strip reaches the
// sink anyway.
func TestDeclaredNodeFailsClosedWhenSupplyNeverArrives(t *testing.T) {
	const (
		workflow = "TestDeclaredNodeFailsClosed-collect"
		nodeName = "decode"
		supply   = "TestDeclaredNodeFailsClosed-rules"
	)
	supplyDeclarations.declare(workflow, nodeName, []string{supply})
	t.Cleanup(func() { supplyDeclarations.undeclare(workflow, nodeName, []string{supply}) })

	// A well-formed digest that names no artifact. Registration cannot reach a
	// configured pool, so the guard must refuse.
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	out, err := (&ScriptNode{}).Execute(context.Background(), &types.Input{
		WorkflowName: workflow,
		NodeName:     nodeName,
		Params: map[string]any{
			"language":        "wasm",
			"runtime":         "wazero",
			"artifact_digest": digest,
			"code":            "AGFzbQEAAAA=", // minimal valid wasm header, base64
		},
	})
	if err == nil {
		t.Fatal("execution succeeded with an unconfigured source-driven module; every " +
			"record would flow through untagged and uncleansed")
	}
	if out != nil {
		t.Fatalf("Execute returned output %+v alongside the error; the record escaped "+
			"the guard", out)
	}
	if !strings.Contains(err.Error(), supply) {
		t.Fatalf("error %q does not name the supply node; an operator cannot tell which "+
			"declaration is unsatisfied", err)
	}
	if strings.Contains(err.Error(), "AGFzbQEAAAA=") {
		t.Fatal("the error echoed the module code; errors must carry only the digest " +
			"and the supply node name")
	}
}

// TestUndeclaredNodeIsUnaffected: the guard must engage only where a declaration
// exists. An inline-code wasm node with no supply must keep running exactly as
// before, or this change breaks every non-supply workflow in the fleet.
func TestUndeclaredNodeIsUnaffected(t *testing.T) {
	_, err := (&ScriptNode{}).Execute(context.Background(), &types.Input{
		WorkflowName: "TestUndeclaredNodeIsUnaffected-wf",
		NodeName:     "plain",
		Params: map[string]any{
			"language": "wasm",
			"runtime":  "wazero",
			"code":     "AGFzbQEAAAA=",
		},
	})
	if err != nil && strings.Contains(err.Error(), "supply") {
		t.Fatalf("an undeclared node hit the supply guard: %v", err)
	}
}

// TestDeclaredNodeFailsClosedWhenDigestUnresolved covers the case the brief's
// original guard condition (`digest != ""`) would have let through: a live
// declaration exists (collectWasmBindings only ever emits one when
// artifact_digest was a non-empty expression at activation time), but at
// execution time the boundary-evaluated artifact_digest came back empty --
// meaning the expression failed to resolve, most commonly because it is rooted
// at $supplies and that root was not yet populated for this message.
//
// Checking `digest != ""` before the declaration lookup would skip the guard
// entirely in exactly this state and run the node's inline code against zero
// rules -- the same silent pass-through this whole task exists to close, just
// reached from the "digest resolved empty" direction instead of "digest never
// configured".
func TestDeclaredNodeFailsClosedWhenDigestUnresolved(t *testing.T) {
	const (
		workflow = "TestDeclaredNodeFailsClosedUnresolved-collect"
		nodeName = "decode"
		supply   = "TestDeclaredNodeFailsClosedUnresolved-rules"
	)
	supplyDeclarations.declare(workflow, nodeName, []string{supply})
	t.Cleanup(func() { supplyDeclarations.undeclare(workflow, nodeName, []string{supply}) })

	out, err := (&ScriptNode{}).Execute(context.Background(), &types.Input{
		WorkflowName: workflow,
		NodeName:     nodeName,
		Params: map[string]any{
			"language": "wasm",
			"runtime":  "wazero",
			// artifact_digest deliberately absent: the boundary expression
			// resolved to empty (or never ran).
			"code": "AGFzbQEAAAA=",
		},
	})
	if err == nil {
		t.Fatal("execution succeeded with a live supply declaration and an empty resolved " +
			"digest; the node ran its inline code against zero rules")
	}
	if out != nil {
		t.Fatalf("Execute returned output %+v alongside the error; the record escaped "+
			"the guard", out)
	}
	if !strings.Contains(err.Error(), supply) {
		t.Fatalf("error %q does not name the supply node; an operator cannot tell which "+
			"declaration is unsatisfied", err)
	}
	if strings.Contains(err.Error(), "AGFzbQEAAAA=") {
		t.Fatal("the error echoed the module code; errors must carry only the digest " +
			"and the supply node name")
	}
}
