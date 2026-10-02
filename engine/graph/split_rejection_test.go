package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestCompile_RejectsSplitNode pins the compile-time tombstone for xflow.split.
//
// The node type was removed on 2026-10-02; its handler never had a working
// execution path. The rejection predates the removal, and it stays after it,
// for two reasons.
//
// Historically: before the rejection existed, a submitted split workflow
// produced no error at all under the marker-sniffing criterion of the time (the
// engine looked for a "_split" marker key) -- every batch failed for want of a
// body, retried with backoff, and the execution hung until its deadline. Under
// today's body criterion the failure would instead be silent and wrong: the
// descriptor committed as the node's ordinary output.
//
// Now: Compile is the only check every submission path shares. The SDK rejects
// a workflow naming a type it has no handler for before it compiles, but the
// HTTP register and execute paths never run that scan, so without this rule a
// stale definition naming the removed type would register cleanly and fail only
// at first dispatch.
//
// Either way, a stale definition gets an immediate, named error at the point it
// is defined.
func TestCompile_RejectsSplitNode(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "uses-split",
		Nodes: []types.NodeDef{
			{Name: "s", Type: "xflow.split", Parameters: map[string]any{"items": "$input.items"}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("Compile accepted an xflow.split node; without the tombstone a stale " +
			"definition registers cleanly (the HTTP register and execute paths never run " +
			"the SDK's handler scan) and only fails at first dispatch as an unregistered " +
			"type -- the whole point of the rejection is that the failure must happen here " +
			"instead")
	}
	// The message has to name the node and say what to use instead: an author
	// hitting this needs to know which node broke and what replaces it, not just
	// that something is unsupported.
	if !strings.Contains(err.Error(), `"s"`) || !strings.Contains(err.Error(), "xflow.map") {
		t.Errorf("rejection message %q does not both name the offending node and point at "+
			"xflow.map as the replacement", err)
	}
}

// The tombstone reaches body members too: the body projection compiles its
// members through the same registerNodes pass, which is why
// bannedBodyMemberTypes no longer carries an xflow.split entry of its own.
func TestCompile_RejectsSplitNodeInsideABody(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "uses-split-in-body",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.rows",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{map[string]any{"name": "inner", "type": "xflow.split"}},
					},
				},
			}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("Compile accepted an xflow.split node as a body member")
	}
	if !strings.Contains(err.Error(), `"inner"`) {
		t.Errorf("rejection message %q does not name the offending member", err)
	}
}

// A workflow with no split node must still compile: the rejection above is
// type-specific, not a blanket tightening of the fan-out path. Without this,
// a rejection that accidentally matched every node would pass the test above.
func TestCompile_StillAcceptsMapAfterSplitRejection(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "uses-map",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{map[string]any{"name": "member", "type": "test.noop"}},
					},
				},
			}},
		},
	}
	if _, err := Compile(def); err != nil {
		t.Fatalf("Compile rejected a plain xflow.map workflow: %v", err)
	}
}
