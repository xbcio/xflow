package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestCompile_RejectsSplitNode pins the compile-time rejection of xflow.split.
//
// split was never implemented. Its handler (node/internal/flow/split.go) emits
// a batches descriptor, but xflow.split declares no body parameter and is not
// in transformNodeTypes, so projectNodeBodies never projects one for it.
//
// Measured end to end before this rejection was added, under the criterion of
// the time (the engine sniffed the output for a "_split" marker key): a
// submitted split workflow produced no error at all, it simply never completed
// -- every batch failed for want of a body, retried with backoff, and the
// execution hung until its deadline. The criterion has since moved to the
// compiled body, which changes the failure but does not remove it: a split node
// would now not expand at all, and its descriptor would be committed as the
// node's ordinary output -- a silent wrong answer in place of a hang.
//
// Rejecting at compile time turns either outcome into an immediate, named error
// at the point the workflow is defined.
func TestCompile_RejectsSplitNode(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "uses-split",
		Nodes: []types.NodeDef{
			{Name: "s", Type: "xflow.split", Parameters: map[string]any{"items": "$input.items"}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("Compile accepted an xflow.split node; at run time this workflow does not " +
			"fail, it hangs forever -- the whole point of the rejection is that the failure " +
			"must happen here instead")
	}
	// The message has to name the node and say what to use instead: an author
	// hitting this needs to know which node broke and what replaces it, not just
	// that something is unsupported.
	if !strings.Contains(err.Error(), `"s"`) || !strings.Contains(err.Error(), "xflow.map") {
		t.Errorf("rejection message %q does not both name the offending node and point at "+
			"xflow.map as the replacement", err)
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
