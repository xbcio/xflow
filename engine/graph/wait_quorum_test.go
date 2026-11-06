package graph_test

import (
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	_ "github.com/xbcio/xflow/node" // register the builtin descriptors
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// A negative quorum asks for fewer than no signals at all, so the wait
// descriptor bounds it at 0; 0 keeps its documented "all signals" meaning.
func TestWaitQuorumBounds(t *testing.T) {
	var desc *types.Descriptor
	for _, rd := range registry.Descriptors() {
		if rd.Type == "xflow.wait" {
			d := rd.Descriptor
			desc = &d
			break
		}
	}
	if desc == nil {
		t.Fatal("xflow.wait is not registered")
	}
	params := func(quorum any) map[string]any {
		return map[string]any{"mode": "signal", "signals": []any{"a", "b"}, "quorum": quorum}
	}
	if issues := graph.ValidateParams(*desc, params(-1)); !graph.HasParamErrors(issues) {
		t.Fatalf("negative quorum passed validation: %v", issues)
	}
	if issues := graph.ValidateParams(*desc, params(0)); graph.HasParamErrors(issues) {
		t.Fatalf("quorum 0 must stay valid (all signals): %v", issues)
	}
}
