package flow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestStartNodeOutputPortDefaultsToMain pins Output.Port at the empty string.
//
// types.Output.Port defaults to "main" when empty, and engine/commit.go and
// engine/atomic_commit.go both read this field to pick the node's outgoing
// edge set instead of its declared default. xflow.start declares exactly one
// output port, "main", so this handler must leave Port unset — setting it to
// anything else (in particular "error", which engine/commit.go:447 also reads
// to decide whether an output represents a business error) would route a
// plain submission down an edge set the descriptor never promised, even
// though no *types.Error was ever produced.
func TestStartNodeOutputPortDefaultsToMain(t *testing.T) {
	n := Start()
	out, err := n.Execute(context.Background(), &types.Input{Data: map[string]any{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	// "" and "main" are the same routing decision (the engine treats an empty
	// Port as "main"), so both are accepted: pinning the empty string alone
	// would fail on an equivalent rewrite that spells the port out. What must
	// not pass is any *third* value.
	if out.Port != "" && out.Port != "main" {
		t.Fatalf("Output.Port = %q, want \"\" or \"main\": xflow.start has only a \"main\" "+
			"output and must not route through any other port", out.Port)
	}
}

// TestStartNodeReturnsEmptyDataForNilInput mirrors EndNode's own nil-input
// test (end_test.go's TestEndNodeReturnsEmptyDataForNilInput). Nothing
// exercised this branch on StartNode's side: no test in this package ever
// calls Execute with a nil *types.Input, so the literal this branch returns
// could carry a stray key, or the branch could vanish and panic on
// input.Data, without any test in this package noticing.
func TestStartNodeReturnsEmptyDataForNilInput(t *testing.T) {
	n := Start()

	out, err := n.Execute(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || len(out.Data) != 0 {
		t.Fatalf("output = %+v, want empty data", out)
	}
}
