package flow_test

import (
	"testing"

	"github.com/xbcio/xflow/node"
)

// TestMap_ContinueOnErrorSetter mirrors TestMap_BodyConcurrencySetter for the
// sibling builder method. node/internal/flow has no other test that calls
// MapNode.ContinueOnError() itself — map_expression_test.go only sets
// "continue_on_error" by hand in a params map to exercise evalItemsInline's
// semantics, never through this setter — so a setter that stopped writing
// n.KeepGoing (or wrote the wrong bool) would still let the DSL's only way to
// opt in to ContinueOnError silently do nothing, with no test in this package
// telling the two apart.
func TestMap_ContinueOnErrorSetter(t *testing.T) {
	params := node.Map("items", 10).ContinueOnError().RawParams().(map[string]any)
	if params["continue_on_error"] != true {
		t.Fatalf("continue_on_error = %v, want true", params["continue_on_error"])
	}
}

// Off by default, matching the descriptor's Default: false. A setter that
// flipped the field's zero value would make every ordinary node.Map(...) call
// (without .ContinueOnError()) tolerate partial failures it was never asked to.
func TestMap_ContinueOnErrorDefaultsToFalse(t *testing.T) {
	params := node.Map("items", 10).RawParams().(map[string]any)
	if params["continue_on_error"] != false {
		t.Fatalf("continue_on_error = %v, want false", params["continue_on_error"])
	}
}
