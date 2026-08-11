package graph_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	_ "github.com/xbcio/xflow/node" // trigger all init() registrations
	"github.com/xbcio/xflow/node/registry"
)

// TestEvaluableParamsCoversAllRegisteredTypes verifies that every node type
// known to the global registry has an entry in evaluableParams. A missing
// entry means a template in that type's parameters silently degrades to a
// warning instead of being rejected -- the exact gap this test exists to
// prevent from reopening.
//
// The test imports package node (blank import above) to trigger every init()
// that registers handlers. Without it the registry is empty and this test is
// vacuously green -- a classic false-test shape. The reverse probe confirms
// this: removing the import must make the test fail.
func TestEvaluableParamsCoversAllRegisteredTypes(t *testing.T) {
	ep := graph.EvaluableParams()

	// Collect all registered types: action types from Types() and trigger-only
	// types from TriggerTypes(). Some types appear in both (dual-registered),
	// so deduplicate.
	seen := map[string]bool{}
	for _, typ := range registry.Types() {
		seen[typ] = true
	}
	for _, typ := range registry.TriggerTypes() {
		seen[typ] = true
	}

	if len(seen) == 0 {
		t.Fatal("registry is empty -- the blank import of package node is not triggering init(); " +
			"this test is vacuously green without registered types")
	}

	var missing []string
	for typ := range seen {
		if _, ok := ep[typ]; !ok {
			missing = append(missing, typ)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("evaluableParams is missing entries for %d registered type(s):\n  %s\n\n"+
			"Each registered node type must have an entry in evaluableParams (engine/graph/template_reject.go). "+
			"Types with no evaluable parameters should map to an empty map {}.",
			len(missing), strings.Join(missing, "\n  "))
	}
}
