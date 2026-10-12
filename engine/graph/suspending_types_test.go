package graph_test

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	_ "github.com/xbcio/xflow/node" // trigger all init() registrations
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestSuspendingNodeTypesCoversRegisteredHandlers pins the compiler's static
// suspending-type list against the handler registry.
//
// engine/graph cannot import node/ (the compiler would pull every handler into
// it), so validateGroupMode knows suspend-capable types only by name. A new
// suspending handler added to node/ would silently fall outside that list and
// a transient group containing it would compile — then park a sub-execution
// the group lease cannot resume. This test is what makes the list's staleness
// a build failure instead of a production surprise.
//
// The blank import is what makes the assertion non-vacuous: without it the
// registry is empty and the loop below checks nothing.
func TestSuspendingNodeTypesCoversRegisteredHandlers(t *testing.T) {
	known := graph.SuspendingNodeTypes()

	seen := map[string]bool{}
	var candidates []string
	for _, typ := range registry.Types() {
		seen[typ] = true
	}
	for _, typ := range registry.TriggerTypes() {
		seen[typ] = true
	}
	for typ := range seen {
		candidates = append(candidates, typ)
	}
	if len(candidates) == 0 {
		t.Fatal("registry is empty -- the blank import of package node is not triggering init()")
	}

	var missing []string
	for _, typ := range candidates {
		if h, ok := registry.Lookup(typ); ok {
			if _, suspends := h.(types.SuspendingHandler); suspends && !known[typ] {
				missing = append(missing, typ)
			}
		}
		if h, ok := registry.LookupTrigger(typ); ok {
			if _, suspends := h.(types.SuspendingHandler); suspends && !known[typ] {
				missing = append(missing, typ)
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("graph.suspendingNodeTypes is missing %d suspend-capable registered type(s):\n  %s\n\n"+
			"Add them to engine/graph/group_mode.go's suspendingNodeTypes, or a transient group "+
			"containing one will compile even though its member cannot suspend inside a group.",
			len(missing), strings.Join(missing, "\n  "))
	}

	// Reverse probe: every declared entry must still name a real suspending
	// handler, or the list has grown a stale entry that rejects a type nobody
	// can write.
	for typ := range known {
		h, ok := registry.Lookup(typ)
		if !ok {
			t.Errorf("suspendingNodeTypes names %q, which is not a registered node type", typ)
			continue
		}
		if _, suspends := h.(types.SuspendingHandler); !suspends {
			t.Errorf("suspendingNodeTypes names %q, whose handler does not implement types.SuspendingHandler", typ)
		}
	}
}
