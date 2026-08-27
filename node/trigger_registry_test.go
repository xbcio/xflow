package node_test

import (
	"testing"

	_ "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
)

// TestBuiltInTriggersAreRegisteredViaNodeImport asserts that importing node
// alone is enough to make every built-in trigger resolvable through the
// registry.
//
// The chain under test:
//
//	node/node.go            → blank import of node/trigger
//	node/trigger/trigger.go → factory functions returning the five subpackages'
//	                          *Node types (real uses, not blank imports)
//	each subpackage init()   → registry.RegisterTrigger
//
// The failure this exists to catch: delete the blank import in node/node.go and
// everything still builds, `go vet` is clean, and every test under
// node/trigger/... still passes (they construct nodes directly and never touch
// the registry). Only a runner notices — LookupTrigger returns not-found and
// activation fails for every trigger type.
//
// This test MUST live in the node package's test binary. Putting it under
// node/trigger would import node/trigger directly and bypass the exact hop it
// is meant to guard.
//
// The node type strings are frozen contract: they are persisted in workflow
// definitions, sent over the server/runner protocol, and declared in runner
// capabilities. They are written out literally here rather than derived from the
// factories so that a rename shows up as a failure instead of silently
// following along.
func TestBuiltInTriggersAreRegisteredViaNodeImport(t *testing.T) {
	for _, nodeType := range []string{
		"xflow.trigger.timer",
		"xflow.trigger.cron",
		"xflow.trigger.webhook",
		"xflow.trigger.kafka",
		"xflow.trigger.redis",
	} {
		h, ok := registry.LookupTrigger(nodeType)
		if !ok {
			t.Errorf("registry.LookupTrigger(%q) = not found; importing node must run "+
				"the trigger subpackages' init() — check the blank import of "+
				"node/trigger in node/node.go", nodeType)
			continue
		}
		if got := h.Descriptor().Type; got != nodeType {
			t.Errorf("LookupTrigger(%q) returned a handler whose Descriptor().Type = %q",
				nodeType, got)
		}
	}
}
