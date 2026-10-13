package graph

import (
	"fmt"

	"github.com/xbcio/xflow/types"
)

// validateSupplyPullNode rejects a malformed xflow.supply.http declaration at
// compile time.
//
// The rules live in types.ParseSupplyPullSource, which service/control calls
// again when it derives a unit's activation -- engine/graph cannot import node/,
// so types is the only spelling all three layers share, and running the same
// parse is what keeps the two from disagreeing about what a definition means.
//
// The rule is here, rather than only at activation, for the same reason the
// xflow.split tombstone is (see registerNodes): Compile is the only check every
// submission path shares. The SDK rejects a workflow naming a type it has no
// handler for before it compiles, but the HTTP register and execute paths never
// run that scan, so without this a definition whose source is http://, embeds
// credentials, or carries a `resource` key would register cleanly and fail only
// when a runner first tried to activate it -- and for `resource` the failure
// would be quieter than an error: the node would read as store-backed while the
// runner actually fetched it from its own URL.
func validateSupplyPullNode(nd types.NodeDef) error {
	if nd.Type != types.SupplyPullNodeType {
		return nil
	}
	if _, err := types.ParseSupplyPullSource(nd.Parameters); err != nil {
		return fmt.Errorf("node %q: %w", nd.Name, err)
	}
	return nil
}
