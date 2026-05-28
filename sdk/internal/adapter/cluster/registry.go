package cluster

import (
	"fmt"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// clusterRegistry implements core.HandlerRegistry for the cluster adapter.
// Only global type-based lookup is supported — closures are not serializable
// across process boundaries.
type clusterRegistry struct{}

func newClusterRegistry() *clusterRegistry { return &clusterRegistry{} }

func (r *clusterRegistry) Get(_ types.ExecutionID, _ string, nodeType string, version int) (node.TaskHandler, error) {
	if version > 0 {
		if h, ok := node.LookupVersion(nodeType, version); ok {
			return h, nil
		}
	}
	h, ok := node.Lookup(nodeType)
	if !ok {
		return nil, fmt.Errorf("no handler registered for node type %q", nodeType)
	}
	return h, nil
}
