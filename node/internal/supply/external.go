// Package supply holds the two supply node types. They are declaration-only:
// neither has an Execute nor a registered handler. A supply node names a piece
// of content the workflow consumes and states the readiness policy; the content
// itself arrives through the SupplyResource store (external) or travels with the
// definition (static).
//
// Parameter reading lives in the exported node/supply package, because the
// control plane needs it too.
package supply

import (
	nodeinternal "github.com/xbcio/xflow/node/internal"
	xsupply "github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// ExternalNode declares that this workflow consumes a named SupplyResource whose
// content is written by something outside the workflow (an external PUT to
// /v1/supplies/{name}, or a future pull-mode collector).
//
// It has no Execute and no registered handler: nothing about it ever runs. The
// node exists so the dependency is visible on the graph — which is what makes
// the readiness gate and the reverse index possible.
type ExternalNode struct {
	nodeinternal.BaseNode
	ResourceValue     string
	RequireReadyValue bool
}

// External returns a supply declaration for the given resource name.
// require_ready defaults to true (spec §5.9.2: the default must not produce
// wrong data).
func External(resource string) *ExternalNode {
	return &ExternalNode{ResourceValue: resource, RequireReadyValue: true}
}

// RequireReady sets the readiness policy. false means the runner takes over the
// entry activation even with no content and the consumer runs with empty
// semantics — only choose it when "no configuration means no filtering" is an
// acceptable business semantic, and watch xflow_supply_unavailable_serving.
func (n *ExternalNode) RequireReady(v bool) *ExternalNode {
	n.RequireReadyValue = v
	return n
}

func (n *ExternalNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.supply.external",
		Kind:        types.NodeKindSupply,
		DisplayName: "External Supply",
		Params: []types.ParamSpec{
			{Name: xsupply.ParamResource, DisplayName: "Resource", Type: types.ParamString,
				Description: "SupplyResource name; defaults to the node name"},
			{Name: xsupply.ParamRequireReady, DisplayName: "Require Ready", Type: types.ParamBool, Default: true,
				Description: "When true (default) a runner will not take over the entry activation until this supply has content"},
		},
		// No Outputs: a supply node is never connected by a dataflow edge.
	}
}

func (n *ExternalNode) NodeType() string { return "xflow.supply.external" }

func (n *ExternalNode) RawParams() any {
	return map[string]any{
		xsupply.ParamResource:     n.ResourceValue,
		xsupply.ParamRequireReady: n.RequireReadyValue,
	}
}

// OnError satisfies types.Builder. A supply node never executes, so the strategy
// is inert; the method exists only to complete the interface.
func (n *ExternalNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
