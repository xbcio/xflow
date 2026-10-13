package flow

import (
	"context"
	"fmt"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// SubWorkflowNodeType is the canonical type identifier for the sub-workflow
// node. It aliases types.SubWorkflowNodeType rather than restating the literal:
// the compiler and the engine match on the same string.
const SubWorkflowNodeType = types.SubWorkflowNodeType

// SubWorkflowNode implements xflow.subworkflow — runs another workflow as a
// child execution and exposes its conclusion as this node's output.
//
// The node never executes in a runner. The handler below is a fail-closed stub,
// registered only because the registration precheck and the runner-side
// registry both require a handler for every declared type; the engine owns this
// type's dispatch and routes it to its own sub-workflow path long before any
// runner sees a task.
type SubWorkflowNode struct {
	nodeinternal.BaseNode
	Workflow   string
	VersionRef string
	EntryName  string
	InputMap   map[string]any
	OutputMap  map[string]any
}

// SubWorkflow creates a node that runs the named workflow in the same namespace.
//
//	node.SubWorkflow("payment-processing").Version("v1")
func SubWorkflow(workflow string) *SubWorkflowNode {
	return &SubWorkflowNode{Workflow: workflow}
}

// Version pins the child workflow version. Empty (the default) resolves to the
// latest version at registration time; the resolved version is baked into the
// compiled graph, so a later release does not change what this node runs.
func (n *SubWorkflowNode) Version(v string) *SubWorkflowNode {
	n.VersionRef = v
	return n
}

// Entry starts the child execution at the named node instead of the child
// workflow's default start.
func (n *SubWorkflowNode) Entry(name string) *SubWorkflowNode {
	n.EntryName = name
	return n
}

// Input maps expression strings to the child execution's params.
func (n *SubWorkflowNode) Input(expressions map[string]any) *SubWorkflowNode {
	n.InputMap = expressions
	return n
}

// Output maps expression strings evaluated over the child execution's public
// node outputs ($child) to this node's output.
func (n *SubWorkflowNode) Output(expressions map[string]any) *SubWorkflowNode {
	n.OutputMap = expressions
	return n
}

func (n *SubWorkflowNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        SubWorkflowNodeType,
		DisplayName: "Sub-workflow",
		Params: []types.ParamSpec{
			{Name: "workflow", DisplayName: "Workflow", Type: types.ParamString, Required: true, Description: "Name of the workflow to run, in the same namespace"},
			{Name: "version", DisplayName: "Version", Type: types.ParamString, Required: false, Description: "Version to pin; empty resolves to the latest version at registration time"},
			{Name: "entry", DisplayName: "Entry", Type: types.ParamString, Required: false, Description: "Entry node of the child workflow; empty uses its default start"},
			{Name: "input", DisplayName: "Input", Type: types.ParamObject, Required: false, Description: "Expressions producing the child execution's params",
				Widget: nodeinternal.WidgetKeyExpression},
			{Name: "output", DisplayName: "Output", Type: types.ParamObject, Required: false, Description: "Expressions over the child execution's public node outputs ($child) producing this node's output",
				Widget: nodeinternal.WidgetKeyExpression},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *SubWorkflowNode) NodeType() string { return SubWorkflowNodeType }
func (n *SubWorkflowNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *SubWorkflowNode) RawParams() any {
	params := map[string]any{"workflow": n.Workflow}
	if n.VersionRef != "" {
		params["version"] = n.VersionRef
	}
	if n.EntryName != "" {
		params["entry"] = n.EntryName
	}
	if len(n.InputMap) > 0 {
		params["input"] = n.InputMap
	}
	if len(n.OutputMap) > 0 {
		params["output"] = n.OutputMap
	}
	return params
}

// Execute fails closed. xflow.subworkflow is an engine-owned type: a dispatch
// that reaches a handler means the engine's routing is broken, and an error
// keeps that failure where it can be diagnosed instead of letting a stub
// fabricate an empty child result.
func (n *SubWorkflowNode) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, fmt.Errorf("%s: engine-owned node type must not be dispatched to a handler", SubWorkflowNodeType)
}

func init() { registry.Register(&SubWorkflowNode{}) }
