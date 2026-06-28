package node

import "context"

// StartNode implements xflow.start, the explicit entry point for cyclic or
// UI-authored workflows.
//
// Cyclic workflows require exactly one start node. The engine submits cyclic
// executions by enqueueing this node first, even if the start node has an
// incoming edge from a later loop. The node itself has no parameters and simply
// forwards the workflow submission params to its main output.
type StartNode struct {
	BaseNode
}

// Start returns the built-in xflow.start node builder.
//
// Use it with WorkflowBuilder.AllowCycles:
//
//	wf := xflow.Workflow("approval").AllowCycles(100)
//	start := wf.Node("Start", node.Start())
//
// For ordinary DAG workflows, a start node is optional; zero in-degree nodes
// still receive workflow submission params directly.
func Start() *StartNode {
	return &StartNode{}
}

func (n *StartNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.start",
		DisplayName: "Start",
		Kind:        "action",
		Outputs:     []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *StartNode) NodeType() string { return "xflow.start" }
func (n *StartNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}
func (n *StartNode) RawParams() any { return nil }

func (n *StartNode) Execute(_ context.Context, input *Input) (*Output, error) {
	if input == nil {
		return &Output{Data: map[string]any{}}, nil
	}
	return &Output{Data: input.Data}, nil
}

func init() { Register(&StartNode{}) }
