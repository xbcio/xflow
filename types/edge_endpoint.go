package types

// EdgeEndpoint is implemented by types usable as workflow edge endpoints.
// *workflowbuilder.NodeRef (returns node name + "main" port) and
// OutputPort/InputPort (return their Node + Port fields) satisfy it.
type EdgeEndpoint interface {
	NodePort() (node, port string)
}

// NodePort returns the node and port this output port refers to.
func (p OutputPort) NodePort() (string, string) { return p.Node, p.Port }

// NodePort returns the node and port this input port refers to.
func (p InputPort) NodePort() (string, string) { return p.Node, p.Port }
