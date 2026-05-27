package node

// OutputPort is a reference to a node output port (the src argument to Connect).
type OutputPort struct {
	Node string
	Port string
}

// InputPort is a reference to a node input port (the dst argument to Connect, used for multi-input edges).
type InputPort struct {
	Node string
	Port string
}
