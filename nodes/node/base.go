package node

// BaseNode provides common fields for all node types.
// Embed this in concrete node structs.
type BaseNode struct {
	version int
	onError OnError
}

// NodeVersion returns the major version of this node.
func (b *BaseNode) NodeVersion() int {
	if b.version == 0 {
		return 1
	}
	return b.version
}

// OnErrorStrategy returns the error handling strategy.
func (b *BaseNode) OnErrorStrategy() OnError { return b.onError }
