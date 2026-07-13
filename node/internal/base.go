package internal

import "github.com/xbcio/xflow/types"

// BaseNode provides common fields for all node types.
// Embed this in concrete node structs.
type BaseNode struct {
	version int
	onError types.OnError
}

// NodeVersion returns the major version of this node.
func (b *BaseNode) NodeVersion() int {
	if b.version == 0 {
		return 1
	}
	return b.version
}

// OnErrorStrategy returns the error handling strategy.
func (b *BaseNode) OnErrorStrategy() types.OnError { return b.onError }

// SetOnError sets the error handling strategy. In-package callers may continue
// to assign b.onError directly.
func (b *BaseNode) SetOnError(s types.OnError) { b.onError = s }
