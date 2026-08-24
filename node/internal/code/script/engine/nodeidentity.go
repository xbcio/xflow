package engine

import "context"

// nodeIdentityKey is the context key carrying which node is running the script.
type nodeIdentityKeyType struct{}

var nodeIdentityKey nodeIdentityKeyType

// NodeIdentity names the node whose parameters produced the script now running.
//
// It travels in the context rather than in Source or Helpers because the layer
// that needs it is neither: an engine's observer reports cost, and cost is only
// actionable once it can be attributed to a node. Threading it through the
// Engine interface would force every engine to accept and forward a value it
// does not itself use.
type NodeIdentity struct {
	// Workflow is the compiled graph's name. It qualifies Node, which is only
	// unique within one workflow — without it, two workflows with a node called
	// "transform" report as one series.
	Workflow string
	// Node is the node name from the workflow definition.
	Node string
}

// WithNodeIdentity returns ctx carrying id. The node layer calls this before
// handing control to an engine.
func WithNodeIdentity(ctx context.Context, id NodeIdentity) context.Context {
	return context.WithValue(ctx, nodeIdentityKey, id)
}

// NodeIdentityFromContext returns the identity attached by the node layer, or
// the zero value when the caller reached an engine without going through it —
// a direct engine.Lookup in a test, for instance.
//
// Both fields are workflow-definition names, so they are bounded by what is
// deployed and safe as metric labels. Neither is derived from a message.
func NodeIdentityFromContext(ctx context.Context) NodeIdentity {
	id, _ := ctx.Value(nodeIdentityKey).(NodeIdentity)
	return id
}
