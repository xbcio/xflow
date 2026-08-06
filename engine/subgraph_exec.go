package engine

// WithRemoteBatchExecution makes map/split batch tasks escape this engine so a
// Dispatcher can route them to a remote runner, instead of being executed here.
//
// The escape is opt-in, not the default, because it only works where there is
// something to escape TO. An embedded engine (sdk.NewLocal, every inner
// sub-graph execution) has no runner directory: a batch that escapes there is
// routed nowhere and the map node waits forever on a child generation that
// never reports. The control plane opts in; everyone else keeps running
// batches in process.
//
// What the control plane buys by opting in: a batch carries the map node's
// runnerSelector, and a batch consumed on the control plane never reaches the
// directory that matches labels — body work would silently run wherever the
// control plane runs rather than on the runner the selector chose.
//
// This is a flag rather than a pluggable executor on purpose: local execution
// has exactly one implementation (Engine.ExecuteBatch), and remote execution is
// not an executor at all — the runner reports its batch result back through the
// normal commit path.
func WithRemoteBatchExecution() Option {
	return func(e *Engine) { e.remoteBatchExecution = true }
}
