package engine

// WithLocalBatchExecution keeps map/split batch tasks on this engine instead of
// letting the Dispatcher route them to a remote runner.
//
// The escape is the default because a batch carries the map node's
// runnerSelector, and a batch consumed here never reaches the directory that
// matches labels — body work would silently run wherever the engine runs. In
// an embedded deployment there is no directory and no runner to route to, so
// those callers opt back in.
//
// This is a flag rather than a pluggable executor on purpose: local execution
// has exactly one implementation (Engine.ExecuteBatch), and remote execution
// is not an executor at all — the runner reports its batch result back through
// the normal commit path.
func WithLocalBatchExecution() Option {
	return func(e *Engine) { e.localBatchExecution = true }
}
