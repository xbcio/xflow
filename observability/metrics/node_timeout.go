package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/execution"
)

// Compile-time assertion that *NodeTimeoutMetrics satisfies execution's
// TimeoutObserver. The interface lives in execution (the runner owns the events
// it observes); the adapter lives here. The assertion keeps them from drifting.
var _ execution.TimeoutObserver = (*NodeTimeoutMetrics)(nil)

// Node execution timeout metric names live in engine.go alongside the other
// node metric constants (metricNodeTimedOut, metricNodeStarted, ...). They are
// DISTINCT from metricNodeTimedOut (xflow_node_timed_out_total): that one counts
// SUSPENDED nodes whose park timer fired and which wake normally; the ones
// below count handler invocations that exceeded their execution deadline and
// terminated. Reusing the suspend metric would conflate a normal wake with a
// terminal failure.

// NodeTimeoutMetrics is the concrete observer adapter that turns node execution
// timeout events into xflow_ Prometheus metrics. It implements execution's
// TimeoutObserver interface.
//
// It MUST be constructed via NewNodeTimeoutMetrics and used as a pointer: the
// abandoned gauge tracks a per-node_type running count behind a mutex, and a
// value copy would split that mutex (each copy gets its own, so concurrent
// abandon/return pairs would race). Returning a pointer keeps a single shared
// mutex, matching how the runner hands the same observer to many goroutines.
//
// Label policy (security): the ONLY label dimensions are node_type (bounded
// cardinality — a small fixed set of node types) and source ("runner"/"server"
// for the timeout counter). Node name, execution ID, params, and any node
// output are NEVER labels — an upstream node's output is routinely an HTTP
// response body carrying a token.
type NodeTimeoutMetrics struct {
	Metrics *Metrics

	abandonedMu sync.Mutex
	abandoned   map[string]float64 // node_type -> current count
}

// NewNodeTimeoutMetrics creates the observer adapter backed by metrics.
func NewNodeTimeoutMetrics(metrics *Metrics) *NodeTimeoutMetrics {
	return &NodeTimeoutMetrics{Metrics: metrics, abandoned: make(map[string]float64)}
}

// OnNodeExecutionTimeout records a terminal execution timeout. source is
// "runner" when the runner detected it (the in-process handler deadline fired)
// or "server" when the control plane's lease-renewal backstop did.
func (n *NodeTimeoutMetrics) OnNodeExecutionTimeout(_ context.Context, nodeType, source string) {
	n.Metrics.Inc(metricNodeTimeouts, map[string]string{"node_type": nodeType, "source": source})
}

// OnHandlerAbandoned adjusts the abandoned gauge by delta (+1 when a handler is
// left running past its deadline, -1 when it finally returns). It must be able
// to fall back to zero: a persistently non-zero value is the signal that some
// node type ignores ctx and is leaking goroutines. The underlying GaugeSink is
// absolute, not incremental, so the adapter keeps a per-node_type running count
// and Sets the gauge to the absolute value.
//
// The Set is performed UNDER abandonedMu, not after releasing it. If it were
// released first, a goroutine that computed an intermediate count (say 3)
// could Set(3) AFTER a later goroutine already Set(0), leaving the gauge stuck
// at a stale non-zero value once every handler had actually returned -- a
// false-alarm goroutine-leak signal. Holding the lock across the Set
// serializes the writes so the last writer always reflects the final count.
func (n *NodeTimeoutMetrics) OnHandlerAbandoned(_ context.Context, nodeType string, delta float64) {
	n.abandonedMu.Lock()
	defer n.abandonedMu.Unlock()
	current := n.abandoned[nodeType] + delta
	if current < 0 {
		current = 0
	}
	n.abandoned[nodeType] = current
	n.Metrics.Set(metricNodeTimeoutAbandoned, map[string]string{"node_type": nodeType}, current)
}

// OnHandlerDuration records the wall-clock duration of one handler invocation.
func (n *NodeTimeoutMetrics) OnHandlerDuration(_ context.Context, nodeType string, elapsed time.Duration) {
	n.Metrics.Observe(metricNodeExecDuration, map[string]string{"node_type": nodeType}, elapsed)
}
