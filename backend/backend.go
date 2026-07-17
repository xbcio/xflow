package backend

import (
	"context"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Provider supplies the engine backends and lifecycle binding for an embedded deployment.
type Provider interface {
	State() engine.StateStore
	Queue() engine.TaskQueue
	Registry() engine.HandlerRegistry
	WorkflowRegistry() WorkflowRegistry
	TriggerPrimitives() TriggerPrimitives
	Bind(eng *engine.Engine) func()
}

// Waiter is an optional backend capability for event-driven completion waits.
type Waiter interface {
	WaitDone(ctx context.Context, id types.ExecutionID) (types.Result, error)
}

// TaskHandlerBinder is a mandatory capability for a Provider used as a
// control-plane backend. It binds an externally-supplied task handler — the
// control-plane dispatcher — into the backend's queue/transport, and starts
// the durable scheduling outbox dispatcher (plus, where applicable, the lease
// timeout monitor). It returns a stop function for graceful shutdown.
//
// This is distinct from Provider.Bind, which wires the *embedded* execution
// dispatcher for SDK local/cluster use. A control plane must route tasks to
// remote runners via the injected handler rather than executing handlers
// in-process, so ControlPlane.Start fails closed when the configured backend
// does not implement TaskHandlerBinder instead of silently falling back to
// Provider.Bind (which would run the embedded handler inside the server
// process and bypass remote dispatch).
type TaskHandlerBinder interface {
	BindTaskHandler(eng *engine.Engine, handler func(context.Context, *engine.Task) error) (func(), error)
}
