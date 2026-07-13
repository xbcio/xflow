package execution

import (
	"context"
	"database/sql"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/internal/noderuntime"
	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
)

// ResourcePool is the per-process pool of network resources shared across
// node handler invocations.
type ResourcePool interface {
	SQL(ctx context.Context, driver, dsn string) (*sql.DB, error)
	GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	Close() error
}

// Runner executes task leases using a handler registry.
type Runner struct {
	registry engine.HandlerRegistry
	pool     ResourcePool
}

// RunnerOption customizes a Runner.
type RunnerOption func(*Runner)

// WithResourcePool installs a ResourcePool. The Runner attaches it to the
// per-call context so resource-aware nodes (DatabaseNode, GRPCNode) can pool
// their connections. nil pool is a valid no-op.
func WithResourcePool(p ResourcePool) RunnerOption {
	return func(r *Runner) { r.pool = p }
}

// NewRunner creates an in-process task runner.
func NewRunner(registry engine.HandlerRegistry, opts ...RunnerOption) *Runner {
	r := &Runner{registry: registry}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Execute runs a task lease. The returned handled flag is false when the task
// requires engine-managed compatibility behavior.
func (r *Runner) Execute(ctx context.Context, lease *engine.TaskLease) (engine.TaskResult, error) {
	handler, err := r.registry.Get(
		types.ExecutionID(lease.Task.ExecutionID),
		lease.Task.NodeName,
		lease.NodeType,
		lease.NodeVersion,
	)
	if err != nil {
		return engine.TaskResult{}, err
	}
	if r.pool != nil {
		ctx = noderuntime.WithResourcePool(ctx, r.pool)
	}
	if sh, ok := handler.(types.SuspendingHandler); ok {
		return r.executeSuspending(ctx, lease, sh)
	}

	output, sysErr := handler.Execute(ctx, lease.Input)
	return engine.TaskResult{Output: output, Error: sysErr}, nil
}

func (r *Runner) executeSuspending(ctx context.Context, lease *engine.TaskLease, sh types.SuspendingHandler) (engine.TaskResult, error) {
	if lease.Task.Type == engine.TaskTypeNodeResume {
		output, err := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
		if err != nil {
			return engine.TaskResult{Output: output, Error: err}, nil
		}
		if output != nil && output.Resuspend {
			input := lease.Input
			if output.Data != nil {
				input = cloneInputWithData(lease.Input, output.Data)
			}
			spec, err := sh.PrepareSuspend(ctx, input)
			if err != nil {
				return engine.TaskResult{Output: output, Error: err}, nil
			}
			return engine.TaskResult{Output: output, Suspend: spec}, nil
		}
		return engine.TaskResult{Output: output}, nil
	}

	spec, err := sh.PrepareSuspend(ctx, lease.Input)
	if err != nil {
		return engine.TaskResult{Error: err}, nil
	}
	return engine.TaskResult{Suspend: spec}, nil
}

func cloneInputWithData(input *types.Input, data map[string]any) *types.Input {
	if input == nil {
		return &types.Input{Data: data}
	}
	cp := *input
	cp.Data = data
	return &cp
}
