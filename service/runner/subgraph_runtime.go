package runner

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// ErrBatchWithoutABody reports a batch lease that carries no body package. The
// runner cannot run what it was not sent, and reporting the batch's items back
// — as the pre-body pass-through did — makes a truncated or misrouted lease
// look like a successful iteration.
var ErrBatchWithoutABody = errors.New("batch lease carries no body package")

// SubgraphRuntime adapts a batch lease to the runner's execution surface. It is
// the map counterpart of GroupRuntime: the control plane hands it one batch of
// a map expansion, it runs the body once per item, and the result commits
// through the expansion barrier rather than the ordinary node path.
//
// Both runtimes are thin: the execution itself lives in execution/subgraph,
// which cannot tell a group from a map body. What this type contributes is the
// batch shape — items, batch index, and the global-index arithmetic that makes
// batch_size invisible to the body.
type SubgraphRuntime struct {
	bodies *subgraph.MapBodyExecutor
}

// NewSubgraphRuntime creates a runtime that resolves body member handlers from
// the given registry and validates body packages through cache.
//
// Suspend is disabled inside a body unconditionally: a body runs once per item
// with no external identity to resume against, so a suspended item would park a
// sub-execution nothing can ever signal.
func NewSubgraphRuntime(reg *execution.Registry, cache *PackageCache) *SubgraphRuntime {
	executor := subgraph.NewExecutor(reg, cache, func() subgraph.Backend {
		return local.New(local.WithRegistry(reg), local.WithConcurrency(1))
	})
	return &SubgraphRuntime{bodies: subgraph.NewMapBodyExecutor(executor, true)}
}

// Execute runs one batch and returns the result the control plane commits
// through CommitSubgraphResult.
func (r *SubgraphRuntime) Execute(ctx context.Context, lease *engine.TaskLease) (engine.TaskResult, error) {
	if lease == nil || lease.SubgraphPayload == nil {
		return engine.TaskResult{}, fmt.Errorf("subgraph runtime: nil lease or payload")
	}
	payload := lease.SubgraphPayload
	if payload.Package == nil {
		return engine.TaskResult{}, fmt.Errorf("subgraph runtime: batch %d of %q: %w",
			payload.BatchIndex, payload.ParentNode, ErrBatchWithoutABody)
	}

	results, err := r.bodies.ExecuteBatchBody(ctx, engine.BatchBodyRequest{
		ExecutionID:     string(lease.Task.ExecutionID),
		ParentNode:      payload.ParentNode,
		Body:            payload.Package,
		BodyHash:        payload.PackageHash,
		BatchIndex:      payload.BatchIndex,
		BatchSize:       payload.BatchSize,
		Items:           payload.Items,
		AllItems:        payload.AllItems,
		ContinueOnError: payload.ContinueOnError,
	})
	if err != nil {
		// The body could not be RUN at all — package validation, compile,
		// backend construction. That is the batch's failure, reported as the
		// lease's error so the control plane's retry policy applies. It is not
		// per-item Err, which means "the body ran and this item failed".
		return engine.TaskResult{}, fmt.Errorf("subgraph runtime: batch %d of %q: %w",
			payload.BatchIndex, payload.ParentNode, err)
	}

	data, failure := engine.BatchResultForCommit(results, payload.ContinueOnError)
	return engine.TaskResult{
		Output: &types.Output{Data: data},
		Error:  failure,
	}, nil
}
