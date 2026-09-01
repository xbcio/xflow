package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// SubgraphRuntimeOption configures a SubgraphRuntime.
type SubgraphRuntimeOption func(*subgraphRuntimeConfig)

type subgraphRuntimeConfig struct {
	artifactCode          func(ctx context.Context, digest string) ([]byte, error)
	mapConcurrencyLimiter *subgraph.MapConcurrencyLimiter
	hooks                 engine.Hooks
}

// WithSubgraphArtifactCodeResolver installs the digest -> script bytes resolver
// on every per-item inner backend. See WithGroupArtifactCodeResolver for why
// the outer runner's Config.ArtifactCodeResolver does not reach here on its own:
// a map body's xflow.script member resolves its code through the backend this
// runtime builds, not through the runner's top-level dispatcher.
func WithSubgraphArtifactCodeResolver(fn func(ctx context.Context, digest string) ([]byte, error)) SubgraphRuntimeOption {
	return func(c *subgraphRuntimeConfig) { c.artifactCode = fn }
}

// WithSubgraphHooks makes the nodes inside a map body item observable. See
// subgraph.WithHooks: the hooks must write their own metric family, because a
// map over N items produces N inner executions under one outer execution.
func WithSubgraphHooks(h engine.Hooks) SubgraphRuntimeOption {
	return func(c *subgraphRuntimeConfig) { c.hooks = h }
}

// WithSubgraphMapConcurrencyLimiter installs the runner-scoped active-batch
// and active-item budgets used by top-level map batch leases.
func WithSubgraphMapConcurrencyLimiter(limiter *subgraph.MapConcurrencyLimiter) SubgraphRuntimeOption {
	return func(c *subgraphRuntimeConfig) { c.mapConcurrencyLimiter = limiter }
}

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
func NewSubgraphRuntime(reg *execution.Registry, cache *PackageCache, opts ...SubgraphRuntimeOption) *SubgraphRuntime {
	cfg := &subgraphRuntimeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	execOpts := []subgraph.ExecutorOption{
		subgraph.WithMapConcurrencyLimiter(cfg.mapConcurrencyLimiter),
	}
	if cfg.hooks != nil {
		execOpts = append(execOpts, subgraph.WithHooks(cfg.hooks))
	}
	executor := subgraph.NewExecutor(reg, cache, func() subgraph.Backend {
		backendOpts := []local.Option{
			local.WithRegistry(reg),
			local.WithConcurrency(1),
			local.WithQueueCapacity(embeddedSubgraphQueueCapacity),
		}
		if cfg.artifactCode != nil {
			backendOpts = append(backendOpts, local.WithArtifactCodeResolver(cfg.artifactCode))
		}
		return local.New(backendOpts...)
	}, execOpts...)
	// No outer deadline to forward: this runtime is built once at runner
	// startup, before any lease (and its payload.Deadline, which
	// BuildSubgraphLease never populates today -- see SUBGRAPH-ENGINE-TODO.md)
	// exists. See MapBodyExecutor's deadline field doc in
	// execution/subgraph/map_body.go.
	return &SubgraphRuntime{bodies: subgraph.NewMapBodyExecutor(executor, true, time.Time{})}
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

	// Same one-batch lifetime as engine.runBatchBody: the durable path assembles
	// its batch result here instead of in the engine, so it must seed and read
	// its own collector or the runner-side path would attest nothing.
	ctx, uses := types.WithArtifactUseCollector(ctx)
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
		BodyConcurrency: payload.BodyConcurrency,
		Runtime:         payload.Runtime,
		OuterNodes:      payload.OuterNodes,
		TraceID:         payload.TraceID,
		SpanID:          payload.SpanID,
	})
	if err != nil {
		// The body could not be RUN at all — package validation, compile,
		// backend construction. That is the batch's failure, reported as the
		// lease's error so the control plane's retry policy applies. It is not
		// per-item Err, which means "the body ran and this item failed".
		return engine.TaskResult{}, fmt.Errorf("subgraph runtime: batch %d of %q: %w",
			payload.BatchIndex, payload.ParentNode, err)
	}

	data, failure := engine.BatchResultForCommit(results, payload.ContinueOnError, uses.Uses())
	return engine.TaskResult{
		Output: &types.Output{Data: data},
		Error:  failure,
	}, nil
}
