package xflow

import (
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
)

// bodyPackageCacheEntries bounds the projected-body cache. A body is projected
// once per map node at compile time, so the entry count tracks distinct map
// nodes across the workflows one engine hosts, not executions or items.
const bodyPackageCacheEntries = 64

// newBatchBodyExecutor builds the executor that runs a map node's body once per
// item, or nil when the registry cannot supply the handler inventory a body
// package must be validated against.
//
// Both modes get one. A batch escapes to a runner only where the control plane
// opts into it (engine.WithBatchEscape); everywhere else — every sdk engine,
// every inner sub-graph execution — the batch runs in this process and needs a
// body executor here, or ExecuteBatch refuses with ErrNoBatchBodyExecutor and
// the map node waits forever on a child generation that never reports.
//
// Each body attempt gets a FRESH local backend rather than the engine's own:
// the inner execution's tasks must not land on the outer queue, where the outer
// scheduler would drain them as if they belonged to the outer graph. This
// mirrors service/runner's group runtime, which does the same for group units.
func newBatchBodyExecutor(reg engine.HandlerRegistry, suspendDisabled bool) engine.BatchBodyExecutor {
	concrete, ok := reg.(*execution.Registry)
	if !ok {
		// A body package is validated against the registry's handler inventory
		// before it runs (subgraph.registryInventory). A registry that is not
		// the concrete type cannot supply one, so there is nothing to validate
		// against and no executor to build.
		return nil
	}
	cache := subgraph.NewPackageCache(subgraph.PackageCacheConfig{MaxEntries: bodyPackageCacheEntries})
	executor := subgraph.NewExecutor(concrete, cache, func() subgraph.Backend {
		return backendlocal.New(backendlocal.WithRegistry(concrete), backendlocal.WithConcurrency(1))
	})
	// No outer deadline to forward: this executor is built once at engine
	// startup, before any per-call Request exists (see MapBodyExecutor's
	// deadline field doc in execution/subgraph/map_body.go).
	return subgraph.NewMapBodyExecutor(executor, suspendDisabled, time.Time{})
}
