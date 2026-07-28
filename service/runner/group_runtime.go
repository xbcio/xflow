package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/runner/internal/groupnode"
	"github.com/xbcio/xflow/types"
)

// GroupRuntimeOption configures a GroupRuntime.
type GroupRuntimeOption func(*GroupRuntime)

// WithSuspendDisabled makes the inner engine reject suspend nodes.
func WithSuspendDisabled() GroupRuntimeOption {
	return func(r *GroupRuntime) { r.suspendDisabled = true }
}

// GroupRuntime executes group subgraphs on an embedded local engine.
type GroupRuntime struct {
	registry        *execution.Registry
	cache           *PackageCache
	suspendDisabled bool
}

// NewGroupRuntime creates a group runtime that uses the given registry for
// handler resolution and the cache for package validation.
func NewGroupRuntime(reg *execution.Registry, cache *PackageCache, opts ...GroupRuntimeOption) *GroupRuntime {
	r := &GroupRuntime{registry: reg, cache: cache}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Execute runs the group subgraph defined by the lease's GroupPayload.
// It returns a GroupResult suitable for reporting to the control plane.
func (r *GroupRuntime) Execute(ctx context.Context, lease *engine.TaskLease) (engine.GroupResult, error) {
	if lease == nil || lease.GroupPayload == nil {
		return engine.GroupResult{Outcome: engine.GroupOutcomeFailed, Error: "nil lease or payload"}, nil
	}
	payload := lease.GroupPayload

	compiled, pkg, err := r.cache.Resolve(payload, r.inventoryFromRegistry())
	if err != nil {
		return engine.GroupResult{}, err
	}

	// Pre-allocate inner execution ID.
	innerExecID := engine.NewExecutionID()

	// Create per-attempt collector.
	collector := groupnode.NewCollector(pkg)

	// Create per-attempt failure observer.
	observer := &failureCapture{}

	// Build a per-attempt backend with the outer registry injected.
	innerBackend := local.New(local.WithRegistry(r.registry), local.WithConcurrency(1))

	// Register collector handlers scoped to the inner execution.
	groupnode.Register(r.registry, innerExecID, collector)

	// Build inner engine options.
	engineOpts := []engine.Option{
		engine.WithNodeFailureObserver(observer),
	}
	if r.suspendDisabled {
		engineOpts = append(engineOpts, engine.WithSuspendDisabled(nil))
	}
	if payload.Deadline.After(time.Now()) {
		ttl := time.Until(payload.Deadline)
		engineOpts = append(engineOpts, engine.WithDefaultLeaseTTL(ttl))
	}

	innerEngine := engine.New(innerBackend.State(), innerBackend.Queue(), engineOpts...)

	// Wire the dispatcher and start the inner queue.
	stop := innerBackend.Bind(innerEngine)
	defer stop()

	// Apply deadline via context.
	execCtx := ctx
	var cancel context.CancelFunc
	if !payload.Deadline.IsZero() && payload.Deadline.After(time.Now()) {
		execCtx, cancel = context.WithDeadline(ctx, payload.Deadline)
		defer cancel()
	}

	// Submit with seeded input for the entry node and pre-allocated ID.
	entryInput := payload.Input
	submitCtx := engine.WithExecutionID(execCtx, innerExecID)
	submitCtx = engine.WithSeededInputs(submitCtx, map[string]engine.SeededInput{
		pkg.EntryNode: buildSeed(entryInput),
	})

	_, err = innerEngine.Submit(submitCtx, compiled, inputDataAsParams(entryInput))
	if err != nil {
		return engine.GroupResult{
			Outcome: engine.GroupOutcomeFailed,
			Error:   fmt.Sprintf("inner submit: %v", err),
		}, nil
	}

	// Wait for inner execution completion.
	finalStatus := waitExecution(execCtx, innerBackend.State(), innerExecID)

	// Build result.
	result := engine.GroupResult{
		ProtocolVersion: payload.ProtocolVersion,
		GroupExecID:     payload.GroupExecID,
		Attempt:         lease.Attempt,
	}

	switch finalStatus {
	case types.ExecutionStatusSuccess:
		result.Outcome = engine.GroupOutcomeSuccess
		result.Exits = collector.Exits()
	case types.ExecutionStatusFailed:
		result.Outcome = engine.GroupOutcomeFailed
		if f := observer.fatal(); f != nil {
			result.Error = f.Err.Error()
		} else {
			result.Error = "inner execution failed"
		}
	case types.ExecutionStatusCanceled:
		result.Outcome = engine.GroupOutcomeCanceled
	default:
		if execCtx.Err() != nil {
			result.Outcome = engine.GroupOutcomeTimeout
			result.Error = "deadline exceeded"
		} else {
			result.Outcome = engine.GroupOutcomeFailed
			result.Error = fmt.Sprintf("unexpected status: %s", finalStatus)
		}
	}

	return result, nil
}

// inventoryFromRegistry builds a HandlerInventory from the outer registry.
func (r *GroupRuntime) inventoryFromRegistry() HandlerInventory {
	return &registryInventory{reg: r.registry}
}

type registryInventory struct {
	reg *execution.Registry
}

func (ri *registryInventory) Has(nodeType string, version int) bool {
	// Try to resolve via the registry — global handlers only.
	_, err := ri.reg.Get("", "", nodeType, version)
	return err == nil
}

func (ri *registryInventory) Runtimes() []string    { return nil }
func (ri *registryInventory) Resources() []string   { return nil }
func (ri *registryInventory) Credentials() []string { return nil }

// failureCapture collects Fatal node failures from the inner engine.
type failureCapture struct {
	mu       sync.Mutex
	failures []engine.ObservedNodeFailure
}

func (fc *failureCapture) ObserveNodeFailure(_ types.ExecutionID, f engine.ObservedNodeFailure) {
	if !f.Fatal {
		return
	}
	fc.mu.Lock()
	fc.failures = append(fc.failures, f)
	fc.mu.Unlock()
}

func (fc *failureCapture) fatal() *engine.ObservedNodeFailure {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.failures) == 0 {
		return nil
	}
	return &fc.failures[len(fc.failures)-1]
}

// inputDataAsParams extracts Data from the entry input as submission params.
func inputDataAsParams(input *types.Input) map[string]any {
	if input == nil {
		return nil
	}
	return input.Data
}

// buildSeed converts a types.Input into a SeededInput.
func buildSeed(input *types.Input) engine.SeededInput {
	if input == nil {
		return engine.SeededInput{}
	}
	return engine.SeededInput{
		Data:    input.Data,
		Inputs:  input.Inputs,
		Runtime: input.Runtime,
	}
}

// waitExecution polls the inner execution until it reaches a terminal status.
func waitExecution(ctx context.Context, state engine.StateStore, id types.ExecutionID) types.ExecutionStatus {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ""
		case <-ticker.C:
			snap, err := state.GetExecution(ctx, id)
			if err != nil || snap == nil {
				return ""
			}
			if types.IsTerminalExecutionStatus(snap.Status) {
				return snap.Status
			}
		}
	}
}
