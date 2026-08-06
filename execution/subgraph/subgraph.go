// Package subgraph runs a compiled sub-graph package to completion on an
// embedded engine backend. A node group and a map body are the same thing at
// this layer: both compile down to a graph.SubgraphPackage and both submit a
// Request. Executor cannot tell which kind of caller produced the Request --
// that is the point: the sub-graph/node-group duality (see docs/design) means
// there is exactly one execution path underneath both.
package subgraph

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// Outcome is the terminal state of a sub-graph execution.
type Outcome string

const (
	OutcomeSuccess  Outcome = "success"
	OutcomeFailed   Outcome = "failed"
	OutcomeTimeout  Outcome = "timeout"
	OutcomeCanceled Outcome = "canceled"
)

// Backend is the minimal capability surface Executor needs from an embedded
// engine backend: state/queue access, lifecycle binding, and event-driven
// completion waiting. Its shape is the intersection of backend.Provider and
// backend.Waiter (backend/backend.go), copied rather than imported because
// execution/subgraph must not import backend/providers/local -- local already
// imports execution, so that import would cycle. local.Backend satisfies this
// interface without any changes.
type Backend interface {
	State() engine.StateStore
	Queue() engine.TaskQueue
	WaitDone(ctx context.Context, id types.ExecutionID) (types.Result, error)
	Bind(eng *engine.Engine) func()
}

// Request is one sub-graph execution request. It carries no map- or
// group-specific concept (no item, no batch index): Package plus an entry
// Input is everything a sub-graph execution needs, regardless of who is
// asking for it.
type Request struct {
	Package         *graph.SubgraphPackage
	PackageHash     string
	Input           *types.Input
	Deadline        time.Time
	SuspendDisabled bool
}

// Result is the outcome of one sub-graph execution.
type Result struct {
	Outcome Outcome
	Exits   []graph.SubgraphExitResult
	Error   string
}

// Executor runs sub-graph packages on a fresh embedded backend per execution.
type Executor struct {
	registry   *execution.Registry
	cache      *PackageCache
	newBackend func() Backend
}

// NewExecutor creates a sub-graph executor. reg resolves member node handlers
// (both for the compiled package's dispatch and for validating the package's
// declared Requirements against the caller's handler inventory); cache
// validates and caches compiled packages by hash; newBackend constructs a
// fresh per-execution backend (e.g. local.New) -- injected because
// execution/subgraph cannot import backend/providers/local.
func NewExecutor(reg *execution.Registry, cache *PackageCache, newBackend func() Backend) *Executor {
	return &Executor{registry: reg, cache: cache, newBackend: newBackend}
}

// Execute compiles (or fetches from cache) req.Package, runs it to completion
// on a fresh backend, and returns its collected boundary exits.
func (e *Executor) Execute(ctx context.Context, req Request) (Result, error) {
	payload := &engine.GroupLeasePayload{
		PackageHash: req.PackageHash,
		Package:     req.Package,
	}
	compiled, pkg, err := e.cache.Resolve(payload, e.inventoryFromRegistry())
	if err != nil {
		return Result{}, err
	}

	// Pre-allocate inner execution ID.
	innerExecID := engine.NewExecutionID()

	// Create per-attempt collector.
	collector := NewCollector(pkg)

	// Create per-attempt failure observer.
	observer := &failureCapture{}

	// Build a per-attempt backend via the injected constructor.
	innerBackend := e.newBackend()

	// Register collector handlers scoped to the inner execution. Deferred
	// unregister runs on every exit path below, including the two early error
	// returns (Resolve failure has nothing to unregister yet, so it is placed
	// after this line): without it, every call here leaks its entries into the
	// registry for the life of the process. That was survivable for a group
	// (Register runs once per group EXECUTION), but MapBodyExecutor calls
	// Execute once per map-body ITEM, turning this into unbounded growth on any
	// runner serving a map workflow with a non-trivial items array.
	Register(e.registry, innerExecID, collector)
	defer e.registry.UnregisterExecution(innerExecID)

	// Build inner engine options.
	engineOpts := []engine.Option{
		engine.WithNodeFailureObserver(observer),
	}
	if req.SuspendDisabled {
		engineOpts = append(engineOpts, engine.WithSuspendDisabled(nil))
	}
	if req.Deadline.After(time.Now()) {
		ttl := time.Until(req.Deadline)
		engineOpts = append(engineOpts, engine.WithDefaultLeaseTTL(ttl))
	}

	innerEngine := engine.New(innerBackend.State(), innerBackend.Queue(), engineOpts...)

	// Wire the dispatcher and start the inner queue.
	stop := innerBackend.Bind(innerEngine)
	defer stop()

	// Apply deadline via context.
	execCtx := ctx
	var cancel context.CancelFunc
	if !req.Deadline.IsZero() && req.Deadline.After(time.Now()) {
		execCtx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}

	// Submit with seeded input for the entry node and pre-allocated ID.
	entryInput := req.Input
	submitCtx := engine.WithExecutionID(execCtx, innerExecID)
	submitCtx = engine.WithSeededInputs(submitCtx, map[string]engine.SeededInput{
		pkg.EntryNode: buildSeed(entryInput),
	})

	_, err = innerEngine.Submit(submitCtx, compiled, inputDataAsParams(entryInput))
	if err != nil {
		return Result{
			Outcome: OutcomeFailed,
			Error:   fmt.Sprintf("inner submit: %v", err),
		}, nil
	}

	// Wait for inner execution completion -- signal-driven via the backend's
	// WaitDone, not a hand-rolled poll.
	waitResult, waitErr := innerBackend.WaitDone(execCtx, innerExecID)
	finalStatus := waitResult.Status
	if waitErr != nil {
		finalStatus = ""
	}

	// Build result.
	result := Result{}
	switch finalStatus {
	case types.ExecutionStatusSuccess:
		result.Outcome = OutcomeSuccess
		result.Exits = collector.Exits()
	case types.ExecutionStatusFailed:
		result.Outcome = OutcomeFailed
		if f := observer.fatal(); f != nil {
			result.Error = f.Err.Error()
		} else {
			result.Error = "inner execution failed"
		}
	case types.ExecutionStatusCanceled:
		result.Outcome = OutcomeCanceled
	default:
		if execCtx.Err() != nil {
			result.Outcome = OutcomeTimeout
			result.Error = "deadline exceeded"
		} else {
			result.Outcome = OutcomeFailed
			result.Error = fmt.Sprintf("unexpected status: %s", finalStatus)
		}
	}

	return result, nil
}

// inventoryFromRegistry builds a HandlerInventory from the outer registry.
func (e *Executor) inventoryFromRegistry() HandlerInventory {
	return &registryInventory{reg: e.registry}
}

type registryInventory struct {
	reg *execution.Registry
}

func (ri *registryInventory) Has(nodeType string, version int) bool {
	// A name-scoped handler (sdk.LocalNode) has no portable node type: its
	// NodeDef.Type is the synthetic "__direct__/<node name>" and nothing is
	// registered under that string as a type. Resolve it by the name it encodes
	// instead, or a body assembled from LocalNode members could never validate.
	if name, ok := execution.DirectHandlerNodeName(nodeType); ok {
		return ri.reg.HasNodeHandler(name)
	}
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
