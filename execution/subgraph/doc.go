// Package subgraph runs a compiled graph.SubgraphPackage to completion on a
// fresh embedded backend. It is the shared execution path for two graph
// constructs that compile down to the same wire shape:
//
//   - A node group: executes once, submitting a single Request.
//   - A map body:   executes once per item; MapBodyExecutor.ExecuteBatchBody
//     drives the fan-out and is called by engine/expand.go.
//
// This package sits one level below execution.Dispatcher in the main pipeline:
//
//	[engine] → [execution.Dispatcher] → [Executor (subgraph)]
//	                                          │
//	                                    fresh embedded backend
//	                                    (backend/providers/local)
//	                                          │
//	                                    inner engine + dispatcher
//	                                          │
//	                                    WaitDone → Result
//
// Upstream callers are backend/providers/distributed (GroupRuntime, service/runner
// SubgraphRuntime) and sdk/xflow's engine-startup wiring. Downstream this
// package builds a per-execution local backend; it must not import
// backend/providers/local directly (that package imports execution, which would
// cycle). The Backend interface in subgraph.go is the decoupling seam.
//
// # Core invariants
//
// Executor treats group and map-body as identical: both provide a
// graph.SubgraphPackage and an entry Input; Executor cannot distinguish them.
//
// PackageCache validates packages before first use and caches by hash:
//   1. Recompute hash from the package bytes (graph.ComputePackageHash) and
//      compare to the claimed hash — a mismatch is a permanent failure.
//   2. Check handler inventory (Has by type+version), runtimes, resources,
//      credentials.
//   3. Compile via graph.CompileProjectedPackage.
// Subsequent calls with the same hash skip validation entirely.
//
// MapBodyExecutor.ExecuteBatchBody is the real implementation of
// engine.BatchBodyExecutor, not a stub. It is registered into every inner
// engine that Executor builds, so a group-member xflow.map can recursively
// run its body items through the same Executor without hitting
// ErrNoBatchBodyExecutor.
//
// This package must not import backend/providers/local, backend/providers/
// distributed, or any storage/network package. It uses the Backend interface
// (State, Queue, WaitDone, Bind) declared in subgraph.go.
//
// # Key flow
//
//	MapBodyExecutor.ExecuteBatchBody(ctx, req)
//	  │
//	  ├─ serial or concurrent (req.BodyConcurrency > 1)
//	  │
//	  └─ per item: Executor.Execute(ctx, Request{Package, Input, Scope, Deadline})
//	                  │
//	                  ├─ PackageCache.Resolve (validate + compile, cache by hash)
//	                  ├─ Register collector handlers scoped to innerExecID
//	                  │   defer UnregisterExecution(innerExecID)
//	                  ├─ newBackend() → fresh local backend
//	                  ├─ engine.New + innerBackend.Bind (wire inner dispatcher)
//	                  ├─ innerEngine.Submit (entry Input + Scope)
//	                  └─ innerBackend.WaitDone → Result{Outcome, Exits, Error}
//
// # Traps for maintainers
//
// Registry leak on map-body items: Executor.Execute calls
// execution.Registry.UnregisterExecution in a defer. If that defer is removed,
// every map-body item registers handler entries that are never cleaned up,
// causing unbounded growth in execution.Registry.executionHandlers for any
// runner serving a map workflow (see the UnregisterExecution doc comment).
//
// PackageCache keys on hash, not on package identity. A package whose bytes
// changed but whose hash was not recomputed will be served from cache without
// re-validation. Callers must recompute and pass the canonical hash; the cache
// verifies the first time but trusts subsequent hits completely.
//
// SuspendDisabled must propagate from the outer Request into the inner engine
// (engine.WithSuspendDisabled). A map body has no external identity to resume
// against; allowing suspend inside a body item parks a sub-execution that
// nothing can ever signal. The floor is enforced in Executor.Execute; do not
// bypass it with a fresh Request that omits SuspendDisabled.
//
// The outer Deadline must reach the inner Executor via req.Deadline, not via
// a fresh context deadline. context.Background() is the lineage for inner
// worker goroutines (memory_queue.go does not inherit the submitter's context),
// so a context-only deadline silently does not bound member handlers.
package subgraph
