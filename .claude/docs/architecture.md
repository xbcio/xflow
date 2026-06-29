# Architecture

```
types/           Public contracts: DSL, execution IDs, handler interfaces, descriptors, handler IO
node/            Builtin action node constructors and implementations
      ↑
engine/          Pure scheduling algorithm (Graph, Scheduler, ErrorPolicy)
      ↑
execution/         Reusable task execution boundary (Dispatcher, Runner, Executor)
      ↑                    ↑
backend/         Backend Provider abstraction
backend/memory   In-memory backend provider
backend/asynq    Redis + Asynq backend provider
      ↑                    ↑
sdk/             cmd/server + cmd/runner
```

## Dependency Rules

- `engine/` must NOT import redis/asynq/mysql/sql — only stdlib + types + node
- `execution/` must NOT import redis/asynq/mysql/sql or network transports; it only adapts engine leases to an in-process or protocol-backed executor
- `backend/` owns the reusable `Provider` interface: `StateStore + TaskQueue + HandlerRegistry + lifecycle binding`
- `backend/memory/` is a reusable in-memory provider for embedded and test deployments; it must remain free of Redis/Asynq/MySQL/network dependencies
- `backend/asynq/` is a reusable Redis + Asynq provider for SDK cluster mode and future server-side control-plane reuse
- SDK assembles reusable packages and must not become the owner of reusable backend behavior
- cmd/server and cmd/runner use `engine/`, `execution/`, and reusable backend packages directly to build cluster infrastructure

Naming note: backend packages are named by implementation capability, not SDK
deployment mode. `local` and `cluster` remain public SDK factory names, while
the reusable implementations are `backend/memory` and `backend/asynq`.

## Engine Core's 2 Interfaces

Engine Core depends on exactly 2 interfaces:

- **StateStore** — State storage (lifecycle, node state, scheduling atomics, suspend/signal, data read/write)
- **TaskQueue** — Task enqueue (immediate / delayed)

Design decisions:
- `Hooks` is an observer, does not affect algorithm correctness, injected via constructor options
- No `Clock` interface needed — callers pass `now time.Time` where current time is required

## Reusable Execution Runtime

The `execution/` package is the shared task execution boundary for all
topologies:

- **Dispatcher** turns queued `engine.Task` values into `engine.TaskLease`
  values, calls an `Executor`, and commits `engine.TaskResult` through lease
  token fencing.
- **Runner** is the embedded in-process executor that resolves
  `node.TaskHandler` through `engine.HandlerRegistry`; action execution and
  suspending node prepare/resume both return runtime results.
- **Executor** is the extension point for future remote Runner Protocol
  transports.
- **Registry** is the reusable embedded handler registry. It supports direct
  node handlers for local execution and type/version lookup through
  `node.Register` for cluster and future runner processes.

This package is intentionally not under `sdk/internal/`: SDK local/cluster,
future `cmd/server`, and future `cmd/runner` all need the same lease/result
semantics.

## Reusable Backend Packages

- `backend/` defines `Provider`, the common assembly contract implemented by
  concrete backends, plus optional backend capabilities such as `Waiter`.
- `backend/memory/` contains the in-memory `StateStore`, in-memory
  `TaskQueue`, embedded `execution.Registry`, and embedded lifecycle binding.
  It is used by `xflow.NewLocal` and by tests.
- `backend/asynq/` contains the Redis-backed `StateStore`, Asynq-backed
  `TaskQueue`, timeout monitor, embedded `execution.Registry`, and embedded
  lifecycle binding. It is used by `xflow.NewCluster` today. Future server
  deployments may reuse the storage and queue semantics here, but remote
  runner processes must still communicate through Runner Protocol rather than
  connecting to Redis or Asynq directly.

## Key Boundaries

- Engine Core never imports IO packages
- Engine-owned execution metadata, such as submission TTL hints, lives in
  `engine/` instead of a concrete Redis/cluster package.
- SDK only assembles core packages and provides public API conveniences
- Concrete backend packages map Engine Core's 2 interfaces to concrete IO implementations
- Reusable packages should be named by capability, not by the generic term `adapter`
- Graph IR is immutable after compile, shared lock-free at runtime
- Scheduling advances via `StateStore.DecrementInDegree` atomic operation
- OnError four strategies consolidated in a single `ApplyOnError` function
- Suspend via `SuspendingHandler` optional interface, no string hardcoding
