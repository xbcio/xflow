# Architecture — Current Implementation

> Status: **implemented**. This describes the architecture as built today.
> For target/future service-layer design (server clustering, Relay Gateway,
> Raft HA), see [CORE-COMPONENTS.md](./CORE-COMPONENTS.md) and its
> MASTER/GATEWAY/WORKER-COMPONENTS sub-docs, which are explicitly marked as
> target design. For SDK deployment modes (local/cluster/remote) and the
> server/runner cluster's current-vs-planned status, see
> [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md).

## Layered Dependency Graph

```
types/           Public contracts: DSL, execution IDs, handler interfaces, descriptors, handler IO
node/            Public node DSL and builtin node implementations
      ↑
engine/          Pure scheduling algorithm (Graph, Scheduler, ErrorPolicy)
      ↑
execution/         Reusable task execution boundary (Dispatcher, Runner, Executor)
      ↑                    ↑
backend/         Backend Provider abstraction
backend/memory   In-memory backend provider
backend/distributed    Redis + Asynq backend provider
      ↑                    ↑
sdk/             cmd/server + cmd/runner
```

## Dependency Rules

- `engine/` must NOT import redis/asynq/mysql/sql — only stdlib + types + node
- `execution/` must NOT import redis/asynq/mysql/sql or network transports; it only adapts engine leases to an in-process or protocol-backed executor
- `backend/` owns the reusable `Provider` interface: `StateStore + TaskQueue + HandlerRegistry + WorkflowRegistry + TriggerPrimitives + lifecycle binding`
- `backend/memory/` is a reusable in-memory provider for embedded and test deployments; it must remain free of Redis/Asynq/MySQL/network dependencies
- `backend/distributed/` is a reusable Redis + Asynq provider for SDK cluster mode and server-side control-plane reuse (`service/control`)
- SDK assembles reusable packages and must not become the owner of reusable backend behavior
- cmd/server and cmd/runner use `engine/`, `execution/`, and reusable backend packages directly to build cluster infrastructure

Naming note: backend packages are named by implementation capability, not SDK
deployment mode. `local` and `cluster` remain public SDK factory names, while
the reusable implementations are `backend/memory` and `backend/distributed`.

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
  `types.ActionHandler` through `engine.HandlerRegistry`; action execution and
  suspending node prepare/resume both return runtime results.
- **Executor** is the extension point for remote Runner Protocol transports
  (see `service/protocol`).
- **Registry** is the reusable embedded handler registry. It supports direct
  node handlers for local execution and type/version lookup through
  `registry.Register` for cluster and runner processes (`service/runner`).

This package is intentionally not under `sdk/internal/`: SDK local/cluster,
`cmd/server`, and `cmd/runner` all need the same lease/result semantics.

## Reusable Backend Packages

- `backend/` defines `Provider`, the common assembly contract implemented by
  concrete backends, plus optional backend capabilities such as `Waiter`.
  Workflow registration and trigger coordination live here so SDK local,
  cluster, and server modes (`service/control`) share the same identity, dedup,
  and locking contracts.
- `backend/memory/` contains the in-memory `StateStore`, in-memory
  `TaskQueue`, embedded `execution.Registry`, and embedded lifecycle binding.
  It is used by `xflow.NewLocal` and by tests.
- `backend/distributed/` contains the Redis-backed `StateStore`, Asynq-backed
  `TaskQueue`, timeout monitor, embedded `execution.Registry`, and embedded
  lifecycle binding. It is used by `xflow.NewCluster` today. Server
  deployments (`service/control`) reuse the storage and queue semantics here,
  but remote runner processes must still communicate through Runner Protocol
  rather than connecting to Redis or Asynq directly.

## Key Boundaries

- Engine Core never imports IO packages
- Engine-owned execution metadata, such as submission TTL hints, lives in
  `engine/` instead of a concrete Redis/cluster package.
- Trigger listener lifecycle stays outside Engine Core. The graph only marks
  explicit entries (`xflow.start` and trigger nodes); SDK/backend/service layers
  activate listeners, perform dedup/locks/state, and call `Invoke`.
- SDK only assembles core packages and provides public API conveniences
- Concrete backend packages map Engine Core's 2 interfaces to concrete IO implementations
- Reusable packages should be named by capability, not by the generic term `adapter`
- Graph IR is immutable after compile, shared lock-free at runtime
- Scheduling advances via `StateStore.DecrementInDegree` atomic operation
- OnError four strategies consolidated in a single `ApplyOnError` function
- Suspend via `SuspendingHandler` optional interface, no string hardcoding

## Node Handlers (node/)

- Regular nodes implement `ActionHandler` interface
- Suspendable nodes additionally implement `SuspendingHandler` interface
- Registered globally via `node.Register()`
- Public constructors and builtin handler implementations live under `node/`;
  internal helpers live under `node/internal/`.

## Adding New Node Types

1. Add the implementation under `node/`, grouped by filename and node kind
   (`action`, `code`, `transform`, `flow`, `group`, or `trigger`)
2. Implement `DescriptorProvider` to provide type metadata
3. Call `node.Register()` in `init()`
4. Add unit tests
5. Add integration example in `examples/`

## SuspendingHandler Design

```go
// Regular synchronous node
type ActionHandler interface {
    Execute(ctx context.Context, input *Input) (*Output, error)
}

// Suspendable node — explicit optional interface
type SuspendingHandler interface {
    PrepareSuspend(ctx context.Context, input *Input) (*SuspendSpec, error)
    OnResume(ctx context.Context, input *Input, signal *SignalPayload) (*Output, error)
}
```

Engine Core checks suspend capability: `if h, ok := handler.(SuspendingHandler); ok` — no string hardcoding, no Capability index.

`xflow.wait` degrades to a ~60-line builtin handler implementing `SuspendingHandler`. Any user node can also implement this interface (manual approval, sub-workflow callback, async-callback).

## ErrorPolicy

Four strategies: stop / errorOutput / mainOutput / continueOnError

Consolidated in a single `ApplyOnError` function; both Adapters consume the same outcome.

## HandlerRegistry

- Engine Core resolves handlers via `registry.Get(execID, nodeName, nodeType, version)`, agnostic to source.
- `execution.Registry` is the reusable embedded implementation.
- Local mode uses `execution.Registry` direct node handlers for inline `ActionHandler` values, then falls back to type/version lookup.
- Cluster mode uses the same `execution.Registry` but does not register direct handlers, because closures are not serializable across process boundaries.
- No `if local then ...` branches inside Core
