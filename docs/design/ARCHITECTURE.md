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
engine/          Pure scheduling algorithm (Graph, Scheduler, ErrorPolicy, lease/result semantics)
      ↑
execution/         Reusable task execution boundary (Dispatcher, Runner, Executor)
      ↑                    ↑
backend/         Backend Provider abstraction
backend/providers/local   In-memory backend provider
backend/providers/distributed    Redis + Asynq backend provider
namespace/      Server-issued permission namespace context primitive
      ↑                    ↑
sdk/             Public embedded API (NewLocal / NewCluster / NewServer)
cmd/server + cmd/runner
```

## Dependency Rules

- `engine/` must NOT import redis/asynq/mysql/sql or network transports. It may depend on public contracts (`types`, `namespace`, `engine/graph`) and the narrow `observability/tracing` facade for engine-owned spans. It must not depend on concrete metrics/logging exporters, storage drivers, queue transports, HTTP/gRPC servers, or provider packages.
- `execution/` must NOT import redis/asynq/mysql/sql or network transports; it only adapts engine leases to an in-process or protocol-backed executor
- `backend/` owns the reusable `Provider` interface: `StateStore + TaskQueue + HandlerRegistry + WorkflowRegistry + TriggerPrimitives + lifecycle binding`
- `backend/providers/local/` is a reusable in-memory provider for embedded and test deployments; it must remain free of Redis/Asynq/MySQL/network dependencies
- `backend/providers/distributed/` is a reusable Redis + Asynq provider for SDK cluster mode and server-side control-plane reuse (`service/control`)
- `namespace/` owns the server-issued namespace context primitive used for permission isolation across engine, execution, backend providers, service, and observability
- SDK assembles reusable packages and must not become the owner of reusable backend behavior. `sdk/xflow` may import `service/apiserver` and `service/control` only for the supported `xflow.NewServer` embedded control-plane facade; server-specific state machines and runner protocol logic still live under `service/`.
- cmd/server and cmd/runner use `engine/`, `execution/`, and reusable backend packages directly to build cluster infrastructure

Naming note: backend packages are named by implementation capability, not SDK
deployment mode. `local` and `cluster` remain public SDK factory names, while
the reusable implementations are `backend/providers/local` and `backend/providers/distributed`.

## Engine Core's 2 Interfaces

Engine Core is constructed with exactly 2 interfaces:

- **StateStore** — State storage (lifecycle, node state, scheduling atomics, suspend/signal, data read/write)
- **TaskQueue** — Task enqueue (immediate / delayed)

Design decisions:
- `StateStore` is intentionally a broad facade composed from smaller domains
  (executions, graphs, nodes, scheduling, signals, sub-executions, outputs, and
  events). This keeps Engine construction stable, but it means every concrete
  backend must satisfy the full state contract.
- Advanced durability is exposed as optional capabilities owned by the
  backend, such as `AtomicStateStore`, durable suspend/signal delivery,
  dead-letter storage, replay receipts, and observers. Engine may discover
  these by interface assertion, but each production backend must document and
  contract-test the capabilities it claims.
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
- `backend/providers/local/` contains the in-memory `StateStore`, in-memory
  `TaskQueue`, embedded `execution.Registry`, and embedded lifecycle binding.
  It is used by `xflow.NewLocal` and by tests.
- `backend/providers/distributed/` contains the Redis-backed `StateStore`, Asynq-backed
  `TaskQueue`, timeout monitor, embedded `execution.Registry`, and embedded
  lifecycle binding. It is used by `xflow.NewCluster` today. Server
  deployments (`service/control`) reuse the storage and queue semantics here,
  but remote runner processes must still communicate through Runner Protocol
  rather than connecting to Redis or Asynq directly.
- `backend/providers/distributed/internal/rstate/` is the Redis authority
  sub-system. Keep state-machine changes grouped and reviewed by contract
  area: execution/node lifecycle, lease fencing and repair, durable outbox and
  dead-letter, suspend/signal, SQL audit projection, and namespace isolation.
  Changes in one area should extend that area's backend contract tests rather
  than relying only on end-to-end tests.

## Key Boundaries

- Engine Core never imports business IO, storage, queue, or network transport packages
- Engine-owned tracing spans may use the narrow observability facade; exporter
  setup and concrete OTel/Prometheus/logging providers remain outside Engine
  Core.
- Engine-owned execution metadata, such as submission TTL hints, lives in
  `engine/` instead of a concrete Redis/cluster package.
- Trigger listener lifecycle stays outside Engine Core. The graph only marks
  explicit entries (`xflow.start` and trigger nodes); SDK/backend/service layers
  activate listeners, perform dedup/locks/state, and call `Invoke`.
- SDK only assembles core packages and provides public API conveniences,
  including the supported `xflow.NewServer` embedded server facade. SDK must
  not own reusable backend behavior or server-side runner state machines.
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
