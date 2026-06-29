# Implementation Guide

## Engine Core (engine/)

- Graph IR is immutable after compile, shared lock-free at runtime
- Scheduling advances via `StateStore.DecrementInDegree` atomic operation
- OnError four strategies consolidated in a single `ApplyOnError` function
- Suspend via `SuspendingHandler` optional interface, no string hardcoding

## Node Handlers (node/)

- Regular nodes implement `TaskHandler` interface
- Suspendable nodes additionally implement `SuspendingHandler` interface
- Registered globally via `node.Register()`

## Adding New Node Types

1. Create file under `node/`, implement `TaskHandler` (+ optional `SuspendingHandler`)
2. Implement `DescriptorProvider` to provide type metadata
3. Call `node.Register()` in `init()`
4. Add unit tests
5. Add integration example in `examples/`

## SuspendingHandler Design

```go
// Regular synchronous node
type TaskHandler interface {
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
- Local mode uses `execution.Registry` direct node handlers for inline `TaskHandler` values, then falls back to type/version lookup.
- Cluster mode uses the same `execution.Registry` but does not register direct handlers, because closures are not serializable across process boundaries.
- No `if local then ...` branches inside Core
