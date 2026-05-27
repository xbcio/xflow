# Architecture

```
engine/          Pure algorithm (Graph, Scheduler, ErrorPolicy)
    ↑                    ↑
sdk/             cmd/server + cmd/worker
    ↓                    ↓
sdk/internal/    Respective IO implementations
adapter/
```

## Dependency Rules

- `engine/` must NOT import redis/asynq/mysql/sql — only stdlib + types + node
- `sdk/internal/adapter/` is SDK-private, cmd must not import it
- cmd/server and cmd/worker use `engine/` public interfaces directly to build cluster infrastructure

## Engine Core's 2 Interfaces

Engine Core depends on exactly 2 interfaces:

- **StateBackend** — State storage (lifecycle, node state, scheduling atomics, suspend/signal, data read/write)
- **TaskQueue** — Task enqueue (immediate / delayed)

Design decisions:
- `Hooks` is an observer, does not affect algorithm correctness, injected via constructor options
- No `Clock` interface needed — callers pass `now time.Time` where current time is required

## Key Boundaries

- Engine Core never imports IO packages
- Adapters map Engine Core's 2 interfaces to concrete IO implementations
- Graph IR is immutable after compile, shared lock-free at runtime
- Scheduling advances via `StateBackend.DecrementInDegree` atomic operation
- OnError four strategies consolidated in a single `ApplyOnError` function
- Suspend via `SuspendingHandler` optional interface, no string hardcoding
