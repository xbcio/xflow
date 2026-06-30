# Spec: Node Resource Pool

**Status**: draft
**Tracks**: review concern #1 — DatabaseNode / GRPCNode open connections per Execute
**Severity**: high (verifier-confirmed)

## Problem

- `nodes/node/database.go:107-111`: `sql.Open` + `defer db.Close()` on every
  Execute. `*sql.DB` is itself a pool, so this defeats Go's standard
  pooling and incurs auth/TCP overhead per task.
- `nodes/node/grpc.go:124-136`: `grpc.NewClient` + `defer conn.Close()` per
  Execute. No KeepAlive, no DNS cache, no HTTP/2 reuse.

Under realistic workflow load these are connection-exhaustion / latency
amplifiers.

## Goals

- Pool `*sql.DB` and `*grpc.ClientConn` at process scope, keyed by
  `(driver, dsn)` and `(host, tls)` respectively.
- Inject the pool through `Input.Runtime` so handlers stay stateless.
- Keep node API and DSL unchanged.
- Lifecycle bound to `Engine.Stop()` for clean shutdown.

## Non-goals

- HTTP client pooling — `http.DefaultTransport` already pools; node will
  switch from per-call clients to a shared `*http.Client` (small drive-by, not
  the core of this spec).
- DSN encryption / credential management changes.

## Design

### Public interface (execution package)

`execution/resource_pool.go` (new):

```go
package execution

import (
    "context"
    "database/sql"

    "google.golang.org/grpc"
)

type ResourcePool interface {
    SQL(ctx context.Context, driver, dsn string) (*sql.DB, error)
    GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error)
    Close() error
}
```

Default implementation:

```go
type defaultPool struct {
    sqlMu  sync.Mutex
    sqlDBs map[string]*sql.DB    // key = driver + "|" + dsn

    grpcMu sync.Mutex
    conns  map[string]*grpc.ClientConn // key = host + "|" + secureFlag

    sqlConfig  SQLPoolConfig
    grpcConfig GRPCPoolConfig
}

type SQLPoolConfig struct {
    MaxOpenConns    int           // default 25
    MaxIdleConns    int           // default 5
    ConnMaxLifetime time.Duration // default 30m
}

type GRPCPoolConfig struct {
    KeepaliveTime    time.Duration // default 30s
    KeepaliveTimeout time.Duration // default 10s
}
```

Concurrency: double-checked init under per-key mutex (`SQL` and `GRPC` both).

### Exposing through `Input.Runtime`

`types/runtime.go`:

```go
type Runtime interface {
    // ... existing methods
    Pool() ResourcePoolView
}

type ResourcePoolView interface {
    SQL(ctx context.Context, driver, dsn string) (*sql.DB, error)
    GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}
```

Two-interface trick:

- `execution.ResourcePool` owns the pool (has `Close`).
- `types.ResourcePoolView` is what node handlers see (no `Close`).
- `types.Runtime.Pool()` returns the view.

Backwards compatibility: existing `types.Runtime` implementations that don't
implement `Pool()` will fail to satisfy the new interface. Mitigation: keep
`Pool()` as an **optional** capability accessed via type assertion:

```go
type runtimeWithPool interface {
    Pool() ResourcePoolView
}

func pool(in types.Input) ResourcePoolView {
    if rt, ok := in.Runtime.(runtimeWithPool); ok {
        return rt.Pool()
    }
    return nil // node falls back to per-call construction
}
```

This avoids breaking external `Runtime` implementations during the rollout.

### Node changes

`nodes/node/database.go`:

```go
func (n *DatabaseNode) Execute(ctx context.Context, in types.Input) (*types.Output, error) {
    driver, dsn, err := resolveCredentials(in)
    if err != nil { return errorOutput(err), nil }

    db, err := acquireSQL(in, driver, dsn)
    if err != nil { return errorOutput(err), nil }
    // NOTE: no defer db.Close() — pool owns the lifecycle

    // ... rest of Execute unchanged
}

func acquireSQL(in types.Input, driver, dsn string) (*sql.DB, error) {
    if p := pool(in); p != nil {
        return p.SQL(in.Ctx, driver, dsn)
    }
    db, err := sql.Open(driver, dsn)
    // legacy fallback path; OK to close at function exit, callers must defer
    return db, err
}
```

The fallback path is intentional during rollout — once all backends inject a
pool, the fallback can be removed.

`nodes/node/grpc.go`: same pattern — `acquireGRPC(in, host, secure, opts...)`.

### Wiring

`backend/memory/backend.go` and `backend/asynq/backend.go`:

```go
p := execution.NewDefaultPool(execution.DefaultPoolConfig())
runner := execution.NewRunner(..., execution.WithPool(p))
```

`Runtime` provided to handlers is constructed at task dispatch time; the
Runner injects `p` into the runtime view.

### Shutdown

`Engine.Stop(ctx)`:
- Existing cleanup, plus `pool.Close()` to close all open `*sql.DB` and
  `*grpc.ClientConn`.
- `pool.Close()` is idempotent.

## Observability hooks (placeholder)

Pool methods emit events through an optional `PoolObserver`:

```go
type PoolObserver interface {
    OnSQLOpen(driver, dsn string)
    OnSQLReuse(driver, dsn string)
    OnGRPCDial(host string)
    OnGRPCReuse(host string)
}
```

Default observer is nil. Wired up properly in spec #6 (observability).

## Testing

- Unit: 100 concurrent `SQL(ctx, "sqlite3", ":memory:")` calls return the
  same `*sql.DB` instance.
- Unit: `Close()` shuts down all stored DBs/conns.
- Integration: DatabaseNode with the pool runs 1000 sequential queries; only
  one `sql.Open` observed via observer.
- Integration: GRPCNode with the pool reuses one ClientConn across many
  Executes; KeepAlive PINGs visible in mock server logs.
- Regression: GRPCNode with no pool (nil runtime view) still works on its
  own — backward compatible path.

## Migration

- Phase 1: ship pool + nodes prefer pool when available, fall back otherwise.
- Phase 2 (after one release): remove fallback, require pool. Document
  breaking change for external `Runtime` implementations.

## Acceptance

- DatabaseNode and GRPCNode no longer call `Close()` on connections.
- Both backends wire a `defaultPool` by default.
- `go test ./... -race` passes.
- Manual smoke: run a workflow with 1000 HTTP→Database fan-out, observe
  steady-state connection count, not unbounded growth.
