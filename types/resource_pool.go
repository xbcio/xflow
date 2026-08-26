package types

import (
	"context"
	"database/sql"
	"time"

	"google.golang.org/grpc"
)

// ResourcePool is the per-process pool of network resources shared across
// node handler invocations. DatabaseNode and GRPCNode require a pool to
// acquire connections — there is no per-call fallback.
//
// Implementations must be safe for concurrent use. Close releases all
// resources and is idempotent. ctx bounds the close wait; on timeout the
// close continues in the background.
type ResourcePool interface {
	SQL(ctx context.Context, driver, dsn string) (*sql.DB, error)
	GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	Close(ctx context.Context) error
}

// SQLPoolConfig tunes how each *sql.DB returned by the pool behaves.
type SQLPoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// GRPCPoolConfig tunes keep-alive behavior on pooled gRPC connections.
type GRPCPoolConfig struct {
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
}

// MinGRPCKeepaliveTime is the floor grpc-go enforces on a client's keepalive
// ping interval. WithKeepaliveParams raises anything smaller to this value
// (grpc@v1.81.1 dialoptions.go:562, internal.KeepaliveMinPingTime) and reports
// it only through the grpc library logger, which is not where an operator who
// just tuned the interval down will be looking.
//
// A KeepaliveTime below this is therefore not a faster ping — it is the same
// ping with a config value that says otherwise, which is worse than either.
// Callers that take the interval from an operator should reject a smaller
// value rather than pass it on.
const MinGRPCKeepaliveTime = 10 * time.Second

// ResourcePoolConfig groups all tunables for the default pool implementation.
type ResourcePoolConfig struct {
	SQL  SQLPoolConfig
	GRPC GRPCPoolConfig
}

// DefaultResourcePoolConfig returns the recommended defaults.
func DefaultResourcePoolConfig() ResourcePoolConfig {
	return ResourcePoolConfig{
		SQL:  SQLPoolConfig{MaxOpenConns: 25, MaxIdleConns: 5, ConnMaxLifetime: 30 * time.Minute},
		GRPC: GRPCPoolConfig{KeepaliveTime: 30 * time.Second, KeepaliveTimeout: 10 * time.Second},
	}
}

// ---------------------------------------------------------------------------
// Context plumbing
// ---------------------------------------------------------------------------

type poolCtxKey struct{}

// WithResourcePool returns a context carrying the given pool. Used by the
// execution runner; callers shouldn't need it directly.
// nil pool = no injection; resource-aware nodes will error at runtime.
func WithResourcePool(ctx context.Context, pool ResourcePool) context.Context {
	if pool == nil {
		return ctx
	}
	return context.WithValue(ctx, poolCtxKey{}, pool)
}

// ResourcePoolFromContext returns the pool installed by the runner, or nil
// if no pool is attached. Callers must treat nil as an error and refuse to
// construct per-call resources.
func ResourcePoolFromContext(ctx context.Context) ResourcePool {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(poolCtxKey{}).(ResourcePool)
	return v
}
