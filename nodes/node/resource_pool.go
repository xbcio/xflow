package node

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// ResourcePool is the per-process pool of network resources shared across
// node handler invocations. Without a pool, DatabaseNode and GRPCNode
// rebuild their *sql.DB / *grpc.ClientConn on every Execute, which defeats
// stdlib pooling and stresses upstream auth/TCP setup under realistic
// workflow load.
//
// Implementations must be safe for concurrent use. Close releases all
// resources and is idempotent.
//
// Spec: .claude/docs/specs/resource-pool.md
type ResourcePool interface {
	SQL(ctx context.Context, driver, dsn string) (*sql.DB, error)
	GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	Close() error
}

// SQLPoolConfig tunes how each *sql.DB returned by the pool behaves.
// Zero values use sensible defaults.
type SQLPoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func (c SQLPoolConfig) withDefaults() SQLPoolConfig {
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = 25
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 5
	}
	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = 30 * time.Minute
	}
	return c
}

// GRPCPoolConfig tunes keep-alive behavior on pooled gRPC connections.
type GRPCPoolConfig struct {
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
}

func (c GRPCPoolConfig) withDefaults() GRPCPoolConfig {
	if c.KeepaliveTime <= 0 {
		c.KeepaliveTime = 30 * time.Second
	}
	if c.KeepaliveTimeout <= 0 {
		c.KeepaliveTimeout = 10 * time.Second
	}
	return c
}

// ResourcePoolConfig groups all tunables for the default pool implementation.
type ResourcePoolConfig struct {
	SQL  SQLPoolConfig
	GRPC GRPCPoolConfig
}

// DefaultResourcePoolConfig returns the recommended defaults.
func DefaultResourcePoolConfig() ResourcePoolConfig {
	return ResourcePoolConfig{
		SQL:  SQLPoolConfig{}.withDefaults(),
		GRPC: GRPCPoolConfig{}.withDefaults(),
	}
}

// defaultResourcePool is the in-process implementation backing
// NewDefaultResourcePool. SQL handles are keyed by driver|dsn; gRPC
// connections by host|secureFlag.
type defaultResourcePool struct {
	cfg ResourcePoolConfig

	sqlMu sync.Mutex
	dbs   map[string]*sql.DB

	grpcMu sync.Mutex
	conns  map[string]*grpc.ClientConn

	closed bool
}

// NewDefaultResourcePool builds a process-scope pool. Pass to a backend via
// the backend's WithResourcePool option (memory or asynq).
func NewDefaultResourcePool(cfg ResourcePoolConfig) ResourcePool {
	return &defaultResourcePool{
		cfg:   cfg,
		dbs:   make(map[string]*sql.DB),
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (p *defaultResourcePool) SQL(_ context.Context, driver, dsn string) (*sql.DB, error) {
	key := driver + "|" + dsn
	p.sqlMu.Lock()
	defer p.sqlMu.Unlock()
	if p.closed {
		return nil, errPoolClosed
	}
	if db, ok := p.dbs[key]; ok {
		return db, nil
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	cfg := p.cfg.SQL.withDefaults()
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	p.dbs[key] = db
	return db, nil
}

func (p *defaultResourcePool) GRPC(_ context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	key := host + "|" + boolFlag(secure)
	p.grpcMu.Lock()
	defer p.grpcMu.Unlock()
	if p.closed {
		return nil, errPoolClosed
	}
	if conn, ok := p.conns[key]; ok {
		return conn, nil
	}
	cfg := p.cfg.GRPC.withDefaults()
	dialOpts := append([]grpc.DialOption{}, opts...)
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                cfg.KeepaliveTime,
		Timeout:             cfg.KeepaliveTimeout,
		PermitWithoutStream: true,
	}))
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return nil, err
	}
	p.conns[key] = conn
	return conn, nil
}

// Close shuts down every cached resource. Subsequent SQL/GRPC calls return
// errPoolClosed. Idempotent.
func (p *defaultResourcePool) Close() error {
	p.sqlMu.Lock()
	dbs := p.dbs
	p.dbs = nil
	p.closed = true
	p.sqlMu.Unlock()
	for _, db := range dbs {
		_ = db.Close()
	}
	p.grpcMu.Lock()
	conns := p.conns
	p.conns = nil
	p.grpcMu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return nil
}

func boolFlag(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// errPoolClosed is returned when SQL/GRPC are called after Close.
var errPoolClosed = poolClosedErr{}

type poolClosedErr struct{}

func (poolClosedErr) Error() string { return "node: resource pool is closed" }

// ---------------------------------------------------------------------------
// Context plumbing
// ---------------------------------------------------------------------------

type poolCtxKey struct{}

// WithResourcePool returns a context carrying the given pool. Used by the
// execution runner; callers shouldn't need it directly.
func WithResourcePool(ctx context.Context, pool ResourcePool) context.Context {
	if pool == nil {
		return ctx
	}
	return context.WithValue(ctx, poolCtxKey{}, pool)
}

// ResourcePoolFromContext returns the pool installed by the runner, or nil
// if no pool is attached. Nodes should treat nil as "fall back to per-call
// construction" so they keep working in test setups that don't wire a pool.
func ResourcePoolFromContext(ctx context.Context) ResourcePool {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(poolCtxKey{}).(ResourcePool)
	return v
}
