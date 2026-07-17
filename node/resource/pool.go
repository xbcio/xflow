// Package resource provides the default in-process ResourcePool
// implementation shared by DatabaseNode and GRPCNode.
package resource

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// defaultResourcePool is the in-process implementation backing
// NewDefaultResourcePool. SQL handles are keyed by driver|dsn; gRPC
// connections by host|secureFlag.
type defaultResourcePool struct {
	cfg types.ResourcePoolConfig

	sqlMu sync.Mutex
	dbs   map[string]*sql.DB

	grpcMu sync.Mutex
	conns  map[string]*grpc.ClientConn

	closed atomic.Bool
}

// NewDefaultResourcePool builds a process-scope pool. Pass to a backend via
// the backend's WithResourcePool option (memory or distributed).
func NewDefaultResourcePool(cfg types.ResourcePoolConfig) types.ResourcePool {
	cfg = normalizeConfig(cfg)
	return &defaultResourcePool{
		cfg:   cfg,
		dbs:   make(map[string]*sql.DB),
		conns: make(map[string]*grpc.ClientConn),
	}
}

func normalizeConfig(cfg types.ResourcePoolConfig) types.ResourcePoolConfig {
	if cfg.SQL.MaxOpenConns <= 0 {
		cfg.SQL.MaxOpenConns = 25
	}
	if cfg.SQL.MaxIdleConns <= 0 {
		cfg.SQL.MaxIdleConns = 5
	}
	if cfg.SQL.ConnMaxLifetime <= 0 {
		cfg.SQL.ConnMaxLifetime = 30 * time.Minute
	}
	if cfg.GRPC.KeepaliveTime <= 0 {
		cfg.GRPC.KeepaliveTime = 30 * time.Second
	}
	if cfg.GRPC.KeepaliveTimeout <= 0 {
		cfg.GRPC.KeepaliveTimeout = 10 * time.Second
	}
	return cfg
}

func (p *defaultResourcePool) SQL(_ context.Context, driver, dsn string) (*sql.DB, error) {
	key := driver + "|" + dsn
	p.sqlMu.Lock()
	defer p.sqlMu.Unlock()
	if p.closed.Load() {
		return nil, errPoolClosed
	}
	if db, ok := p.dbs[key]; ok {
		return db, nil
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(p.cfg.SQL.MaxOpenConns)
	db.SetMaxIdleConns(p.cfg.SQL.MaxIdleConns)
	db.SetConnMaxLifetime(p.cfg.SQL.ConnMaxLifetime)
	p.dbs[key] = db
	return db, nil
}

func (p *defaultResourcePool) GRPC(_ context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	key := host + "|" + boolFlag(secure)
	p.grpcMu.Lock()
	defer p.grpcMu.Unlock()
	if p.closed.Load() {
		return nil, errPoolClosed
	}
	if conn, ok := p.conns[key]; ok {
		return conn, nil
	}
	dialOpts := append([]grpc.DialOption{}, opts...)
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                p.cfg.GRPC.KeepaliveTime,
		Timeout:             p.cfg.GRPC.KeepaliveTimeout,
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
// errPoolClosed. Idempotent. ctx bounds the wait for in-flight close
// goroutines; on timeout the close continues in the background.
func (p *defaultResourcePool) Close(ctx context.Context) error {
	p.sqlMu.Lock()
	dbs := p.dbs
	p.dbs = nil
	p.closed.Store(true)
	p.sqlMu.Unlock()

	// Note: ctx timeout is only checked after acquiring grpcMu below. In
	// practice grpc.NewClient is non-blocking, so GRPC holds grpcMu briefly;
	// this is a theoretical constraint, not a practical one.
	p.grpcMu.Lock()
	conns := p.conns
	p.conns = nil
	p.grpcMu.Unlock()

	errCh := make(chan error, 2)
	go func() { errCh <- closeAllDBs(dbs) }()
	go func() { errCh <- closeAllConns(conns) }()

	for i := 0; i < 2; i++ {
		select {
		case <-errCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func closeAllDBs(dbs map[string]*sql.DB) error {
	var firstErr error
	for _, db := range dbs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func closeAllConns(conns map[string]*grpc.ClientConn) error {
	var firstErr error
	for _, c := range conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
