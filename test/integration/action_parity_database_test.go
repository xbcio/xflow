//go:build integration

package integration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	mysqldriver "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

// TestDatabaseActionErrorParity exercises the xflow.database action error
// classification through the local embedded topology. The same classified error
// is produced by a deterministic fake ResourcePool so the tests do not require
// a real MySQL server.
//
// The server-runner topology is covered by a sibling test,
// TestDatabaseActionErrorParityServerRunner in action_parity_database_server_test.go,
// which drives the same four fixtures through the real Redis/HTTP runner
// against a live MySQL container.
func TestDatabaseActionErrorParity(t *testing.T) {
	inner, ok := registry.Lookup("xflow.database")
	if !ok {
		t.Fatal("xflow.database handler not registered")
	}

	cases := []struct {
		name        string
		pool        types.ResourcePool
		maxAttempts int
		wantAttempt int
		wantStatus  types.ExecutionStatus
		errContains string
	}{
		{
			name:        "db_no_pool_permanent",
			pool:        nil,
			maxAttempts: 3,
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "database.no_pool",
		},
		{
			name:        "db_bad_conn_transient_exhausted",
			pool:        newFakeDBPool(t, driver.ErrBadConn),
			maxAttempts: 2,
			wantAttempt: 2,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "database.connection_lost",
		},
		{
			name: "db_deadlock_transient_exhausted",
			pool: newFakeDBPool(t, &mysqldriver.MySQLError{
				Number:  1213,
				Message: "Deadlock found when trying to get lock; try restarting transaction",
			}),
			maxAttempts: 2,
			wantAttempt: 2,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "1213",
		},
		{
			name: "db_constraint_permanent",
			pool: newFakeDBPool(t, &mysqldriver.MySQLError{
				Number:  1062,
				Message: "Duplicate entry '1' for key 'PRIMARY'",
			}),
			maxAttempts: 3,
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "1062",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := node.Database("select", "users", "db")
			source := types.NodeDef{
				Name:       "start",
				Type:       "xflow.database",
				Parameters: db.RawParams().(map[string]any),
			}
			retry := &types.RetrySettings{
				MaxAttempts:     tc.maxAttempts,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			register := func(reg engine.HandlerRegistrar) {
				reg.RegisterGlobal("xflow.database", &databaseParityHandler{
					inner: inner,
					cred: map[string]any{
						"dsn":    "fake://fake",
						"driver": "mysql",
					},
					pool: tc.pool,
				})
			}

			localOut := RunParityLocal(t, def, register)

			if localOut.Attempt != tc.wantAttempt {
				t.Errorf("attempt=%d, want %d", localOut.Attempt, tc.wantAttempt)
			}
			if localOut.Status != tc.wantStatus {
				t.Errorf("status=%s, want %s", localOut.Status, tc.wantStatus)
			}
			if tc.errContains != "" && !strings.Contains(localOut.ErrStr, tc.errContains) {
				t.Errorf("error=%q, want substring %q", localOut.ErrStr, tc.errContains)
			}
		})
	}
}

// TestDatabaseActionErrorParityServerRunner is implemented in
// action_parity_database_server_test.go (real Redis + real MySQL server-runner
// topology).

// databaseParityHandler wraps the real xflow.database handler and injects a
// deterministic credential resolver and optional ResourcePool. This lets parity
// fixtures exercise the DatabaseNode's error classification through the engine
// without requiring a real MySQL server.
type databaseParityHandler struct {
	inner types.ActionHandler
	cred  map[string]any
	pool  types.ResourcePool
}

func (h *databaseParityHandler) Descriptor() types.Descriptor { return h.inner.Descriptor() }

func (h *databaseParityHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	input.SetCredentialResolver(func(name string) map[string]any { return h.cred })
	if h.pool != nil {
		ctx = types.WithResourcePool(ctx, h.pool)
	}
	return h.inner.Execute(ctx, input)
}

// fake driver machinery

var fakeDriverCounter int64

type fakeDBPool struct {
	driverName string
	dsn        string
	mu         sync.Mutex
	dbs        []*sql.DB
}

// newFakeDBPool registers a fresh database/sql driver that returns err on every
// Prepare and returns a ResourcePool backed by that driver. The pool tracks all
// *sql.DB instances it creates so they can be closed cleanly.
func newFakeDBPool(t *testing.T, err error) *fakeDBPool {
	t.Helper()
	n := atomic.AddInt64(&fakeDriverCounter, 1)
	driverName := fmt.Sprintf("fake_db_driver_%d", n)
	sql.Register(driverName, &fakeDriver{err: err})
	return &fakeDBPool{driverName: driverName, dsn: "fake://fake"}
}

func (p *fakeDBPool) SQL(ctx context.Context, driver, dsn string) (*sql.DB, error) {
	db, err := sql.Open(p.driverName, p.dsn)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.dbs = append(p.dbs, db)
	p.mu.Unlock()
	return db, nil
}

func (p *fakeDBPool) GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, errors.New("fakeDBPool: GRPC not supported")
}

func (p *fakeDBPool) Close(ctx context.Context) error {
	p.mu.Lock()
	dbs := p.dbs
	p.dbs = nil
	p.mu.Unlock()
	for _, db := range dbs {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

type fakeDriver struct {
	err error
}

func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	return &fakeConn{err: d.err}, nil
}

type fakeConn struct {
	err error
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	if c.err != nil {
		return nil, c.err
	}
	return nil, driver.ErrBadConn
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake driver: transactions not supported")
}
