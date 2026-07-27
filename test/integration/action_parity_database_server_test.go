//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// TestDatabaseActionErrorParityServerRunner closes the A3 §6 server-runner
// row of the xflow.database action-error parity matrix. It drives the same
// four fixtures as TestDatabaseActionErrorParity (local topology) through the
// real Redis/HTTP server-runner topology, but instead of a fake database/sql
// driver it points the node at a real MySQL container so the production
// classifier (node/internal/action/db_errors.go) is exercised against live
// InnoDB errors.
//
// The four fixtures and their expected outcomes:
//
//   - db_no_pool_permanent          — pool: nil  → permanent database.no_pool (attempt 1)
//   - db_bad_conn_transient_exhausted — accept-and-close TCP listener → MySQL
//     driver v1.10.0 wraps the handshake EOF as ErrInvalidConn ("invalid
//     connection"), a plain error that is not driver.ErrBadConn/io.EOF, so the
//     classifier routes it to database.unknown (still transient). The local
//     fake row asserts database.connection_lost; the server-runner row asserts
//     database.unknown. Both are transient + exhausted at attempt 2, so the
//     parity contract (transient retry exhausts MaxAttempts) is preserved — only
//     the specific classified code differs, due to how the real driver surfaces
//     a handshake-time connection drop. See task-39-design.md §5.2 and the
//     task-40 report.
//   - db_lock_wait_transient_exhausted — real InnoDB row lock held by a background
//     transaction with innodb_lock_wait_timeout=1 → MySQL 1205 (lock-wait
//     timeout) → transient (attempt 2). 1205 shares the same classifier branch
//     as the local fake's 1213 deadlock; the parity contract (transient +
//     exhausted) is preserved. See .superpowers/sdd-remediation/task-39-design.md §5.3.
//   - db_constraint_permanent       — pre-inserted row id=1 → MySQL 1062 (attempt 1)
//
// This test deliberately uses the databaseParityHandler wrapper path (injected
// real resource.NewDefaultResourcePool + credential resolver pointing at the
// real MySQL DSN) so it can reproduce the no-pool and bad-conn edge cases that
// the production wiring cannot (pool: nil, arbitrary DSN). The production
// ResourcePool/credential-resolver wiring is covered by the sibling
// TestDatabaseActionErrorParityServerRunnerProductionWiring below, which
// registers the real (unwrapped) xflow.database handler and installs the pool
// via runnersvc.Config.
func TestDatabaseActionErrorParityServerRunner(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	// Open admin handle first, lower the lock-wait timeout, then apply the
	// idempotent parity schema. Shortening the timeout before the cleanup DELETE
	// ensures a stale row lock from a crashed prior run does not hang for 50s.
	adminDB := openParityAdminDB(t, dsn)
	shortenLockWaitTimeout(t, adminDB, 1)
	setupParitySchema(t, adminDB)

	inner, ok := registry.Lookup("xflow.database")
	if !ok {
		t.Fatal("xflow.database handler not registered")
	}

	cases := []struct {
		name                   string
		wantAttempt            int
		wantStatus             types.ExecutionStatus
		errContains            string
		wantHandlerInvocations int
		build                  func(t *testing.T, dsn string, inner types.ActionHandler) (types.NodeDef, func(engine.HandlerRegistrar), func() int)
	}{
		{
			name:                   "db_no_pool_permanent",
			wantAttempt:            1,
			wantStatus:             types.ExecutionStatusFailed,
			errContains:            "database.no_pool",
			wantHandlerInvocations: 1,
			build: func(t *testing.T, dsn string, inner types.ActionHandler) (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				db := node.Database("select", "parity_constraint", "db")
				source := types.NodeDef{
					Name:       "start",
					Type:       "xflow.database",
					Parameters: db.RawParams().(map[string]any),
				}
				var cur *databaseParityHandler
				register := func(reg engine.HandlerRegistrar) {
					h := &databaseParityHandler{
						inner: inner,
						cred: map[string]any{
							"dsn":    dsn,
							"driver": "mysql",
						},
						pool: nil, // no pool → database.no_pool (permanent)
					}
					cur = h
					reg.RegisterGlobal("xflow.database", h)
				}
				invocations := func() int {
					if cur == nil {
						return 0
					}
					return cur.Count()
				}
				return source, register, invocations
			},
		},
		{
			name:                   "db_bad_conn_transient_exhausted",
			wantAttempt:            2,
			wantStatus:             types.ExecutionStatusFailed,
			errContains:            "database.unknown",
			wantHandlerInvocations: 2,
			build: func(t *testing.T, dsn string, inner types.ActionHandler) (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				badAddr := newAcceptCloseListener(t)
				badDSN := badConnDSN(t, badAddr)
				pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = pool.Close(ctx)
				})
				db := node.Database("select", "parity_constraint", "db")
				source := types.NodeDef{
					Name:       "start",
					Type:       "xflow.database",
					Parameters: db.RawParams().(map[string]any),
				}
				var cur *databaseParityHandler
				register := func(reg engine.HandlerRegistrar) {
					h := &databaseParityHandler{
						inner: inner,
						cred: map[string]any{
							"dsn":    badDSN,
							"driver": "mysql",
						},
						pool: pool,
					}
					cur = h
					reg.RegisterGlobal("xflow.database", h)
				}
				invocations := func() int {
					if cur == nil {
						return 0
					}
					return cur.Count()
				}
				return source, register, invocations
			},
		},
		{
			name:                   "db_lock_wait_transient_exhausted",
			wantAttempt:            2,
			wantStatus:             types.ExecutionStatusFailed,
			errContains:            "1205",
			wantHandlerInvocations: 2,
			build: func(t *testing.T, dsn string, inner types.ActionHandler) (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				release := holdDeadlockRow(t, dsn)
				t.Cleanup(release)
				pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = pool.Close(ctx)
				})
				db := node.Database("update", "parity_deadlock", "db").
					SetWhere(map[string]any{"id": 1}).
					SetData(map[string]any{"v": 2})
				source := types.NodeDef{
					Name:       "start",
					Type:       "xflow.database",
					Parameters: db.RawParams().(map[string]any),
				}
				var cur *databaseParityHandler
				register := func(reg engine.HandlerRegistrar) {
					h := &databaseParityHandler{
						inner: inner,
						cred: map[string]any{
							"dsn":    dsn,
							"driver": "mysql",
						},
						pool: pool,
					}
					cur = h
					reg.RegisterGlobal("xflow.database", h)
				}
				invocations := func() int {
					if cur == nil {
						return 0
					}
					return cur.Count()
				}
				return source, register, invocations
			},
		},
		{
			name:                   "db_constraint_permanent",
			wantAttempt:            1,
			wantStatus:             types.ExecutionStatusFailed,
			errContains:            "1062",
			wantHandlerInvocations: 1,
			build: func(t *testing.T, dsn string, inner types.ActionHandler) (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				seedConstraintRow(t, adminDB)
				pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = pool.Close(ctx)
				})
				db := node.Database("insert", "parity_constraint", "db").
					SetData(map[string]any{"id": 1})
				source := types.NodeDef{
					Name:       "start",
					Type:       "xflow.database",
					Parameters: db.RawParams().(map[string]any),
				}
				var cur *databaseParityHandler
				register := func(reg engine.HandlerRegistrar) {
					h := &databaseParityHandler{
						inner: inner,
						cred: map[string]any{
							"dsn":    dsn,
							"driver": "mysql",
						},
						pool: pool,
					}
					cur = h
					reg.RegisterGlobal("xflow.database", h)
				}
				invocations := func() int {
					if cur == nil {
						return 0
					}
					return cur.Count()
				}
				return source, register, invocations
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, register, inv := tc.build(t, dsn, inner)
			retry := &types.RetrySettings{
				MaxAttempts:     2,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			// server-runner topology (real Redis/HTTP + real MySQL).
			serverOut := RunParityServerRunner(t, addr, def, register, nil, tc.name, "server-runner")

			// Cluster-durable topology: the databaseParityHandler wrapper
			// self-injects cred+pool, so the same register closure works for the
			// cluster's in-process EmbeddedDispatcher (no production wiring
			// change — no distributed.WithCredentialResolver needed for the
			// wrapper path). Both server-runner and cluster hit the SAME real
			// MySQL, so their classified codes must match exactly (assertParity).
			//
			// The local-fake topology (TestDatabaseActionErrorParity) uses a
			// fake driver whose codes diverge from real MySQL for bad-conn
			// (database.connection_lost vs database.unknown) and deadlock (1213
			// vs 1205) — a documented divergence — so three-way ErrStr equality
			// does not hold and is not asserted here. The local row is still
			// covered by its own test; this matrix closes the
			// server-runner↔cluster real-MySQL pair.
			clusterOut := RunParityCluster(t, addr, def, register, nil, tc.name, "cluster-durable")

			invocations := invCount(inv)
			serverOut.HandlerInvocations = invocations
			clusterOut.HandlerInvocations = invocations

			pc := parityCase{
				Name:                   tc.name,
				WantAttempt:            tc.wantAttempt,
				WantStatus:             tc.wantStatus,
				ErrContains:            tc.errContains,
				WantHandlerInvocations: tc.wantHandlerInvocations,
				WantKind:               "",
				WantRetryable:          false,
			}
			switch tc.name {
			case "db_no_pool_permanent", "db_constraint_permanent":
				pc.WantKind, pc.WantRetryable = string(types.ErrorKindPermanent), false
			case "db_bad_conn_transient_exhausted", "db_lock_wait_transient_exhausted":
				pc.WantKind, pc.WantRetryable = string(types.ErrorKindTransient), true
			}
			logParityMatrixRow(t, pc, "server-runner", serverOut)
			logParityMatrixRow(t, pc, "cluster-durable", clusterOut)

			// Asserts server-runner == cluster exact parity (Attempt, Status,
			// SourceStatus, ErrStr, ErrCode, Port, kind/retryable, handler
			// invocations) AND that both independently match the contract
			// (wantAttempt/wantStatus/errContains/wantHandlerInvocations).
			assertParity(t, pc, serverOut, clusterOut)
		})
	}
}

// openParityAdminDB opens an admin *sql.DB against the real MySQL DSN. The
// returned handle is reused by per-case setup helpers; it is closed via t.Cleanup.
func openParityAdminDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open admin db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// setupParitySchema applies the idempotent parity DDL and clears any leftover
// rows from a previous interrupted run. The caller must supply an already-open
// admin *sql.DB.
func setupParitySchema(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, ddl := range []string{
		"CREATE TABLE IF NOT EXISTS parity_constraint (id INT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS parity_deadlock (id INT PRIMARY KEY, v INT) ENGINE=InnoDB",
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("apply parity DDL %q: %v", ddl, err)
		}
	}
	// Start each run from a clean slate so leftover rows from a previous run
	// (e.g. an interrupted test) cannot flip a 1062/constraint outcome.
	if _, err := db.ExecContext(ctx, "DELETE FROM parity_constraint"); err != nil {
		t.Fatalf("clear parity_constraint: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM parity_deadlock"); err != nil {
		t.Fatalf("clear parity_deadlock: %v", err)
	}
}

// shortenLockWaitTimeout lowers the GLOBAL innodb_lock_wait_timeout so the
// lock-wait fixture's blocked UPDATE returns MySQL 1205 within ~1s instead of
// the default 50s. The previous value is restored in t.Cleanup. This affects
// the shared test container for the test's duration; the save/restore plus
// the case's short runtime make concurrent interference unlikely.
func shortenLockWaitTimeout(t *testing.T, db *sql.DB, seconds int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var prev int
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.innodb_lock_wait_timeout").Scan(&prev); err != nil {
		t.Fatalf("read innodb_lock_wait_timeout: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL innodb_lock_wait_timeout = %d", seconds)); err != nil {
		t.Fatalf("set innodb_lock_wait_timeout: %v", err)
	}
	t.Cleanup(func() {
		c2, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, _ = db.ExecContext(c2, fmt.Sprintf("SET GLOBAL innodb_lock_wait_timeout = %d", prev))
	})
}

// holdDeadlockRow opens a dedicated connection, begins a transaction, and
// inserts (or touches) row id=1 in parity_deadlock without committing. The
// held transaction keeps an exclusive row lock so the node's concurrent UPDATE
// blocks until innodb_lock_wait_timeout elapses (MySQL 1205). The returned
// release func rolls the transaction back and closes the connection so the lock
// is freed for subsequent cases.
func holdDeadlockRow(t *testing.T, dsn string) func() {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open deadlock db: %v", err)
	}
	connCtx, connCancel := context.WithCancel(context.Background())
	conn, err := db.Conn(connCtx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("acquire deadlock conn: %v", err)
	}
	tx, err := conn.BeginTx(connCtx, nil)
	if err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("begin deadlock tx: %v", err)
	}
	// INSERT ... ON DUPLICATE KEY UPDATE creates the row if absent and acquires
	// an X lock on id=1 regardless. Held without commit.
	if _, err := tx.ExecContext(connCtx,
		"INSERT INTO parity_deadlock (id, v) VALUES (1, 0) ON DUPLICATE KEY UPDATE v = v"); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("seed+lock parity_deadlock row: %v", err)
	}

	return func() {
		_ = tx.Rollback()
		connCancel()
		_ = conn.Close()
		_ = db.Close()
	}
}

// seedConstraintRow ensures parity_constraint has row id=1 so the node's
// INSERT of {id:1} yields MySQL 1062. The row is removed in t.Cleanup so the
// table is left empty for the next run.
func seedConstraintRow(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "DELETE FROM parity_constraint"); err != nil {
		t.Fatalf("clear parity_constraint: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO parity_constraint (id) VALUES (1)"); err != nil {
		t.Fatalf("seed parity_constraint: %v", err)
	}
	t.Cleanup(func() {
		c2, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, _ = db.ExecContext(c2, "DELETE FROM parity_constraint")
	})
}

// newAcceptCloseListener starts a TCP listener on an ephemeral port that
// accepts each inbound connection and immediately closes it. The MySQL driver
// dials, begins the handshake, and receives EOF; the driver wraps it as
// ErrInvalidConn ("invalid connection"), a plain error that the classifier
// routes to database.unknown (transient). The loop serves one accept per engine
// attempt (MaxAttempts=2 → two accepts). The listener is closed in t.Cleanup.
func newAcceptCloseListener(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return // listener closed
			}
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().String()
}

// badConnDSN builds a MySQL DSN whose host:port points at the accept-and-close
// listener. Credentials are sourced from the same env vars as mysqlDSN (no new
// secrets); the connection never authenticates because the listener closes
// before the handshake completes.
func badConnDSN(t *testing.T, addr string) string {
	t.Helper()
	pw := envOr("MYSQL_ROOT_PASSWORD", "xflow")
	dbName := envOr("MYSQL_DATABASE", "xflow")
	return fmt.Sprintf("root:%s@tcp(%s)/%s?parseTime=true&multiStatements=true", pw, addr, dbName)
}

// TestDatabaseActionErrorParityServerRunnerProductionWiring exercises the
// production ResourcePool + CredentialResolver wiring added in task-42.
// Unlike TestDatabaseActionErrorParityServerRunner (which registers the
// databaseParityHandler test wrapper to inject a pool + resolver), this test
// builds a runnersvc.Config with ResourcePool and CredentialResolver set, and
// registers the real xflow.database handler directly (no wrapper) into the
// execution.Registry. This proves the production gap is closed end-to-end:
// pool/credential-consuming nodes work in real server-runner mode.
//
// The constraint fixture asserts a successful SELECT against the real MySQL
// container through the production wiring path. The no-pool fixture is covered
// by the wrapper-based test above (the production path always supplies a pool
// when configured, so it cannot reproduce database.no_pool by design).
func TestDatabaseActionErrorParityServerRunnerProductionWiring(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	adminDB := openParityAdminDB(t, dsn)
	shortenLockWaitTimeout(t, adminDB, 1)
	setupParitySchema(t, adminDB)

	inner, ok := registry.Lookup("xflow.database")
	if !ok {
		t.Fatal("xflow.database handler not registered")
	}

	cases := []struct {
		name        string
		wantAttempt int
		wantStatus  types.ExecutionStatus
		errContains string
		cred        map[string]map[string]any
		build       func(t *testing.T, dsn string) (types.NodeDef, func(engine.HandlerRegistrar))
	}{
		{
			name:        "db_production_wiring_select_success",
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusSuccess,
			errContains: "",
			cred: map[string]map[string]any{
				"db": {"dsn": dsn, "driver": "mysql"},
			},
			build: func(t *testing.T, dsn string) (types.NodeDef, func(engine.HandlerRegistrar)) {
				seedConstraintRow(t, adminDB) // ensures parity_constraint has row id=1
				db := node.Database("select", "parity_constraint", "db")
				source := types.NodeDef{
					Name:       "start",
					Type:       "xflow.database",
					Parameters: db.RawParams().(map[string]any),
				}
				// Production wiring: register the real handler directly. The
				// pool and credential resolver come from runnersvc.Config below,
				// not from a wrapper.
				register := func(reg engine.HandlerRegistrar) {
					reg.RegisterGlobal("xflow.database", inner)
				}
				return source, register
			},
		},
		{
			name:        "db_production_wiring_constraint_permanent",
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "1062",
			cred: map[string]map[string]any{
				"db": {"dsn": dsn, "driver": "mysql"},
			},
			build: func(t *testing.T, dsn string) (types.NodeDef, func(engine.HandlerRegistrar)) {
				seedConstraintRow(t, adminDB)
				db := node.Database("insert", "parity_constraint", "db").
					SetData(map[string]any{"id": 1})
				source := types.NodeDef{
					Name:       "start",
					Type:       "xflow.database",
					Parameters: db.RawParams().(map[string]any),
				}
				register := func(reg engine.HandlerRegistrar) {
					reg.RegisterGlobal("xflow.database", inner)
				}
				return source, register
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, register := tc.build(t, dsn)
			retry := &types.RetrySettings{
				MaxAttempts:     2,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = pool.Close(ctx)
			})
			cred := tc.cred
			resolver := func(namespace namespace.Namespace, name string) map[string]any { return cred[name] }

			out := runParityServerRunnerWithPool(t, addr, def, register, pool, resolver, nil, tc.name, "server-runner")

			if out.Attempt != tc.wantAttempt {
				t.Errorf("server-runner attempt=%d, want %d", out.Attempt, tc.wantAttempt)
			}
			if out.Status != tc.wantStatus {
				t.Errorf("server-runner status=%s, want %s", out.Status, tc.wantStatus)
			}
			if tc.errContains != "" && !strings.Contains(out.ErrStr, tc.errContains) {
				t.Errorf("server-runner error=%q, want substring %q", out.ErrStr, tc.errContains)
			}
		})
	}
}

// runParityServerRunnerWithPool is a variant of RunParityServerRunner that
// installs a ResourcePool and CredentialResolver on the runnersvc.Config,
// exercising the production wiring path. The register callback installs the
// real (unwrapped) handler into the execution.Registry.
func runParityServerRunnerWithPool(t *testing.T, addr string, def *types.WorkflowDef, register func(engine.HandlerRegistrar), pool types.ResourcePool, resolver func(namespace namespace.Namespace, name string) map[string]any, rec *evidenceRecorder, fixture, topology string) ParityOutcome {
	t.Helper()
	h := newServerRunnerHarness(t, addr, 1)
	if len(def.Nodes) == 0 {
		t.Fatal("runParityServerRunnerWithPool: workflow has no nodes")
	}

	reg := execution.NewRegistry()
	if register != nil {
		register(reg)
	}

	capSet := make(map[string]struct{})
	for _, n := range def.Nodes {
		capSet[n.Type] = struct{}{}
	}
	caps := make([]protocol.Capability, 0, len(capSet))
	for nodeType := range capSet {
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		reg,
		runnersvc.Config{
			RunnerID:           "runner-parity-prod-1",
			Concurrency:        1,
			Capabilities:       caps,
			PollWait:           5 * time.Millisecond,
			ResourcePool:       pool,
			CredentialResolver: resolver,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-parity-prod-1")

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, nil)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, def.Nodes[0].Name)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	return collectParityOutcome(t, h.state, execID, result, def, h.evidence, rec, fixture, topology)
}
