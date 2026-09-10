package action_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

// Before this file, every DatabaseNode test in the package stopped at
// acquireSQL's "no resource pool" permanent error (see
// TestDatabase_NoPoolIsPermanent in database_test.go). That means execSelect,
// execInsert, execInsertMany, execUpdate and execDelete -- the SQL-building
// and row-scanning bodies of the node -- had NEVER executed under any test.
// In particular the mutation this file exists to catch, swapping the two
// arguments of `fmt.Sprintf("SELECT %s FROM %s", cols, table)` at
// database.go:160, compiles cleanly and produces a query that MySQL rejects,
// but nothing in the suite ever sent that query anywhere.
//
// This gives DatabaseNode.Execute a real *sql.DB backed by the local test
// MySQL (see test/env/docker-compose.yml) through a minimal stub
// types.ResourcePool, and drives insert / insert_many / select / update /
// delete through the real wire protocol end to end.

// probeMySQLDSN mirrors testMySQLDSN in cmd/xflow/dead_letter_reconcile_test.go
// (same local test MySQL: test/env/docker-compose.yml, localhost:3306, db
// "xflow", root password "xflow"). Duplicated rather than imported because
// that helper lives in package main.
func probeMySQLDSN() string {
	if dsn := os.Getenv("XFLOW_TEST_MYSQL_DSN"); dsn != "" {
		return dsn
	}
	port := envOrDefault("MYSQL_PORT", "3306")
	pw := envOrDefault("MYSQL_ROOT_PASSWORD", "xflow")
	dbname := envOrDefault("MYSQL_DATABASE", "xflow")
	return fmt.Sprintf("root:%s@tcp(127.0.0.1:%s)/%s?parseTime=true&multiStatements=true", pw, port, dbname)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// probeMySQLEndpoint describes where probeMySQLDSN points, WITHOUT the
// credential: the DSN embeds MYSQL_ROOT_PASSWORD, and skip text reaches CI
// artifacts and terminal scrollback (test/integration/harness.go:88-92 makes
// the same call for its own message). A caller-supplied XFLOW_TEST_MYSQL_DSN
// is named rather than parsed, so that password stays one parsing bug further
// from the log.
func probeMySQLEndpoint() string {
	if os.Getenv("XFLOW_TEST_MYSQL_DSN") != "" {
		return "the DSN in $XFLOW_TEST_MYSQL_DSN"
	}
	return "127.0.0.1:" + envOrDefault("MYSQL_PORT", "3306")
}

// skipOrFailProbeMySQL skips, or fails under XFLOW_REQUIRE_MYSQL_INTEGRATION=1,
// mirroring test/integration/harness.go's requireMySQL. Without the escalation
// branch, the only coverage execSelect / execInsert / execInsertMany /
// execUpdate / execDelete have against a real parser vanishes into a reported
// ok whenever MySQL is down. Callers must pass an endpoint, never a DSN.
func skipOrFailProbeMySQL(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("XFLOW_REQUIRE_MYSQL_INTEGRATION") == "1" {
		t.Fatalf("XFLOW_REQUIRE_MYSQL_INTEGRATION=1: "+format, args...)
	}
	t.Skipf(format, args...)
}

// stubSQLPool implements types.ResourcePool by handing back one pre-opened
// *sql.DB regardless of the requested driver/dsn. DatabaseNode never sees the
// difference: acquireSQL only cares that pool.SQL(...) returns a *sql.DB, and
// the node's own DSN/driver validation already ran before acquireSQL is
// reached. Routing through a real MySQL connection (rather than a
// database/sql/driver fake) means the exact SQL text DatabaseNode builds has
// to survive a real parser.
type stubSQLPool struct{ db *sql.DB }

func (p *stubSQLPool) SQL(ctx context.Context, driver, dsn string) (*sql.DB, error) {
	return p.db, nil
}

func (p *stubSQLPool) GRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, fmt.Errorf("stubSQLPool: GRPC is not supported by this database-only stub")
}

func (p *stubSQLPool) Close(ctx context.Context) error { return p.db.Close() }

// requireMySQLProbe opens the local test MySQL, skipping the test when it is
// unreachable, and creates a fresh probe table for the caller. The table name
// is a fixed constant, so it is dropped both before creation and during
// cleanup: the pre-drop keeps a run that died mid-test from poisoning the
// next one, and the cleanup keeps this package from leaving residue in a
// MySQL instance other packages share.
func requireMySQLProbe(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := probeMySQLDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		skipOrFailProbeMySQL(t, "mysql dsn unusable: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		skipOrFailProbeMySQL(t, "mysql unavailable at %s: %v (run `make env-up && make env-migrate` for real-DB coverage)",
			probeMySQLEndpoint(), err)
	}

	const table = "xflow_action_dbnode_probe"
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		_ = db.Close()
		t.Fatalf("drop probe table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + table + ` (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(64) NOT NULL,
		age INT NOT NULL,
		tag VARCHAR(64) NOT NULL
	) ENGINE=InnoDB`); err != nil {
		_ = db.Close()
		t.Fatalf("create probe table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		_ = db.Close()
	})
	return db, table
}

// TestDatabaseNode_MySQL_RoundTrip drives all five DatabaseNode operations
// against a real MySQL table through the registry-looked-up handler, the same
// path a running workflow takes.
func TestDatabaseNode_MySQL_RoundTrip(t *testing.T) {
	db, table := requireMySQLProbe(t)
	pool := &stubSQLPool{db: db}
	ctx := types.WithResourcePool(context.Background(), pool)

	h, ok := registry.Lookup("xflow.database")
	if !ok {
		t.Fatal("xflow.database is not registered")
	}

	credResolver := func(namespace.Namespace, string) map[string]any {
		return map[string]any{"dsn": "unused-by-stub-pool", "driver": "mysql"}
	}

	run := func(params map[string]any) (*types.Output, error) {
		input := &types.Input{Params: params}
		input.SetNamespace(namespace.Default)
		input.SetCredentialResolver(credResolver)
		return h.Execute(ctx, input)
	}

	t.Run("Insert", func(t *testing.T) {
		b := node.Database("insert", table, "db").
			SetData(map[string]any{"name": "alice", "age": 30, "tag": "vip"})
		out, err := run(b.RawParams().(map[string]any))
		if err != nil {
			t.Fatalf("insert: unexpected error: %v", err)
		}
		if out.Data["rows_affected"] != int64(1) {
			t.Fatalf("insert: rows_affected = %v, want 1", out.Data["rows_affected"])
		}
		aliceID, _ := out.Data["last_insert_id"].(int64)
		if aliceID <= 0 {
			t.Fatalf("insert: last_insert_id = %v, want > 0", out.Data["last_insert_id"])
		}
	})

	t.Run("InsertMany", func(t *testing.T) {
		// node.Database's SetData only accepts a map (single-row shape), so
		// insert_many's array-of-rows param is built directly, the same way
		// the surrounding database_test.go builds params it has no builder
		// method for.
		out, err := run(map[string]any{
			"operation":  "insert_many",
			"table":      table,
			"credential": "db",
			"data": []any{
				map[string]any{"name": "bob", "age": 25, "tag": "std"},
				map[string]any{"name": "carol", "age": 40, "tag": "std"},
			},
		})
		if err != nil {
			t.Fatalf("insert_many: unexpected error: %v", err)
		}
		if out.Data["rows_affected"] != int64(2) {
			t.Fatalf("insert_many: rows_affected = %v, want 2", out.Data["rows_affected"])
		}
	})

	// Select is the exact mutation target at database.go:160:
	// `fmt.Sprintf("SELECT %s FROM %s", cols, table)`. Swapping the two
	// arguments turns the projected column list into the FROM clause and the
	// table name into the projection -- MySQL rejects that outright (no such
	// table "id, age") rather than returning wrong-but-plausible data, so a
	// mutation here must turn this subtest red, not silently corrupt it.
	// The WHERE clause and column projection are also real assertions here,
	// not just "did it error": a swapped or dropped WHERE/columns argument
	// would return rows with the wrong shape or the wrong tenant's data
	// while still reporting success.
	t.Run("Select", func(t *testing.T) {
		out, err := run(map[string]any{
			"operation":  "select",
			"table":      table,
			"credential": "db",
			"columns":    []any{"id", "name", "age"},
			"where":      map[string]any{"tag": "std"},
			"limit":      10,
		})
		if err != nil {
			t.Fatalf("select: unexpected error: %v", err)
		}
		if out.Data["count"] != 2 {
			t.Fatalf("select: count = %v, want 2 (bob+carol; alice is tag=vip)", out.Data["count"])
		}
		rows, _ := out.Data["rows"].([]map[string]any)
		if len(rows) != 2 {
			t.Fatalf("select: rows = %#v, want 2 entries", rows)
		}
		names := map[string]bool{}
		for _, row := range rows {
			name, _ := row["name"].(string)
			names[name] = true
			if _, hasTag := row["tag"]; hasTag {
				t.Fatalf("select: row %#v has column %q, which was not in the requested columns list", row, "tag")
			}
			if _, hasID := row["id"]; !hasID {
				t.Fatalf("select: row %#v is missing requested column %q", row, "id")
			}
		}
		if !names["bob"] || !names["carol"] {
			t.Fatalf("select: got names %v, want exactly {bob, carol}", names)
		}
		if names["alice"] {
			t.Fatal("select: WHERE tag='std' returned alice (tag=vip) -- filter was not applied")
		}
	})

	t.Run("Update", func(t *testing.T) {
		out, err := run(map[string]any{
			"operation":  "update",
			"table":      table,
			"credential": "db",
			"data":       map[string]any{"tag": "updated"},
			"where":      map[string]any{"name": "alice"},
		})
		if err != nil {
			t.Fatalf("update: unexpected error: %v", err)
		}
		if out.Data["rows_affected"] != int64(1) {
			t.Fatalf("update: rows_affected = %v, want 1", out.Data["rows_affected"])
		}
		var tag string
		if err := db.QueryRow("SELECT tag FROM "+table+" WHERE name = ?", "alice").Scan(&tag); err != nil {
			t.Fatalf("verify update: %v", err)
		}
		if tag != "updated" {
			t.Fatalf("verify update: alice.tag = %q, want %q -- the SET clause the node built did not "+
				"actually change the targeted row", tag, "updated")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		out, err := run(map[string]any{
			"operation":  "delete",
			"table":      table,
			"credential": "db",
			"where":      map[string]any{"name": "bob"},
		})
		if err != nil {
			t.Fatalf("delete: unexpected error: %v", err)
		}
		if out.Data["rows_affected"] != int64(1) {
			t.Fatalf("delete: rows_affected = %v, want 1", out.Data["rows_affected"])
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("verify delete: %v", err)
		}
		if count != 2 {
			t.Fatalf("verify delete: remaining rows = %d, want 2 (alice, carol) -- either the wrong row "+
				"was deleted or none was", count)
		}
	})
}
