package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// testMySQLDSN resolves a DSN for the local test MySQL (see
// test/env/docker-compose.yml: localhost:3306, db "xflow", root password
// "xflow") and skips the test when it is unreachable. This keeps
// runReconcile's only real-infra dependency from breaking `go test ./cmd/...`
// on a machine that never brought up the local test environment
// (`make env-up && make env-migrate`).
//
// It follows test/integration/harness.go's requireMySQL in both respects that
// matter, because this helper previously followed neither:
//
//   - Under XFLOW_REQUIRE_MYSQL_INTEGRATION=1 it FAILS instead of skipping.
//     A hand-rolled gate with no escalation branch is invisible to CI's
//     required mode: the dependency goes down, the test reports skip, the
//     suite reports ok, and nobody learns that reconcile's only real-DB
//     coverage did not run. This repo has already been bitten by exactly that
//     shape once (see the note at the top of
//     backend/providers/distributed/internal/rstate/deadletter_replay_test.go's
//     realRedisAddr).
//   - Neither branch echoes the DSN. It embeds MYSQL_ROOT_PASSWORD, and skip
//     text lands in CI artifacts and terminal scrollback; harness.go:88-92
//     makes the same point about its own message.
func testMySQLDSN(t *testing.T) string {
	t.Helper()
	dsn := envOr("XFLOW_TEST_MYSQL_DSN", "")
	if dsn == "" {
		port := envOr("MYSQL_PORT", "3306")
		pw := envOr("MYSQL_ROOT_PASSWORD", "xflow")
		db := envOr("MYSQL_DATABASE", "xflow")
		dsn = fmt.Sprintf("root:%s@tcp(127.0.0.1:%s)/%s?parseTime=true&multiStatements=true", pw, port, db)
	}
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		skipOrFailMySQL(t, "mysql dsn unusable: %v", err)
	}
	defer sqlDB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		skipOrFailMySQL(t, "mysql unavailable at %s: %v (run `make env-up && make env-migrate` for real-DB reconcile coverage)",
			mysqlEndpoint(), err)
	}
	return dsn
}

// mysqlEndpoint describes where testMySQLDSN points, without the credential.
//
// When the DSN came from XFLOW_TEST_MYSQL_DSN its shape is caller-supplied and
// unknown, so the variable is NAMED rather than parsed: extracting a host from
// an arbitrary DSN would put someone else's password one parsing bug away from
// the test log, and the operator who set the variable already knows what is in
// it.
func mysqlEndpoint() string {
	if envOr("XFLOW_TEST_MYSQL_DSN", "") != "" {
		return "the DSN in $XFLOW_TEST_MYSQL_DSN"
	}
	return "127.0.0.1:" + envOr("MYSQL_PORT", "3306")
}

// skipOrFailMySQL skips, or fails under XFLOW_REQUIRE_MYSQL_INTEGRATION=1.
// Callers must pass an endpoint, never a DSN.
func skipOrFailMySQL(t *testing.T, format string, args ...any) {
	t.Helper()
	if envOr("XFLOW_REQUIRE_MYSQL_INTEGRATION", "") == "1" {
		t.Fatalf("XFLOW_REQUIRE_MYSQL_INTEGRATION=1: "+format, args...)
	}
	t.Skipf(format, args...)
}

// seedReplayReceipt writes a raw Redis replay-receipt hash in the exact shape
// backend/providers/distributed/internal/rstate.ScanReplayReceipts expects
// (xflow:ns:<ns>:exec:{<execID>}:replay:receipt:<requestID>), plus the
// namespace-registry SADD so the scan's per-namespace fan-out visits it. The
// CLI never builds this key itself (Redis is authoritative and opaque to the
// CLI); this mirrors seedDeadLetterRedis in dead_letter_cli_test.go for the
// receipt shape instead of the dead-letter-entry shape.
func seedReplayReceipt(t *testing.T, addr, namespaceName, execID, requestID, auditID string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.SAdd(ctx, "xflow:namespaces", namespaceName).Err(); err != nil {
		t.Fatalf("sadd namespace: %v", err)
	}
	key := fmt.Sprintf("xflow:ns:%s:exec:{%s}:replay:receipt:%s", namespaceName, execID, requestID)
	if err := rdb.HSet(ctx, key,
		"audit_id", auditID,
		"node", "review",
		"activation", "1",
		"outcome", "replayed",
		"operator", "cli:tester",
		"reason", "operator rationale",
		"entry_id", "entry-"+execID,
		"ts_ms", fmt.Sprintf("%d", time.Now().UnixMilli()),
	).Err(); err != nil {
		t.Fatalf("hset receipt: %v", err)
	}
}

// TestRunReconcileProjectsThenSkipsOnRerun is the first coverage of
// runReconcile/reconcileStats: before this test, both were entirely
// unexercised, so a broken diff-scan (e.g. always reporting Projected, or
// never checking whether a receipt was already projected) could ship without
// any test going red.
//
// It seeds two authoritative Redis replay receipts, runs the diff-scan
// projector once (expecting both counted as newly Projected, 0 Skipped), and
// runs it again unchanged (expecting both counted as Skipped, 0 Projected —
// the idempotency guarantee the reconcile command's docstring promises: it
// "never re-executes a replay mutation" and re-running must not double-append
// SQL rows for the same receipt).
func TestRunReconcileProjectsThenSkipsOnRerun(t *testing.T) {
	dsn := testMySQLDSN(t)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ns := fmt.Sprintf("reconcile-test-%d", time.Now().UnixNano())
	auditA := fmt.Sprintf("audit-a-%d", time.Now().UnixNano())
	auditB := fmt.Sprintf("audit-b-%d", time.Now().UnixNano())
	seedReplayReceipt(t, mr.Addr(), ns, "exec-a", "req-a", auditA)
	seedReplayReceipt(t, mr.Addr(), ns, "exec-b", "req-b", auditB)

	t.Cleanup(func() {
		p, err := mysqlstore.New(dsn)
		if err != nil {
			return
		}
		p.DB().WithContext(context.Background()).
			Exec("DELETE FROM xflow_audit_events WHERE receipt_audit_id IN (?, ?)", auditA, auditB)
	})

	var out bytes.Buffer
	opts := &deadLetterOptions{mysqlDSN: dsn, out: &out}

	if err := runReconcile(opts, mr.Addr(), false); err != nil {
		t.Fatalf("runReconcile (1st run): %v", err)
	}
	stats := decodeReconcileStats(t, out.Bytes())
	if stats.Scanned != 2 || stats.Projected != 2 || stats.Skipped != 0 || stats.Failed != 0 {
		t.Fatalf("1st run stats = %+v, want Scanned=2 Projected=2 Skipped=0 Failed=0", stats)
	}

	out.Reset()
	if err := runReconcile(opts, mr.Addr(), false); err != nil {
		t.Fatalf("runReconcile (2nd run): %v", err)
	}
	stats2 := decodeReconcileStats(t, out.Bytes())
	if stats2.Scanned != 2 || stats2.Projected != 0 || stats2.Skipped != 2 || stats2.Failed != 0 {
		t.Fatalf("2nd run stats = %+v, want Scanned=2 Projected=0 Skipped=2 Failed=0 (idempotent re-run must not double-project)", stats2)
	}
}

// TestRunReconcileDryRunDoesNotWrite proves --dry-run reports the diff
// without inserting any SQL row: a receipt not yet projected is counted as
// Projected (the "would project" classification the dry-run branch uses,
// distinct from the non-dry-run branch's meaning of the same field), but a
// following non-dry-run pass still finds it un-projected and inserts it for
// real. If dry-run accidentally called projector.Project instead of the
// read-only lookup, this test would see the second (real) run report it as
// already Skipped instead of Projected.
func TestRunReconcileDryRunDoesNotWrite(t *testing.T) {
	dsn := testMySQLDSN(t)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ns := fmt.Sprintf("reconcile-dryrun-%d", time.Now().UnixNano())
	auditID := fmt.Sprintf("audit-dry-%d", time.Now().UnixNano())
	seedReplayReceipt(t, mr.Addr(), ns, "exec-dry", "req-dry", auditID)

	t.Cleanup(func() {
		p, err := mysqlstore.New(dsn)
		if err != nil {
			return
		}
		p.DB().WithContext(context.Background()).
			Exec("DELETE FROM xflow_audit_events WHERE receipt_audit_id = ?", auditID)
	})

	var out bytes.Buffer
	opts := &deadLetterOptions{mysqlDSN: dsn, out: &out}

	if err := runReconcile(opts, mr.Addr(), true); err != nil {
		t.Fatalf("runReconcile (dry-run): %v", err)
	}
	dryStats := decodeReconcileStats(t, out.Bytes())
	if !dryStats.DryRun {
		t.Fatalf("dry-run stats.DryRun = false, want true")
	}
	if dryStats.Scanned != 1 || dryStats.Projected != 1 {
		t.Fatalf("dry-run stats = %+v, want Scanned=1 Projected=1 (unprojected receipt reported, not written)", dryStats)
	}

	out.Reset()
	if err := runReconcile(opts, mr.Addr(), false); err != nil {
		t.Fatalf("runReconcile (real run after dry-run): %v", err)
	}
	realStats := decodeReconcileStats(t, out.Bytes())
	if realStats.Scanned != 1 || realStats.Projected != 1 || realStats.Skipped != 0 {
		t.Fatalf("real run after dry-run stats = %+v, want Scanned=1 Projected=1 Skipped=0 "+
			"(dry-run must not have already written the row)", realStats)
	}
}

func decodeReconcileStats(t *testing.T, raw []byte) reconcileStats {
	t.Helper()
	var stats reconcileStats
	if err := json.Unmarshal(bytes.TrimSpace(raw), &stats); err != nil {
		t.Fatalf("decode reconcile stats %q: %v", raw, err)
	}
	return stats
}
