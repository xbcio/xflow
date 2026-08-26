package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// TestDeadLetterReconcileCLIUsesLocalRedisAddrOverride drives the reconcile
// subcommand through executeRootWith end to end. Every other reconcile test
// in this package (dead_letter_reconcile_test.go) calls runReconcile directly,
// bypassing newDeadLetterReconcileCommand's RunE entirely — go tool cover
// confirms the RunE closure body (dead_letter_reconcile.go:52-59, including
// the "--redis-addr overrides --redis-addr" resolution and the --mysql-dsn
// required check) has zero executions across the whole suite.
//
// This test passes the reconcile subcommand's own --redis-addr (declared at
// dead_letter_reconcile.go:63) pointing at a real miniredis holding a seeded
// receipt. If the resolution regresses to always using opts.redisAddr instead
// of preferring the subcommand's local flag, the command connects elsewhere
// and the seeded receipt is never scanned.
//
// Note on the mechanism, because it is not what the flag's help text implies:
// the subcommand declares --redis-addr under the SAME name as the parent's
// persistent --redis-addr, so pflag's flagset merge resolves the name to the
// subcommand's local flag and the parent's value never reaches opts.redisAddr
// at all. The "defaults to --redis-addr" fallback at
// dead_letter_reconcile.go:56-58 is therefore dead for this subcommand: with
// the mutation applied, opts.redisAddr still holds its default localhost:6379
// (verified — the mutated run fails with `connect redis "localhost:6379"`,
// NOT with the 127.0.0.1:1 passed to the parent below). The parent flag is
// still passed here to document that it has no effect, not because the
// failure would come from it.
func TestDeadLetterReconcileCLIUsesLocalRedisAddrOverride(t *testing.T) {
	dsn := testMySQLDSN(t)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ns := fmt.Sprintf("reconcile-cli-%d", time.Now().UnixNano())
	auditID := fmt.Sprintf("audit-cli-%d", time.Now().UnixNano())
	seedReplayReceipt(t, mr.Addr(), ns, "exec-cli", "req-cli", auditID)

	t.Cleanup(func() {
		p, err := mysqlstore.New(dsn)
		if err != nil {
			return
		}
		p.DB().WithContext(context.Background()).
			Exec("DELETE FROM xflow_audit_events WHERE receipt_audit_id = ?", auditID)
	})

	var out bytes.Buffer
	// The parent's persistent --redis-addr points at a closed port purely to
	// document that it is inert here (see the note above): pflag shadowing
	// means it never reaches opts.redisAddr, so it is not what makes a
	// regression fail.
	err = executeRootWith(&out, "dead-letter",
		"--redis-addr", "127.0.0.1:1", "--mysql-dsn", dsn,
		"reconcile", "--redis-addr", mr.Addr())
	if err != nil {
		t.Fatalf("dead-letter reconcile (CLI dispatch): %v", err)
	}
	stats := decodeReconcileStats(t, out.Bytes())
	if stats.Scanned != 1 || stats.Projected != 1 {
		t.Fatalf("stats = %+v, want Scanned=1 Projected=1 (local --redis-addr override must win over the persistent one)", stats)
	}
}
