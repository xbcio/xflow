package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// TestRunReconcileCountsAProjectionFailureAsFailed pins the classification of a
// receipt whose durable projection is rejected by SQL.
//
// runReconcile deliberately does not abort the scan when one receipt fails to
// project (dead_letter_reconcile.go:125 returns nil): one poisoned row must not
// stop the backfill of every other. The whole record of what happened is then
// the stats line on stdout, and Failed and Skipped mean opposite things there.
// Skipped is "a durable row already exists, nothing to do"; Failed is "this
// receipt still has no durable row and you must come back for it". Changing
// `stats.Failed++` to `stats.Skipped++` leaves the whole cmd/xflow package
// green — no existing reconcile test ever makes a projection fail, so every
// assertion is written over runs where Failed is 0 either way.
//
// The consequence is not a wrong number in a report. reconcile exists to
// backfill the audit trail after a SQL outage — a partial outage that rejects
// some rows and accepts others is exactly its reason to exist. Counting the
// rejects as skips makes the run read as fully settled, so the operator stops
// re-running it and the missing audit rows never come back.
//
// The failure is produced without touching production code: the receipt's
// execution id is longer than xflow_audit_events.execution_id (varchar(64),
// store/sqlstore/audit.go:30), so the insert is rejected by MySQL in strict
// mode. A healthy receipt is scanned alongside it so the test also pins that
// one rejected row does not cost the others their projection.
func TestRunReconcileCountsAProjectionFailureAsFailed(t *testing.T) {
	dsn := testMySQLDSN(t)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ns := fmt.Sprintf("reconcile-fail-%d", time.Now().UnixNano())
	// 200 characters: comfortably over execution_id's 64, and still under
	// resource's 255 (the row is "dead-letters/"+execution id), so the rejection
	// is attributable to one column rather than to whichever MySQL reports first.
	longExecID := strings.Repeat("e", 200)
	badAuditID := fmt.Sprintf("audit-fail-%d", time.Now().UnixNano())
	goodAuditID := fmt.Sprintf("audit-ok-%d", time.Now().UnixNano())

	seedReplayReceipt(t, mr.Addr(), ns, longExecID, "req-fail", badAuditID)
	seedReplayReceipt(t, mr.Addr(), ns, "exec-ok", "req-ok", goodAuditID)

	t.Cleanup(func() {
		p, err := mysqlstore.New(dsn)
		if err != nil {
			return
		}
		p.DB().WithContext(context.Background()).
			Exec("DELETE FROM xflow_audit_events WHERE receipt_audit_id IN (?, ?)", badAuditID, goodAuditID)
	})

	var out bytes.Buffer
	opts := &deadLetterOptions{out: &out, mysqlDSN: dsn}
	if err := runReconcile(opts, mr.Addr(), false); err != nil {
		t.Fatalf("runReconcile: %v; a single unprojectable receipt must not abort "+
			"the scan — the command's whole job is to backfill everything it can", err)
	}

	stats := decodeReconcileStats(t, out.Bytes())
	if stats.Scanned != 2 {
		t.Fatalf("stats = %+v, want Scanned=2", stats)
	}
	if stats.Failed != 1 {
		t.Fatalf("stats = %+v, want Failed=1: the oversize receipt has no durable "+
			"audit row and the operator has to come back for it; counting it "+
			"anywhere else reports the backfill as complete when it is not", stats)
	}
	if stats.Skipped != 0 {
		t.Fatalf("stats = %+v, want Skipped=0: skipped means a durable row already "+
			"exists, which is the opposite of what happened", stats)
	}
	if stats.Projected != 1 {
		t.Fatalf("stats = %+v, want Projected=1: the healthy receipt must still be "+
			"projected despite its neighbour being rejected", stats)
	}

	// The counters are a report about the database, so check the database
	// agrees: the healthy receipt landed, the rejected one did not.
	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	ctx := context.Background()
	if _, err := p.AuditByReceiptAuditID(ctx, goodAuditID); err != nil {
		t.Fatalf("the receipt counted as Projected has no durable row: %v", err)
	}
	if _, err := p.AuditByReceiptAuditID(ctx, badAuditID); err == nil {
		t.Fatalf("the receipt counted as Failed does have a durable row; the "+
			"rejection this test relies on did not happen, so the assertion above "+
			"proved nothing (audit_id %s)", badAuditID)
	}
}
