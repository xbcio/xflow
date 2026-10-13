package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// auditStubAuthority is a test control.AdmissionAuthority: scripted effect per
// request_id, Indeterminate for anything unlisted (never fabricate).
type auditStubAuthority struct {
	effects map[string]control.MutationEffect
}

func (a auditStubAuthority) Probe(_ context.Context, rec *store.AuditRecord) (control.MutationEffect, error) {
	if eff, ok := a.effects[rec.RequestID]; ok {
		return eff, nil
	}
	return control.EffectIndeterminate, nil
}

// auditAdmissionRow builds an admitted-mutation audit row like the apiserver
// authz wrapper writes before a handler runs.
func auditAdmissionRow(reqID, namespace, execID string, ts time.Time) *store.AuditRecord {
	return &store.AuditRecord{
		RequestID:   reqID,
		Principal:   "alice",
		Namespace:   namespace,
		Operation:   "workflow.create",
		ExecutionID: execID,
		Decision:    "allow",
		Outcome:     store.AuditOutcomeAdmitted,
		Phase:       store.AuditPhaseAdmission,
		Timestamp:   ts,
	}
}

// seedAuditBacklog appends the three-admission backlog the CLI tests share:
// one confirmable, one no-effect, one indeterminate.
func seedAuditBacklog(t *testing.T, st *memstore.Store) auditStubAuthority {
	t.Helper()
	ctx := context.Background()
	old := time.Now().Add(-time.Minute)
	rows := []struct{ req, exec string }{
		{"req-confirmed", "exec-confirmed"},
		{"req-absent", "exec-absent"},
		{"req-unknown", "exec-unknown"},
	}
	for _, row := range rows {
		if err := st.AppendAudit(ctx, auditAdmissionRow(row.req, "namespace-cli", row.exec, old)); err != nil {
			t.Fatalf("seed admission %s: %v", row.req, err)
		}
	}
	return auditStubAuthority{effects: map[string]control.MutationEffect{
		"req-confirmed": control.EffectConfirmed,
		"req-absent":    control.EffectAbsent,
		"req-unknown":   control.EffectIndeterminate,
	}}
}

// decodeAuditReport parses the single-line JSON report and pins both the
// typed values and the exact key set (the field names are the command's
// public contract for scripts).
func decodeAuditReport(t *testing.T, raw []byte) (auditReconcileStats, map[string]any) {
	t.Helper()
	line := bytes.TrimSpace(raw)
	if bytes.Contains(line, []byte("\n")) {
		t.Fatalf("report is not a single line: %q", raw)
	}
	var stats auditReconcileStats
	if err := json.Unmarshal(line, &stats); err != nil {
		t.Fatalf("decode report %q: %v", line, err)
	}
	var keys map[string]any
	if err := json.Unmarshal(line, &keys); err != nil {
		t.Fatalf("decode report keys %q: %v", line, err)
	}
	wantKeys := []string{
		"scanned", "reconciled", "failed", "skipped_indeterminate",
		"errors", "pending_before", "pending_after", "dry_run",
	}
	if len(keys) != len(wantKeys) {
		t.Fatalf("report keys = %v, want exactly %v", keys, wantKeys)
	}
	for _, k := range wantKeys {
		if _, ok := keys[k]; !ok {
			t.Fatalf("report %v is missing key %q", keys, k)
		}
	}
	return stats, keys
}

func outcomeRowsByReq(st *memstore.Store) map[string]*store.AuditRecord {
	byReq := map[string]*store.AuditRecord{}
	for _, r := range st.AuditRecords() {
		if r.Phase == store.AuditPhaseOutcome {
			byReq[r.RequestID] = r
		}
	}
	return byReq
}

// TestAuditReconcileSettlesThroughMemstore drives the CLI core over a memstore
// reconciler + stub authority: the two classifiable admissions (confirmed →
// reconciled, absent → failed) get outcome rows, the indeterminate one stays
// pending, and the JSON report breaks the run down field by field.
func TestAuditReconcileSettlesThroughMemstore(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	authority := seedAuditBacklog(t, st)

	var out bytes.Buffer
	err := reconcileAudit(ctx, st, authority, &auditOptions{
		out:        &out,
		batch:      control.DefaultReconcileBatch,
		backlogAge: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("reconcileAudit: %v", err)
	}
	stats, _ := decodeAuditReport(t, out.Bytes())
	if stats.Scanned != 3 || stats.Reconciled != 1 || stats.Failed != 1 ||
		stats.SkippedIndeterminate != 1 || stats.Errors != 0 ||
		stats.PendingBefore != 3 || stats.PendingAfter != 1 || stats.DryRun {
		t.Fatalf("stats = %+v, want scanned=3 reconciled=1 failed=1 indeterminate=1 errors=0 pending=3→1 dry_run=false", stats)
	}

	outcomes := outcomeRowsByReq(st)
	if len(outcomes) != 2 {
		t.Fatalf("outcome rows = %d, want 2", len(outcomes))
	}
	if got := outcomes["req-confirmed"]; got == nil || got.Outcome != store.AuditOutcomeReconciled || got.Reason != "authority_confirmed" {
		t.Fatalf("req-confirmed outcome = %+v, want reconciled/authority_confirmed", got)
	}
	if got := outcomes["req-absent"]; got == nil || got.Outcome != store.AuditOutcomeFailed || got.Reason != "no_effect" {
		t.Fatalf("req-absent outcome = %+v, want failed/no_effect", got)
	}
	if outcomes["req-unknown"] != nil {
		t.Fatalf("req-unknown got an outcome row; indeterminate must stay pending")
	}
}

// TestAuditReconcileDryRunWritesNothing is the --dry-run contract: the
// classification is reported exactly as a real run would settle it, but no
// row is written and the pending count is unchanged.
func TestAuditReconcileDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	authority := seedAuditBacklog(t, st)

	var out bytes.Buffer
	err := reconcileAudit(ctx, st, authority, &auditOptions{
		out:        &out,
		batch:      control.DefaultReconcileBatch,
		backlogAge: time.Millisecond,
		dryRun:     true,
	})
	if err != nil {
		t.Fatalf("reconcileAudit: %v", err)
	}
	stats, _ := decodeAuditReport(t, out.Bytes())
	if stats.Scanned != 3 || stats.Reconciled != 1 || stats.Failed != 1 ||
		stats.SkippedIndeterminate != 1 || stats.Errors != 0 ||
		stats.PendingBefore != 3 || stats.PendingAfter != 3 || !stats.DryRun {
		t.Fatalf("stats = %+v, want the same classification as a real run with pending=3→3 dry_run=true", stats)
	}
	if outcomes := outcomeRowsByReq(st); len(outcomes) != 0 {
		t.Fatalf("dry run wrote %d outcome rows, want 0", len(outcomes))
	}
}

// TestAuditReconcileCommandValidatesFlags proves the flag contract without any
// connection: both stores are required, and a non-positive batch/backlog age
// is rejected before any I/O.
func TestAuditReconcileCommandValidatesFlags(t *testing.T) {
	t.Setenv("XFLOW_MYSQL_DSN", "")
	t.Setenv("XFLOW_REDIS_ADDR", "")
	dsn := "xflow:xflow@tcp(127.0.0.1:1)/xflow?parseTime=true&timeout=1s"

	var out bytes.Buffer
	if err := executeRootWith(&out, "audit", "reconcile"); err == nil || !strings.Contains(err.Error(), "--mysql-dsn") {
		t.Fatalf("err = %v, want a --mysql-dsn requirement", err)
	}
	if err := executeRootWith(&out, "audit", "reconcile", "--mysql-dsn", dsn); err == nil || !strings.Contains(err.Error(), "--redis-addr") {
		t.Fatalf("err = %v, want a --redis-addr requirement", err)
	}
	if err := executeRootWith(&out, "audit", "reconcile", "--mysql-dsn", dsn, "--redis-addr", "127.0.0.1:1", "--batch", "0"); err == nil || !strings.Contains(err.Error(), "--batch") {
		t.Fatalf("err = %v, want a --batch rejection", err)
	}
	if err := executeRootWith(&out, "audit", "reconcile", "--mysql-dsn", dsn, "--redis-addr", "127.0.0.1:1", "--backlog-age", "0s"); err == nil || !strings.Contains(err.Error(), "--backlog-age") {
		t.Fatalf("err = %v, want a --backlog-age rejection", err)
	}
}

// TestAuditReconcileConnectionFailureIsAnError proves the exit-code contract:
// an unreachable store surfaces as a command error (the CLI maps it to exit
// 1), and the DSN's credentials do not leak into the message.
func TestAuditReconcileConnectionFailureIsAnError(t *testing.T) {
	t.Setenv("XFLOW_MYSQL_DSN", "")
	t.Setenv("XFLOW_REDIS_ADDR", "")
	secret := "opensecret"
	dsn := "xflow:" + secret + "@tcp(127.0.0.1:1)/xflow?parseTime=true&timeout=1s"

	var out bytes.Buffer
	err := executeRootWith(&out, "audit", "reconcile", "--mysql-dsn", dsn, "--redis-addr", "127.0.0.1:1")
	if err == nil {
		t.Fatal("reconcile against an unreachable mysql succeeded; want an error (exit 1)")
	}
	if !strings.Contains(err.Error(), "open mysql") {
		t.Fatalf("err = %v, want an open-mysql failure", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error message leaks the DSN credential: %v", err)
	}
}
