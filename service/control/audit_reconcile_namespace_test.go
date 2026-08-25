package control

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// namespaceObserver records the namespace each reconcile observation was filed
// under, keyed by callback. It reads the namespace the way
// observability/metrics.ReconcileMetrics does — namespace.FromContext on the
// ctx it was handed — so a value it files under "default" is a value the real
// observer would label "default" too.
type namespaceObserver struct {
	seen map[string][]namespace.Namespace
}

func newNamespaceObserver() *namespaceObserver {
	return &namespaceObserver{seen: make(map[string][]namespace.Namespace)}
}

func (o *namespaceObserver) record(callback string, ctx context.Context) {
	o.seen[callback] = append(o.seen[callback], namespace.FromContext(ctx))
}

func (o *namespaceObserver) OnReconcileScan(ctx context.Context, _ int, _ time.Duration, _ error) {
	o.record("OnReconcileScan", ctx)
}
func (o *namespaceObserver) OnReconcileSettled(ctx context.Context, _ string, _ bool, _ int64) {
	o.record("OnReconcileSettled", ctx)
}
func (o *namespaceObserver) OnReconcileSkipped(ctx context.Context, _ string) {
	o.record("OnReconcileSkipped", ctx)
}
func (o *namespaceObserver) OnReconcileError(ctx context.Context, _ string, _ error) {
	o.record("OnReconcileError", ctx)
}
func (o *namespaceObserver) OnReconcileBacklog(ctx context.Context, _ time.Duration, _ int) {
	o.record("OnReconcileBacklog", ctx)
}

// scriptedAuthority returns a per-request effect OR a per-request error.
// fakeAuthority's single err field cannot express that: it fails every probe in
// the sweep, which would collapse the four arms below into one.
type scriptedAuthority struct {
	effects map[string]MutationEffect
	errs    map[string]error
}

func (a *scriptedAuthority) Probe(_ context.Context, rec *store.AuditRecord) (MutationEffect, error) {
	if err, ok := a.errs[rec.RequestID]; ok {
		return EffectIndeterminate, err
	}
	if eff, ok := a.effects[rec.RequestID]; ok {
		return eff, nil
	}
	return EffectIndeterminate, nil
}

// TestReconcileObservationsCarryTheRecordNamespace pins which namespace each
// reconcile observation is attributed to.
//
// The sweep is cross-namespace by design: ListUnreconciledAdmissions scans the
// whole audit table and every row carries its own Namespace. The worker's ctx
// therefore has no namespace, and a per-admission observation built from it is
// filed under namespace.Default no matter whose row produced it. That is what
// settle() did for OnReconcileError, OnReconcileSkipped and OnReconcileSettled;
// only the authority probe got a namespaced context.
//
// It matters because those counters are the ones that answer "which tenant's
// audit rows are not settling". Collapsed onto one label set they answer only
// "somebody's are", which is a question the operator already knew the answer to.
//
// Four arms, because the callbacks sit on four different branches of settle()
// and nothing makes them share a fix — the state this test was written against
// had the namespaced context computed and then used on exactly one of them
// (the probe, which reports nothing). The two OnReconcileError branches get
// separate tenants for the same reason: a fix applied to the probe-error site
// alone leaves the append-error site filing under default, and a test asserting
// only that "the tenant appears" would not see it.
func TestReconcileObservationsCarryTheRecordNamespace(t *testing.T) {
	const (
		tenantSettled   = "tenant-settled"
		tenantSkipped   = "tenant-skipped"
		tenantProbeErr  = "tenant-probe-err"
		tenantAppendErr = "tenant-append-err"
	)

	authority := &scriptedAuthority{
		effects: map[string]MutationEffect{
			"req-settled":    EffectConfirmed,
			"req-skipped":    EffectIndeterminate,
			"req-append-err": EffectConfirmed,
		},
		errs: map[string]error{"req-probe-err": errors.New("authority unreachable")},
	}

	audit := &fakeAuditReconciler{
		// Only the append-error arm fails to append; the settled arm must still
		// get through, or OnReconcileSettled never fires.
		failNext: func(rec *store.AuditRecord) error {
			if rec.RequestID == "req-append-err" {
				return errors.New("sql down")
			}
			return nil
		},
	}
	old := time.Now().Add(-time.Minute)
	audit.addAdmission(admissionForRow("req-settled", tenantSettled, opWorkflowCreate, "exec-1", old))
	audit.addAdmission(admissionForRow("req-skipped", tenantSkipped, opWorkflowCreate, "exec-2", old))
	audit.addAdmission(admissionForRow("req-probe-err", tenantProbeErr, opWorkflowCreate, "exec-3", old))
	audit.addAdmission(admissionForRow("req-append-err", tenantAppendErr, opWorkflowCreate, "exec-4", old))

	obs := newNamespaceObserver()
	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, obs)
	// A namespace-free sweep context, which is what Run passes in production.
	w.ReconcileOnce(context.Background())

	perRecord := map[string][]namespace.Namespace{
		"OnReconcileSettled": {tenantSettled},
		"OnReconcileSkipped": {tenantSkipped},
		"OnReconcileError":   {tenantAppendErr, tenantProbeErr},
	}
	for callback, want := range perRecord {
		got := append([]namespace.Namespace(nil), obs.seen[callback]...)
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		if len(got) != len(want) {
			t.Fatalf("%s fired %d times (%v), want %d. The fixture drives one "+
				"record down each branch; a different count means a branch was not "+
				"reached and the namespace assertion below proves nothing about it.",
				callback, len(got), got, len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s sample %d was filed under namespace %q, want %q "+
					"(all: %v). The observation was built from the sweep's context, "+
					"which is cross-namespace, so every tenant's rows land on one "+
					"label set and the counter can no longer say whose audit "+
					"backlog is stuck.", callback, i+1, got[i], want[i], got)
			}
		}
	}

	// The mirror-image error, asserted so a future change cannot "fix" the above
	// by namespacing the whole sweep. OnReconcileScan and OnReconcileBacklog are
	// whole-table quantities counted across every namespace; attributing them to
	// one tenant would overstate that tenant and hide the rest.
	for _, callback := range []string{"OnReconcileScan", "OnReconcileBacklog"} {
		got := obs.seen[callback]
		if len(got) != 1 {
			t.Fatalf("%s fired %d times, want exactly 1 per sweep", callback, len(got))
		}
		if got[0] != namespace.Default {
			t.Errorf("%s was filed under namespace %q, want %q. This is a "+
				"whole-table quantity: it counts rows from every namespace, so "+
				"pinning it to one makes that tenant's gauge report other tenants' "+
				"backlog.", callback, got[0], namespace.Default)
		}
	}
}
