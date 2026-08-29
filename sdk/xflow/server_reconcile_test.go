package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// The audit reconcile worker settles admissions whose outcome row was lost to a
// crash: a mutation that was admitted (audited before execution), then executed,
// then died before its outcome could be appended. Nothing re-drives it — the
// admission stays pending forever, and the audit trail says a mutation was
// authorized with no record of whether it happened.
//
// cmd/server builds this worker and runs it in a goroutine. The SDK had no
// reconciler concept at all, so a caller who correctly wired a durable
// apiserver.NewSQLAuditSink got the pending rows and nothing to settle them.
// The failure is invisible in every direction: mutations succeed, the API
// responds normally, and the backlog only shows up in the audit table.
//
// The probe below goes through the server's own backend StateStore, so the
// worker is exercised with the authority it will actually consult rather than
// a stand-in: exec-1 is absent from state, and an absent execution for a
// workflow.create settles the admission as no-effect.

// A pending admission older than the backlog age must receive an outcome row.
// Asserted by reading the store back, not by observing that a worker exists:
// a worker that runs and settles nothing is the exact failure mode here.
func TestServerReconcilerSettlesAPendingAdmission(t *testing.T) {
	ms := memstore.New()
	admitted := &store.AuditRecord{
		Namespace:   "default",
		RequestID:   "req-crashed",
		Phase:       store.AuditPhaseAdmission,
		Operation:   "workflow.create",
		Outcome:     "admitted",
		ExecutionID: "exec-1",
		// Older than the worker's default backlog age, so it is a candidate
		// rather than presumed in-flight.
		Timestamp: time.Now().Add(-time.Hour),
	}
	if err := ms.AppendAudit(context.Background(), admitted); err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(ServerConfig{Store: ms}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	rec := srv.Reconciler()
	if rec == nil {
		t.Fatal("Reconciler() = nil with a durable audit store configured: " +
			"admissions whose outcome was lost to a crash stay pending forever")
	}
	if settled := rec.ReconcileOnce(context.Background()); settled != 1 {
		t.Fatalf("ReconcileOnce settled %d admissions, want 1", settled)
	}

	pending, _, err := ms.CountUnreconciledAdmissions(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("%d admissions still pending after a sweep; the outcome row was "+
			"never appended", pending)
	}
}

// Without a store there is nothing to reconcile against, so the reconciler is
// absent rather than a worker that scans nothing. Pinned so the nil case stays
// an explicit answer a caller can check, not an incidental one.
func TestServerReconcilerAbsentWithoutADurableStore(t *testing.T) {
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	if rec := srv.Reconciler(); rec != nil {
		t.Error("Reconciler() non-nil with no store: it would scan a store that " +
			"does not exist")
	}
}

// Start must run the worker, not merely construct it. A reconciler that exists
// and is never driven settles nothing, and reads as wired from every angle a
// caller can check — Reconciler() returns non-nil, the config looks right, and
// the backlog grows silently. cmd/server runs it in its own goroutine; an
// embedded caller has no equivalent hook it could be expected to know about.
func TestServerStartRunsTheReconciler(t *testing.T) {
	ms := memstore.New()
	if err := ms.AppendAudit(context.Background(), &store.AuditRecord{
		Namespace:   "default",
		RequestID:   "req-crashed-2",
		Phase:       store.AuditPhaseAdmission,
		Operation:   "workflow.create",
		Outcome:     "admitted",
		ExecutionID: "exec-2",
		Timestamp:   time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(ServerConfig{Store: ms}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	})

	// The worker sweeps once at startup rather than waiting a full period, so
	// this settles without depending on the sweep interval. Poll rather than
	// sleep a fixed amount: the assertion is "it happens", and a fixed sleep
	// either flakes or wastes the difference.
	deadline := time.Now().Add(5 * time.Second)
	for {
		pending, _, err := ms.CountUnreconciledAdmissions(context.Background(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d admissions still pending 5s after Start; the reconciler "+
				"was built but never run, so nothing settles the backlog", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Run is the self-hosting path: it starts the transports and blocks, and it
// does NOT go through Server.Start — apiserver.Run calls the apiserver's own
// Start directly. So wiring the worker into Server.Start alone leaves the
// self-hosting caller, which is the one that looks most like cmd/server, with
// the same silent backlog.
func TestServerRunRunsTheReconciler(t *testing.T) {
	ms := memstore.New()
	if err := ms.AppendAudit(context.Background(), &store.AuditRecord{
		Namespace:   "default",
		RequestID:   "req-crashed-3",
		Phase:       store.AuditPhaseAdmission,
		Operation:   "workflow.create",
		Outcome:     "admitted",
		ExecutionID: "exec-3",
		Timestamp:   time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(ServerConfig{Store: ms}, WithServerHTTPAddr("127.0.0.1:0"), WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		pending, _, err := ms.CountUnreconciledAdmissions(context.Background(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d admissions still pending 5s after Run; the self-hosting "+
				"path never drives the reconciler", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
