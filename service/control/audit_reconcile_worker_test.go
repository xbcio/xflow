package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// fakeAuditReconciler is an in-memory store.AuditReconciler for worker unit
// tests. It mirrors the SQL provider's check-then-append idempotency: a
// duplicate outcome append (same tenant+request_id) returns appended=false.
// failNext lets a test simulate a transient SQL outage.
type fakeAuditReconciler struct {
	mu       sync.Mutex
	rows     []*store.AuditRecord
	failNext func(rec *store.AuditRecord) error
}

var _ store.AuditReconciler = (*fakeAuditReconciler)(nil)

func (f *fakeAuditReconciler) addAdmission(rec *store.AuditRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *rec
	if cp.Phase == "" {
		cp.Phase = store.AuditPhaseAdmission
	}
	f.rows = append(f.rows, &cp)
}

func (f *fakeAuditReconciler) ListUnreconciledAdmissions(_ context.Context, before time.Time, limit int) ([]*store.AuditRecord, error) {
	if limit <= 0 {
		limit = 256
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	hasOutcome := make(map[string]bool, len(f.rows))
	for _, r := range f.rows {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.TenantID+"|"+r.RequestID] = true
		}
	}
	var out []*store.AuditRecord
	for _, r := range f.rows {
		if r.Phase != store.AuditPhaseAdmission || r.Outcome != store.AuditOutcomeAdmitted {
			continue
		}
		if !r.Timestamp.IsZero() && !r.Timestamp.Before(before) {
			continue
		}
		if r.RequestID != "" && hasOutcome[r.TenantID+"|"+r.RequestID] {
			continue
		}
		cp := *r
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeAuditReconciler) AppendOutcomeIfAbsent(_ context.Context, rec *store.AuditRecord) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec.Phase = store.AuditPhaseOutcome
	if rec.RequestID != "" && rec.TenantID != "" {
		key := rec.TenantID + "|" + rec.RequestID
		for _, r := range f.rows {
			if r.Phase == store.AuditPhaseOutcome && r.TenantID+"|"+r.RequestID == key {
				return false, nil
			}
		}
	}
	if f.failNext != nil {
		if err := f.failNext(rec); err != nil {
			return false, err
		}
	}
	cp := *rec
	f.rows = append(f.rows, &cp)
	return true, nil
}

func (f *fakeAuditReconciler) outcomeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.rows {
		if r.Phase == store.AuditPhaseOutcome {
			n++
		}
	}
	return n
}

func (f *fakeAuditReconciler) outcomes() []*store.AuditRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*store.AuditRecord, 0)
	for _, r := range f.rows {
		if r.Phase == store.AuditPhaseOutcome {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out
}

// fakeAuthority is a test AdmissionAuthority. It returns a scripted effect per
// request_id and tracks probe calls so tests can assert the worker consulted
// authority (and did NOT re-execute the mutation — there is no mutation path
// to call).
type fakeAuthority struct {
	mu      sync.Mutex
	effects map[string]MutationEffect
	err     error
	probes  int
}

func (a *fakeAuthority) Probe(_ context.Context, rec *store.AuditRecord) (MutationEffect, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.probes++
	if a.err != nil {
		return EffectIndeterminate, a.err
	}
	if eff, ok := a.effects[rec.RequestID]; ok {
		return eff, nil
	}
	return EffectIndeterminate, nil
}

func (a *fakeAuthority) probeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.probes
}

// toggleLeaderGate is a LeaderGate whose leadership can be flipped at runtime.
type toggleLeaderGate struct{ leader atomic.Bool }

func (g *toggleLeaderGate) IsLeader() bool { return g.leader.Load() }

func newWorkerForTest(audit store.AuditReconciler, authority AdmissionAuthority, elector LeaderGate, obs ReconcileObserver) *AuditReconcileWorker {
	return NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
		Period:    10 * time.Millisecond,
		BacklogAge: 1 * time.Millisecond,
		Batch:     64,
		Observer:  obs,
		Elector:   elector,
	})
}

// admissionForRow builds an admitted-mutation audit record (phase=admission,
// outcome=admitted) like the apiserver authz wrapper writes before a handler
// runs. ts is the admission timestamp; older than the worker backlog age it
// becomes a reconcile candidate.
func admissionForRow(reqID, tenant, op, execID string, ts time.Time) *store.AuditRecord {
	return &store.AuditRecord{
		RequestID:   reqID,
		Principal:   "alice",
		TenantID:    tenant,
		Operation:   op,
		ExecutionID: execID,
		Decision:    "allow",
		Outcome:     store.AuditOutcomeAdmitted,
		Phase:       store.AuditPhaseAdmission,
		TraceID:     "trace-" + reqID,
		Timestamp:   ts,
	}
}

// TestReconcilePreHandlerCrashRecordsNoEffect proves fault scenario 1: the
// admission audit landed (fail-closed), but the handler crashed before the
// mutation touched authoritative state. The worker consults authority, sees
// the mutation did NOT land (EffectAbsent), and appends a failed/no-effect
// outcome. The mutation is never re-executed (fakeAuthority has no mutation
// path).
func TestReconcilePreHandlerCrashRecordsNoEffect(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-1": EffectAbsent}}
	audit.addAdmission(admissionForRow("req-1", "tenant-a", opWorkflowCreate, "exec-1", time.Now().Add(-time.Minute)))

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	settled := w.ReconcileOnce(context.Background())
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}
	outcomes := audit.outcomes()
	if len(outcomes) != 1 {
		t.Fatalf("outcome rows = %d, want 1", len(outcomes))
	}
	o := outcomes[0]
	if o.Outcome != store.AuditOutcomeFailed {
		t.Fatalf("outcome = %q, want failed (no-effect/aborted)", o.Outcome)
	}
	if o.Phase != store.AuditPhaseOutcome {
		t.Fatalf("phase = %q, want outcome", o.Phase)
	}
	if o.RequestID != "req-1" {
		t.Fatalf("request_id = %q, want req-1", o.RequestID)
	}
	if o.TraceID != "trace-req-1" {
		t.Fatalf("trace_id = %q, want trace-req-1 (must propagate)", o.TraceID)
	}
	if o.Reason != "no_effect" {
		t.Fatalf("reason = %q, want no_effect", o.Reason)
	}
}

// TestReconcilePostMutationCrashAppendsReconciledOnce proves fault scenario 2:
// the mutation succeeded (authoritative state confirms it) but the process
// crashed before the inline outcome audit appended. The worker appends a
// reconciled outcome exactly once; a second sweep is a no-op.
func TestReconcilePostMutationCrashAppendsReconciledOnce(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-2": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-2", "tenant-a", opWorkflowCreate, "exec-2", time.Now().Add(-time.Minute)))

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	if n := w.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("first sweep settled = %d, want 1", n)
	}
	// A second sweep must not append a duplicate outcome.
	if n := w.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("second sweep settled = %d, want 0 (idempotent)", n)
	}
	if got := audit.outcomeCount(); got != 1 {
		t.Fatalf("outcome rows after two sweeps = %d, want 1 (idempotent)", got)
	}
	o := audit.outcomes()[0]
	if o.Outcome != store.AuditOutcomeReconciled {
		t.Fatalf("outcome = %q, want reconciled", o.Outcome)
	}
	if o.Reason != "authority_confirmed" {
		t.Fatalf("reason = %q, want authority_confirmed", o.Reason)
	}
}

// TestReconcileAuthorityUnreachableLeavesPending proves the worker never
// fabricates an outcome: when authority is unreachable (probe error), the
// admission stays pending and is retried next sweep. No outcome is appended.
func TestReconcileAuthorityUnreachableLeavesPending(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{err: errors.New("redis down")}
	audit.addAdmission(admissionForRow("req-3", "tenant-a", opWorkflowCreate, "exec-3", time.Now().Add(-time.Minute)))

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	if n := w.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("settled = %d, want 0 (authority unreachable)", n)
	}
	if got := audit.outcomeCount(); got != 0 {
		t.Fatalf("outcome rows = %d, want 0 (must not fabricate)", got)
	}
	// Recover authority; the next sweep settles the admission.
	authority.mu.Lock()
	authority.err = nil
	authority.effects = map[string]MutationEffect{"req-3": EffectConfirmed}
	authority.mu.Unlock()
	if n := w.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("settled after recovery = %d, want 1", n)
	}
	if got := audit.outcomeCount(); got != 1 {
		t.Fatalf("outcome rows after recovery = %d, want 1", got)
	}
}

// TestReconcileSQLDownThenConverges proves fault scenario 3: when the audit
// store is temporarily unavailable, the pending scan fails (no fabrication),
// and once it recovers the worker converges. A transient append failure
// leaves the admission pending for the next sweep.
func TestReconcileSQLDownThenConverges(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-4": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-4", "tenant-a", opWorkflowCreate, "exec-4", time.Now().Add(-time.Minute)))

	// Simulate a transient SQL outage on the outcome append.
	audit.failNext = func(*store.AuditRecord) error { return errors.New("sql down") }

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	if n := w.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("settled during SQL outage = %d, want 0", n)
	}
	if got := audit.outcomeCount(); got != 0 {
		t.Fatalf("outcome rows during outage = %d, want 0", got)
	}
	// Recover SQL; the next sweep converges.
	audit.failNext = nil
	if n := w.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("settled after SQL recovery = %d, want 1", n)
	}
	if got := audit.outcomeCount(); got != 1 {
		t.Fatalf("outcome rows after recovery = %d, want 1", got)
	}
}

// TestReconcileConcurrentWorkersNoDuplicateOutcomes proves fault scenario 4:
// two workers (e.g. a leader switch overlap) scanning the same admission do
// not append duplicate outcome rows. The check-then-append idempotency
// (mirroring the SQL unique phase_key index) guarantees exactly one outcome
// per (tenant, request_id, phase).
func TestReconcileConcurrentWorkersNoDuplicateOutcomes(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-5": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-5", "tenant-a", opWorkflowCreate, "exec-5", time.Now().Add(-time.Minute)))

	w1 := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	w2 := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)

	var wg sync.WaitGroup
	for _, w := range []*AuditReconcileWorker{w1, w2} {
		wg.Add(1)
		go func(w *AuditReconcileWorker) {
			defer wg.Done()
			w.ReconcileOnce(context.Background())
		}(w)
	}
	wg.Wait()

	if got := audit.outcomeCount(); got != 1 {
		t.Fatalf("outcome rows after concurrent reconcile = %d, want 1 (no dups)", got)
	}
}

// TestReconcileLeaderGatedNoOpWhenNotLeader proves the worker does not scan
// when this replica is not the leader — only the leader reconciles, so under
// steady state a single replica appends outcomes.
func TestReconcileLeaderGatedNoOpWhenNotLeader(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-6": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-6", "tenant-a", opWorkflowCreate, "exec-6", time.Now().Add(-time.Minute)))

	gate := &toggleLeaderGate{}
	gate.leader.Store(false)
	w := newWorkerForTest(audit, authority, gate, nil)
	if n := w.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("settled while not leader = %d, want 0", n)
	}
	if got := authority.probeCount(); got != 0 {
		t.Fatalf("authority probes while not leader = %d, want 0", got)
	}
	// Promote to leader; the next sweep reconciles.
	gate.leader.Store(true)
	if n := w.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("settled after promotion = %d, want 1", n)
	}
}

// TestReconcileIndeterminateEffectRetries proves an ambiguous authority result
// (EffectIndeterminate, not an error) leaves the admission pending — the
// worker never fabricates a result it cannot prove.
func TestReconcileIndeterminateEffectRetries(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-7": EffectIndeterminate}}
	audit.addAdmission(admissionForRow("req-7", "tenant-a", "execution.signal", "exec-7", time.Now().Add(-time.Minute)))

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	if n := w.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("settled = %d, want 0 (indeterminate)", n)
	}
	if got := audit.outcomeCount(); got != 0 {
		t.Fatalf("outcome rows = %d, want 0 (indeterminate → retry)", got)
	}
}

// TestReconcileDoesNotReExecuteMutation is the explicit security invariant:
// the worker only probes authority and appends audit. The fakeAuthority has
// no mutation method, and the worker's settle path has no call into the
// engine or backend beyond the read-only Probe + AppendOutcomeIfAbsent.
// This test exists to fail loudly if a future change adds a mutation call.
func TestReconcileDoesNotReExecuteMutation(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-8": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-8", "tenant-a", opWorkflowCreate, "exec-8", time.Now().Add(-time.Minute)))

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	w.ReconcileOnce(context.Background())
	// Exactly one probe (read) and one append (audit). No mutation path was
	// invoked: there is none to invoke on fakeAuthority/fakeAuditReconciler.
	if got := authority.probeCount(); got != 1 {
		t.Fatalf("probe count = %d, want 1", got)
	}
	if got := audit.outcomeCount(); got != 1 {
		t.Fatalf("outcome rows = %d, want 1", got)
	}
}

// TestExecutionAuthorityProbesGetExecution exercises the default
// AdmissionAuthority against a fake engine.Executions. create/invoke with
// execution found → Confirmed; not found → Absent; unreachable error →
// Indeterminate.
func TestExecutionAuthorityProbesGetExecution(t *testing.T) {
	t.Run("create found -> confirmed", func(t *testing.T) {
		st := &fakeExecutions{snap: &engine.ExecutionSnapshot{}}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opWorkflowCreate, ExecutionID: "exec-x"})
		if err != nil || eff != EffectConfirmed {
			t.Fatalf("eff=%v err=%v, want confirmed/nil", eff, err)
		}
	})
	t.Run("create not found -> absent", func(t *testing.T) {
		st := &fakeExecutions{notFound: true}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opWorkflowCreate, ExecutionID: "exec-x"})
		if err != nil || eff != EffectAbsent {
			t.Fatalf("eff=%v err=%v, want absent/nil (no-effect)", eff, err)
		}
	})
	t.Run("create nil snapshot -> absent", func(t *testing.T) {
		st := &fakeExecutions{snap: nil}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opWorkflowCreate, ExecutionID: "exec-x"})
		if err != nil || eff != EffectAbsent {
			t.Fatalf("eff=%v err=%v, want absent/nil", eff, err)
		}
	})
	t.Run("signal on missing exec -> indeterminate", func(t *testing.T) {
		st := &fakeExecutions{notFound: true}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: "execution.signal", ExecutionID: "exec-x"})
		if err != nil || eff != EffectIndeterminate {
			t.Fatalf("eff=%v err=%v, want indeterminate/nil", eff, err)
		}
	})
	t.Run("authority error -> indeterminate + err", func(t *testing.T) {
		st := &fakeExecutions{err: errors.New("redis unreachable")}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opWorkflowCreate, ExecutionID: "exec-x"})
		if err == nil || eff != EffectIndeterminate {
			t.Fatalf("eff=%v err=%v, want indeterminate/err", eff, err)
		}
	})
	t.Run("no execution id -> indeterminate", func(t *testing.T) {
		a := NewExecutionAuthority(&fakeExecutions{})
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opWorkflowCreate})
		if err != nil || eff != EffectIndeterminate {
			t.Fatalf("eff=%v err=%v, want indeterminate/nil", eff, err)
		}
	})
}

// fakeExecutions is a minimal engine.Executions for authority tests.
type fakeExecutions struct {
	snap     *engine.ExecutionSnapshot
	err      error
	notFound bool
}

func (f *fakeExecutions) CreateExecution(context.Context, *engine.ExecutionSnapshot) error {
	return nil
}
func (f *fakeExecutions) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	return nil
}
func (f *fakeExecutions) GetExecution(_ context.Context, _ types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.notFound {
		return nil, store.ErrNotFound
	}
	return f.snap, nil
}
