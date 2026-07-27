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
// duplicate outcome append (same namespace+request_id) returns appended=false.
// failNext lets a test simulate a transient SQL outage.
type fakeAuditReconciler struct {
	mu       sync.Mutex
	rows     []*store.AuditRecord
	nextSeq  uint64
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
	f.nextSeq++
	cp.SeqID = f.nextSeq
	cp.ID = f.nextSeq
	f.rows = append(f.rows, &cp)
}

func (f *fakeAuditReconciler) ListUnreconciledAdmissions(_ context.Context, before time.Time, afterSeqID uint64, limit int) ([]*store.AuditRecord, error) {
	if limit <= 0 {
		limit = 256
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	hasOutcome := make(map[string]bool, len(f.rows))
	for _, r := range f.rows {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.Namespace+"|"+r.RequestID] = true
		}
	}
	var out []*store.AuditRecord
	for _, r := range f.rows {
		if afterSeqID > 0 && r.SeqID <= afterSeqID {
			continue
		}
		if r.Phase != store.AuditPhaseAdmission || r.Outcome != store.AuditOutcomeAdmitted {
			continue
		}
		if !r.Timestamp.IsZero() && !r.Timestamp.Before(before) {
			continue
		}
		if r.RequestID != "" && hasOutcome[r.Namespace+"|"+r.RequestID] {
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
	if rec.RequestID != "" && rec.Namespace != "" {
		key := rec.Namespace + "|" + rec.RequestID
		for _, r := range f.rows {
			if r.Phase == store.AuditPhaseOutcome && r.Namespace+"|"+r.RequestID == key {
				return false, nil
			}
		}
	}
	if f.failNext != nil {
		if err := f.failNext(rec); err != nil {
			return false, err
		}
	}
	f.nextSeq++
	rec.SeqID = f.nextSeq
	rec.ID = f.nextSeq
	cp := *rec
	f.rows = append(f.rows, &cp)
	return true, nil
}

func (f *fakeAuditReconciler) CountUnreconciledAdmissions(_ context.Context, before time.Time) (int, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hasOutcome := make(map[string]bool, len(f.rows))
	for _, r := range f.rows {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.Namespace+"|"+r.RequestID] = true
		}
	}
	var count int
	var oldest time.Time
	for _, r := range f.rows {
		if r.Phase != store.AuditPhaseAdmission || r.Outcome != store.AuditOutcomeAdmitted {
			continue
		}
		if !r.Timestamp.IsZero() && !r.Timestamp.Before(before) {
			continue
		}
		if r.RequestID != "" && hasOutcome[r.Namespace+"|"+r.RequestID] {
			continue
		}
		count++
		if oldest.IsZero() || (!r.Timestamp.IsZero() && r.Timestamp.Before(oldest)) {
			oldest = r.Timestamp
		}
	}
	return count, oldest, nil
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
		Period:     10 * time.Millisecond,
		BacklogAge: 1 * time.Millisecond,
		Batch:      64,
		Observer:   obs,
		Elector:    elector,
	})
}

// admissionForRow builds an admitted-mutation audit record (phase=admission,
// outcome=admitted) like the apiserver authz wrapper writes before a handler
// runs. ts is the admission timestamp; older than the worker backlog age it
// becomes a reconcile candidate.
func admissionForRow(reqID, namespace, op, execID string, ts time.Time) *store.AuditRecord {
	return &store.AuditRecord{
		RequestID:   reqID,
		Principal:   "alice",
		Namespace:   namespace,
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
	audit.addAdmission(admissionForRow("req-1", "namespace-a", opWorkflowCreate, "exec-1", time.Now().Add(-time.Minute)))

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
	audit.addAdmission(admissionForRow("req-2", "namespace-a", opWorkflowCreate, "exec-2", time.Now().Add(-time.Minute)))

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
	audit.addAdmission(admissionForRow("req-3", "namespace-a", opWorkflowCreate, "exec-3", time.Now().Add(-time.Minute)))

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
	audit.addAdmission(admissionForRow("req-4", "namespace-a", opWorkflowCreate, "exec-4", time.Now().Add(-time.Minute)))

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
// per (namespace, request_id, phase).
func TestReconcileConcurrentWorkersNoDuplicateOutcomes(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-5": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-5", "namespace-a", opWorkflowCreate, "exec-5", time.Now().Add(-time.Minute)))

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
	audit.addAdmission(admissionForRow("req-6", "namespace-a", opWorkflowCreate, "exec-6", time.Now().Add(-time.Minute)))

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
	audit.addAdmission(admissionForRow("req-7", "namespace-a", "execution.signal", "exec-7", time.Now().Add(-time.Minute)))

	w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
	if n := w.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("settled = %d, want 0 (indeterminate)", n)
	}
	if got := audit.outcomeCount(); got != 0 {
		t.Fatalf("outcome rows = %d, want 0 (indeterminate → retry)", got)
	}
}

// TestReconcileSignalMutationReachableSettles proves R3.2a: a reachable
// execution now settles execution.signal/revoke/cancel as reconciled. Before
// R3.2a the real ExecutionAuthority returned EffectIndeterminate for these
// operations, leaving the admission permanently pending; now a reachable
// execution (the crash-after-atomic-op-before-outcome-audit window) is
// settled with a reconciled outcome so the admission converges.
func TestReconcileSignalMutationReachableSettles(t *testing.T) {
	for _, op := range []string{opExecutionSignal, opExecutionRevoke, opExecutionCancel} {
		t.Run(op, func(t *testing.T) {
			audit := &fakeAuditReconciler{}
			authority := NewExecutionAuthority(&fakeExecutions{snap: &engine.ExecutionSnapshot{}})
			audit.addAdmission(admissionForRow("req-signal", "namespace-a", op, "exec-signal", time.Now().Add(-time.Minute)))

			w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
			if n := w.ReconcileOnce(context.Background()); n != 1 {
				t.Fatalf("settled = %d, want 1 (%s on reachable exec is confirmed)", n, op)
			}
			outcomes := audit.outcomes()
			if len(outcomes) != 1 {
				t.Fatalf("outcome rows for %s = %d, want 1", op, len(outcomes))
			}
			if outcomes[0].Outcome != store.AuditOutcomeReconciled {
				t.Fatalf("outcome for %s = %q, want reconciled", op, outcomes[0].Outcome)
			}
		})
	}
}

// TestReconcileSignalMutationMissingExecRetries proves the not-found half of
// R3.2a: an execution-scoped mutation whose execution is absent stays
// indeterminate (retry) — unlike create/invoke it is NOT settled as no-effect,
// because a signal/revoke/cancel target may have completed and been evicted.
func TestReconcileSignalMutationMissingExecRetries(t *testing.T) {
	for _, op := range []string{opExecutionSignal, opExecutionRevoke, opExecutionCancel} {
		t.Run(op, func(t *testing.T) {
			audit := &fakeAuditReconciler{}
			authority := NewExecutionAuthority(&fakeExecutions{notFound: true})
			audit.addAdmission(admissionForRow("req-signal", "namespace-a", op, "exec-gone", time.Now().Add(-time.Minute)))

			w := newWorkerForTest(audit, authority, backend.AlwaysLeader{}, nil)
			if n := w.ReconcileOnce(context.Background()); n != 0 {
				t.Fatalf("settled = %d, want 0 (%s on missing exec is indeterminate)", n, op)
			}
			if got := audit.outcomeCount(); got != 0 {
				t.Fatalf("outcome rows for %s = %d, want 0 (must not fabricate)", op, got)
			}
		})
	}
}

// the worker only probes authority and appends audit. The fakeAuthority has
// no mutation method, and the worker's settle path has no call into the
// engine or backend beyond the read-only Probe + AppendOutcomeIfAbsent.
// This test exists to fail loudly if a future change adds a mutation call.
func TestReconcileDoesNotReExecuteMutation(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-8": EffectConfirmed}}
	audit.addAdmission(admissionForRow("req-8", "namespace-a", opWorkflowCreate, "exec-8", time.Now().Add(-time.Minute)))

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
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opExecutionSignal, ExecutionID: "exec-x"})
		if err != nil || eff != EffectIndeterminate {
			t.Fatalf("eff=%v err=%v, want indeterminate/nil", eff, err)
		}
	})
	t.Run("signal on found exec -> confirmed", func(t *testing.T) {
		st := &fakeExecutions{snap: &engine.ExecutionSnapshot{}}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opExecutionSignal, ExecutionID: "exec-x"})
		if err != nil || eff != EffectConfirmed {
			t.Fatalf("eff=%v err=%v, want confirmed/nil (R3.2a: reachable execution settles signal)", eff, err)
		}
	})
	t.Run("revoke on found exec -> confirmed", func(t *testing.T) {
		st := &fakeExecutions{snap: &engine.ExecutionSnapshot{}}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opExecutionRevoke, ExecutionID: "exec-x"})
		if err != nil || eff != EffectConfirmed {
			t.Fatalf("eff=%v err=%v, want confirmed/nil (R3.2a: reachable execution settles revoke)", eff, err)
		}
	})
	t.Run("cancel on found exec -> confirmed", func(t *testing.T) {
		st := &fakeExecutions{snap: &engine.ExecutionSnapshot{}}
		a := NewExecutionAuthority(st)
		eff, err := a.Probe(context.Background(), &store.AuditRecord{Operation: opExecutionCancel, ExecutionID: "exec-x"})
		if err != nil || eff != EffectConfirmed {
			t.Fatalf("eff=%v err=%v, want confirmed/nil (R3.2a: reachable execution settles cancel)", eff, err)
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

// ---------------------------------------------------------------------------
// R3.4: Cursor pagination tests — proving the worker advances past
// permanently-Indeterminate rows so they do not starve the rest of the backlog.
// ---------------------------------------------------------------------------

// TestReconcileCursorAdvancesPastIndeterminate proves the core R3.4 defect fix:
// permanently-Indeterminate rows at the head of the oldest-first scan queue no
// longer starve resolvable rows further back. The worker's cursor advances past
// them so subsequent sweeps see the later rows.
func TestReconcileCursorAdvancesPastIndeterminate(t *testing.T) {
	audit := &fakeAuditReconciler{}
	// 3 admissions: first two are indeterminate (will never settle), third is
	// confirmed. With batch=3, one sweep sees all three. The cursor advances
	// to the last row's SeqID. A second sweep with afterSeqID=last wraps to 0
	// since rows < batch (all rows already seen).
	authority := &fakeAuthority{effects: map[string]MutationEffect{
		"req-ind-1": EffectIndeterminate,
		"req-ind-2": EffectIndeterminate,
		"req-ok-3":  EffectConfirmed,
	}}
	old := time.Now().Add(-5 * time.Minute)
	audit.addAdmission(admissionForRow("req-ind-1", "namespace-a", opWorkflowCreate, "exec-ind-1", old))
	audit.addAdmission(admissionForRow("req-ind-2", "namespace-a", opWorkflowCreate, "exec-ind-2", old))
	audit.addAdmission(admissionForRow("req-ok-3", "namespace-a", opWorkflowCreate, "exec-ok-3", old))

	w := NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
		Period:     10 * time.Millisecond,
		BacklogAge: 1 * time.Millisecond,
		Batch:      3,
		Elector:    backend.AlwaysLeader{},
	})

	// First sweep: sees all 3, settles req-ok-3 (the two indeterminate are
	// skipped). Cursor advances. Since len(candidates)=3 == batch=3, cursor =
	// last.SeqID (doesn't wrap).
	settled := w.ReconcileOnce(context.Background())
	if settled != 1 {
		t.Fatalf("first sweep settled = %d, want 1", settled)
	}
	if w.cursor == 0 {
		t.Fatal("cursor should have advanced (not zero) after full-batch sweep")
	}
	savedCursor := w.cursor

	// Second sweep: cursor is past the indeterminate rows. Since
	// req-ind-1/req-ind-2 are already settled (req-ok-3 got its outcome) and
	// only req-ind-1/req-ind-2 remain pending, but they are at SeqID <= cursor,
	// the query returns 0 rows. Since 0 < batch, cursor wraps to 0.
	settled2 := w.ReconcileOnce(context.Background())
	if settled2 != 0 {
		t.Fatalf("second sweep settled = %d, want 0 (indeterminate rows still pending)", settled2)
	}
	if w.cursor != 0 {
		t.Fatalf("cursor = %d, want 0 (wrap on partial batch)", w.cursor)
	}
	_ = savedCursor
}

// TestReconcileCursorWrapsToZeroOnPartialBatch proves the wrap semantics:
// when ListUnreconciledAdmissions returns fewer rows than the batch size,
// the cursor wraps to 0 so the next sweep starts from the beginning again.
func TestReconcileCursorWrapsToZeroOnPartialBatch(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{"req-w1": EffectConfirmed}}
	old := time.Now().Add(-5 * time.Minute)
	// Single row; batch=10 → rows returned (1) < batch (10) → cursor wraps.
	audit.addAdmission(admissionForRow("req-w1", "namespace-a", opWorkflowCreate, "exec-w1", old))

	w := NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
		Period:     10 * time.Millisecond,
		BacklogAge: 1 * time.Millisecond,
		Batch:      10,
		Elector:    backend.AlwaysLeader{},
	})

	settled := w.ReconcileOnce(context.Background())
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}
	if w.cursor != 0 {
		t.Fatalf("cursor = %d, want 0 (partial batch → wrap)", w.cursor)
	}
}

// TestReconcileCursorDoesNotWrapOnFullBatch proves that when the batch is
// exactly full, the cursor advances to the last SeqID (does NOT wrap), so
// the next sweep continues from where this one left off.
func TestReconcileCursorDoesNotWrapOnFullBatch(t *testing.T) {
	audit := &fakeAuditReconciler{}
	authority := &fakeAuthority{effects: map[string]MutationEffect{
		"req-b1": EffectIndeterminate,
		"req-b2": EffectIndeterminate,
	}}
	old := time.Now().Add(-5 * time.Minute)
	audit.addAdmission(admissionForRow("req-b1", "namespace-a", opWorkflowCreate, "exec-b1", old))
	audit.addAdmission(admissionForRow("req-b2", "namespace-a", opWorkflowCreate, "exec-b2", old))

	w := NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
		Period:     10 * time.Millisecond,
		BacklogAge: 1 * time.Millisecond,
		Batch:      2, // exactly matches row count
		Elector:    backend.AlwaysLeader{},
	})

	settled := w.ReconcileOnce(context.Background())
	if settled != 0 {
		t.Fatalf("settled = %d, want 0 (all indeterminate)", settled)
	}
	if w.cursor == 0 {
		t.Fatal("cursor should NOT wrap (batch was exactly full)")
	}
}

// TestReconcileBacklogMetricsEmitted proves that the worker emits full-table
// backlog metrics (pending count + oldest age) via CountUnreconciledAdmissions,
// independent of the cursor position.
func TestReconcileBacklogMetricsEmitted(t *testing.T) {
	audit := &fakeAuditReconciler{}
	old := time.Now().Add(-10 * time.Minute)
	audit.addAdmission(admissionForRow("req-m1", "namespace-a", opWorkflowCreate, "exec-m1", old))
	audit.addAdmission(admissionForRow("req-m2", "namespace-a", opWorkflowCreate, "exec-m2", old.Add(time.Minute)))

	authority := &fakeAuthority{effects: map[string]MutationEffect{
		"req-m1": EffectIndeterminate,
		"req-m2": EffectIndeterminate,
	}}

	obs := &recordingObserver{}
	w := NewAuditReconcileWorker(audit, authority, AuditReconcileConfig{
		Period:     10 * time.Millisecond,
		BacklogAge: 1 * time.Millisecond,
		Batch:      64,
		Elector:    backend.AlwaysLeader{},
		Observer:   obs,
	})

	w.ReconcileOnce(context.Background())

	if obs.lastBacklogPending != 2 {
		t.Fatalf("backlog pending = %d, want 2", obs.lastBacklogPending)
	}
	// The oldest age should be approximately 10 minutes (the first admission).
	if obs.lastBacklogOldestAge < 9*time.Minute {
		t.Fatalf("backlog oldest age = %v, want >= 9m", obs.lastBacklogOldestAge)
	}
}

// recordingObserver captures the last OnReconcileBacklog call for assertions.
type recordingObserver struct {
	lastBacklogPending   int
	lastBacklogOldestAge time.Duration
}

func (o *recordingObserver) OnReconcileScan(_ context.Context, _ int, _ time.Duration, _ error) {}
func (o *recordingObserver) OnReconcileSettled(_ context.Context, _ string, _ bool, _ int64)    {}
func (o *recordingObserver) OnReconcileSkipped(_ context.Context, _ string)                     {}
func (o *recordingObserver) OnReconcileError(_ context.Context, _ string, _ error)              {}
func (o *recordingObserver) OnReconcileBacklog(_ context.Context, oldestAge time.Duration, pending int) {
	o.lastBacklogPending = pending
	o.lastBacklogOldestAge = oldestAge
}
