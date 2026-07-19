package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Audit reconcile phase constants reused from the store package. The worker
// appends outcome-phase rows; admission rows are written by the apiserver
// authz wrapper. See store.AuditPhase*.
const (
	auditPhaseAdmission = store.AuditPhaseAdmission
	auditPhaseOutcome   = store.AuditPhaseOutcome
)

// Operation vocabulary the authority probes by. These mirror the
// service/apiserver operation constants; the control package cannot import
// apiserver (apiserver depends on control), so the strings are duplicated
// here. The audit record's Operation field is set by the apiserver authz
// wrapper using the same strings.
const (
	opWorkflowCreate = "workflow.create"
	opWorkflowInvoke  = "workflow.invoke"
)

// DefaultReconcilePeriod is how often the worker scans for unreconciled
// admissions when AuditReconcileConfig.Period is unset.
const DefaultReconcilePeriod = 15 * time.Second

// DefaultReconcileBacklog is the age an admission must reach before the
// worker considers it unreconciled. Younger admissions are presumed
// in-flight (the handler has not yet returned and the inline outcome audit
// has not yet appended). The backlog age is the floor for the backlog-age
// alarm metric.
const DefaultReconcileBacklog = 30 * time.Second

// DefaultReconcileBatch bounds one sweep so a large backlog cannot starve
// the leader's other maintenance work or hold a scan lock too long.
const DefaultReconcileBatch = 256

// MutationEffect is the worker's authoritative judgment of whether an
// admitted mutation took effect. It is derived ONLY by observing
// authoritative state (engine StateStore); the worker never re-executes
// the mutation.
type MutationEffect int

const (
	// EffectIndeterminate means authoritative state could not be reached or
	// is ambiguous for this operation. The worker does NOT append an outcome;
	// the admission stays pending and is retried on the next sweep. Never
	// fabricate a result.
	EffectIndeterminate MutationEffect = iota
	// EffectConfirmed means authoritative state confirms the mutation landed.
	// The worker appends a reconciled outcome.
	EffectConfirmed
	// EffectAbsent means authoritative state confirms the mutation did NOT
	// land (e.g. a crash before the handler ran). The worker appends a
	// no-effect/failed outcome so the admission is settled.
	EffectAbsent
)

// AdmissionAuthority consults authoritative state to determine whether an
// admitted mutation took effect. It MUST NOT re-execute the mutation or
// mutate any state; it only reads. The engine StateStore (Redis for the
// distributed backend) is the authoritative source; the SQL audit log is the
// durable secondary being reconciled.
type AdmissionAuthority interface {
	// Probe returns the effect of the admitted mutation recorded in rec. A
	// non-nil error (other than not-found, which is a legitimate Absent
	// signal for create/invoke) means authority is unreachable; the worker
	// retries the admission next sweep without appending an outcome.
	Probe(ctx context.Context, rec *store.AuditRecord) (MutationEffect, error)
}

// LeaderGate is the subset of backend.LeaderElector the worker uses: a
// non-blocking leadership check that gates each sweep so only the leader
// replica scans. The control plane's leader campaign (separate from this
// worker) acquires leadership; the worker only asks "am I the leader now?".
// Nil means "always run" (single-replica / dev).
type LeaderGate interface {
	IsLeader() bool
}

// ReconcileObserver receives reconcile outcomes so observability layers can
// emit metrics and alarms. All methods must be non-blocking.
type ReconcileObserver interface {
	// OnReconcileScan reports one scan cycle: candidates found and the
	// scan latency. err is non-nil when the pending-list query failed.
	OnReconcileScan(ctx context.Context, candidates int, elapsed time.Duration, err error)
	// OnReconcileSettled reports one admission settled: outcome is
	// "reconciled" (mutation landed) or "failed" (no-effect/aborted).
	// appended=false means a concurrent worker had already appended the
	// outcome (idempotent skip).
	OnReconcileSettled(ctx context.Context, outcome string, appended bool, ageMs int64)
	// OnReconcileSkipped reports an admission left pending: effect was
	// indeterminate (authority unreachable/ambiguous) and will be retried.
	OnReconcileSkipped(ctx context.Context, reason string)
	// OnReconcileError reports a per-admission error (probe failure or
	// append failure). The admission stays pending and is retried.
	OnReconcileError(ctx context.Context, requestID string, err error)
	// OnReconcileBacklog reports the oldest pending admission age observed
	// this sweep so a persistent backlog is observable as an alarm.
	OnReconcileBacklog(ctx context.Context, oldestAge time.Duration, pending int)
}

// AuditReconcileConfig configures the worker.
type AuditReconcileConfig struct {
	// Period is the interval between sweeps. Defaults to DefaultReconcilePeriod.
	Period time.Duration
	// BacklogAge is the age an admission must reach before it is a candidate
	// for reconciliation (younger = presumed in-flight). Defaults to
	// DefaultReconcileBacklog.
	BacklogAge time.Duration
	// Batch bounds one sweep. Defaults to DefaultReconcileBatch.
	Batch int
	// Logger receives structured info/error messages. Optional.
	Logger engine.Logger
	// Observer receives metrics. Optional.
	Observer ReconcileObserver
	// Elector gates leader-only execution: when set and IsLeader() is false,
	// a sweep is a no-op. Nil means "always run" (single-replica / dev).
	Elector LeaderGate
}

// AuditReconcileWorker is the crash-safe audit reconciliation worker (T9). It
// scans admitted mutation audit rows that never received a post-handler
// outcome (e.g. a crash between a successful mutation and its outcome
// audit append), consults authoritative state (engine StateStore) to
// determine whether the mutation took effect, and appends the missing
// outcome idempotently. It NEVER re-executes the mutation — it only observes
// authoritative state and appends audit.
//
// Leader gating: when Elector is set and this replica is not the leader, a
// sweep is a no-op. Only the leader scans, so under steady state a single
// replica appends outcomes. Idempotency (AppendOutcomeIfAbsent + the
// uk_phase_key unique index) makes a leader switch safe: the loser of a race
// to append an outcome gets appended=false, not a duplicate row.
//
// Backoff: a per-admission probe or append error does not abort the sweep;
// the admission is retried on the next sweep with the standard period. The
// worker does not re-probe immediately, giving transient authority/SQL
// outages time to recover.
type AuditReconcileWorker struct {
	audit     store.AuditReconciler
	authority AdmissionAuthority
	elector   LeaderGate
	observer  ReconcileObserver
	log       engine.Logger
	period    time.Duration
	backlog   time.Duration
	batch     int
	clock     func() time.Time
	sleepFunc func(context.Context, time.Duration) error
}

// NewAuditReconcileWorker builds a worker. audit and authority are required
// in production (production also requires a non-nil elector for leader
// gating; the apiserver validates this via validateProduction). When audit
// is nil the worker is a no-op — this is the dev path where no durable audit
// sink is configured.
func NewAuditReconcileWorker(audit store.AuditReconciler, authority AdmissionAuthority, cfg AuditReconcileConfig) *AuditReconcileWorker {
	if cfg.Period <= 0 {
		cfg.Period = DefaultReconcilePeriod
	}
	if cfg.BacklogAge <= 0 {
		cfg.BacklogAge = DefaultReconcileBacklog
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultReconcileBatch
	}
	return &AuditReconcileWorker{
		audit:     audit,
		authority: authority,
		elector:   cfg.Elector,
		observer:  cfg.Observer,
		log:       cfg.Logger,
		period:    cfg.Period,
		backlog:   cfg.BacklogAge,
		batch:     cfg.Batch,
		clock:     func() time.Time { return time.Now().UTC() },
		sleepFunc: sleepWithContext,
	}
}

// Run drives the reconcile loop until ctx is canceled. Blocks the caller;
// spawn it in a goroutine. A nil audit reconciler (dev) is a no-op loop.
func (w *AuditReconcileWorker) Run(ctx context.Context) error {
	if w == nil || w.audit == nil {
		<-ctx.Done()
		return nil
	}
	// Reconcile once at startup so a clean restart settles any backlog from a
	// crashed predecessor immediately rather than after one period.
	w.ReconcileOnce(ctx)
	for {
		if err := w.sleepFunc(ctx, w.period); err != nil {
			return nil
		}
		w.ReconcileOnce(ctx)
	}
}

// ReconcileOnce executes exactly one sweep. Exported so tests and admin
// tooling can trigger a reconcile without waiting for the next tick.
// Returns the number of admissions settled (appended an outcome) this sweep.
func (w *AuditReconcileWorker) ReconcileOnce(ctx context.Context) int {
	if w == nil || w.audit == nil {
		return 0
	}
	if w.elector != nil && !w.elector.IsLeader() {
		return 0
	}
	before := w.clock().Add(-w.backlog)
	started := time.Now()
	candidates, err := w.audit.ListUnreconciledAdmissions(ctx, before, w.batch)
	w.observe(func(o ReconcileObserver) {
		o.OnReconcileScan(ctx, len(candidates), time.Since(started), err)
	})
	if err != nil {
		if w.log != nil {
			w.log.Error("audit reconcile: list unreconciled admissions", "err", err)
		}
		return 0
	}
	settled := 0
	var oldestAge time.Duration
	for _, rec := range candidates {
		if age := w.clock().Sub(rec.Timestamp); age > oldestAge {
			oldestAge = age
		}
		settled += w.settle(ctx, rec)
	}
	w.observe(func(o ReconcileObserver) {
		o.OnReconcileBacklog(ctx, oldestAge, len(candidates))
	})
	return settled
}

// settle probes authoritative state for one admission and appends the
// missing outcome idempotently. Returns 1 when a new outcome row was
// appended, 0 otherwise (skipped / error / already settled by a concurrent
// worker). Never re-executes the mutation.
func (w *AuditReconcileWorker) settle(ctx context.Context, rec *store.AuditRecord) int {
	probeCtx := ctx
	if rec.TenantID != "" {
		probeCtx = tenant.WithTenant(probeCtx, tenant.TenantID(rec.TenantID))
	}
	effect, err := w.authority.Probe(probeCtx, rec)
	if err != nil {
		w.observe(func(o ReconcileObserver) {
			o.OnReconcileError(ctx, rec.RequestID, err)
		})
		if w.log != nil {
			w.log.Error("audit reconcile: probe authority", "request_id", rec.RequestID, "err", err)
		}
		return 0
	}
	outcome := ""
	reason := ""
	switch effect {
	case EffectConfirmed:
		outcome = store.AuditOutcomeReconciled
		reason = "authority_confirmed"
	case EffectAbsent:
		outcome = store.AuditOutcomeFailed
		reason = "no_effect"
	case EffectIndeterminate:
		w.observe(func(o ReconcileObserver) {
			o.OnReconcileSkipped(ctx, "indeterminate")
		})
		return 0
	}
	ageMs := w.clock().Sub(rec.Timestamp).Milliseconds()
	appended, err := w.audit.AppendOutcomeIfAbsent(ctx, &store.AuditRecord{
		RequestID:   rec.RequestID,
		Principal:   rec.Principal,
		TenantID:    rec.TenantID,
		Operation:   rec.Operation,
		Resource:    rec.Resource,
		WorkflowID:  rec.WorkflowID,
		ExecutionID: rec.ExecutionID,
		Decision:    rec.Decision,
		Reason:      reason,
		Outcome:     outcome,
		Phase:       auditPhaseOutcome,
		TraceID:     rec.TraceID,
		Timestamp:   w.clock(),
	})
	if err != nil {
		w.observe(func(o ReconcileObserver) {
			o.OnReconcileError(ctx, rec.RequestID, err)
		})
		if w.log != nil {
			w.log.Error("audit reconcile: append outcome", "request_id", rec.RequestID, "err", err)
		}
		return 0
	}
	w.observe(func(o ReconcileObserver) {
		o.OnReconcileSettled(ctx, outcome, appended, ageMs)
	})
	if appended {
		return 1
	}
	return 0
}

func (w *AuditReconcileWorker) observe(fn func(ReconcileObserver)) {
	if w == nil || w.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(w.observer)
}

// ExecutionAuthority is the default AdmissionAuthority. It consults the
// engine StateStore (Redis for the distributed backend) to determine whether
// an admitted mutation took effect. For workflow.create / workflow.invoke
// the existence of the execution in authoritative state is the clean
// signal: found → confirmed (the create landed), not found → absent (the
// handler crashed before the execution was persisted). For execution-scoped
// mutations (signal/revoke/cancel) the execution being reachable means the
// admission's target exists; the worker treats a reachable execution as
// confirmed and an unreachable/missing one as indeterminate (the admission
// is retried rather than fabricating an outcome).
//
// It NEVER mutates state: GetExecution is a read. The Redis receipt (dead-
// letter replay) remains authoritative for replay mutations and is
// reconciled by T4's ReceiptProjector + diff-scan command, not here.
type ExecutionAuthority struct {
	state engine.Executions
}

// NewExecutionAuthority builds an AdmissionAuthority over the engine's
// execution state store. The state store is the authoritative execution
// state (Redis in the distributed backend).
func NewExecutionAuthority(state engine.Executions) *ExecutionAuthority {
	return &ExecutionAuthority{state: state}
}

// Probe consults GetExecution to determine the mutation's effect. See the
// type doc for the decision table.
func (a *ExecutionAuthority) Probe(ctx context.Context, rec *store.AuditRecord) (MutationEffect, error) {
	if a == nil || a.state == nil {
		return EffectIndeterminate, nil
	}
	if rec.ExecutionID == "" {
		// No execution correlation id to probe; cannot determine effect
		// without fabricating. Leave pending.
		return EffectIndeterminate, nil
	}
	snap, err := a.state.GetExecution(ctx, types.ExecutionID(rec.ExecutionID))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		// Authority unreachable. Do not fabricate; retry next sweep.
		return EffectIndeterminate, fmt.Errorf("get execution %q: %w", rec.ExecutionID, err)
	}
	if snap == nil || err != nil {
		// Execution not found in authoritative state.
		switch rec.Operation {
		case opWorkflowCreate, opWorkflowInvoke:
			// A create/invoke that left no execution in authoritative state
			// did not land: the handler crashed before persisting the
			// execution. Settle as no-effect.
			return EffectAbsent, nil
		default:
			// An execution-scoped mutation (signal/revoke/cancel) on an
			// execution that no longer exists is ambiguous (it may have been
			// created and since removed, or the admission referenced a
			// not-yet-created execution). Retry rather than fabricate.
			return EffectIndeterminate, nil
		}
	}
	// Execution found in authoritative state → the mutation's target is
	// present. For create/invoke this confirms the create landed. For
	// execution-scoped mutations the admission was authorized and the
	// execution is reachable; the tiny crash window between mutation and
	// outcome append means the most defensible judgment is confirmed.
	return EffectConfirmed, nil
}

// Compile-time: *Backend satisfies LeaderGate via IsLeader(). The worker
// accepts the narrower interface so a fake elector can drive unit tests.
var _ LeaderGate = backend.AlwaysLeader{}
