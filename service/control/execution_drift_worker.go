package control

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Execution drift repair defaults. The staleness window is deliberately much
// larger than a status projection's worst-case latency: a row still reading
// "running" after five minutes is not a write in flight, it is a write that
// was lost (or an execution whose process died), and the repair's job is to
// reconcile it against Redis rather than to race an active runner.
const (
	// DefaultExecutionDriftPeriod is how often the worker scans when
	// ExecutionDriftConfig.Period is unset.
	DefaultExecutionDriftPeriod = time.Minute
	// DefaultExecutionDriftStaleAfter is how long a durable "running" row
	// must have been untouched before it becomes a repair candidate.
	DefaultExecutionDriftStaleAfter = 5 * time.Minute
	// DefaultExecutionDriftBatch bounds one sweep so a large drift backlog
	// cannot hold a scan lock or starve the leader's other maintenance work.
	DefaultExecutionDriftBatch = 256
)

// Skip reasons reported through ExecutionDriftObserver.OnExecutionDriftSkipped.
// They name the authoritative answer that forbade a write; a skip is never an
// error and never a repair.
const (
	// ExecutionDriftSkipRunning means Redis still reports a non-terminal
	// status (usually "running"): the execution is alive, and the durable row
	// is stale only in the sense that activity has not reached it yet.
	ExecutionDriftSkipRunning = "running"
	// ExecutionDriftSkipNonTerminal means Redis reports a non-terminal status
	// other than running (queued, pending, paused, canceling). The projection
	// is not behind a terminal transition yet, so it is left alone.
	ExecutionDriftSkipNonTerminal = "non_terminal"
	// ExecutionDriftSkipNotFound means Redis has no status key for the
	// execution — it has passed its TTL and been collected. Absence is not
	// evidence of a terminal status; fabricating one here would turn a
	// retention artifact into a durable lie.
	ExecutionDriftSkipNotFound = "not_found"
)

// ExecutionDriftObserver receives drift repair outcomes so observability
// layers can emit metrics. All methods must be non-blocking.
type ExecutionDriftObserver interface {
	// OnExecutionDriftScan reports one scan cycle: candidates found and the
	// scan latency. err is non-nil when the stale-row query failed.
	OnExecutionDriftScan(ctx context.Context, candidates int, elapsed time.Duration, err error)
	// OnExecutionDriftRepaired reports one candidate whose authoritative
	// status is terminal and differs from the durable projection. applied is
	// false in DryRun mode: the classification happened, the write did not.
	// age is how long the durable row had been stale.
	OnExecutionDriftRepaired(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, age time.Duration, applied bool)
	// OnExecutionDriftSkipped reports a candidate left untouched, by reason
	// (ExecutionDriftSkipRunning, ExecutionDriftSkipNonTerminal,
	// ExecutionDriftSkipNotFound).
	OnExecutionDriftSkipped(ctx context.Context, reason string)
	// OnExecutionDriftError reports a per-candidate error (authority read or
	// projection write). The row stays as it was and is retried next sweep.
	OnExecutionDriftError(ctx context.Context, id types.ExecutionID, err error)
}

// ExecutionDriftConfig configures the worker.
type ExecutionDriftConfig struct {
	// Period is the interval between sweeps. Defaults to
	// DefaultExecutionDriftPeriod.
	Period time.Duration
	// StaleAfter is how long a durable "running" row must have been
	// untouched to become a candidate. Defaults to
	// DefaultExecutionDriftStaleAfter.
	StaleAfter time.Duration
	// Batch bounds one sweep. Defaults to DefaultExecutionDriftBatch.
	Batch int
	// DryRun classifies candidates (authority read + terminal decision) and
	// reports them through the observer, but writes nothing: every row stays
	// stale for the next real sweep. It exists so an operator can preview what
	// the repair would change before enabling it.
	DryRun bool
	// Namespaces restricts the scan to the durable rows of these namespaces,
	// so a deployment sharing its SQL table never rewrites another one's
	// projections. nil (or only empty entries) is the legacy cross-namespace
	// scan.
	Namespaces []namespace.Namespace
	// Elector gates leader-only execution: when set and IsLeader() is false,
	// a sweep is a no-op. Nil means "always run" (single-replica / dev).
	Elector LeaderGate
	// Logger receives structured info/error messages. Optional.
	Logger engine.Logger
	// Observer receives metrics. Optional.
	Observer ExecutionDriftObserver
}

// ExecutionDriftWorker repairs executions whose durable SQL projection still
// reads "running" after Redis has moved the execution to a terminal status.
//
// Redis is authoritative. A status projection is a best-effort mirror: when
// the write fails (or the process exits before issuing it) the durable row
// keeps the last status it saw, and nothing per-write can fix it — the failed
// write is gone. This worker closes that gap by scanning the durable rows that
// are stale by age and asking Redis what the execution's status actually is.
//
// The authority boundary is one-directional and deliberately narrow:
//
//   - Redis reports a terminal status (success/failed/canceled/timeout) that
//     the row does not have: the row is wrong, so rewrite it.
//   - Redis reports a non-terminal status: the row may simply be behind. Never
//     write; a live execution is not drift.
//   - Redis has no status key: the execution's retention window has elapsed.
//     Absence is not evidence of any outcome, so never write. Counting it is
//     the only honest signal available.
//
// The worker never invents an outcome, never writes a status Redis did not
// report, and never rewrites a row that already agrees with Redis.
type ExecutionDriftWorker struct {
	scanner    store.ExecutionDriftScanner
	statuses   engine.ExecutionStatusReader
	executions store.Executions
	elector    LeaderGate
	observer   ExecutionDriftObserver
	log        engine.Logger
	period     time.Duration
	staleAfter time.Duration
	batch      int
	dryRun     bool
	owned      []namespace.Namespace
	clock      func() time.Time
	sleepFunc  func(context.Context, time.Duration) error
}

// NewExecutionDriftWorker builds a worker. scanner, statuses and executions
// are required in production; when any is nil the worker is a no-op, which is
// how a store that does not implement store.ExecutionDriftScanner (an
// in-memory one) is handled without a second nil check at every call site.
func NewExecutionDriftWorker(scanner store.ExecutionDriftScanner, statuses engine.ExecutionStatusReader, executions store.Executions, cfg ExecutionDriftConfig) *ExecutionDriftWorker {
	if cfg.Period <= 0 {
		cfg.Period = DefaultExecutionDriftPeriod
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = DefaultExecutionDriftStaleAfter
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultExecutionDriftBatch
	}
	// Empty entries are dropped, and a set with nothing usable left is the
	// legacy unscoped scan — the same collapse namespace.NormalizeOwned applies
	// and the audit reconcile worker performs for its own scope.
	var owned []namespace.Namespace
	for _, ns := range cfg.Namespaces {
		if ns != "" {
			owned = append(owned, ns)
		}
	}
	return &ExecutionDriftWorker{
		scanner:    scanner,
		statuses:   statuses,
		executions: executions,
		elector:    cfg.Elector,
		observer:   cfg.Observer,
		log:        cfg.Logger,
		period:     cfg.Period,
		staleAfter: cfg.StaleAfter,
		batch:      cfg.Batch,
		dryRun:     cfg.DryRun,
		owned:      owned,
		clock:      func() time.Time { return time.Now().UTC() },
		sleepFunc:  sleepWithContext,
	}
}

// Run drives the repair loop until ctx is canceled. Blocks the caller; spawn
// it in a goroutine. A worker missing its dependencies is a no-op loop.
func (w *ExecutionDriftWorker) Run(ctx context.Context) error {
	if w == nil || w.scanner == nil || w.statuses == nil || w.executions == nil {
		<-ctx.Done()
		return nil
	}
	// Sweep once at startup so a clean restart repairs any drift left by a
	// crashed predecessor immediately rather than after one period.
	w.ReconcileOnce(ctx)
	for {
		if err := w.sleepFunc(ctx, w.period); err != nil {
			return nil
		}
		w.ReconcileOnce(ctx)
	}
}

// ReconcileOnce executes exactly one sweep and returns the number of durable
// rows rewritten. In DryRun mode nothing is written, so the return is always 0
// and the classifications arrive through the observer instead.
//
// A per-candidate error does not abort the sweep: each row is independent, and
// stopping at the first transient failure would let one bad row (or one
// authority blip) starve every older row behind it. Failed rows stay stale and
// are retried next sweep.
func (w *ExecutionDriftWorker) ReconcileOnce(ctx context.Context) int {
	if w == nil || w.scanner == nil || w.statuses == nil || w.executions == nil {
		return 0
	}
	if w.elector != nil && !w.elector.IsLeader() {
		return 0
	}
	before := w.clock().Add(-w.staleAfter)
	started := time.Now()
	candidates, err := w.scanner.ListStaleRunningExecutions(ctx, w.owned, before, w.batch)
	w.observe(func(o ExecutionDriftObserver) {
		o.OnExecutionDriftScan(ctx, len(candidates), time.Since(started), err)
	})
	if err != nil {
		if w.log != nil {
			w.log.Error("execution drift repair: list stale running executions", "err", err)
		}
		return 0
	}
	repaired := 0
	for i, candidate := range candidates {
		// Leadership can change while a bounded sweep is in flight; stop
		// writing rather than keep repairing projections the new leader will
		// scan again (the rewrite is idempotent, but the work is not ours).
		if i > 0 && i%leaderRecheckInterval == 0 && leaderLost(w.elector) {
			if w.log != nil {
				w.log.Info("execution drift repair stopped early",
					"repaired", repaired,
					"inspected", i,
					"candidates", len(candidates),
				)
			}
			break
		}
		if w.repair(ctx, candidate) {
			repaired++
		}
	}
	return repaired
}

// repair reconciles one candidate against authoritative state. Returns true
// only when the durable row was actually rewritten.
func (w *ExecutionDriftWorker) repair(ctx context.Context, candidate store.ExecutionDriftCandidate) bool {
	// The candidate's own namespace addresses the authoritative status key,
	// and everything about this row below uses it. The sweep is deliberately
	// cross-namespace (the durable table is scanned as a whole and each row
	// carries its namespace), so the sweep ctx has no namespace to speak of —
	// reading the status under it would file every tenant's execution under
	// namespace.Default and miss the key.
	recCtx := ctx
	if candidate.Namespace != "" {
		recCtx = namespace.WithNamespace(recCtx, candidate.Namespace)
	}
	status, found, err := w.statuses.GetExecutionStatus(recCtx, candidate.ID)
	if err != nil {
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftError(recCtx, candidate.ID, err)
		})
		if w.log != nil {
			w.log.Error("execution drift repair: read authoritative status",
				"execution_id", string(candidate.ID), "err", err)
		}
		return false
	}
	if !found {
		// The status key has passed its retention window. There is no
		// authoritative terminal status to copy, and "not found" is not one.
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftSkipped(recCtx, ExecutionDriftSkipNotFound)
		})
		if w.log != nil {
			w.log.Debug("execution drift repair skipped; authoritative status is gone",
				"execution_id", string(candidate.ID))
		}
		return false
	}
	if !types.IsTerminalExecutionStatus(status) {
		reason := ExecutionDriftSkipNonTerminal
		if status == types.ExecutionStatusRunning {
			reason = ExecutionDriftSkipRunning
		}
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftSkipped(recCtx, reason)
		})
		return false
	}
	// The scan only returns rows whose durable status is "running", so a
	// terminal authoritative status is by construction a different status.
	age := w.clock().Sub(candidate.UpdatedAt)
	if w.dryRun {
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftRepaired(recCtx, candidate.ID, status, age, false)
		})
		return false
	}
	if err := w.executions.UpdateExecutionStatus(recCtx, candidate.ID, status, ""); err != nil {
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftError(recCtx, candidate.ID, err)
		})
		if w.log != nil {
			w.log.Error("execution drift repair: update durable projection",
				"execution_id", string(candidate.ID), "status", string(status), "err", err)
		}
		return false
	}
	w.observe(func(o ExecutionDriftObserver) {
		o.OnExecutionDriftRepaired(recCtx, candidate.ID, status, age, true)
	})
	if w.log != nil {
		w.log.Info("execution drift repaired",
			"execution_id", string(candidate.ID),
			"status", string(status),
			"stale_ms", age.Milliseconds(),
		)
	}
	return true
}

// observe calls the observer defensively: a panicking metrics adapter must not
// take the repair loop down, and a nil observer is the common dev case.
func (w *ExecutionDriftWorker) observe(fn func(ExecutionDriftObserver)) {
	if w == nil || w.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(w.observer)
}
