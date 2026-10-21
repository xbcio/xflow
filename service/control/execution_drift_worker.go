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
	// DefaultExecutionDriftBatch bounds one scan page so a large drift backlog
	// cannot hold a scan lock or starve the leader's other maintenance work.
	DefaultExecutionDriftBatch = 256
	// MaxExecutionDriftBatch is the ceiling ExecutionDriftConfig.Batch is
	// clamped to. One page is one SQL read plus an authoritative status read
	// per row, so a mistyped --execution-drift-batch must not turn a page into
	// a table-sized pass.
	MaxExecutionDriftBatch = 1024
	// DefaultExecutionDriftMaxPages bounds how many pages one sweep walks.
	// Paging is what stops unrepairable rows at the head of the scan order
	// from starving the rows behind them, but an unbounded walk would turn one
	// sweep into a full-table pass; with the default batch this caps a sweep
	// at 2048 candidate rows.
	DefaultExecutionDriftMaxPages = 8
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
	// ExecutionDriftSkipAlreadyAdvanced means the conditional repair matched
	// no row: between the scan and the write the ordinary projection path
	// advanced the durable row itself (with its own error_msg), or the row was
	// deleted. Nothing is wrong and nothing is written.
	ExecutionDriftSkipAlreadyAdvanced = "already_advanced"
	// ExecutionDriftSkipRepairUnsupported means the store advertises the scan
	// but not the conditional write (store.ExecutionDriftRepairer). The sweep
	// can still classify candidates, but it cannot repair them.
	ExecutionDriftSkipRepairUnsupported = "repair_unsupported"
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
	// ExecutionDriftSkipNotFound, ExecutionDriftSkipAlreadyAdvanced,
	// ExecutionDriftSkipRepairUnsupported).
	OnExecutionDriftSkipped(ctx context.Context, reason string)
	// OnExecutionDriftError reports a per-candidate error (authority read or
	// projection write). The row stays as it was and is retried by the next
	// sweep that starts from the beginning; a sweep stopped by its page budget
	// waits for the cursor to wrap back to the head.
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
	// Batch bounds one scan page. Defaults to DefaultExecutionDriftBatch;
	// values above MaxExecutionDriftBatch are clamped, because a page is one
	// SQL read plus an authoritative read per row. Skipped candidates count
	// toward the page, so a page is a bound on rows examined, not rows
	// rewritten.
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
//
// Each sweep walks pages of candidates in the scan's total order, carrying a
// cursor between pages so an unrepairable row at the head of the order cannot
// starve the rows behind it. The cursor is kept across sweeps (a sweep capped
// by DefaultExecutionDriftMaxPages, or interrupted by a lost leadership,
// resumes where it stopped) and reset to the beginning when a sweep reaches the
// end of the candidate set, so the rows skipped on the way are retried next
// time.
//
// The write itself is conditional (store.ExecutionDriftRepairer): the scan and
// the repair are not atomic, and the ordinary projection path may advance a
// candidate between them. A row that is no longer "running" is left alone.
type ExecutionDriftWorker struct {
	scanner    store.ExecutionDriftScanner
	statuses   engine.ExecutionStatusReader
	executions store.Executions
	repairer   store.ExecutionDriftRepairer
	elector    LeaderGate
	observer   ExecutionDriftObserver
	log        engine.Logger
	period     time.Duration
	staleAfter time.Duration
	batch      int
	maxPages   int
	dryRun     bool
	owned      []namespace.Namespace
	// cursor is the position of the last candidate the current sweep
	// classified. ReconcileOnce is driven by a single Run goroutine; it is not
	// safe to call concurrently with itself or with Run.
	cursor    store.ExecutionDriftCursor
	clock     func() time.Time
	sleepFunc func(context.Context, time.Duration) error
}

// NewExecutionDriftWorker builds a worker. scanner, statuses and executions
// are required in production; when any is nil the worker is a no-op, which is
// how a store that does not implement store.ExecutionDriftScanner (an
// in-memory one) is handled without a second nil check at every call site.
//
// executions should also implement store.ExecutionDriftRepairer, the
// conditional write. Without it the sweep still scans and classifies but skips
// every write with ExecutionDriftSkipRepairUnsupported: repairing through the
// unconditional Executions.UpdateExecutionStatus would erase an error_msg the
// ordinary projection path recorded between the scan and the write.
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
	if cfg.Batch > MaxExecutionDriftBatch {
		if cfg.Logger != nil {
			cfg.Logger.Warn("execution drift repair: batch clamped to the page ceiling",
				"configured", cfg.Batch, "max", MaxExecutionDriftBatch)
		}
		cfg.Batch = MaxExecutionDriftBatch
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
	// The conditional write is a separate optional capability from the
	// Executions surface the worker is given. Resolving it once here keeps the
	// per-candidate path free of repeated assertions; a store that has the scan
	// but not the write still classifies, it just cannot repair.
	var repairer store.ExecutionDriftRepairer
	if executions != nil {
		repairer, _ = executions.(store.ExecutionDriftRepairer)
	}
	return &ExecutionDriftWorker{
		scanner:    scanner,
		statuses:   statuses,
		executions: executions,
		repairer:   repairer,
		elector:    cfg.Elector,
		observer:   cfg.Observer,
		log:        cfg.Logger,
		period:     cfg.Period,
		staleAfter: cfg.StaleAfter,
		batch:      cfg.Batch,
		maxPages:   DefaultExecutionDriftMaxPages,
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
// A sweep walks pages of at most Batch candidates until a short page says the
// candidate set is exhausted (then the cursor resets, so the next sweep retries
// the rows skipped on the way), it has walked DefaultExecutionDriftMaxPages
// pages (the cursor is kept, so the next sweep resumes), or leadership is lost.
// Without that paging an unrepairable row at the head of the order (its Redis
// status has expired) would be re-read forever and every row behind it would
// never be examined.
//
// A per-candidate error does not abort the sweep: each row is independent, and
// stopping at the first transient failure would let one bad row (or one
// authority blip) starve every older row behind it. Failed rows stay stale and
// are retried by the next sweep that starts from the beginning; when a sweep
// stops on its page budget, that retry waits for the cursor to wrap back to the
// head.
func (w *ExecutionDriftWorker) ReconcileOnce(ctx context.Context) int {
	if w == nil || w.scanner == nil || w.statuses == nil || w.executions == nil {
		return 0
	}
	if w.elector != nil && !w.elector.IsLeader() {
		return 0
	}
	before := w.clock().Add(-w.staleAfter)
	started := time.Now()
	repaired, scanned, examined := 0, 0, 0
	var scanErr error
	for page := 0; page < w.maxPages; page++ {
		candidates, err := w.scanner.ListStaleRunningExecutions(ctx, w.owned, before, w.cursor, w.batch)
		if err != nil {
			scanErr = err
			break
		}
		scanned += len(candidates)
		stopped := false
		for _, candidate := range candidates {
			// Leadership can change while a bounded sweep is in flight; stop
			// writing rather than keep repairing projections the new leader will
			// scan again (the rewrite is idempotent, but the work is not ours).
			// examined is cumulative across pages on purpose: a page-local index
			// never reaches the interval when Batch < leaderRecheckInterval, which
			// would silently disable the re-check for a small page bound.
			if examined > 0 && examined%leaderRecheckInterval == 0 && leaderLost(w.elector) {
				if w.log != nil {
					w.log.Info("execution drift repair stopped early",
						"repaired", repaired,
						"inspected", examined,
						"candidates", scanned,
					)
				}
				stopped = true
				break
			}
			// Advance before classifying: this candidate has been examined
			// either way, and a failed authority read or write is retried on
			// the next sweep that starts from the beginning, not by re-reading
			// it inside this one.
			w.cursor = store.ExecutionDriftCursor{UpdatedAt: candidate.UpdatedAt, RowID: candidate.RowID}
			examined++
			if w.repair(ctx, candidate) {
				repaired++
			}
		}
		if stopped {
			break
		}
		if len(candidates) < w.batch {
			// Short page: the candidate set is exhausted, so restart the next
			// sweep from the oldest row to retry what this one skipped.
			w.cursor = store.ExecutionDriftCursor{}
			break
		}
	}
	w.observe(func(o ExecutionDriftObserver) {
		o.OnExecutionDriftScan(ctx, scanned, time.Since(started), scanErr)
	})
	if scanErr != nil {
		if w.log != nil {
			w.log.Error("execution drift repair: list stale running executions", "err", scanErr)
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
	if w.repairer == nil {
		// The store can be scanned but not conditionally written. Classifying
		// without repairing is still the honest thing to do: the alternative
		// is an unconditional rewrite, which would erase a real transition.
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftSkipped(recCtx, ExecutionDriftSkipRepairUnsupported)
		})
		if w.log != nil {
			w.log.Debug("execution drift repair skipped; store does not support the conditional repair write",
				"execution_id", string(candidate.ID))
		}
		return false
	}
	applied, err := w.repairer.RepairRunningExecutionStatus(recCtx, candidate.ID, status)
	if err != nil {
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftError(recCtx, candidate.ID, err)
		})
		if w.log != nil {
			w.log.Error("execution drift repair: update durable projection",
				"execution_id", string(candidate.ID), "status", string(status), "err", err)
		}
		return false
	}
	if !applied {
		// The row stopped reading "running" between the scan and this write:
		// the ordinary projection path won the race, possibly with the very
		// terminal status (and error_msg) this repair would have written. The
		// conditional write matched nothing, which is the desired outcome.
		w.observe(func(o ExecutionDriftObserver) {
			o.OnExecutionDriftSkipped(recCtx, ExecutionDriftSkipAlreadyAdvanced)
		})
		if w.log != nil {
			w.log.Debug("execution drift repair skipped; durable row advanced before the write",
				"execution_id", string(candidate.ID), "status", string(status))
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
