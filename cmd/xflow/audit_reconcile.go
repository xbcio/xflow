package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// auditReconcileSweepCap bounds the CLI's sweep loop. The real bound is the
// 5-minute context deadline below; this is only a liveness backstop so a
// caller that hands the loop a deadline-free context still terminates.
const auditReconcileSweepCap = 10000

// auditOptions holds the flags for `xflow audit reconcile`.
type auditOptions struct {
	redisAddr  string
	mysqlDSN   string
	batch      int
	backlogAge time.Duration
	dryRun     bool
	out        io.Writer
}

func newAuditCommand(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Audit-log maintenance commands",
	}
	cmd.AddCommand(newAuditReconcileCommand(out))
	return cmd
}

func newAuditReconcileCommand(out io.Writer) *cobra.Command {
	opts := &auditOptions{out: out}
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Settle audit admissions whose outcome audit was lost (direct maintenance mode)",
		Long: `reconcile scans the durable audit log for admitted mutations that never
received a post-handler outcome (e.g. a crash between the mutation and its
outcome audit), consults authoritative execution state (Redis) to decide
whether each mutation landed, and appends the missing outcome idempotently.
It NEVER re-executes a mutation — it only observes authoritative state.

This is direct maintenance mode, like dead-letter reconcile: it reads Redis
(authoritative) and reads/writes SQL (durable secondary) directly, bypassing
the management API. It runs the SAME worker the server runs, with the same
cursor semantics, idempotency key ((namespace, request_id, phase)) and
"indeterminate → never fabricate" contract.

No leader election: a single CLI run does not campaign. Running concurrently
with the server's worker is safe — both settle through AppendOutcomeIfAbsent,
so an admission is never settled twice — but prefer a maintenance window so
the scan does not compete with live traffic.

--dry-run probes and classifies every pending admission without writing any
row, reporting what a real run would settle.

Exit code 0 means the run completed (even if admissions remain pending);
1 means a flag, connection, scan, or timeout failure. Per-admission probe or
append errors are reported in the JSON (errors field) and leave the admission
pending for the next run — they do not fail the command.

Output is one JSON line: {scanned, reconciled, failed, skipped_indeterminate,
errors, pending_before, pending_after, dry_run}.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts.redisAddr = resolveEnvFlag(cmd, "redis-addr", "XFLOW_REDIS_ADDR", "")
			opts.mysqlDSN = resolveEnvFlag(cmd, "mysql-dsn", "XFLOW_MYSQL_DSN", "")
			return runAuditReconcile(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.redisAddr, "redis-addr", "",
		"Redis address holding the authoritative execution state (env: XFLOW_REDIS_ADDR)")
	cmd.Flags().StringVar(&opts.mysqlDSN, "mysql-dsn", "",
		"MySQL DSN holding the durable audit log (env: XFLOW_MYSQL_DSN)")
	cmd.Flags().IntVar(&opts.batch, "batch", control.DefaultReconcileBatch,
		"Admissions to scan per sweep")
	cmd.Flags().DurationVar(&opts.backlogAge, "backlog-age", control.DefaultReconcileBacklog,
		"Age an admission must reach before it is presumed orphaned (younger = in flight)")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false,
		"Probe and classify without appending any outcome row")
	return cmd
}

func runAuditReconcile(ctx context.Context, opts *auditOptions) error {
	if opts.mysqlDSN == "" {
		return errors.New("--mysql-dsn (or XFLOW_MYSQL_DSN) is required: the reconcile settles rows in the durable audit log")
	}
	if opts.redisAddr == "" {
		return errors.New("--redis-addr (or XFLOW_REDIS_ADDR) is required: the authority probe reads execution state from Redis")
	}
	if opts.batch <= 0 {
		return errors.New("--batch must be positive")
	}
	if opts.backlogAge <= 0 {
		return errors.New("--backlog-age must be positive")
	}

	// *sqlstore.Provider satisfies store.AuditReconciler statically (it embeds
	// the audit repo), which is why this build point needs no
	// apiserver.AuditReconcilable check: a provider that stopped implementing
	// the reconcile scans would fail to compile here, not settle nothing
	// silently.
	provider, err := mysqlstore.New(opts.mysqlDSN)
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}

	// No consumer: this is a one-shot maintenance scan, not a queue participant.
	b, err := distributed.New(opts.redisAddr, nil, distributed.WithConsumer(false))
	if err != nil {
		return fmt.Errorf("connect redis %q: %w", opts.redisAddr, err)
	}
	defer closeRedis(b)

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	return reconcileAudit(runCtx, provider, control.NewExecutionAuthority(b.State()), opts)
}

// auditReconcileCollector implements control.ReconcileObserver to produce the
// JSON report. ReconcileOnce's return only counts appended rows; this collector
// keeps the per-classification breakdown and the last sweep's candidate count,
// which decides when the sweep loop has reached the tail of the backlog.
//
// The pending counts in the report do NOT come from here: OnReconcileBacklog's
// count is taken at sweep start (it cannot show the post-sweep state), so
// reconcileAudit queries CountUnreconciledAdmissions directly before and after
// the loop instead.
type auditReconcileCollector struct {
	scanned        int
	reconciled     int
	failed         int
	indeterminate  int
	errors         int
	lastCandidates int
	scanErr        error
}

func (c *auditReconcileCollector) OnReconcileScan(_ context.Context, candidates int, _ time.Duration, err error) {
	c.scanned += candidates
	c.lastCandidates = candidates
	if err != nil {
		c.scanErr = err
	}
}

func (c *auditReconcileCollector) OnReconcileSettled(_ context.Context, outcome string, _ bool, _ int64) {
	switch outcome {
	case store.AuditOutcomeReconciled:
		c.reconciled++
	case store.AuditOutcomeFailed:
		c.failed++
	}
}

func (c *auditReconcileCollector) OnReconcileSkipped(context.Context, string)      { c.indeterminate++ }
func (c *auditReconcileCollector) OnReconcileError(context.Context, string, error) { c.errors++ }
func (c *auditReconcileCollector) OnReconcileBacklog(context.Context, time.Duration, int) {
}

// auditReconcileStats is the single-line JSON report, following the
// reconcileStats convention of dead-letter reconcile. reconciled / failed /
// skipped_indeterminate are CLASSIFICATIONS (what each admission's effect
// decided), so a dry run reports what a real run would settle; errors counts
// per-admission probe/append failures, which leave the admission pending.
type auditReconcileStats struct {
	Scanned              int  `json:"scanned"`
	Reconciled           int  `json:"reconciled"`
	Failed               int  `json:"failed"`
	SkippedIndeterminate int  `json:"skipped_indeterminate"`
	Errors               int  `json:"errors"`
	PendingBefore        int  `json:"pending_before"`
	PendingAfter         int  `json:"pending_after"`
	DryRun               bool `json:"dry_run"`
}

// reconcileAudit runs the worker over the reconciler and prints the report.
// Split from runAuditReconcile so tests drive it with an in-memory reconciler
// and a fake authority instead of live MySQL/Redis connections.
func reconcileAudit(ctx context.Context, reconciler store.AuditReconciler, authority control.AdmissionAuthority, opts *auditOptions) error {
	before := time.Now().Add(-opts.backlogAge)
	pendingBefore, _, err := reconciler.CountUnreconciledAdmissions(ctx, before)
	if err != nil {
		return fmt.Errorf("audit reconcile: count pending admissions: %w", err)
	}

	collector := &auditReconcileCollector{}
	worker := control.NewAuditReconcileWorker(reconciler, authority, control.AuditReconcileConfig{
		Batch:      opts.batch,
		BacklogAge: opts.backlogAge,
		DryRun:     opts.dryRun,
		Observer:   collector,
		// No Elector on purpose (nil = always run): a single CLI run does not
		// campaign for leadership. Concurrency with the server's leader-gated
		// worker is safe because settlement is idempotent
		// (AppendOutcomeIfAbsent + the phase_key unique index).
	})
	for sweep := 0; sweep < auditReconcileSweepCap; sweep++ {
		worker.ReconcileOnce(ctx)
		if collector.scanErr != nil {
			return fmt.Errorf("audit reconcile: scan pending admissions: %w", collector.scanErr)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("audit reconcile: %w", err)
		}
		// Fewer rows than a full batch means the cursor reached the tail of
		// the backlog; there is nothing further to sweep.
		if collector.lastCandidates < opts.batch {
			break
		}
	}

	pendingAfter, _, err := reconciler.CountUnreconciledAdmissions(ctx, time.Now().Add(-opts.backlogAge))
	if err != nil {
		return fmt.Errorf("audit reconcile: count remaining admissions: %w", err)
	}

	return writeJSONLines(opts.out, auditReconcileStats{
		Scanned:              collector.scanned,
		Reconciled:           collector.reconciled,
		Failed:               collector.failed,
		SkippedIndeterminate: collector.indeterminate,
		Errors:               collector.errors,
		PendingBefore:        pendingBefore,
		PendingAfter:         pendingAfter,
		DryRun:               opts.dryRun,
	})
}
