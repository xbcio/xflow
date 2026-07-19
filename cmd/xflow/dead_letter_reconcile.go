package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// newDeadLetterReconcileCommand implements `dead-letter reconcile`: a one-shot
// diff-scan over the authoritative Redis replay receipts that idempotently
// projects any receipt without a matching SQL row into the durable audit
// table. It does NOT re-execute any replay mutation — the Redis receipt is the
// authoritative record and the SQL projection is the durable secondary; this
// command only backfills the secondary when the in-request projector failed
// (e.g. a transient SQL outage or a process exit after the receipt was written
// but before the projection landed).
//
// Scope (T4): this is the dead-letter-receipt-specific, lightweight
// reconciler. The general leader-gated admission/outcome/reconciled-phase
// audit worker is T9; T4's projector + reconcile share the SQL schema and
// idempotency keys so T9 can reuse them.
func newDeadLetterReconcileCommand(opts *deadLetterOptions) *cobra.Command {
	var dryRun bool
	var redisAddr string
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Diff-scan Redis replay receipts and idempotently project missing ones to SQL",
		Long: `reconcile scans the authoritative Redis replay receipts and, for each one
with no matching SQL audit row, idempotently projects it (append-only, keyed by
the receipt's audit_id). It never re-executes a replay mutation; the Redis
receipt is authoritative and the SQL row is the durable secondary projection.

Use this after a SQL outage or a process exit between the Redis receipt write
and the SQL projection to backfill the durable audit trail. --dry-run reports
the diff without writing.

Requires --mysql-dsn (or XFLOW_MYSQL_DSN) for the durable projection and
--redis-addr (or XFLOW_REDIS_ADDR) for the receipt scan. This command does NOT
use the management API: it reads Redis (authoritative) and writes SQL
(durable secondary) directly, like a maintenance job.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.mysqlDSN == "" {
				return errors.New("--mysql-dsn (or XFLOW_MYSQL_DSN) is required for reconcile")
			}
			addr := redisAddr
			if addr == "" {
				addr = opts.redisAddr
			}
			return runReconcile(opts, addr, dryRun)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Report the diff without writing SQL rows")
	cmd.Flags().StringVar(&redisAddr, "redis-addr", "", "Override Redis address for the receipt scan (defaults to --redis-addr)")
	return cmd
}

// reconcileStats summarizes one reconcile run.
type reconcileStats struct {
	Scanned    int `json:"scanned"`
	Projected  int `json:"projected"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
	DryRun     bool `json:"dry_run"`
}

func runReconcile(opts *deadLetterOptions, redisAddr string, dryRun bool) error {
	// Open the durable SQL appender.
	provider, err := mysqlstore.New(opts.mysqlDSN)
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	defer func() {
		// The MySQL provider does not expose Close directly; the underlying
		// *gorm.DB is owned by mysqlstore.New. A future Close helper can be
		// added when the process needs to release the pool. reconcile is a
		// one-shot command, so the process exit releases the pool.
		_ = provider
	}()
	projector := control.NewReceiptProjector(provider)

	// Open the Redis receipt reader (distributed backend, no consumer).
	b, err := distributed.New(redisAddr, nil, distributed.WithConsumer(false))
	if err != nil {
		return fmt.Errorf("connect redis %q: %w", redisAddr, err)
	}
	defer closeRedis(b)
	reader, ok := b.State().(engine.ReplayReceiptReader)
	if !ok {
		return fmt.Errorf("StateStore %T does not implement ReplayReceiptReader", b.State())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	stats := reconcileStats{DryRun: dryRun}
	err = reader.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		stats.Scanned++
		if r.AuditID == "" {
			stats.Skipped++
			return nil
		}
		if dryRun {
			// In dry-run, attempt a read to classify projected vs missing.
			_, lookupErr := provider.AuditByReceiptAuditID(ctx, r.AuditID)
			if lookupErr == nil {
				stats.Skipped++
				return nil
			}
			stats.Projected++
			return nil
		}
		appended, perr := projector.Project(ctx, r)
		if perr != nil {
			stats.Failed++
			fmt.Fprintf(os.Stderr, "reconcile: project receipt %s failed: %v\n", r.AuditID, perr)
			return nil
		}
		if appended {
			stats.Projected++
		} else {
			stats.Skipped++
		}
		return nil
	})
	if err != nil {
		return err
	}
	return writeJSONLines(opts.out, stats)
}
