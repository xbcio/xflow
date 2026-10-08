package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"github.com/xbcio/xflow/types"
)

// executionDiffOptions holds the flags for `xflow execution diff`: a one-shot,
// strictly read-only comparison between the Redis-authoritative execution
// state and its SQL projection, for one execution ID.
//
// This is NOT a reconciler: STORAGE-CONTRACT.md:300-303 lists an "active
// reconciliation job" as a planned gap. The two reconcilers that exist today
// (dead_letter_reconcile.go, and the dead-letter audit projector it shares
// with T9) both repair a projection by writing to SQL when they find a gap.
// Nothing scans the two execution-state copies for drift and merely reports
// it. This command is that missing read-only scan: it never calls a Redis
// write method (rstate's Create/Update/Upsert family) or a SQL write method
// (sqlstore's Create/Update/Delete family) — see diffExecution below.
type executionDiffOptions struct {
	redisAddr   string
	mysqlDSN    string
	namespace   string
	executionID string
	jsonOut     bool
	out         io.Writer
}

func newExecutionCommand(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "execution",
		Short: "Execution-state inspection commands",
	}
	cmd.AddCommand(newExecutionDiffCommand(out))
	return cmd
}

func newExecutionDiffCommand(out io.Writer) *cobra.Command {
	opts := &executionDiffOptions{out: out}
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Compare one execution's Redis state against its SQL projection (read-only)",
		Long: `diff reads the Redis-authoritative execution-state record and the SQL
projection for the same execution ID and reports any difference between them:
a row missing on either side, a status mismatch, or an error-message mismatch.

It is strictly read-only: it never writes to Redis or SQL. Unlike
dead-letter reconcile (which backfills a missing SQL audit row), this command
only reports drift — closing STORAGE-CONTRACT.md's "planned" gap for an active
diff scan, distinct from the event-driven reconciliation paths that already
exist.

Requires --mysql-dsn (or XFLOW_MYSQL_DSN), --redis-addr (or XFLOW_REDIS_ADDR),
--namespace (Redis execution keys are namespace-sharded, so the scope that
wrote the execution must be supplied explicitly — there is no way to recover
it from the execution ID alone), and --execution-id.

Exit code 0 means no difference was found; non-zero means a difference was
found (or the scan itself failed), so a script can gate on exit status alone.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExecutionDiff(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.redisAddr, "redis-addr", envOr("XFLOW_REDIS_ADDR", ""),
		"Redis address holding the authoritative execution state (env: XFLOW_REDIS_ADDR)")
	cmd.Flags().StringVar(&opts.mysqlDSN, "mysql-dsn", envOr("XFLOW_MYSQL_DSN", ""),
		"MySQL DSN holding the SQL execution projection (env: XFLOW_MYSQL_DSN)")
	cmd.Flags().StringVar(&opts.namespace, "namespace", "",
		"Namespace the execution was created in (required; Redis execution keys are namespace-scoped)")
	cmd.Flags().StringVar(&opts.executionID, "execution-id", "",
		"Execution ID to compare")
	cmd.Flags().BoolVar(&opts.jsonOut, "json", false, "Emit the report as a single JSON line instead of human-readable text")
	return cmd
}

// executionStateReader is the narrow read-only surface this command needs
// from each side. It intentionally excludes every write method so a caller
// cannot accidentally mutate state while diffing it; redisExecutionReader and
// sqlExecutionReader (below) are the only two production implementations, and
// both are compile-time asserted to satisfy exactly this surface and nothing
// mutating.
type executionStateReader interface {
	// readExecution returns (nil, nil) when the execution does not exist on
	// this side. A non-nil error means the read itself failed (network,
	// decode, etc.) and the diff cannot be trusted.
	readExecution(ctx context.Context, ns namespace.Namespace, id types.ExecutionID) (*executionView, error)
}

// executionView is the side-agnostic projection of execution state this
// command compares. Both backends expose far more fields (graph, params,
// runtime, scope, trace ids); the MVP comparison is scoped to presence,
// status, and error, per the task's stated field set.
type executionView struct {
	Status types.ExecutionStatus
	Error  string
}

// redisExecutionReader adapts engine.StateStore (specifically its embedded
// Executions.GetExecution) to executionStateReader. It is read-only: Executions
// also declares CreateExecution and UpdateExecutionStatus, but this type never
// calls them.
type redisExecutionReader struct {
	state engine.StateStore
}

func (r redisExecutionReader) readExecution(ctx context.Context, ns namespace.Namespace, id types.ExecutionID) (*executionView, error) {
	snap, err := r.state.GetExecution(namespace.WithNamespace(ctx, ns), id)
	if err != nil {
		return nil, fmt.Errorf("redis get execution %q: %w", id, err)
	}
	if snap == nil {
		return nil, nil
	}
	return &executionView{Status: snap.Status, Error: snap.Error}, nil
}

// sqlExecutionGetter is the single sqlstore method this command's SQL-side
// adapter uses. *sqlstore.Provider satisfies it directly (it embeds
// store.Executions, whose GetExecution has exactly this signature); tests
// substitute a fake instead of a real MySQL connection — this package's test
// discipline forbids depending on a live database (shared 3306, concurrent
// sessions), unlike dead_letter_reconcile_test.go's existing testMySQLDSN
// helper.
type sqlExecutionGetter interface {
	GetExecution(ctx context.Context, id types.ExecutionID) (*store.ExecutionRecord, error)
}

// sqlExecutionReader adapts a sqlExecutionGetter to executionStateReader.
// Namespace is accepted for interface symmetry with redisExecutionReader but
// unused: store.ExecutionRecord carries its own Namespace field and
// GetExecution looks up by execution_id alone (see store/sqlstore/execution.go)
// — namespace scoping there only governs ListExecutions/CountExecutions, not a
// point lookup by ID.
type sqlExecutionReader struct {
	getter sqlExecutionGetter
}

func (r sqlExecutionReader) readExecution(ctx context.Context, _ namespace.Namespace, id types.ExecutionID) (*executionView, error) {
	row, err := r.getter.GetExecution(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("sql get execution %q: %w", id, err)
	}
	if row == nil {
		return nil, nil
	}
	return &executionView{Status: row.Status, Error: row.Error}, nil
}

// sqlProviderAdapter satisfies sqlExecutionGetter directly from
// *sqlstore.Provider — no translation needed, since Provider's embedded
// executionRepo.GetExecution already has this exact signature. It exists only
// so runExecutionDiff can name the production dependency distinctly from the
// interface, matching the redisExecutionReader/engine.StateStore split above.
type sqlProviderAdapter struct {
	provider *sqlstore.Provider
}

func (a sqlProviderAdapter) GetExecution(ctx context.Context, id types.ExecutionID) (*store.ExecutionRecord, error) {
	return a.provider.GetExecution(ctx, id)
}

// executionDiffResult is the JSON/text report shape for one execution.
type executionDiffResult struct {
	ExecutionID string   `json:"execution_id"`
	Namespace   string   `json:"namespace"`
	RedisFound  bool     `json:"redis_found"`
	SQLFound    bool     `json:"sql_found"`
	Differences []string `json:"differences,omitempty"`
}

// Diverged reports whether any difference (including a one-sided presence
// gap) was found. The CLI entry point uses this to pick the exit code.
func (r executionDiffResult) Diverged() bool {
	return len(r.Differences) > 0
}

// diffExecution compares the two sides for one execution ID. It never calls a
// write method on either reader: executionStateReader's only method is
// readExecution.
func diffExecution(ctx context.Context, redis, sql executionStateReader, ns namespace.Namespace, id types.ExecutionID) (executionDiffResult, error) {
	result := executionDiffResult{ExecutionID: string(id), Namespace: string(ns)}

	redisView, err := redis.readExecution(ctx, ns, id)
	if err != nil {
		return result, err
	}
	sqlView, err := sql.readExecution(ctx, ns, id)
	if err != nil {
		return result, err
	}

	result.RedisFound = redisView != nil
	result.SQLFound = sqlView != nil

	switch {
	case redisView == nil && sqlView == nil:
		result.Differences = append(result.Differences, "execution not found on either side")
		return result, nil
	case redisView == nil:
		result.Differences = append(result.Differences, "missing in redis (authoritative side)")
		return result, nil
	case sqlView == nil:
		result.Differences = append(result.Differences, "missing in sql projection")
		return result, nil
	}

	if redisView.Status != sqlView.Status {
		result.Differences = append(result.Differences, fmt.Sprintf(
			"status mismatch: redis=%q sql=%q", redisView.Status, sqlView.Status))
	}
	if redisView.Error != sqlView.Error {
		result.Differences = append(result.Differences, fmt.Sprintf(
			"error mismatch: redis=%q sql=%q", redisView.Error, sqlView.Error))
	}
	return result, nil
}

func runExecutionDiff(ctx context.Context, opts *executionDiffOptions) error {
	if opts.mysqlDSN == "" {
		return errors.New("--mysql-dsn (or XFLOW_MYSQL_DSN) is required for execution diff")
	}
	if opts.redisAddr == "" {
		return errors.New("--redis-addr (or XFLOW_REDIS_ADDR) is required for execution diff")
	}
	if opts.namespace == "" {
		return errors.New("--namespace is required for execution diff (redis execution keys are namespace-scoped)")
	}
	if opts.executionID == "" {
		return errors.New("--execution-id is required for execution diff")
	}
	ns := namespace.Namespace(opts.namespace)
	if err := namespace.Validate(ns); err != nil {
		return fmt.Errorf("--namespace: %w", err)
	}

	provider, err := mysqlstore.New(opts.mysqlDSN)
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}

	// No consumer: this is a one-shot read-only scan, not a queue participant.
	b, err := distributed.New(opts.redisAddr, nil, distributed.WithConsumer(false))
	if err != nil {
		return fmt.Errorf("connect redis %q: %w", opts.redisAddr, err)
	}
	defer closeRedis(b)

	redisReader := redisExecutionReader{state: b.State()}
	sqlReader := sqlExecutionReader{getter: sqlProviderAdapter{provider: provider}}

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	result, err := diffExecution(runCtx, redisReader, sqlReader, ns, types.ExecutionID(opts.executionID))
	if err != nil {
		return fmt.Errorf("execution diff: %w", err)
	}

	if opts.jsonOut {
		if werr := writeJSONLines(opts.out, result); werr != nil {
			return werr
		}
	} else {
		writeExecutionDiffText(opts.out, result)
	}

	if result.Diverged() {
		return fmt.Errorf("execution diff: %d difference(s) found for execution %q", len(result.Differences), opts.executionID)
	}
	return nil
}

func writeExecutionDiffText(w io.Writer, result executionDiffResult) {
	fmt.Fprintf(w, "execution %s (namespace %s): redis_found=%v sql_found=%v\n",
		result.ExecutionID, result.Namespace, result.RedisFound, result.SQLFound)
	if !result.Diverged() {
		fmt.Fprintln(w, "no differences found")
		return
	}
	fmt.Fprintln(w, "differences:")
	for _, d := range result.Differences {
		fmt.Fprintf(w, "  - %s\n", d)
	}
}
