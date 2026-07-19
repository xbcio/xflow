package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// deadLetterOptions holds the shared --redis-addr flag and the output sink.
type deadLetterOptions struct {
	redisAddr string
	out       io.Writer
}

func newDeadLetterCommand(out io.Writer) *cobra.Command {
	opts := &deadLetterOptions{out: out}
	cmd := &cobra.Command{
		Use:   "dead-letter",
		Short: "Inspect and replay durable outbox dead-letter entries",
		Long: `Inspect and replay durable scheduling outbox entries that exceeded the
delivery attempt limit and were moved to dead-letter storage.

Replay moves an entry atomically back to the ready set; the control plane's
running OutboxDispatcher redelivers it. Replay is activation-safe: it rejects
entries whose node is terminal or whose activation no longer matches the
node's current activation, so a stale cyclic re-entry cannot be resurrected.
Replay is idempotent under --request-id: retrying with the same request-id
after a lost response returns already_replayed with the original audit_id.

Operator identity is derived from the authenticated principal — the CLI
injects "cli:<user>" for the G0 maintenance path; --operator is not accepted.
The authoritative receipt is written to Redis; the stdout audit line is a
secondary projection only.`,
	}
	cmd.PersistentFlags().StringVar(&opts.redisAddr, "redis-addr", envOr("XFLOW_REDIS_ADDR", "localhost:6379"),
		"Redis address backing the distributed StateStore (env: XFLOW_REDIS_ADDR)")

	cmd.AddCommand(newDeadLetterListCommand(opts))
	cmd.AddCommand(newDeadLetterReplayCommand(opts))
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newDeadLetterListCommand(opts *deadLetterOptions) *cobra.Command {
	var executionID string
	var limit int
	var cursor string
	var tenantFlag string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List dead-lettered outbox entries for an execution (read-only, paginated)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if executionID == "" {
				return fmt.Errorf("--execution is required")
			}
			if err := tenant.Validate(tenant.TenantID(tenantFlag)); err != nil {
				return fmt.Errorf("invalid --tenant: %w", err)
			}
			mgr, closeFn, err := openDeadLetterManager(opts.redisAddr, opts.out)
			if err != nil {
				return err
			}
			defer closeFn()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ctx = tenant.WithTenant(ctx, tenant.TenantID(tenantFlag))
			page := engine.DeadLetterPage{Limit: limit, Cursor: cursor}
			list, err := mgr.List(ctx, types.ExecutionID(executionID), page)
			if err != nil {
				return err
			}
			for _, entry := range list.Entries {
				if err := writeJSONLines(opts.out, entry); err != nil {
					return err
				}
			}
			if list.NextCursor != "" {
				fmt.Fprintf(opts.out, `{"next_cursor":%q}`+"\n", list.NextCursor)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&executionID, "execution", "", "Execution ID (required)")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum entries to return per page (bounded)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Opaque cursor from a prior page's next_cursor")
	cmd.Flags().StringVar(&tenantFlag, "tenant", envOr("XFLOW_TENANT", string(tenant.DefaultTenant)), "Tenant namespace (env: XFLOW_TENANT)")
	return cmd
}

func newDeadLetterReplayCommand(opts *deadLetterOptions) *cobra.Command {
	var executionID, entryID, reason, requestID string
	var tenantFlag string
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a dead-lettered entry back to the ready set",
		Long: `Replay moves one dead-lettered entry atomically and activation-safely back
to the ready set so the control plane redelivers it. This is a privileged write
operation: --reason is required and length-bounded; the operator identity is
"cli:<user>" (from the authenticated principal), not self-reported.

Pass --request-id to make the replay recoverable: if the response is lost,
retrying with the same --request-id returns already_replayed and the original
audit_id, proving the operation happened exactly once.

The authoritative receipt is written to Redis; the stdout audit line is a
secondary projection only.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if executionID == "" {
				return fmt.Errorf("--execution is required")
			}
			if entryID == "" {
				return fmt.Errorf("--entry is required")
			}
			if reason == "" {
				return fmt.Errorf("--reason is required (record why this entry is being replayed)")
			}
			if err := tenant.Validate(tenant.TenantID(tenantFlag)); err != nil {
				return fmt.Errorf("invalid --tenant: %w", err)
			}
			mgr, closeFn, err := openDeadLetterManager(opts.redisAddr, opts.out)
			if err != nil {
				return err
			}
			defer closeFn()
			// G0 maintenance path: the CLI is a trusted operator tool with the
			// replay scope. G1 replaces this with the B3 authorizer over the
			// HTTP management API; the CLI must then call the API, not Redis.
			principal := control.DeadLetterReplayPrincipal{
				Subject:  "cli:" + envOr("USER", "unknown"),
				TenantID: tenantFlag,
				Scopes:   []string{control.ScopeDeadLetterReplay},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ctx = tenant.WithTenant(ctx, tenant.TenantID(tenantFlag))
			res, err := mgr.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
				ExecutionID: types.ExecutionID(executionID),
				EntryID:     entryID,
				RequestID:   requestID,
				Reason:      reason,
			})
			if err != nil {
				return err
			}
			return writeJSONLines(opts.out, replayResultJSON(res))
		},
	}
	cmd.Flags().StringVar(&executionID, "execution", "", "Execution ID (required)")
	cmd.Flags().StringVar(&entryID, "entry", "", "Dead-letter entry ID (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "Reason for replay (required, length-bounded)")
	cmd.Flags().StringVar(&requestID, "request-id", "", "Idempotency key; retry with the same value to recover a lost response")
	cmd.Flags().StringVar(&tenantFlag, "tenant", envOr("XFLOW_TENANT", string(tenant.DefaultTenant)), "Tenant namespace (env: XFLOW_TENANT)")
	return cmd
}

func replayResultJSON(res engine.ReplayDeadLetterResult) map[string]any {
	return map[string]any{
		"outcome":      string(res.Outcome),
		"audit_id":     res.AuditID,
		"execution":    string(res.ExecutionID),
		"node":         res.NodeID,
		"activation":   res.ActivationID,
		"occurred_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// openDeadLetterManager connects a distributed backend to the given Redis
// address without starting a consumer or dispatcher, then returns the
// DeadLetterManager over the DeadLetterStore capability. The manager owns
// request validation, the metric outlet, and the audit projection; the store
// owns the Redis atomic contract and the authoritative receipt. The returned
// closer releases the Redis client.
func openDeadLetterManager(addr string, out io.Writer) (*control.DeadLetterManager, func(), error) {
	b, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		return nil, nil, fmt.Errorf("connect redis %q: %w", addr, err)
	}
	store, ok := b.State().(engine.DeadLetterStore)
	if !ok {
		closeRedis(b)
		return nil, nil, fmt.Errorf("StateStore %T does not implement DeadLetterStore; the configured backend cannot serve dead-letter operations", b.State())
	}
	audit := control.NewStdoutDeadLetterAuditSink(func(line string) { fmt.Fprintln(os.Stderr, line) })
	mgr := control.NewDeadLetterManager(store, nil, audit)
	closeFn := func() { closeRedis(b) }
	return mgr, closeFn, nil
}

// closeRedis best-effort closes the backend's Redis client. The Cmdable
// interface does not expose Close, so assert to the concrete *redis.Client.
func closeRedis(b *distributed.Backend) {
	if client, ok := b.RedisClient().(*redis.Client); ok {
		_ = client.Close()
	}
}

func writeJSONLines(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}
