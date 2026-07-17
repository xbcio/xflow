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
	"github.com/xbcio/xflow/engine"
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
running OutboxDispatcher redelivers it. Concurrent replays of the same entry
are idempotent. Replays of terminal or expired executions are rejected.

All commands connect directly to the Redis-backed StateStore through the
DeadLetterStore capability — they never construct Redis keys directly, so the
atomicity and state-guard contract stays inside the backend.`,
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
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List dead-lettered outbox entries for an execution (read-only)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if executionID == "" {
				return fmt.Errorf("--execution is required")
			}
			store, closeFn, err := openDeadLetterStore(opts.redisAddr)
			if err != nil {
				return err
			}
			defer closeFn()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			entries, err := store.ListDeadLetters(ctx, types.ExecutionID(executionID), limit)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err := writeJSONLines(opts.out, entry); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&executionID, "execution", "", "Execution ID (required)")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum entries to return")
	return cmd
}

func newDeadLetterReplayCommand(opts *deadLetterOptions) *cobra.Command {
	var executionID, entryID, reason, operator string
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a dead-lettered entry back to the ready set",
		Long: `Replay moves one dead-lettered entry atomically back to the ready set so the
control plane redelivers it. This is a privileged write operation: record an
operator and reason for every invocation — the audit line is emitted to stdout
as JSON and should be captured by the deployment's log pipeline.`,
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
			if operator == "" {
				operator = os.Getenv("USER")
			}
			store, closeFn, err := openDeadLetterStore(opts.redisAddr)
			if err != nil {
				return err
			}
			defer closeFn()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			outcome, err := store.ReplayDeadLetter(ctx, types.ExecutionID(executionID), entryID)
			audit := replayAudit{
				Operator:   operator,
				Reason:     reason,
				Execution:  executionID,
				Entry:      entryID,
				Outcome:    string(outcome),
				OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
			}
			if err != nil {
				audit.Error = err.Error()
			}
			if err := writeJSONLines(opts.out, audit); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&executionID, "execution", "", "Execution ID (required)")
	cmd.Flags().StringVar(&entryID, "entry", "", "Dead-letter entry ID (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "Reason for replay (required)")
	cmd.Flags().StringVar(&operator, "operator", "", "Operator identity (default: $USER)")
	return cmd
}

// openDeadLetterStore connects a distributed backend to the given Redis
// address without starting a consumer or dispatcher, then returns the
// DeadLetterStore capability. The returned closer releases the Redis client.
func openDeadLetterStore(addr string) (engine.DeadLetterStore, func(), error) {
	b, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		return nil, nil, fmt.Errorf("connect redis %q: %w", addr, err)
	}
	store, ok := b.State().(engine.DeadLetterStore)
	if !ok {
		closeRedis(b)
		return nil, nil, fmt.Errorf("StateStore %T does not implement DeadLetterStore; the configured backend cannot serve dead-letter operations", b.State())
	}
	closeFn := func() { closeRedis(b) }
	return store, closeFn, nil
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

type replayAudit struct {
	Operator   string `json:"operator"`
	Reason     string `json:"reason"`
	Execution  string `json:"execution"`
	Entry      string `json:"entry"`
	Outcome    string `json:"outcome"`
	Error      string `json:"error,omitempty"`
	OccurredAt string `json:"occurred_at"`
}
