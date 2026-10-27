package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/cobra"
)

// executionCancelOutput is the cancel result. status is the terminal status the
// server reaches before answering 200; accepted mirrors the API's own field.
type executionCancelOutput struct {
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
	Accepted    bool   `json:"accepted"`
}

// executionRerunOutput is the rerun result: the NEW execution, never the one
// addressed by the command.
type executionRerunOutput struct {
	ExecutionID   string `json:"execution_id"`
	Status        string `json:"status"`
	QueuePosition int    `json:"queue_position,omitempty"`
	Duplicate     bool   `json:"duplicate"`
}

func newExecutionCancelCommand(out io.Writer) *cobra.Command {
	opts := &managementOptions{out: out}
	cmd := &cobra.Command{
		Use:   "cancel <execution-id>",
		Short: "Cancel an execution through the management API",
		Long: `Requests cancellation of the named execution. The server marks it canceling,
cancels its suspended nodes and child executions, and answers 200 only after the
execution reaches the canceled status; the output reports that status.

Cancel is idempotent and resumable server-side, but an execution that is already
terminal is reported as not found (404) rather than silently accepted.

Requires the execution.cancel scope. The token's namespace is authoritative:
an execution in another namespace is indistinguishable from a missing one.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newManagementClient(cmd, opts)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			var res struct {
				Accepted bool `json:"accepted"`
			}
			if err := client.do(ctx, http.MethodPost,
				"/v1/executions/"+url.PathEscape(args[0])+"/cancel",
				nil, nil, &res); err != nil {
				return err
			}
			if !res.Accepted {
				return errors.New("cancel was not accepted by the server")
			}
			return writeJSONLines(out, executionCancelOutput{
				ExecutionID: args[0],
				Status:      "canceled",
				Accepted:    true,
			})
		},
	}
	addManagementFlags(cmd, opts)
	return cmd
}

func newExecutionRerunCommand(out io.Writer) *cobra.Command {
	opts := &managementOptions{out: out}
	var fromNode string
	var includeSuccess bool
	var idempotencyKey string
	cmd := &cobra.Command{
		Use:   "rerun <execution-id> --from-node <node>",
		Short: "Derive a new execution from a terminal one, starting at a node",
		Long: `Creates a NEW execution derived from a terminal one, starting at --from-node.
Everything upstream of that node is copied from the execution being rerun; the
node and everything downstream run again. The original is never modified.

failed, canceled and timeout sources are always accepted; a success source
requires --include-success. Every other status, including paused, is 409
execution_not_rerunnable.

Requires the execution.rerun scope. Pass --idempotency-key to make the rerun
safe to retry: the key is scoped to the source execution and a retry is answered
with the rerun the first attempt created (duplicate: true) instead of creating a
second execution.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromNode == "" {
				return errors.New("--from-node is required (the node the new execution starts at)")
			}
			client, err := newManagementClient(cmd, opts)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			var headers map[string]string
			if idempotencyKey != "" {
				headers = map[string]string{"Idempotency-Key": idempotencyKey}
			}
			var res executionRerunOutput
			if err := client.do(ctx, http.MethodPost,
				"/v1/executions/"+url.PathEscape(args[0])+"/rerun",
				struct {
					FromNode       string `json:"from_node"`
					IncludeSuccess bool   `json:"include_success"`
				}{FromNode: fromNode, IncludeSuccess: includeSuccess},
				headers, &res); err != nil {
				return err
			}
			return writeJSONLines(out, res)
		},
	}
	cmd.Flags().StringVar(&fromNode, "from-node", "", "Node the new execution starts at (required)")
	cmd.Flags().BoolVar(&includeSuccess, "include-success", false,
		"Accept a successful execution as the rerun source (failed, canceled and timeout are always accepted)")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "",
		"Idempotency key; retry with the same key to recover a lost response")
	addManagementFlags(cmd, opts)
	return cmd
}
