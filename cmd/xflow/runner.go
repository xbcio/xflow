package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

// runnerOptions carries the shared management connection for the runner
// command tree.
type runnerOptions struct {
	managementOptions
}

func newRunnerCommand(out io.Writer) *cobra.Command {
	opts := &runnerOptions{managementOptions: managementOptions{out: out}}
	cmd := &cobra.Command{
		Use:   "runner",
		Short: "Inspect and control the runner fleet through the management API",
		Long: `Inspect and control the runner fleet through the protected management API.

The commands read and mutate server-authoritative runner state; they never
connect to a runner process directly. Configure --server and --token (or
XFLOW_API_ADDR / XFLOW_API_TOKEN). The token is sent only in the Authorization
header and is never logged.

Drain and resume require platform-level management scope: the server checks
management.runner.control_global in addition to the per-action scope. A 200
from either confirms only that the server-side new-work admission gate changed;
it does not mean the runner process stopped, a handler was interrupted, or
every handoff settled.`,
	}
	cmd.PersistentFlags().StringVar(&opts.server, "server", envOr("XFLOW_API_ADDR", ""),
		"Management API base URL, e.g. http://127.0.0.1:8080 (env: XFLOW_API_ADDR)")
	cmd.PersistentFlags().StringVar(&opts.token, "token", envOr("XFLOW_API_TOKEN", ""),
		"Bearer token for the management API (env: XFLOW_API_TOKEN). Sent only in the Authorization header; never logged")
	cmd.AddCommand(newRunnerListCommand(opts))
	cmd.AddCommand(newRunnerGetCommand(opts))
	cmd.AddCommand(newRunnerDrainCommand(opts))
	cmd.AddCommand(newRunnerResumeCommand(opts))
	cmd.AddCommand(newRunnerRevokeIdentityCommand(opts))
	return cmd
}

// runnerListItem mirrors the roster entry the server returns. Optional
// timestamps are pointers so an omitted field stays distinguishable from a
// zero time; -o json prints the server's raw data instead of this projection.
type runnerListItem struct {
	RunnerID      string     `json:"runner_id"`
	Enrolled      bool       `json:"enrolled"`
	State         string     `json:"state"`
	LastHeartbeat *time.Time `json:"last_heartbeat,omitempty"`
	IssuedAt      *time.Time `json:"issued_at,omitempty"`
	RevokedAt     *time.Time `json:"revoked_at,omitempty"`
	DesiredState  string     `json:"desired_state,omitempty"`
}

// runnerSnapshotView is the table projection of one runner snapshot. -o json
// prints the server's raw data so the full field set stays available.
type runnerSnapshotView struct {
	RunnerID      string                 `json:"runner_id"`
	Capacity      int                    `json:"capacity"`
	InFlight      int                    `json:"in_flight"`
	LastHeartbeat *time.Time             `json:"last_heartbeat"`
	Control       *runnerControlSnapshot `json:"control,omitempty"`
}

// runnerControlSnapshot mirrors the durable desired state and its current
// drain projection.
type runnerControlSnapshot struct {
	DesiredState string               `json:"desired_state"`
	Generation   uint64               `json:"generation"`
	RequestedAt  *time.Time           `json:"requested_at,omitempty"`
	Reason       string               `json:"reason,omitempty"`
	Drain        *runnerDrainSnapshot `json:"drain,omitempty"`
}

// runnerDrainSnapshot is the operator-facing drain progress projection.
type runnerDrainSnapshot struct {
	Phase           string     `json:"phase"`
	DeadlineAt      *time.Time `json:"deadline_at,omitempty"`
	ServerQuiescent bool       `json:"server_quiescent"`
	RunnerQuiescent bool       `json:"runner_quiescent"`
}

// runnerControlOutput is the drain/resume result: the server snapshot plus the
// idempotency key that produced it. Echoing the key is what makes a lost
// response recoverable — re-run with --request-id set to this value.
type runnerControlOutput struct {
	RunnerID     string               `json:"runner_id"`
	RequestID    string               `json:"request_id"`
	DesiredState string               `json:"desired_state"`
	Generation   uint64               `json:"generation"`
	RequestedAt  *time.Time           `json:"requested_at,omitempty"`
	Reason       string               `json:"reason,omitempty"`
	Drain        *runnerDrainSnapshot `json:"drain,omitempty"`
}

// runnerRevokeOutput is the revoke-identity result.
type runnerRevokeOutput struct {
	RunnerID string `json:"runner_id"`
	Status   string `json:"status"`
}

func newRunnerListCommand(opts *runnerOptions) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the runner fleet roster",
		Long: `Lists every runner known to the directory plus every enrollment-issued
identity absent from it. state is server-computed liveness, not task-placement
readiness: a draining runner can remain online while it accepts no new work.

A 501 runner_listing_unsupported means the configured backend cannot enumerate
runners at all, not that the fleet is empty. Use 'runner get <id>' for a known
runner on such a deployment.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateOutputFormat(output); err != nil {
				return err
			}
			client, err := newManagementClient(&opts.managementOptions)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			var raw json.RawMessage
			if err := client.do(ctx, http.MethodGet, "/v1/management/runners", nil, nil, &raw); err != nil {
				return err
			}
			if output == "json" {
				return writeRawJSON(opts.out, raw)
			}
			var items []runnerListItem
			if err := json.Unmarshal(raw, &items); err != nil {
				return fmt.Errorf("decode runner roster: %w", err)
			}
			return writeRunnerListTable(opts.out, items)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table or json")
	return cmd
}

func writeRunnerListTable(w io.Writer, items []runnerListItem) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RUNNER_ID\tSTATE\tDESIRED_STATE\tENROLLED\tLAST_HEARTBEAT"); err != nil {
		return err
	}
	for _, item := range items {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%t\t%s\n",
			item.RunnerID, item.State, orDash(item.DesiredState), item.Enrolled,
			formatOptionalTime(item.LastHeartbeat)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func newRunnerGetCommand(opts *runnerOptions) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "get <runner-id>",
		Short: "Inspect one runner's registration and control snapshot",
		Long: `Shows the registration snapshot plus, when the directory exposes it, the
server-authoritative control projection: desired_state, generation, and the
drain phase with its quiescence flags and deadline.

desired_state is scheduling intent, not liveness. drain.server_quiescent means
the server side holds no debt for this runner; drain.runner_quiescent requires a
fresh fenced observation from the runner's current session. Neither proves the
process exited.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateOutputFormat(output); err != nil {
				return err
			}
			client, err := newManagementClient(&opts.managementOptions)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			var raw json.RawMessage
			if err := client.do(ctx, http.MethodGet, "/v1/management/runners/"+url.PathEscape(args[0]), nil, nil, &raw); err != nil {
				return err
			}
			if output == "json" {
				return writeRawJSON(opts.out, raw)
			}
			var snap runnerSnapshotView
			if err := json.Unmarshal(raw, &snap); err != nil {
				return fmt.Errorf("decode runner snapshot: %w", err)
			}
			return writeRunnerGetTable(opts.out, snap)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table or json")
	return cmd
}

func writeRunnerGetTable(w io.Writer, snap runnerSnapshotView) error {
	desired, generation, phase := "-", "-", "-"
	serverQuiescent, runnerQuiescent, deadline := "-", "-", "-"
	if snap.Control != nil {
		desired = orDash(snap.Control.DesiredState)
		generation = strconv.FormatUint(snap.Control.Generation, 10)
		if snap.Control.Drain != nil {
			phase = orDash(snap.Control.Drain.Phase)
			serverQuiescent = strconv.FormatBool(snap.Control.Drain.ServerQuiescent)
			runnerQuiescent = strconv.FormatBool(snap.Control.Drain.RunnerQuiescent)
			deadline = formatOptionalTime(snap.Control.Drain.DeadlineAt)
		}
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	rows := [][2]string{
		{"runner_id", snap.RunnerID},
		{"capacity", strconv.Itoa(snap.Capacity)},
		{"in_flight", strconv.Itoa(snap.InFlight)},
		{"last_heartbeat", formatOptionalTime(snap.LastHeartbeat)},
		{"desired_state", desired},
		{"generation", generation},
		{"drain.phase", phase},
		{"drain.server_quiescent", serverQuiescent},
		{"drain.runner_quiescent", runnerQuiescent},
		{"drain.deadline_at", deadline},
	}
	if _, err := fmt.Fprintln(tw, "FIELD\tVALUE"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", row[0], row[1]); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func newRunnerDrainCommand(opts *runnerOptions) *cobra.Command {
	return newRunnerControlCommand(opts, "drain", "draining", `drain changes the runner's desired scheduling state to draining, closing the
server-side gate for newly queued task claims. A finalized pre-drain lease may
still be replayed to settle existing work.

Requires management.runner.drain plus the platform-only
management.runner.control_global scope. A 200 confirms only that the admission
gate is closed — it does not stop the process, cancel a handler, or force-revoke
a lease.

The server requires X-Request-Id for this mutation. When --request-id is
omitted a random key is generated and echoed in the output; pass that value back
on a retry to recover a lost response instead of applying a second transition.`)
}

func newRunnerResumeCommand(opts *runnerOptions) *cobra.Command {
	return newRunnerControlCommand(opts, "resume", "active", `resume changes the runner's desired scheduling state back to active, allowing
future compatible queue claims again. Existing drain receipts are immutable: a
retried drain with an older --request-id cannot reapply after a newer resume.

Requires management.runner.resume plus the platform-only
management.runner.control_global scope. A 200 changes intent only; it does not
start a process or assign trigger activations synchronously.

The server requires X-Request-Id for this mutation. When --request-id is
omitted a random key is generated and echoed in the output; pass that value back
on a retry to recover a lost response.`)
}

func newRunnerControlCommand(opts *runnerOptions, action, desiredState, long string) *cobra.Command {
	var reason, requestID string
	cmd := &cobra.Command{
		Use:   action + " <runner-id>",
		Short: "Set the runner's desired scheduling state to " + desiredState,
		Long:  long,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if reason == "" {
				return errors.New("--reason is required (record why the runner's scheduling state is changing)")
			}
			if requestID == "" {
				generated, err := newManagementRequestID()
				if err != nil {
					return err
				}
				requestID = generated
			}
			client, err := newManagementClient(&opts.managementOptions)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			var snap runnerControlSnapshot
			err = client.do(ctx, http.MethodPost,
				"/v1/management/runners/"+url.PathEscape(args[0])+"/"+action,
				map[string]string{"reason": reason},
				map[string]string{"X-Request-Id": requestID},
				&snap)
			if err != nil {
				return err
			}
			return writeJSONLines(opts.out, runnerControlOutput{
				RunnerID:     args[0],
				RequestID:    requestID,
				DesiredState: snap.DesiredState,
				Generation:   snap.Generation,
				RequestedAt:  snap.RequestedAt,
				Reason:       snap.Reason,
				Drain:        snap.Drain,
			})
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "Reason for the transition (required, at most 512 bytes)")
	cmd.Flags().StringVar(&requestID, "request-id", "", "Idempotency key; generated when omitted and echoed in the output for replay")
	return cmd
}

func newRunnerRevokeIdentityCommand(opts *runnerOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke-identity <runner-id>",
		Short: "Revoke an enrollment-issued runner identity",
		Long: `Revokes one runner's issued identity. The revocation takes effect on the
runner's next authenticated call.

Only enrollment-issued identities are revocable: a runner authenticated by a
static policy entry has no issued identity to kill. This is a platform-level
operation requiring management.runner.revoke_identity; it is not confined to a
namespace.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newManagementClient(&opts.managementOptions)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			var res runnerRevokeOutput
			if err := client.do(ctx, http.MethodPost,
				"/v1/management/runners/"+url.PathEscape(args[0])+"/revoke-identity",
				nil, nil, &res); err != nil {
				return err
			}
			return writeJSONLines(opts.out, res)
		},
	}
	return cmd
}

// orDash renders an empty server field as "-" so table columns stay aligned.
func orDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

// formatOptionalTime renders an optional timestamp in UTC, or "-" when absent.
func formatOptionalTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339Nano)
}
