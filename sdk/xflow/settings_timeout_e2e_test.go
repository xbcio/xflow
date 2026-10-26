package xflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// This file is the SDK-level end-to-end proof for settings.timeout, the
// execution budget: every case registers a workflow, submits it through
// Engine.Invoke on a NewLocal engine and reads the outcome through Wait and
// Inspect. The workflow builder has no setting for the budget, so a workflow is
// built, given settings.timeout on its definition, compiled and registered
// through the registry the engine wraps (the same back door
// invoke_attribution_test.go uses).
//
// The local backend sweeps for overdue executions every second, so every budget
// here is a few seconds: long enough that a healthy run is nowhere near it,
// short enough that a case stays well under ten seconds of wall clock.

// settingsTimeoutWaitLimit bounds every wait on the engine, so a regression
// that stops an execution from ever concluding fails the case instead of
// hanging the package.
const settingsTimeoutWaitLimit = 20 * time.Second

// newSettingsTimeoutEngine builds a local engine that can run defs. The
// handlers are registered through WithNodes because the back door below skips
// the registration AddWorkflow would do for typed nodes.
func newSettingsTimeoutEngine(t *testing.T, defs ...types.Handler) *Engine {
	t.Helper()
	eng, err := NewLocal(WithNodes(defs...))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	t.Cleanup(eng.Stop)
	return eng
}

// addWorkflowWithTimeout registers wf with settings.timeout = timeout and
// returns the id Invoke addresses it by. A zero timeout is the "no budget"
// spelling, registered the same way so it can be compared like for like.
func addWorkflowWithTimeout(t *testing.T, eng *Engine, wf *WorkflowBuilder, timeout time.Duration) types.WorkflowID {
	t.Helper()
	ctx := context.Background()
	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition() error = %v", err)
	}
	if def.Settings == nil {
		def.Settings = &types.WorkflowSettings{}
	}
	def.Settings.Timezone = "UTC"
	def.Settings.Timeout = timeout
	g, err := graph.CompileWithResolver(ctx, def, eng.compileSubWorkflowResolver(def.Namespace))
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	rec, err := eng.workflowRegistry.AddWorkflow(ctx, backend.WorkflowRecord{
		Key:            workflowKey(def),
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: g.Hash(),
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	return rec.ID
}

// settingsTimeoutHold is a node that does nothing but occupy its lease until
// its context ends: the "still running when the budget runs out" case. It is
// not a suspending node, so the execution deadline sweep cannot find it parked;
// only the node's own lease (clamped to the execution deadline) can.
var settingsTimeoutHold = node.Define("test.settings_timeout.hold",
	func(ctx context.Context, _ *types.Input) (*types.Output, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

// TestSettingsTimeoutRunningNodeEndsTheExecutionAsTimeout is scenario 1: a node
// that is executing (not suspended) when settings.timeout elapses ends the
// whole execution as timeout, whatever the node's own on_error says. The node's
// lease is clamped to the execution deadline, so the node "times out" at the
// same instant, and that timeout must not be routed around: continue would run
// the next node and error_output would take the error port, both leaving an
// execution running past its budget.
func TestSettingsTimeoutRunningNodeEndsTheExecutionAsTimeout(t *testing.T) {
	// Not a whole number of seconds: the local backend sweeps for overdue
	// executions every second from the moment it starts, so a whole-second
	// budget lands on a sweep tick and lets the sweeper, not the node's own
	// commit, decide the outcome. Half a second off the tick keeps this case
	// about the running node.
	const budget = 2500 * time.Millisecond
	cases := []struct {
		name    string
		onError types.OnError
		// follower is the node on the route on_error would take; it must not
		// run, and is reached through the port the strategy uses.
		follower string
		port     string
	}{
		{"stop", types.OnErrorStop, "", ""},
		{"continue", types.OnErrorContinue, "next", "main"},
		{"error_output", types.OnErrorOutput, "onerr", "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := newSettingsTimeoutEngine(t, settingsTimeoutHold)
			ctx, cancel := context.WithTimeout(context.Background(), settingsTimeoutWaitLimit)
			defer cancel()

			wf := Workflow("settings-timeout-running-" + tc.name)
			start := wf.Node("start", node.Start())
			hold := wf.Node("hold", settingsTimeoutHold.New(nil)).OnError(tc.onError)
			wf.Connect(start, hold)
			if tc.follower != "" {
				follower := wf.Node(tc.follower, node.Start())
				wf.Connect(hold.Output(tc.port), follower)
			}
			workflowID := addWorkflowWithTimeout(t, eng, wf, budget)

			id, err := eng.Invoke(ctx, workflowID, Start(), nil)
			if err != nil {
				t.Fatalf("Invoke() error = %v", err)
			}
			result, err := eng.Wait(ctx, id)
			if err != nil {
				t.Fatalf("Wait() error = %v", err)
			}
			if result.Status != types.ExecutionStatusTimeout {
				t.Fatalf("Wait() status = %q error %q, want %q: on_error=%s must not absorb the execution budget\n%s",
					result.Status, result.Error, types.ExecutionStatusTimeout, tc.onError, describeExecution(ctx, eng, id))
			}
			if result.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("Wait() error = %q, want %q", result.Error, engine.ExecutionDeadlineExceededError)
			}

			detail, err := eng.Inspect(ctx, id)
			if err != nil {
				t.Fatalf("Inspect() error = %v", err)
			}
			if detail.Status != result.Status || detail.Error != result.Error {
				t.Fatalf("Inspect() = %q %q, want what Wait() reported (%q %q)",
					detail.Status, detail.Error, result.Status, result.Error)
			}
			// The budget is spent in full before it is enforced: an execution
			// that concludes early was cut short by something other than it.
			if detail.CreatedAt == nil || detail.UpdatedAt == nil {
				t.Fatalf("Inspect() CreatedAt/UpdatedAt = %v/%v, want both stamped", detail.CreatedAt, detail.UpdatedAt)
			}
			if lived := detail.UpdatedAt.Sub(*detail.CreatedAt); lived < budget-100*time.Millisecond {
				t.Fatalf("execution concluded after %v, want at least the %v budget", lived, budget)
			}
			if tc.follower == "" {
				return
			}
			followerDetail, err := eng.Inspect(ctx, id, tc.follower)
			if err != nil {
				t.Fatalf("Inspect(%s) error = %v", tc.follower, err)
			}
			for _, n := range followerDetail.Nodes {
				if n.Status == types.NodeStatusSuccess || n.Status == types.NodeStatusRunning {
					t.Fatalf("on_error=%s route node %q is %q after the budget ended the execution, want it never run",
						tc.onError, n.Name, n.Status)
				}
			}
		})
	}
}

// describeExecution renders an execution's status and every node's status and
// error on one line, so a failed assertion carries the state it saw instead of
// forcing a rerun to find out.
func describeExecution(ctx context.Context, eng *Engine, id types.ExecutionID) string {
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		return "inspect failed: " + err.Error()
	}
	out := fmt.Sprintf("execution %s status=%q error=%q", id, detail.Status, detail.Error)
	if detail.CreatedAt != nil && detail.UpdatedAt != nil {
		out += fmt.Sprintf(" lived=%v", detail.UpdatedAt.Sub(*detail.CreatedAt))
	}
	for _, n := range detail.Nodes {
		out += fmt.Sprintf("; node %s=%s %q", n.Name, n.Status, n.Error)
	}
	return out
}
