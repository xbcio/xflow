package xflow

import (
	"context"
	"fmt"
	"sync/atomic"
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

// settingsTimeoutClockSlackAllowance is how far ahead of the wall clock a timed
// out execution's stored deadline may still be when Wait returns. The engine
// tolerates up to 50ms between a node's monotonic lease timer and the stored
// wall-clock deadline before it calls the node's end an execution timeout; this
// is twice that, so the check still catches a deadline that is genuinely in the
// future (a budget that was never spent) without flaking on the tolerance.
const settingsTimeoutClockSlackAllowance = 100 * time.Millisecond

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
			// Inspect is asked about this one name, so it always returns exactly
			// one entry that echoes the name (a node with no record reads as
			// Pending); the length and name checks below are sanity guards that
			// cannot fail on a healthy engine. The check that discriminates is
			// the Pending status: a route node that ran, or was routed to,
			// would carry a different status.
			if len(followerDetail.Nodes) != 1 {
				t.Fatalf("Inspect(%s) nodes = %+v, want exactly the route node reported\n%s",
					tc.follower, followerDetail.Nodes, describeExecution(ctx, eng, id))
			}
			if n := followerDetail.Nodes[0]; n.Name != tc.follower || n.Status != types.NodeStatusPending {
				t.Fatalf("on_error=%s route node %q is %q after the budget ended the execution, want %q (never run)",
					tc.onError, n.Name, n.Status, types.NodeStatusPending)
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

// settingsTimeoutNapFor is how long settingsTimeoutNap occupies its lease. It is
// long enough that an execution budget cut in half (or applied as zero) ends the
// node mid-flight, and short enough that a case stays a few seconds.
const settingsTimeoutNapFor = 3 * time.Second

// settingsTimeoutNap is a node that works for settingsTimeoutNapFor and then
// succeeds, giving up early only if its context ends first.
var settingsTimeoutNap = node.Define("test.settings_timeout.nap",
	func(ctx context.Context, _ *types.Input) (*types.Output, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(settingsTimeoutNapFor):
			return &types.Output{Data: map[string]any{"napped": true}}, nil
		}
	})

// runNapWorkflow registers start -> nap with settings.timeout = timeout, runs
// it and returns the engine, the execution id and the result Wait reported.
func runNapWorkflow(t *testing.T, name string, timeout time.Duration) (*Engine, types.ExecutionID, types.Result) {
	t.Helper()
	eng := newSettingsTimeoutEngine(t, settingsTimeoutNap)
	ctx, cancel := context.WithTimeout(context.Background(), settingsTimeoutWaitLimit)
	defer cancel()

	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	nap := wf.Node("nap", settingsTimeoutNap.New(nil))
	wf.Connect(start, nap)
	workflowID := addWorkflowWithTimeout(t, eng, wf, timeout)

	id, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return eng, id, result
}

// TestSettingsTimeoutWithinBudgetSucceeds is scenario 2: an execution that
// finishes inside settings.timeout is a success, not a timeout, and it carries
// no error. The budget is also read back from the stored execution, so a
// success cannot be mistaken for an execution that never had one.
func TestSettingsTimeoutWithinBudgetSucceeds(t *testing.T) {
	// Longer than the nap by a wide margin, and shorter than twice the nap:
	// a budget applied at half its value ends the nap mid-flight.
	const budget = 5 * time.Second
	eng, id, result := runNapWorkflow(t, "settings-timeout-within-budget", budget)
	ctx := context.Background()

	if result.Status != types.ExecutionStatusSuccess || result.Error != "" {
		t.Fatalf("Wait() = %q error %q, want success with no error\n%s",
			result.Status, result.Error, describeExecution(ctx, eng, id))
	}
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.Status != result.Status || detail.Error != result.Error {
		t.Fatalf("Inspect() = %q %q, want what Wait() reported (%q %q)",
			detail.Status, detail.Error, result.Status, result.Error)
	}
	if detail.CreatedAt == nil || detail.UpdatedAt == nil {
		t.Fatalf("Inspect() CreatedAt/UpdatedAt = %v/%v, want both stamped", detail.CreatedAt, detail.UpdatedAt)
	}
	// CreatedAt and UpdatedAt are wall-clock stamps while the nap is timed on the
	// monotonic clock, and the OS slews the wall clock (up to 500ppm) when it
	// corrects it: measured here, a 3s nap spans 2.998s of wall clock. The lower
	// bound therefore allows 100ms, the same slack the other cases give their
	// lifetime checks; success above already proves the nap ran to completion.
	const wallSlack = 100 * time.Millisecond
	if lived := detail.UpdatedAt.Sub(*detail.CreatedAt); lived < settingsTimeoutNapFor-wallSlack || lived >= budget {
		t.Fatalf("execution lived %v, want the nap (%v) completed inside the %v budget", lived, settingsTimeoutNapFor, budget)
	}

	snap, err := eng.eng.State().GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v, %v", snap, err)
	}
	if want := snap.CreatedAt.Add(budget); !snap.Deadline.Equal(want) {
		t.Fatalf("stored deadline = %v, want CreatedAt + settings.timeout = %v", snap.Deadline, want)
	}
}

// TestSettingsTimeoutZeroMeansNoLimit is scenario 4, the control for scenario 2:
// the same workflow with settings.timeout = 0 has no budget at all, so the nap
// is not cut short and the stored execution carries no deadline.
func TestSettingsTimeoutZeroMeansNoLimit(t *testing.T) {
	eng, id, result := runNapWorkflow(t, "settings-timeout-zero", 0)
	ctx := context.Background()

	if result.Status != types.ExecutionStatusSuccess || result.Error != "" {
		t.Fatalf("Wait() = %q error %q, want success with no error\n%s",
			result.Status, result.Error, describeExecution(ctx, eng, id))
	}
	snap, err := eng.eng.State().GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v, %v", snap, err)
	}
	if !snap.Deadline.IsZero() {
		t.Fatalf("stored deadline = %v, want none for settings.timeout = 0", snap.Deadline)
	}
	detail, err := eng.Inspect(ctx, id, "nap")
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(detail.Nodes) != 1 || detail.Nodes[0].Status != types.NodeStatusSuccess {
		t.Fatalf("nap node = %+v, want it run to success", detail.Nodes)
	}
}

// TestSettingsTimeoutRerunStartsANewClock is scenario 3: a rerun of an
// execution that timed out is a new execution with a new budget. The parent's
// deadline is already in the past when the rerun is submitted, so a rerun that
// kept it would be expired on arrival; one that gets its own clock runs to
// success inside its own settings.timeout, well past the parent's deadline.
func TestSettingsTimeoutRerunStartsANewClock(t *testing.T) {
	const budget = 2500 * time.Millisecond
	const rerunWork = time.Second

	// The node hangs until its context ends while the first run is on, which is
	// what makes the parent time out, and does a second of work once the test has
	// flipped rerunPhase, which is what lets the rerun finish.
	var rerunPhase atomic.Bool
	work := node.Define("test.settings_timeout.rerun_work",
		func(ctx context.Context, _ *types.Input) (*types.Output, error) {
			wait := time.Duration(-1)
			if rerunPhase.Load() {
				wait = rerunWork
			}
			if wait < 0 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
				return &types.Output{Data: map[string]any{"reran": true}}, nil
			}
		})

	eng := newSettingsTimeoutEngine(t, work)
	ctx, cancel := context.WithTimeout(context.Background(), settingsTimeoutWaitLimit)
	defer cancel()

	wf := Workflow("settings-timeout-rerun")
	start := wf.Node("start", node.Start())
	step := wf.Node("work", work.New(nil))
	wf.Connect(start, step)
	workflowID := addWorkflowWithTimeout(t, eng, wf, budget)

	parentID, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	parent, err := eng.Wait(ctx, parentID)
	if err != nil {
		t.Fatalf("Wait(parent) error = %v", err)
	}
	if parent.Status != types.ExecutionStatusTimeout || parent.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent = %q error %q, want timeout %q\n%s",
			parent.Status, parent.Error, engine.ExecutionDeadlineExceededError, describeExecution(ctx, eng, parentID))
	}
	parentSnap, err := eng.eng.State().GetExecution(ctx, parentID)
	if err != nil || parentSnap == nil {
		t.Fatalf("GetExecution(parent) = %+v, %v", parentSnap, err)
	}
	// The deadline must not be far in the future. It need not be strictly past:
	// the node's lease timer runs on the monotonic clock while the stored deadline
	// is wall clock, so the timer can fire a few milliseconds before the wall
	// clock reaches the deadline, and the engine then treats it as the execution
	// timeout (it allows a small clock slack for exactly that), letting Wait
	// return a moment early.
	if ahead := time.Until(parentSnap.Deadline); ahead > settingsTimeoutClockSlackAllowance {
		t.Fatalf("parent deadline %v is %v in the future once the parent timed out, want at most %v",
			parentSnap.Deadline, ahead, settingsTimeoutClockSlackAllowance)
	}

	rerunPhase.Store(true)
	rerun, err := eng.Rerun(ctx, parentID, "work")
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	if rerun.ExecutionID == "" || rerun.ExecutionID == parentID || rerun.Duplicate {
		t.Fatalf("Rerun() = %+v, want a new execution", rerun)
	}
	result, err := eng.Wait(ctx, rerun.ExecutionID)
	if err != nil {
		t.Fatalf("Wait(rerun) error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess || result.Error != "" {
		t.Fatalf("rerun = %q error %q, want success inside its own budget\n%s",
			result.Status, result.Error, describeExecution(ctx, eng, rerun.ExecutionID))
	}

	detail, err := eng.Inspect(ctx, rerun.ExecutionID)
	if err != nil {
		t.Fatalf("Inspect(rerun) error = %v", err)
	}
	if detail.ParentID != parentID || detail.RerunFrom != "work" {
		t.Fatalf("rerun link = (%q, %q), want (%q, %q)", detail.ParentID, detail.RerunFrom, parentID, "work")
	}
	snap, err := eng.eng.State().GetExecution(ctx, rerun.ExecutionID)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(rerun) = %+v, %v", snap, err)
	}
	if want := snap.CreatedAt.Add(budget); !snap.Deadline.Equal(want) {
		t.Fatalf("rerun deadline = %v, want its own CreatedAt + settings.timeout = %v", snap.Deadline, want)
	}
	if !snap.Deadline.After(parentSnap.Deadline) {
		t.Fatalf("rerun deadline %v is not after the parent's %v: the clock was not restarted", snap.Deadline, parentSnap.Deadline)
	}
	if detail.UpdatedAt == nil || !detail.UpdatedAt.After(parentSnap.Deadline) {
		t.Fatalf("rerun concluded at %v, want it to have run past the parent's deadline %v", detail.UpdatedAt, parentSnap.Deadline)
	}

	// The execution it came from is left exactly as it was.
	again, err := eng.Inspect(ctx, parentID)
	if err != nil {
		t.Fatalf("Inspect(parent) error = %v", err)
	}
	if again.Status != types.ExecutionStatusTimeout || again.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent after the rerun = %q %q, want it still timeout", again.Status, again.Error)
	}
}

// TestSettingsTimeoutCancelsTheSubWorkflowChild is scenario 5: a parent whose
// settings.timeout runs out while its sub-workflow node waits on a child that
// never finishes ends as timeout, and the child it started is canceled by the
// parent's expiry. The child has no budget of its own and parks on a signal
// nobody sends, so nothing but the parent's cascade can end it. The engine-level
// cases (execution_deadline_subworkflow_test.go and the engine's own) pin the
// cascade's details; this one proves it holds through Invoke on the SDK engine.
func TestSettingsTimeoutCancelsTheSubWorkflowChild(t *testing.T) {
	const budget = 2500 * time.Millisecond
	eng := newSettingsTimeoutEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), settingsTimeoutWaitLimit)
	defer cancel()

	child := Workflow("settings-timeout-sw-child")
	childStart := child.Node("start", node.Start())
	childPark := child.Node("park", node.Wait("never-signalled"))
	child.Connect(childStart, childPark)
	if _, err := eng.AddWorkflow(ctx, child); err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}

	parent := Workflow("settings-timeout-sw-parent")
	parentStart := parent.Node("start", node.Start())
	sub := parent.Node("sub", node.SubWorkflow("settings-timeout-sw-child"))
	parent.Connect(parentStart, sub)
	parentWorkflowID := addWorkflowWithTimeout(t, eng, parent, budget)

	parentID, err := eng.Invoke(ctx, parentWorkflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	// The cascade is only worth testing against a child that is really running:
	// wait for the parent's node to name its child and for the child to park.
	var childID types.ExecutionID
	waitForTransientCondition(t, 10*time.Second, func() bool {
		detail, err := eng.Inspect(ctx, parentID, "sub")
		if err != nil || len(detail.Nodes) != 1 {
			return false
		}
		childID = detail.Nodes[0].ChildExecutionID
		return childID != ""
	})
	waitForNodeStatus(t, ctx, eng, childID, "park", types.NodeStatusSuspended)

	result, err := eng.Wait(ctx, parentID)
	if err != nil {
		t.Fatalf("Wait(parent) error = %v", err)
	}
	if result.Status != types.ExecutionStatusTimeout || result.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent = %q error %q, want timeout %q\n%s",
			result.Status, result.Error, engine.ExecutionDeadlineExceededError, describeExecution(ctx, eng, parentID))
	}

	// Wait returns at the parent's status write; the cascade runs right after
	// it, so the child is polled for, with a bound.
	cascadeCtx, cancelCascade := context.WithTimeout(ctx, 8*time.Second)
	defer cancelCascade()
	waitForExecStatus(t, cascadeCtx, eng, childID, types.ExecutionStatusCanceled)

	parentDetail, err := eng.Inspect(ctx, parentID)
	if err != nil {
		t.Fatalf("Inspect(parent) error = %v", err)
	}
	if parentDetail.CreatedAt == nil {
		t.Fatal("Inspect(parent) CreatedAt = nil, want stamped")
	}
	childDetail, err := eng.Inspect(ctx, childID)
	if err != nil {
		t.Fatalf("Inspect(child) error = %v", err)
	}
	if childDetail.ParentID != parentID || childDetail.ParentNode != "sub" {
		t.Fatalf("child linkage = (%q, %q), want (%q, %q)", childDetail.ParentID, childDetail.ParentNode, parentID, "sub")
	}
	// The child was stopped by the parent's expiry, not by anything earlier: it
	// was still parked when the parent's budget began to run out.
	if childDetail.UpdatedAt == nil || childDetail.UpdatedAt.Before(parentDetail.CreatedAt.Add(budget-50*time.Millisecond)) {
		t.Fatalf("child was canceled at %v, want it to outlive the parent's %v budget from %v",
			childDetail.UpdatedAt, budget, parentDetail.CreatedAt)
	}

	// The parent's node keeps the child it waited on and is closed as canceled;
	// the child's cancellation must not turn the parent into anything but its
	// timeout.
	parentSub, err := eng.Inspect(ctx, parentID, "sub")
	if err != nil || len(parentSub.Nodes) != 1 {
		t.Fatalf("Inspect(parent, sub) = %+v, %v", parentSub.Nodes, err)
	}
	if got := parentSub.Nodes[0]; got.Status != types.NodeStatusCanceled || got.ChildExecutionID != childID {
		t.Fatalf("parent sub node = %q child %q, want canceled and still naming %q", got.Status, got.ChildExecutionID, childID)
	}
	if parentSub.Status != types.ExecutionStatusTimeout || parentSub.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent after the cascade = %q %q, want it still timeout", parentSub.Status, parentSub.Error)
	}
}
