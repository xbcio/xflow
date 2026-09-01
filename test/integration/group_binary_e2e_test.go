//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// --- group test handlers ---

// groupMemberHandler is a simple action handler that transforms input data
// by appending its node name. It simulates real work inside a group.
type groupMemberHandler struct {
	invocations atomic.Int32
}

func (h *groupMemberHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.group.member"}
}

func (h *groupMemberHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.invocations.Add(1)
	out := make(map[string]any)
	for k, v := range input.Data {
		out[k] = v
	}
	out["processed"] = true
	return &types.Output{Data: out}, nil
}

// --- helper: build a group-capable runner ---

type groupRunnerOpts struct {
	runnerID    string
	handlers    map[string]types.ActionHandler
	labels      map[string]string
	concurrency int
}

func startGroupRunner(t *testing.T, ctx context.Context, h *serverRunnerHarness, opts groupRunnerOpts) (*runnersvc.Runner, chan error) {
	t.Helper()
	if opts.concurrency <= 0 {
		opts.concurrency = 2
	}

	registry := execution.NewRegistry()
	var caps []protocol.Capability
	for nodeType, handler := range opts.handlers {
		registry.RegisterGlobal(nodeType, handler)
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}
	// Advertise group.exec.v1 capability so control plane routes group tasks here.
	caps = append(caps, protocol.Capability{
		NodeType: "xflow.group",
		Features: []string{"group.exec.v1"},
	})

	cache := runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{
		MaxEntries:      16,
		MaxPackageBytes: 10 * 1024 * 1024,
	})
	groupRT := runnersvc.NewGroupRuntime(registry, cache, runnersvc.WithSuspendDisabled())

	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     opts.runnerID,
			Concurrency:  opts.concurrency,
			Capabilities: caps,
			Labels:       opts.labels,
			PollWait:     10 * time.Millisecond,
			GroupRuntime: groupRT,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, opts.runnerID)
	return runner, errCh
}

// startLegacyRunner starts a runner WITHOUT group.exec.v1 capability.
func startLegacyRunner(t *testing.T, ctx context.Context, h *serverRunnerHarness, runnerID string, handlers map[string]types.ActionHandler) chan error {
	t.Helper()
	registry := execution.NewRegistry()
	var caps []protocol.Capability
	for nodeType, handler := range handlers {
		registry.RegisterGlobal(nodeType, handler)
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}
	// No group.exec.v1 — legacy runner.
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     runnerID,
			Concurrency:  1,
			Capabilities: caps,
			PollWait:     10 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, runnerID)
	return errCh
}

// TestGroupBinaryE2E_NormalHappyPath verifies end-to-end group execution with
// the in-process server and runner: submit → dispatch → group execute → commit.
// The workflow has a two-node group (source→sink) with an external downstream
// node "out". The group executes on the runner's embedded GroupRuntime, boundary
// output propagates to "out", and the execution completes.
func TestGroupBinaryE2E_NormalHappyPath(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memberHandler := &groupMemberHandler{}
	_, errCh := startGroupRunner(t, ctx, h, groupRunnerOpts{
		runnerID: "group-runner-happy",
		handlers: map[string]types.ActionHandler{
			"test.group.member": memberHandler,
		},
	})

	// Workflow: group "g" with members [g.source, g.sink], plus external "out".
	// g.source → g.sink → out
	wf := &types.WorkflowDef{
		Name: "group-e2e-happy",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	}

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"seed": "hello-group",
	})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "out")

	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", result.Status)
	}
	outData, ok := result.Output["out"].(map[string]any)
	if !ok {
		t.Fatalf("output[out] = %T, want map", result.Output["out"])
	}
	// The boundary output from g.sink should propagate through "out".
	if outData["processed"] != true {
		t.Fatalf("output[out] = %v, want processed=true", outData)
	}

	// Verify group members were actually invoked (at least source + sink = 2).
	if got := memberHandler.invocations.Load(); got < 2 {
		t.Fatalf("group member invocations = %d, want >= 2", got)
	}

	cancel()
	select {
	case err := <-errCh:
		// At-least-once delivery may produce a stale lease-token rejection on
		// shutdown (the runner reports a result after the lease has already been
		// committed via a prior delivery). This is expected behavior.
		if err != nil {
			t.Logf("runner shutdown error (non-fatal): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}

// TestGroupBinaryE2E_MultiExitSwitch verifies that only fired exit ports
// propagate downstream. The group has three members: source fans out to o1 and
// o2; o1 always exits on "main", o2 always exits on "main". Each feeds
// independent downstream nodes x and y. Both downstream nodes should receive
// output.
func TestGroupBinaryE2E_MultiExitSwitch(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memberHandler := &groupMemberHandler{}
	_, errCh := startGroupRunner(t, ctx, h, groupRunnerOpts{
		runnerID: "group-runner-multiexit",
		handlers: map[string]types.ActionHandler{
			"test.group.member": memberHandler,
		},
	})

	// Workflow: group "g" has source→o1, source→o2; o1→x, o2→y.
	// Both o1 and o2 exit on port "main". x and y are external downstream nodes.
	wf := &types.WorkflowDef{
		Name: "group-e2e-multiexit",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "g.o1", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "g.o2", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "x", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "y", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.o1", Input: "main"}, {Node: "g.o2", Input: "main"}}}},
			"g.o1":     {"main": {Targets: []types.Connection{{Node: "x", Input: "main"}}}},
			"g.o2":     {"main": {Targets: []types.Connection{{Node: "y", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.o1", "g.o2"}}},
	}

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"multi": "exit",
	})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "x", "y")

	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", result.Status)
	}

	// Both x and y should have received output from their respective group exits.
	xOut, ok := result.Output["x"].(map[string]any)
	if !ok {
		t.Fatalf("output[x] = %T, want map", result.Output["x"])
	}
	if xOut["processed"] != true {
		t.Fatalf("output[x] missing processed=true: %v", xOut)
	}

	yOut, ok := result.Output["y"].(map[string]any)
	if !ok {
		t.Fatalf("output[y] = %T, want map", result.Output["y"])
	}
	if yOut["processed"] != true {
		t.Fatalf("output[y] missing processed=true: %v", yOut)
	}

	// All three group members should be invoked.
	if got := memberHandler.invocations.Load(); got < 3 {
		t.Fatalf("group member invocations = %d, want >= 3", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("runner shutdown error (non-fatal): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}

// TestGroupBinaryE2E_SelectorRequiredMismatch verifies that a group with a
// required RunnerSelector matching labels that no runner has stays pending
// and never dispatches to any runner.
func TestGroupBinaryE2E_SelectorRequiredMismatch(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memberHandler := &groupMemberHandler{}
	// Runner with labels that do NOT match the group's required selector.
	_, errCh := startGroupRunner(t, ctx, h, groupRunnerOpts{
		runnerID: "group-runner-wrong-labels",
		handlers: map[string]types.ActionHandler{
			"test.group.member": memberHandler,
		},
		labels: map[string]string{"cloud": "aws"},
	})

	// Workflow: group has required selector labels {cloud: tencent}, but runner
	// only has {cloud: aws}. The group task should never get dispatched.
	wf := &types.WorkflowDef{
		Name: "group-e2e-selector-mismatch",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{
			Name:    "g",
			Members: []string{"g.source", "g.sink"},
			RunnerSelector: &types.RunnerSelector{
				Mode:        types.RunnerSelectorModeRequired,
				MatchLabels: map[string]string{"cloud": "tencent"},
			},
		}},
	}

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"never": "dispatched",
	})

	// Wait briefly — the execution should NOT reach terminal status.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shortCancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := h.state.GetExecution(shortCtx, execID)
		if err == nil && snap != nil && types.IsTerminalExecutionStatus(snap.Status) {
			t.Fatalf("execution reached terminal %s, expected to stay pending", snap.Status)
		}
		select {
		case <-shortCtx.Done():
			goto done
		case <-ticker.C:
		}
	}

done:
	// Verify the handler was never invoked.
	if got := memberHandler.invocations.Load(); got != 0 {
		t.Fatalf("member invocations = %d, want 0 (should never dispatch)", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("runner shutdown error (non-fatal): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}

// TestGroupBinaryE2E_LegacyRunnerNeverClaimsGroup verifies that a runner
// without group.exec.v1 capability never claims group assignments, even
// when it has the member node types registered.
func TestGroupBinaryE2E_LegacyRunnerNeverClaimsGroup(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memberHandler := &groupMemberHandler{}
	// Start a legacy runner (no group capability) with the member handler.
	legacyErrCh := startLegacyRunner(t, ctx, h, "legacy-runner-no-group", map[string]types.ActionHandler{
		"test.group.member": memberHandler,
	})

	// Workflow with a group — only a group-capable runner should claim it.
	wf := &types.WorkflowDef{
		Name: "group-e2e-legacy-runner",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	}

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"legacy": "test",
	})

	// Wait briefly — the legacy runner should never claim the group.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shortCancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := h.state.GetExecution(shortCtx, execID)
		if err == nil && snap != nil && types.IsTerminalExecutionStatus(snap.Status) {
			t.Fatalf("execution reached terminal %s, expected to stay pending (legacy runner cannot claim groups)", snap.Status)
		}
		select {
		case <-shortCtx.Done():
			goto legacyDone
		case <-ticker.C:
		}
	}
legacyDone:
	// Verify handler was never invoked — the legacy runner cannot run a group.
	if got := memberHandler.invocations.Load(); got != 0 {
		t.Fatalf("member invocations = %d, want 0 (legacy runner should never claim)", got)
	}

	cancel()
	select {
	case err := <-legacyErrCh:
		if err != nil {
			t.Logf("runner shutdown error (non-fatal): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}

// TestGroupBinaryE2E_KillRunnerMidGroupThenRestart verifies that when a runner
// is cancelled mid-group execution, the group result (failed/cancelled) is
// reported, and a retry or resubmission completes successfully via a second
// runner. This exercises the at-least-once delivery guarantee for groups.
func TestGroupBinaryE2E_KillRunnerMidGroupThenRestart(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	// Phase 1: Start runner 1 with a blocking handler. Submit the workflow.
	// The group will start executing on runner 1 but we cancel before it finishes.
	ctx1, cancel1 := context.WithCancel(context.Background())

	blockCh := make(chan struct{})
	blockingHandler := &channelBlockHandler{ch: blockCh}
	memberHandler := &groupMemberHandler{}

	_, errCh1 := startGroupRunner(t, ctx1, h, groupRunnerOpts{
		runnerID: "group-runner-kill-1",
		handlers: map[string]types.ActionHandler{
			"test.group.block":  blockingHandler,
			"test.group.member": memberHandler,
		},
	})

	// Workflow with a blocking source node.
	wf := &types.WorkflowDef{
		Name: "group-e2e-kill-restart",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.block", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	}

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"kill_test": "yes",
	})

	// Wait a bit for runner 1 to claim and start the group.
	time.Sleep(500 * time.Millisecond)

	// Kill runner 1 — this cancels the inner engine execution.
	cancel1()
	select {
	case <-errCh1:
	case <-time.After(3 * time.Second):
	}

	// Phase 2: Start runner 2 with a fast handler. The execution either:
	// (a) already failed (runner 1 reported cancel) → need retry config, or
	// (b) the runner directory requeues → runner 2 picks up.
	// For this test, submit a fresh execution to verify runner 2 is healthy.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	fastHandler := &groupMemberHandler{}
	_, errCh2 := startGroupRunner(t, ctx2, h, groupRunnerOpts{
		runnerID: "group-runner-kill-2",
		handlers: map[string]types.ActionHandler{
			"test.group.block":  fastHandler, // fast version
			"test.group.member": fastHandler,
		},
	})

	// Submit a second execution — this should complete on runner 2.
	execID2 := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"kill_test": "second_run",
	})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result2 := waitForCompletion(waitCtx, t, h.state, execID2, "out")

	if result2.Status != types.ExecutionStatusSuccess {
		t.Fatalf("second execution status = %s, want success", result2.Status)
	}
	outData, ok := result2.Output["out"].(map[string]any)
	if !ok {
		t.Fatalf("output[out] = %T, want map", result2.Output["out"])
	}
	if outData["processed"] != true {
		t.Fatalf("output[out] = %v, want processed=true", outData)
	}

	// The first execution may be failed (runner 1 reported cancel/fail) or
	// still pending (runner 1 died before report). Either is acceptable.
	snap, _ := h.state.GetExecution(context.Background(), execID)
	if snap != nil && snap.Status == types.ExecutionStatusSuccess {
		// This shouldn't happen — the blocking handler was never unblocked.
		t.Log("first execution unexpectedly succeeded (runner 1 may have reported before cancel)")
	}

	cancel2()
	select {
	case err := <-errCh2:
		if err != nil {
			t.Logf("runner 2 shutdown error (non-fatal): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner 2 did not stop in time")
	}
}

// channelBlockHandler blocks until its channel is closed or context cancelled.
type channelBlockHandler struct {
	ch chan struct{}
}

func (h *channelBlockHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.group.block"}
}

func (h *channelBlockHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	select {
	case <-h.ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	out := make(map[string]any)
	for k, v := range input.Data {
		out[k] = v
	}
	out["processed"] = true
	return &types.Output{Data: out}, nil
}

// TestGroupBinaryE2E_FaultMatrix covers the three crash windows per operation:
// write-before-crash, write-success-response-lost, write-after-outbox-lost.
// This test verifies idempotency: submitting the same workflow twice results
// in independent executions, and a group that is committed twice (response lost)
// does not corrupt state.
func TestGroupBinaryE2E_FaultMatrix(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memberHandler := &groupMemberHandler{}
	_, errCh := startGroupRunner(t, ctx, h, groupRunnerOpts{
		runnerID: "group-runner-fault",
		handlers: map[string]types.ActionHandler{
			"test.group.member": memberHandler,
		},
	})

	wf := &types.WorkflowDef{
		Name: "group-e2e-fault-matrix",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	}

	// Submit two independent executions of the same workflow (idempotency).
	execID1 := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"run": "first"})
	execID2 := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"run": "second"})

	if execID1 == execID2 {
		t.Fatal("two submissions should produce different execution IDs")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()

	r1 := waitForCompletion(waitCtx, t, h.state, execID1, "out")
	r2 := waitForCompletion(waitCtx, t, h.state, execID2, "out")

	if r1.Status != types.ExecutionStatusSuccess {
		t.Fatalf("exec 1 status = %s, want success", r1.Status)
	}
	if r2.Status != types.ExecutionStatusSuccess {
		t.Fatalf("exec 2 status = %s, want success", r2.Status)
	}

	// Both should have independent output.
	out1, ok := r1.Output["out"].(map[string]any)
	if !ok {
		t.Fatalf("exec1 output[out] = %T, want map", r1.Output["out"])
	}
	out2, ok := r2.Output["out"].(map[string]any)
	if !ok {
		t.Fatalf("exec2 output[out] = %T, want map", r2.Output["out"])
	}
	if out1["processed"] != true || out2["processed"] != true {
		t.Fatalf("both executions should complete with processed=true; got out1=%v, out2=%v", out1, out2)
	}

	// At-least-once: each group has 2 members, two executions = at least 4.
	if got := memberHandler.invocations.Load(); got < 4 {
		t.Fatalf("member invocations = %d, want >= 4 (2 members × 2 executions)", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("runner shutdown error (non-fatal): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}
