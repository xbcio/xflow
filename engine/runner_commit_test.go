package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

type recordingHooks struct {
	started []string
}

func newRunnerCommitEngine(t *testing.T, opts ...Option) (*Engine, *fakeQueue) {
	t.Helper()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, opts...)
	return eng, queue
}

func submitRunnerCommitWorkflow(t *testing.T, ctx context.Context, eng *Engine) types.ExecutionID {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "runner-commit-helper",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	return execID
}

type coordinatedLeaseState struct {
	*fakeState
	started     chan struct{}
	releaseLock chan struct{}
	releaseOnce sync.Once
	blockOnce   sync.Once
}

func newCoordinatedLeaseState() *coordinatedLeaseState {
	return &coordinatedLeaseState{
		fakeState:   newFakeState(),
		started:     make(chan struct{}, 2),
		releaseLock: make(chan struct{}),
	}
}

func (s *coordinatedLeaseState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	ns, err := s.fakeState.GetNode(ctx, id, name)
	if err == nil && ns == nil {
		s.started <- struct{}{}
	}
	return ns, err
}

func (s *coordinatedLeaseState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.started <- struct{}{}
	return s.fakeState.AcquireTaskLease(ctx, lease)
}

func (s *coordinatedLeaseState) UpsertNode(ctx context.Context, n *NodeSnapshot) error {
	if n != nil && n.Status == types.NodeStatusRunning {
		s.blockOnce.Do(func() {
			<-s.releaseLock
		})
	}
	return s.fakeState.UpsertNode(ctx, n)
}

func (s *coordinatedLeaseState) release() {
	s.releaseOnce.Do(func() {
		close(s.releaseLock)
	})
}

func (h *recordingHooks) OnNodeStart(_ context.Context, _ types.ExecutionID, name string) {
	h.started = append(h.started, name)
}

func (h *recordingHooks) OnNodeComplete(context.Context, types.ExecutionID, string, types.NodeStatus) {
}
func (h *recordingHooks) OnNodeSuspended(context.Context, types.ExecutionID, string) {}
func (h *recordingHooks) OnExecutionComplete(context.Context, types.ExecutionID, types.ExecutionStatus) {
}
func (h *recordingHooks) OnSignalDelivered(context.Context, types.ExecutionID, string, map[string]any) {
}
func (h *recordingHooks) OnSignalRevoked(context.Context, types.ExecutionID, string) {}
func (h *recordingHooks) OnNodeTimeout(context.Context, types.ExecutionID, string)   {}
func (h *recordingHooks) OnNodeRetry(context.Context, types.ExecutionID, string, int, time.Duration) {
}

func TestEngine_BuildTaskLeaseAndCommitTaskResult_RunnerStyleFlow(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-style",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "next", Input: "main"}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{"claim_id": "c-1"})
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("expected start task, got %v", taskNames(tasks))
	}

	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	input := lease.Input
	if input.Data["claim_id"] != "c-1" {
		t.Fatalf("expected root input params, got %v", input.Data)
	}

	err = eng.CommitTaskResult(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"parsed": true}},
	})
	if err != nil {
		t.Fatal(err)
	}

	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "next" {
		t.Fatalf("expected next task after commit, got %v", taskNames(tasks))
	}

	lease, err = eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	input = lease.Input
	if input.Data["parsed"] != true {
		t.Fatalf("expected upstream output as input, got %v", input.Data)
	}
}

func TestEngine_BuildTaskLeaseIncludesRunnerRoutingMetadata(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-lease",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo", Version: 2},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{"claim_id": "c-1"})
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]

	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.ExecutionID != id || lease.Task.NodeName != "start" {
		t.Fatalf("unexpected lease task: %+v", lease.Task)
	}
	if lease.NodeType != "test.echo" || lease.NodeVersion != 2 {
		t.Fatalf("unexpected routing metadata: type=%q version=%d", lease.NodeType, lease.NodeVersion)
	}
	if lease.Input.Data["claim_id"] != "c-1" {
		t.Fatalf("expected root params in lease input, got %v", lease.Input.Data)
	}
}

func TestEngine_TaskRoutingIncludesEffectiveRunnerSelector(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-selector-routing",
		RunnerSelector: &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeDefault,
			MatchLabels: map[string]string{"mode": "remote", "env": "prod"},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name: "approve",
				Type: "xflow.function",
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{"mode": "local"},
				},
			},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "approve", Input: "main"}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatal(err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued tasks = %d, want 1", len(tasks))
	}
	routing, err := eng.TaskRouting(ctx, tasks[0])
	if err != nil {
		t.Fatalf("TaskRouting(start) error = %v", err)
	}
	if got := routing.RunnerSelector.MatchLabels["mode"]; got != "remote" {
		t.Fatalf("start selector mode = %q, want remote", got)
	}

	startLease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{}}}); err != nil {
		t.Fatalf("CommitTaskResult(start) error = %v", err)
	}
	tasks = queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued tasks after start = %d, want 1", len(tasks))
	}
	routing, err = eng.TaskRouting(ctx, tasks[0])
	if err != nil {
		t.Fatalf("TaskRouting(approve) error = %v", err)
	}
	if got := routing.RunnerSelector.MatchLabels["mode"]; got != "local" {
		t.Fatalf("approve selector mode = %q, want local override", got)
	}
	if _, ok := routing.RunnerSelector.MatchLabels["env"]; ok {
		t.Fatalf("approve selector inherited env in default mode: %+v", routing.RunnerSelector.MatchLabels)
	}
}

func TestEngine_BuildTaskLeaseMarksNodeRunningAndFiresStartHook(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-start",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	hooks := &recordingHooks{}
	eng := New(state, queue, WithHooks(hooks))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]

	if _, err := eng.BuildTaskLease(ctx, task); err != nil {
		t.Fatal(err)
	}

	snap, err := state.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || snap.Status != types.NodeStatusRunning {
		t.Fatalf("node status = %+v, want running", snap)
	}
	if len(hooks.started) != 1 || hooks.started[0] != "start" {
		t.Fatalf("started hooks = %v, want [start]", hooks.started)
	}
}

func TestEngine_CommitTaskResult_IgnoresDuplicateTerminalResult(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "duplicate-result",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "next", Input: "main"}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	result := TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}
	if err := eng.CommitTaskResult(ctx, lease, result); err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, lease, result); err != nil {
		t.Fatal(err)
	}

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "next" {
		t.Fatalf("duplicate result should enqueue next once, got %v", taskNames(tasks))
	}
}

func TestEngine_CommitTaskResultRejectsStaleLeaseToken(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "lease-fencing",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]

	first, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	expired, err := eng.State().ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired leases = %d, want 1", len(expired))
	}
	ok, err := eng.ReclaimLease(ctx, expired[0])
	if err != nil || !ok {
		t.Fatalf("ReclaimLease() ok=%v err=%v, want ok", ok, err)
	}
	requeued := queue.Drain()[0]

	second, err := eng.BuildTaskLease(ctx, requeued)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseToken == "" || second.LeaseToken == "" || first.LeaseToken == second.LeaseToken {
		t.Fatalf("expected distinct non-empty lease tokens, got first=%q second=%q", first.LeaseToken, second.LeaseToken)
	}

	err = eng.CommitTaskResult(ctx, first, TaskResult{Output: &types.Output{Data: map[string]any{"stale": true}}})
	if err == nil {
		t.Fatal("expected stale lease commit to fail")
	}
	if !errors.Is(err, ErrInvalidLeaseToken) {
		t.Fatalf("CommitTaskResult() error = %v, want ErrInvalidLeaseToken", err)
	}

	if err := eng.CommitTaskResult(ctx, second, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}); err != nil {
		t.Fatalf("fresh lease commit failed: %v", err)
	}

	out, err := state.GetOutput(ctx, task.ExecutionID, "start")
	if err != nil {
		t.Fatal(err)
	}
	if out["ok"] != true || out["stale"] == true {
		t.Fatalf("output = %v, want fresh result only", out)
	}
}

func TestEngine_CommitTaskResultWithOutcomeClassifiesAcceptedDuplicateAndStale(t *testing.T) {
	ctx := context.Background()
	t.Run("accepted and duplicate terminal", func(t *testing.T) {
		def := &types.WorkflowDef{
			Name: "commit-outcome-duplicate",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.echo"},
				{Name: "next", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start": {"main": []types.Connection{{Node: "next", Input: "main"}}},
			},
		}

		g, err := graph.Compile(def)
		if err != nil {
			t.Fatal(err)
		}
		eng, queue := newRunnerCommitEngine(t)
		if _, err := eng.Submit(ctx, g, nil); err != nil {
			t.Fatal(err)
		}
		task := queue.Drain()[0]
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease() error = %v", err)
		}

		outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}})
		if err != nil {
			t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
		}
		if outcome != CommitOutcomeAccepted {
			t.Fatalf("outcome = %s, want %s", outcome, CommitOutcomeAccepted)
		}

		outcome, err = eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}})
		if err != nil {
			t.Fatalf("duplicate CommitTaskResultWithOutcome() error = %v", err)
		}
		if outcome != CommitOutcomeDuplicateTerminal {
			t.Fatalf("duplicate outcome = %s, want %s", outcome, CommitOutcomeDuplicateTerminal)
		}
	})

	t.Run("stale token", func(t *testing.T) {
		eng, queue := newRunnerCommitEngine(t)
		submitRunnerCommitWorkflow(t, ctx, eng)
		task := queue.Drain()[0]

		first, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease() error = %v", err)
		}

		expired, err := eng.State().ListExpiredLeases(ctx, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatalf("ListExpiredLeases() error = %v", err)
		}
		if len(expired) != 1 {
			t.Fatalf("expired leases = %d, want 1", len(expired))
		}
		ok, err := eng.ReclaimLease(ctx, expired[0])
		if err != nil || !ok {
			t.Fatalf("ReclaimLease() ok=%v err=%v, want ok", ok, err)
		}

		requeued := queue.Drain()[0]
		second, err := eng.BuildTaskLease(ctx, requeued)
		if err != nil {
			t.Fatalf("second BuildTaskLease() error = %v", err)
		}

		outcome, err := eng.CommitTaskResultWithOutcome(ctx, first, TaskResult{Output: &types.Output{Data: map[string]any{"stale": true}}})
		if !errors.Is(err, ErrInvalidLeaseToken) {
			t.Fatalf("stale error = %v, want ErrInvalidLeaseToken", err)
		}
		if outcome != CommitOutcomeStaleToken {
			t.Fatalf("stale outcome = %s, want %s", outcome, CommitOutcomeStaleToken)
		}

		outcome, err = eng.CommitTaskResultWithOutcome(ctx, second, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}})
		if err != nil {
			t.Fatalf("fresh CommitTaskResultWithOutcome() error = %v", err)
		}
		if outcome != CommitOutcomeAccepted {
			t.Fatalf("fresh outcome = %s, want %s", outcome, CommitOutcomeAccepted)
		}
	})
}

func TestEngine_BuildTaskLeaseRejectsActiveUnexpiredLeaseButRoutingIsIdempotent(t *testing.T) {
	ctx := context.Background()
	eng, queue := newRunnerCommitEngine(t, WithDefaultLeaseTTL(time.Minute))
	execID := submitRunnerCommitWorkflow(t, ctx, eng)
	task := queue.Drain()[0]

	first, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("first BuildTaskLease() error = %v", err)
	}
	if first.LeaseToken == "" {
		t.Fatalf("first lease token is empty")
	}

	routing, err := eng.TaskRouting(ctx, task)
	if err != nil {
		t.Fatalf("TaskRouting() error = %v", err)
	}
	if routing.NodeType != first.NodeType || routing.NodeVersion != first.NodeVersion {
		t.Fatalf("routing = %+v, want node type/version from first lease %+v", routing, first)
	}

	second, err := eng.BuildTaskLease(ctx, task)
	if !errors.Is(err, ErrLeaseAlreadyActive) {
		t.Fatalf("second BuildTaskLease() error = %v, want ErrLeaseAlreadyActive", err)
	}
	if second != nil {
		t.Fatalf("second BuildTaskLease() lease = %+v, want nil", second)
	}

	expired, err := eng.State().ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired leases = %d, want 1", len(expired))
	}

	ok, err := eng.ReclaimLease(ctx, expired[0])
	if err != nil || !ok {
		t.Fatalf("ReclaimLease() ok=%v err=%v, want ok", ok, err)
	}

	reclaimedTask := queue.Drain()[0]
	reissued, err := eng.BuildTaskLease(ctx, reclaimedTask)
	if err != nil {
		t.Fatalf("BuildTaskLease() after reclaim error = %v", err)
	}
	if reissued.LeaseToken == first.LeaseToken {
		t.Fatalf("reissued lease token = %q, want a fresh token", reissued.LeaseToken)
	}

	_ = execID
}

func TestEngine_BuildTaskLeaseConcurrentAcquireReturnsSingleLease(t *testing.T) {
	ctx := context.Background()
	state := newCoordinatedLeaseState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithDefaultLeaseTTL(time.Minute))
	submitRunnerCommitWorkflow(t, ctx, eng)
	task := queue.Drain()[0]

	type result struct {
		lease *TaskLease
		err   error
	}

	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			lease, err := eng.BuildTaskLease(ctx, task)
			results <- result{lease: lease, err: err}
		}()
	}
	close(start)

	seen := 0
	timeout := time.After(time.Second)
	for seen < 2 {
		select {
		case <-state.started:
			seen++
		case <-timeout:
			state.release()
			goto collect
		}
	}
	state.release()

collect:
	first := <-results
	second := <-results

	var issued *TaskLease
	duplicateErrs := 0
	for _, got := range []result{first, second} {
		switch {
		case got.err == nil:
			if issued != nil {
				t.Fatalf("issued multiple leases: first=%+v second=%+v", issued, got.lease)
			}
			issued = got.lease
		case errors.Is(got.err, ErrLeaseAlreadyActive):
			duplicateErrs++
		default:
			t.Fatalf("BuildTaskLease() error = %v, want nil or ErrLeaseAlreadyActive", got.err)
		}
	}
	if issued == nil {
		t.Fatal("no lease issued")
	}
	if duplicateErrs != 1 {
		t.Fatalf("duplicate errors = %d, want 1", duplicateErrs)
	}

	ns, err := state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if ns == nil {
		t.Fatal("node snapshot missing after concurrent acquire")
	}
	if ns.LeaseToken != issued.LeaseToken {
		t.Fatalf("stored lease token = %q, want %q", ns.LeaseToken, issued.LeaseToken)
	}
	if ns.Attempt != 1 {
		t.Fatalf("node attempt = %d, want 1", ns.Attempt)
	}
}

func TestEngine_CommitTaskResultParksSuspendRequestWithoutHandlerExecution(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runtime-suspend",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.suspend"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	hooks := &recordingHooks{}
	eng := New(state, queue, WithHooks(hooks))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	err = eng.CommitTaskResult(ctx, lease, TaskResult{
		Suspend: &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	ns, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if ns == nil || ns.Status != types.NodeStatusSuspended {
		t.Fatalf("node state = %+v, want suspended", ns)
	}
	if tasks := queue.Drain(); len(tasks) != 0 {
		t.Fatalf("expected no queued tasks after parking, got %v", taskNames(tasks))
	}
}

func TestEngine_CommitTaskResultFailsSuspendWhenDisabled(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runtime-suspend-disabled",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.suspend"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithSuspendDisabled(ErrSuspendUnsupported))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	err = eng.CommitTaskResult(ctx, lease, TaskResult{
		Suspend: &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	ns, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if ns == nil || ns.Status != types.NodeStatusFailed {
		t.Fatalf("node state = %+v, want failed", ns)
	}
	if ns.Error != ErrSuspendUnsupported.Error() {
		t.Fatalf("node error = %q, want %q", ns.Error, ErrSuspendUnsupported.Error())
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %q, want failed", snap.Status)
	}
}
