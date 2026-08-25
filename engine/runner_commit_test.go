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

type claimTaskLeaseErrorState struct {
	*fakeState
	err error
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

func (s *claimTaskLeaseErrorState) ClaimTaskLease(_ context.Context, _ *TaskLease) (*NodeSnapshot, bool, error) {
	return nil, false, s.err
}

func (s *claimTaskLeaseErrorState) CommitNode(_ context.Context, _ CommitNodeRequest) (CommitNodeResult, error) {
	return CommitNodeResult{}, s.err
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
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
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
			"start": {"main": {Targets: []types.Connection{{Node: "approve", Input: "main"}}}},
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
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
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
				"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
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

// TestEngine_CommitTaskResultWithOutcomeClassifiesDuplicateTerminal replays one
// lease twice against a single-node workflow, so the second commit is a
// redelivery of a node that already committed terminal AND lands on an
// execution the first commit finished. Both descriptions are true; the outcome
// reports the first.
//
// It used to report the second. That was not a decision — both backends check
// node-dedup before execution-terminal (memoryState.CommitNode in
// backend/providers/local/atomic_state.go, and commitNodeLua in
// backend/providers/distributed/internal/rstate/atomic_state.go, whose leading
// GET tests only whether the status key still EXISTS; its terminal test sits
// below the dedup block). The engine simply happened to ask about the execution
// before calling CommitNode at all, so the coarser answer arrived first.
// Dropping that pre-check — it cost a round trip on every ordinary commit to
// pre-empt a rare one — lets CommitNode's own ordering decide, and CommitNode
// names the nearer cause: this exact commit already happened.
//
// The fake this runs against mirrors that ordering. Reordering it flips this
// test to execution_inactive, which is what makes the assertion a statement
// about the ordering rather than about the fake.
//
// Nothing downstream tells the two apart. Both release leased capacity, both
// set RemoveSeen, both return Accepted:true to the runner, and neither reaches
// the wire — protocol.ReportResultResponse carries no outcome field. The one
// visible difference is which xflow_commit_outcomes_total label increments.
func TestEngine_CommitTaskResultWithOutcomeClassifiesDuplicateTerminal(t *testing.T) {
	ctx := context.Background()
	eng, queue := newRunnerCommitEngine(t)
	submitRunnerCommitWorkflow(t, ctx, eng)
	task := queue.Drain()[0]

	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if err != nil {
		t.Fatalf("first CommitTaskResultWithOutcome() error = %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("first outcome = %s, want %s", outcome, CommitOutcomeAccepted)
	}

	outcome, err = eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"late": true}},
	})
	if err != nil {
		t.Fatalf("replayed CommitTaskResultWithOutcome() error = %v, want nil", err)
	}
	if outcome != CommitOutcomeDuplicateTerminal {
		t.Fatalf("replayed outcome = %s, want %s", outcome, CommitOutcomeDuplicateTerminal)
	}
}

// TestEngine_CommitTaskResultWithOutcomeClassifiesExecutionInactive covers the
// case the duplicate test above no longer does: a node that has NOT committed,
// on an execution that is over. With node-dedup unable to fire, CommitNode's
// execution-terminal check is what answers — which is the point, because that
// check is now the only thing standing where the engine's pre-check used to be.
// Remove it from the fake and this test reports accepted.
func TestEngine_CommitTaskResultWithOutcomeClassifiesExecutionInactive(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	// Two nodes so cancelling leaves a live lease on an uncommitted node.
	id, err := eng.Submit(ctx, compileCommitProbeGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	lease, err := eng.BuildTaskLease(ctx, queue.Drain()[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"late": true}},
	})
	if err != nil {
		t.Fatalf("inactive CommitTaskResultWithOutcome() error = %v, want nil", err)
	}
	if outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("inactive outcome = %s, want %s", outcome, CommitOutcomeExecutionInactive)
	}
	// The node must not have been written: an execution-inactive verdict that
	// still committed would be a fence that reports without fencing.
	node, err := state.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node != nil && types.IsTerminalNodeStatus(node.Status) {
		t.Fatalf("node committed %s despite an inactive verdict", node.Status)
	}
}

func TestEngine_CommitTaskResultWithOutcomeClassifiesTransientError(t *testing.T) {
	ctx := context.Background()
	state := &claimTaskLeaseErrorState{
		fakeState: newFakeState(),
		err:       errors.New("claim task lease failed"),
	}
	queue := &fakeQueue{}
	eng := New(state, queue)
	submitRunnerCommitWorkflow(t, ctx, eng)
	task := queue.Drain()[0]

	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if !errors.Is(err, state.err) {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v, want %v", err, state.err)
	}
	if outcome != CommitOutcomeTransientError {
		t.Fatalf("outcome = %s, want %s", outcome, CommitOutcomeTransientError)
	}
}

func TestCommitOutcome_ReleasesLeasedCapacity(t *testing.T) {
	tests := []struct {
		name    string
		outcome CommitOutcome
		want    bool
	}{
		{name: "accepted", outcome: CommitOutcomeAccepted, want: true},
		{name: "duplicate terminal", outcome: CommitOutcomeDuplicateTerminal, want: true},
		{name: "stale token", outcome: CommitOutcomeStaleToken, want: true},
		{name: "execution inactive", outcome: CommitOutcomeExecutionInactive, want: true},
		{name: "transient error", outcome: CommitOutcomeTransientError, want: false},
		{name: "unknown outcome", outcome: CommitOutcome("unknown"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.outcome.ReleasesLeasedCapacity(); got != tt.want {
				t.Fatalf("ReleasesLeasedCapacity(%q) = %v, want %v", tt.outcome, got, tt.want)
			}
		})
	}
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

	var issuedToken LeaseToken
	issuedCount := 0
	duplicateErrs := 0
	for _, got := range []result{first, second} {
		switch {
		case got.err == nil:
			issuedCount++
			if issuedCount > 1 {
				t.Fatalf("issued multiple leases: first token=%q second token=%q", issuedToken, got.lease.LeaseToken)
			}
			issuedToken = got.lease.LeaseToken
		case errors.Is(got.err, ErrLeaseAlreadyActive):
			duplicateErrs++
		default:
			t.Fatalf("BuildTaskLease() error = %v, want nil or ErrLeaseAlreadyActive", got.err)
		}
	}
	if issuedCount != 1 {
		t.Fatalf("issued leases = %d, want 1", issuedCount)
	}
	if duplicateErrs != 1 {
		t.Fatalf("duplicate errors = %d, want 1", duplicateErrs)
	}

	ns, err := state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if ns.LeaseToken != issuedToken {
		t.Fatalf("stored lease token = %q, want %q", ns.LeaseToken, issuedToken)
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

func TestEngineReleaseTaskLeaseRequeuesExactTaskAndFencesStaleLease(t *testing.T) {
	ctx := context.Background()
	eng, queue := newRunnerCommitEngine(t, WithDefaultLeaseTTL(time.Minute))
	submitRunnerCommitWorkflow(t, ctx, eng)
	task := queue.Drain()[0]
	task.Type = TaskTypeNodeResume
	task.Payload = &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval",
		Data:      map[string]any{"approved": true},
	}
	task.ActivationID = 7
	task.AutoDepth = 3

	first, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	released, err := eng.ReleaseTaskLease(ctx, first)
	if err != nil || !released {
		t.Fatalf("ReleaseTaskLease() released=%v err=%v, want true/nil", released, err)
	}

	requeued := queue.Drain()
	if len(requeued) != 1 {
		t.Fatalf("requeued task count = %d, want 1", len(requeued))
	}
	got := requeued[0]
	if got.Type != TaskTypeNodeResume || got.ActivationID != 7 || got.AutoDepth != 3 || got.Payload == nil || got.Payload.Name != "approval" || got.Payload.Data["approved"] != true {
		t.Fatalf("requeued task = %+v, want original resume task metadata and payload", got)
	}

	second, err := eng.BuildTaskLease(ctx, got)
	if err != nil {
		t.Fatalf("second BuildTaskLease() error = %v", err)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatalf("second lease token = %q, want a fresh token", second.LeaseToken)
	}

	released, err = eng.ReleaseTaskLease(ctx, first)
	if err != nil || released {
		t.Fatalf("stale ReleaseTaskLease() released=%v err=%v, want false/nil", released, err)
	}
	if lingering := queue.Drain(); len(lingering) != 0 {
		t.Fatalf("stale release requeued %d task(s), want none", len(lingering))
	}

	node, err := eng.State().GetNode(ctx, first.Task.ExecutionID, first.Task.NodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.LeaseToken != second.LeaseToken || node.Status != types.NodeStatusRunning {
		t.Fatalf("node after stale release = %+v, want active second lease", node)
	}
}

func TestEngineReclaimsClaimedResumeLeaseWithFence(t *testing.T) {
	ctx := context.Background()
	def := &types.WorkflowDef{
		Name:    "claimed-resume-recovery",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithDefaultLeaseTTL(time.Minute))
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	task.Type = TaskTypeNodeResume
	task.Payload = &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval",
		Data:      map[string]any{"approved": true},
	}
	task.ActivationID = 1
	task.AutoDepth = 4

	first, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease(first) error = %v", err)
	}
	claimed, valid, err := state.ClaimTaskLease(ctx, first)
	if err != nil || !valid || claimed.Status != types.NodeStatusCommitting {
		t.Fatalf("ClaimTaskLease() = (%+v, %v, %v), want committing claim", claimed, valid, err)
	}
	if claimed.LeaseToken != first.LeaseToken || claimed.LeaseTaskType != TaskTypeNodeResume || claimed.LeasePayload == nil || claimed.LeasePayload.Name != "approval" {
		t.Fatalf("claimed lease metadata = %+v, want original recoverable resume task", claimed)
	}

	expired, err := state.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil || len(expired) != 1 {
		t.Fatalf("ListExpiredLeases() = %+v, %v; want claimed lease", expired, err)
	}
	if expired[0].TaskType != TaskTypeNodeResume || expired[0].Payload == nil || expired[0].Payload.Name != "approval" || expired[0].Payload.Data["approved"] != true || expired[0].ActivationID != 1 || expired[0].AutoDepth != 4 {
		t.Fatalf("expired lease = %+v, want exact resume task metadata", expired[0])
	}

	results := make(chan bool, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			ok, reclaimErr := eng.ReclaimLease(ctx, expired[0])
			results <- ok
			errs <- reclaimErr
		}()
	}
	wins := 0
	for range 2 {
		if reclaimErr := <-errs; reclaimErr != nil {
			t.Fatalf("ReclaimLease() error = %v", reclaimErr)
		}
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("reclaim winners = %d, want exactly one", wins)
	}

	requeued := queue.Drain()
	if len(requeued) != 1 {
		t.Fatalf("requeued tasks = %+v, want one", requeued)
	}
	if got := requeued[0]; got.Type != TaskTypeNodeResume || got.Payload == nil || got.Payload.Name != "approval" || got.Payload.Data["approved"] != true || got.ActivationID != 1 || got.AutoDepth != 4 {
		t.Fatalf("requeued task = %+v, want exact resume metadata", got)
	}

	second, err := eng.BuildTaskLease(ctx, requeued[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(second) error = %v", err)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatalf("recovered lease token = %q, want fresh token", second.LeaseToken)
	}
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, first, TaskResult{Output: &types.Output{Data: map[string]any{"stale": true}}})
	if !errors.Is(err, ErrInvalidLeaseToken) || outcome != CommitOutcomeStaleToken {
		t.Fatalf("stale claimed result = (%s, %v), want stale token", outcome, err)
	}
	if err := eng.CommitTaskResult(ctx, second, TaskResult{Output: &types.Output{Data: map[string]any{"fresh": true}}}); err != nil {
		t.Fatalf("CommitTaskResult(second) error = %v", err)
	}
	output, err := state.GetOutput(ctx, second.Task.ExecutionID, second.Task.NodeName)
	if err != nil {
		t.Fatal(err)
	}
	if output["fresh"] != true || output["stale"] == true {
		t.Fatalf("output = %v, want only fresh result", output)
	}
}
