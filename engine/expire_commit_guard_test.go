package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// guardState is the expire fixture store with a counter on full-snapshot reads.
// It embeds the concrete expireState, not the StateStore interface, so every
// optional capability the commit paths probe for (atomic commit, narrow reads)
// is still promoted and the paths under test are the ones production takes.
//
// It also implements ExecutionStatusReader the way both real backends do, so
// the activeness probes cost a narrow status read and a full-snapshot read
// (GetExecution) is left for the code that wants the snapshot itself, such as
// the execution deadline.
type guardState struct {
	*expireState

	mu          sync.Mutex
	reads       int
	nodeCommits int
	afterRead   func()
}

var _ ExecutionStatusReader = (*guardState)(nil)

func (s *guardState) GetExecutionStatus(_ context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	snap := f.executions[id]
	if snap == nil {
		return "", false, nil
	}
	return snap.Status, true, nil
}

func (s *guardState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	snap, err := s.expireState.GetExecution(ctx, id)
	// One-shot, and run after the snapshot is taken: the read that triggers it
	// still sees the stored state as it was, the next one sees what the hook did.
	s.mu.Lock()
	hook := s.afterRead
	s.afterRead = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return snap, err
}

func (s *guardState) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *guardState) resetReads() {
	s.mu.Lock()
	s.reads = 0
	s.mu.Unlock()
}

// CommitNode counts the acyclic node commits that reach the store. The store
// fences a terminal execution on its own, so a guard that handled a timeout but
// let the commit run on anyway would be invisible in the final state; the count
// is what shows the commit never started.
func (s *guardState) CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.mu.Lock()
	s.nodeCommits++
	s.mu.Unlock()
	return s.expireState.CommitNode(ctx, req)
}

// CommitLeasedNode is the cyclic path's committer; the same count applies.
func (s *guardState) CommitLeasedNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.mu.Lock()
	s.nodeCommits++
	s.mu.Unlock()
	return s.expireState.CommitLeasedNode(ctx, req)
}

func (s *guardState) nodeCommitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeCommits
}

// guardSpec is the workflow a guardRig runs. bounded declares settings.timeout
// (the only graphs whose execution has a deadline at all); the node under test
// is start, with next on its main port and onerr on its error port.
type guardSpec struct {
	onError string
	cyclic  bool
	bounded bool
}

// guardRig is one running execution with start leased, driven through an
// engine whose store and hooks feed one expireLog.
type guardRig struct {
	eng   *Engine
	state *guardState
	queue *fakeQueue
	hooks *expireHooks
	g     *graph.Graph
	id    types.ExecutionID
	lease *TaskLease
}

// newGuardRig submits the workflow, overwrites the stored execution deadline
// with an exact instant (bounded graphs only) and leases start. The deadline is
// written before the lease is built because the engine memoizes the first read.
func newGuardRig(t *testing.T, spec guardSpec, deadline time.Time) *guardRig {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "expire-commit-guard",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo", OnError: spec.onError},
			{Name: "next", Type: "test.echo"},
			{Name: "onerr", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {
				"main":  {Targets: []types.Connection{{Node: "next", Input: "main"}}},
				"error": {Targets: []types.Connection{{Node: "onerr", Input: "main"}}},
			},
		},
	}
	if spec.bounded {
		def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
	}
	if spec.cyclic {
		// A cyclic workflow must have exactly one xflow.start node, and the
		// leased node is the one a trigger-less submission schedules first.
		def.Options = &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10}
		def.Nodes[0].Type = "xflow.start"
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	log := &expireLog{}
	state := &guardState{expireState: &expireState{subWorkflowLinkState: newSubWorkflowLinkState(), log: log}}
	hooks := &expireHooks{log: log}
	queue := &fakeQueue{}
	eng := New(state, queue, WithHooks(hooks))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	log.track = id
	state.InitInDegrees(id, g)
	if spec.bounded {
		state.fakeState.mu.Lock()
		state.fakeState.executions[id].Deadline = deadline
		state.fakeState.mu.Unlock()
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("submission enqueued %v, want [start]", taskNames(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	log.clear()
	return &guardRig{eng: eng, state: state, queue: queue, hooks: hooks, g: g, id: id, lease: lease}
}

func (r *guardRig) snapshot(t *testing.T) *ExecutionSnapshot {
	t.Helper()
	snap, err := r.state.GetExecution(context.Background(), r.id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	return snap
}

func (r *guardRig) nodeStatus(t *testing.T, name string) types.NodeStatus {
	t.Helper()
	node, err := r.state.GetNode(context.Background(), r.id, name)
	if err != nil || node == nil {
		t.Fatalf("GetNode(%q) = (%v, %v)", name, node, err)
	}
	return node.Status
}

// requireExpired pins the whole contract of an expiry that a node timeout
// triggered: the execution is in timeout for its deadline, nothing downstream
// was scheduled, and the completion hook announced timeout exactly once.
func (r *guardRig) requireExpired(t *testing.T) {
	t.Helper()
	snap := r.snapshot(t)
	if snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("execution status = %q, want %q", snap.Status, types.ExecutionStatusTimeout)
	}
	if snap.Error != ExecutionDeadlineExceededError {
		t.Fatalf("execution error = %q, want %q", snap.Error, ExecutionDeadlineExceededError)
	}
	if got := taskNames(r.queue.Drain()); len(got) != 0 {
		t.Fatalf("downstream enqueued %v, want nothing past the execution deadline", got)
	}
	if got := r.state.nodeCommitCount(); got != 0 {
		t.Fatalf("node commits reaching the store = %d, want 0: the node must not be committed at all", got)
	}
	want := []types.ExecutionStatus{types.ExecutionStatusTimeout}
	if got := r.hooks.completions(); !slices.Equal(got, want) {
		t.Fatalf("OnExecutionComplete statuses = %v, want %v", got, want)
	}
}

// guardEntry is one way a node timeout reaches the engine: the control plane's
// renewal backstop (CommitTaskTimeout) or the runner reporting it itself.
type guardEntry struct {
	name   string
	commit func(t *testing.T, r *guardRig)
}

var guardEntries = []guardEntry{
	{"CommitTaskTimeout", func(t *testing.T, r *guardRig) {
		t.Helper()
		if err := r.eng.CommitTaskTimeout(context.Background(), r.lease, nil); err != nil {
			t.Fatalf("CommitTaskTimeout() error = %v", err)
		}
	}},
	{"CommitTaskResult", func(t *testing.T, r *guardRig) {
		t.Helper()
		result := TaskResult{Error: types.NewPermanentError(types.NodeTimeoutErrorCode, "node execution exceeded its 1s timeout")}
		if err := r.eng.CommitTaskResult(context.Background(), r.lease, result); err != nil {
			t.Fatalf("CommitTaskResult() error = %v", err)
		}
	}},
}

var guardOnErrors = []string{"", "continue", "error_output", "main_output"}

func guardName(onError string) string {
	if onError == "" {
		return "on_error_default"
	}
	return "on_error_" + onError
}

// TestNodeTimeoutPastTheExecutionDeadlineExpiresTheExecution: once the
// execution's own budget is gone, a node timeout is the execution's timeout. It
// must not be routed through the node's on_error strategy, which would end the
// execution as failed (stop), keep it running past its budget (continue), or
// finish it as a success through the error port.
func TestNodeTimeoutPastTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	for _, entry := range guardEntries {
		for _, onError := range guardOnErrors {
			t.Run(entry.name+"/"+guardName(onError), func(t *testing.T) {
				rig := newGuardRig(t, guardSpec{onError: onError, bounded: true}, time.Now().Add(-time.Minute))
				entry.commit(t, rig)
				rig.requireExpired(t)
			})
		}
	}
}

// TestNodeTimeoutPastTheExecutionDeadlineExpiresACyclicExecution covers the
// cyclic graph, whose server-side timeout commit used to be an unconditional
// fatal and whose runner-reported one goes through the legacy committer.
func TestNodeTimeoutPastTheExecutionDeadlineExpiresACyclicExecution(t *testing.T) {
	for _, entry := range guardEntries {
		t.Run(entry.name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{cyclic: true, bounded: true}, time.Now().Add(-time.Minute))
			entry.commit(t, rig)
			rig.requireExpired(t)
		})
	}
}

// TestNodeTimeoutBeforeTheExecutionDeadlineStillAppliesOnError is the control
// for the test above: the node's own budget ran out while the execution still
// has time, so the node's on_error strategy decides, exactly as before.
func TestNodeTimeoutBeforeTheExecutionDeadlineStillAppliesOnError(t *testing.T) {
	cases := []struct {
		onError    string
		wantExec   types.ExecutionStatus
		wantNode   types.NodeStatus
		wantQueued string
	}{
		{"", types.ExecutionStatusFailed, types.NodeStatusFailed, ""},
		{"continue", types.ExecutionStatusRunning, types.NodeStatusContinued, "next"},
		{"error_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "onerr"},
		{"main_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "next"},
	}
	for _, entry := range guardEntries {
		for _, tc := range cases {
			t.Run(entry.name+"/"+guardName(tc.onError), func(t *testing.T) {
				rig := newGuardRig(t, guardSpec{onError: tc.onError, bounded: true}, time.Now().Add(time.Hour))
				entry.commit(t, rig)
				requireOnErrorApplied(t, rig, tc.wantExec, tc.wantNode, tc.wantQueued)
			})
		}
	}
}

// requireOnErrorApplied pins what ApplyOnError did: the execution and node
// statuses, and which single downstream node (if any) was scheduled.
func requireOnErrorApplied(t *testing.T, rig *guardRig, wantExec types.ExecutionStatus, wantNode types.NodeStatus, wantQueued string) {
	t.Helper()
	if got := rig.snapshot(t).Status; got != wantExec {
		t.Fatalf("execution status = %q, want %q", got, wantExec)
	}
	if got := rig.nodeStatus(t, "start"); got != wantNode {
		t.Fatalf("node status = %q, want %q", got, wantNode)
	}
	var want []string
	if wantQueued != "" {
		want = []string{wantQueued}
	}
	if got := taskNames(rig.queue.Drain()); !slices.Equal(got, want) {
		t.Fatalf("downstream enqueued %v, want %v", got, want)
	}
}

// TestOtherErrorsPastTheExecutionDeadlineStillApplyOnError: the guard is about
// the node timeout code and nothing else. A node that fails for any other reason
// keeps its on_error semantics even after the execution budget is gone; the
// maintenance sweep ends such an execution.
func TestOtherErrorsPastTheExecutionDeadlineStillApplyOnError(t *testing.T) {
	errs := map[string]error{
		"another permanent code": types.NewPermanentError("node.http.status", "bad gateway"),
		"plain error":            errors.New("boom"),
	}
	for name, cause := range errs {
		t.Run(name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(-time.Minute))
			err := rig.eng.CommitTaskResult(context.Background(), rig.lease, TaskResult{Error: cause})
			if err != nil {
				t.Fatalf("CommitTaskResult() error = %v", err)
			}
			requireOnErrorApplied(t, rig, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
			if got := rig.hooks.completions(); len(got) != 0 {
				t.Fatalf("OnExecutionComplete statuses = %v, want none: the execution is still running", got)
			}
		})
	}
}

// TestExecutionDeadlineBoundaryBelongsToTheExecution pins the tie-break: a node
// budget and the execution budget running out at the same instant is the
// execution's timeout, so the deadline counts as reached AT its instant and not
// only after it. The clock is a parameter of the guard, which is what makes the
// instant exact.
func TestExecutionDeadlineBoundaryBelongsToTheExecution(t *testing.T) {
	// In the real past: ExpireExecution re-checks the deadline against the wall
	// clock, so the injected instant only decides whether the guard calls it.
	deadline := time.Now().Add(-time.Hour)
	cases := []struct {
		name        string
		now         time.Time
		wantHandled bool
		wantStatus  types.ExecutionStatus
	}{
		{"a nanosecond before", deadline.Add(-time.Nanosecond), false, types.ExecutionStatusRunning},
		{"exactly at the deadline", deadline, true, types.ExecutionStatusTimeout},
		{"a nanosecond after", deadline.Add(time.Nanosecond), true, types.ExecutionStatusTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{bounded: true}, deadline)
			g, err := rig.eng.loadGraph(context.Background(), rig.id)
			if err != nil || g == nil {
				t.Fatalf("loadGraph() = (%v, %v)", g, err)
			}
			handled, err := rig.eng.expireIfExecutionDeadlineReached(context.Background(), rig.id, g, tc.now)
			if err != nil {
				t.Fatalf("guard error = %v", err)
			}
			if handled != tc.wantHandled {
				t.Fatalf("handled = %v, want %v", handled, tc.wantHandled)
			}
			if got := rig.snapshot(t).Status; got != tc.wantStatus {
				t.Fatalf("execution status = %q, want %q", got, tc.wantStatus)
			}
		})
	}
}

// TestUnboundedGraphNodeTimeoutNeverReadsTheExecutionSnapshot: a graph without
// settings.timeout has no execution deadline, so the guard answers from the
// graph alone. Not one snapshot read may be added to its commit path, and its
// node timeout stays an on_error matter whatever the clock says.
func TestUnboundedGraphNodeTimeoutNeverReadsTheExecutionSnapshot(t *testing.T) {
	t.Run("the guard itself", func(t *testing.T) {
		rig := newGuardRig(t, guardSpec{}, time.Time{})
		g, err := rig.eng.loadGraph(context.Background(), rig.id)
		if err != nil || g == nil {
			t.Fatalf("loadGraph() = (%v, %v)", g, err)
		}
		rig.state.resetReads()
		far := time.Now().AddDate(100, 0, 0)
		handled, err := rig.eng.expireIfExecutionDeadlineReached(context.Background(), rig.id, g, far)
		if err != nil || handled {
			t.Fatalf("guard = (%v, %v), want (false, nil)", handled, err)
		}
		if got := rig.state.readCount(); got != 0 {
			t.Fatalf("GetExecution calls = %d, want 0", got)
		}
	})
	for _, entry := range guardEntries {
		t.Run(entry.name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: "continue"}, time.Time{})
			rig.state.resetReads()
			entry.commit(t, rig)
			if got := rig.state.readCount(); got != 0 {
				t.Fatalf("GetExecution calls on the commit path = %d, want 0", got)
			}
			requireOnErrorApplied(t, rig, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
		})
	}
}

// TestNodeTimeoutPastTheExecutionDeadlineReturnsAStoreFailure: an expiry that
// cannot finish is not "handled" quietly. The store error comes back to the
// caller, who retries; the status write already landed, so the execution is
// in timeout and the deadline sweep can finish the cascade.
func TestNodeTimeoutPastTheExecutionDeadlineReturnsAStoreFailure(t *testing.T) {
	boom := errors.New("store unavailable")
	for _, entry := range guardEntries {
		t.Run(entry.name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(-time.Minute))
			rig.state.failListSuspended(boom)
			var err error
			switch entry.name {
			case "CommitTaskTimeout":
				err = rig.eng.CommitTaskTimeout(context.Background(), rig.lease, nil)
			default:
				err = rig.eng.CommitTaskResult(context.Background(), rig.lease, TaskResult{
					Error: types.NewPermanentError(types.NodeTimeoutErrorCode, "node execution exceeded its deadline"),
				})
			}
			if !errors.Is(err, boom) {
				t.Fatalf("error = %v, want it to wrap %v", err, boom)
			}
			if got := rig.snapshot(t).Status; got != types.ExecutionStatusTimeout {
				t.Fatalf("execution status = %q, want %q: the status write precedes the cascade", got, types.ExecutionStatusTimeout)
			}
		})
	}
}

// TestNodeTimeoutPastTheExecutionDeadlineYieldsToACancel: a cancel that got to
// the execution first owns it. The expiry reports canceling, writes nothing,
// and the commit is answered as handled rather than falling through to on_error.
func TestNodeTimeoutPastTheExecutionDeadlineYieldsToACancel(t *testing.T) {
	for _, entry := range guardEntries {
		t.Run(entry.name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(-time.Minute))
			rig.state.fakeState.mu.Lock()
			rig.state.fakeState.executions[rig.id].Status = types.ExecutionStatusCanceling
			rig.state.fakeState.mu.Unlock()

			entry.commit(t, rig)

			if got := rig.snapshot(t).Status; got != types.ExecutionStatusCanceling {
				t.Fatalf("execution status = %q, want %q kept", got, types.ExecutionStatusCanceling)
			}
			if got := taskNames(rig.queue.Drain()); len(got) != 0 {
				t.Fatalf("downstream enqueued %v, want nothing", got)
			}
			if got := rig.state.nodeCommitCount(); got != 0 {
				t.Fatalf("node commits reaching the store = %d, want 0: the cancel owns the execution", got)
			}
			if got := rig.hooks.completions(); len(got) != 0 {
				t.Fatalf("OnExecutionComplete statuses = %v, want none", got)
			}
		})
	}
}
