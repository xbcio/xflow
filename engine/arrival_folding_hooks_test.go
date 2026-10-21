package engine

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileHookFoldGraph compiles fetch -> {process, report} so a commit of fetch
// has two downstream arrivals to fold. fetch is pinned or disabled according to
// the flags, which is what routes its TaskTypeNodeExec task into the
// corresponding hook instead of the lease path.
func compileHookFoldGraph(t *testing.T, pinned, disabled bool) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "hook-fold-probe",
		Nodes: []types.NodeDef{
			{Name: "fetch", Type: "test.echo", Disabled: disabled},
			{Name: "process", Type: "test.echo"},
			{Name: "report", Type: "test.echo"},
		},
		Connections: types.Connections{
			"fetch": {"main": {Targets: []types.Connection{
				{Node: "process", Input: "main"},
				{Node: "report", Input: "main"},
			}}},
		},
	}
	if pinned {
		def.Settings = &types.WorkflowSettings{PinDataMode: types.PinDataModeAlways}
		def.PinData = map[string]any{"fetch": map[string]any{"seed": 1.0}}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// commitRequestLog records every CommitNodeRequest the engine sends, which is
// the only place the scheduling carrier is visible: the arrival-folding path
// succeeds either way, so a test that only observed the downstream deliveries
// could not tell a folded commit from a fallback one.
type commitRequestLog struct {
	mu   sync.Mutex
	reqs []CommitNodeRequest
}

func (l *commitRequestLog) add(req CommitNodeRequest) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reqs = append(l.reqs, req)
}

func (l *commitRequestLog) snapshot() []CommitNodeRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]CommitNodeRequest(nil), l.reqs...)
}

// foldingCommitRecorder is a folding-capable store that records the request.
// Embedding *fakeState promotes its FoldsArrivalsIntoCommit marker, which is
// exactly the capability a backend declares to receive folded arrivals.
type foldingCommitRecorder struct {
	*fakeState
	log *commitRequestLog
}

var _ ArrivalFoldingCommitter = (*foldingCommitRecorder)(nil)

func (s *foldingCommitRecorder) CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.log.add(req)
	return s.fakeState.CommitNode(ctx, req)
}

// nonFoldingCommitRecorder is the same store shape without the capability
// marker: it embeds the interfaces rather than *fakeState so the promoted
// FoldsArrivalsIntoCommit does not sneak the capability back in.
type nonFoldingCommitRecorder struct {
	StateStore
	AtomicStateStore
	log *commitRequestLog
}

func (s *nonFoldingCommitRecorder) CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.log.add(req)
	return s.AtomicStateStore.CommitNode(ctx, req)
}

// leaseHookRoot flushes the initial intents and returns the delivery for node,
// leaving it unleased: the caller drives BuildTaskLease to enter the hook.
func leaseHookRoot(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue, id types.ExecutionID, node string) *Task {
	t.Helper()
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	var found *Task
	for _, task := range queue.Drain() {
		if task.NodeName == node {
			found = task
		}
	}
	if found == nil {
		t.Fatalf("no root delivery for %q", node)
	}
	return found
}

// assertFoldedHookCommit pins the carrier and the arrival payload a folding
// store must receive from either hook: the two destinations of fetch, each with
// one arrival on the active "main" port, and no advance intent.
func assertFoldedHookCommit(t *testing.T, g *graph.Graph, log *commitRequestLog) {
	t.Helper()
	reqs := log.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("CommitNode requests = %d, want exactly 1 (the hook's commit)", len(reqs))
	}
	req := reqs[0]
	if req.AdvanceTask != nil {
		t.Fatalf("commit carried an AdvanceTask beside the folded arrivals: " +
			"the two carriers are alternatives (CommitNodeRequest.Validate)")
	}
	if len(req.Arrivals) != 2 {
		t.Fatalf("folded arrivals = %+v, want the two destinations of fetch", req.Arrivals)
	}
	if req.Port != "main" {
		t.Fatalf("commit port = %q, want main (the folded arrivals' active port)", req.Port)
	}
	wantNames := []string{"process", "report"}
	for i, arrival := range req.Arrivals {
		if arrival.NodeName != wantNames[i] {
			t.Fatalf("arrivals[%d] destination = %q, want %q (sorted by node index)",
				i, arrival.NodeName, wantNames[i])
		}
		if arrival.ArrivalCount != 1 || arrival.ActiveCount != 1 {
			t.Fatalf("arrival for %q = %d total / %d active, want 1/1 on the main port",
				arrival.NodeName, arrival.ArrivalCount, arrival.ActiveCount)
		}
		if arrival.ExecTaskType != TaskTypeNodeExec {
			t.Fatalf("arrival for %q task type = %v, want node exec", arrival.NodeName, arrival.ExecTaskType)
		}
		idx, ok := g.NodeIndex(arrival.NodeName)
		if !ok || arrival.NodeIdx != idx || arrival.UnitIdx != g.UnitIndexForNode(idx) {
			t.Fatalf("arrival for %q addresses node/unit %d/%d, want %d/%d",
				arrival.NodeName, arrival.NodeIdx, arrival.UnitIdx, idx, g.UnitIndexForNode(idx))
		}
	}
	// The carriers are mutually exclusive and this request must be the folded
	// shape, so Validate must accept it as-is.
	if err := req.Validate(); err != nil {
		t.Fatalf("folded commit request is invalid: %v", err)
	}
}

// assertDownstreamDelivered proves the folded transition actually scheduled
// both destinations: a carrier that parses but schedules nothing would still
// satisfy the request-shape assertions.
func assertDownstreamDelivered(t *testing.T, queue *fakeQueue) {
	t.Helper()
	var names []string
	for _, task := range queue.Drain() {
		if task.Type == TaskTypeNodeExec {
			names = append(names, task.NodeName)
		}
	}
	want := []string{"process", "report"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("delivered downstream exec tasks = %v, want %v", names, want)
	}
}

func TestPinnedNodeFoldsDownstreamArrivals(t *testing.T) {
	ctx := context.Background()
	log := &commitRequestLog{}
	state := &foldingCommitRecorder{fakeState: newFakeState(), log: log}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g := compileHookFoldGraph(t, true, false)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if _, err := eng.BuildTaskLease(ctx, leaseHookRoot(t, ctx, eng, queue, id, "fetch")); err != ErrSystemTaskHandled {
		t.Fatalf("BuildTaskLease(pinned) err = %v, want ErrSystemTaskHandled", err)
	}

	assertFoldedHookCommit(t, g, log)
	assertDownstreamDelivered(t, queue)

	node, err := state.GetNode(ctx, id, "fetch")
	if err != nil || node == nil || node.Status != types.NodeStatusPinned || node.Port != "main" {
		t.Fatalf("fetch = %+v, %v; want pinned/main", node, err)
	}
}

func TestDisabledNodeFoldsDownstreamArrivals(t *testing.T) {
	ctx := context.Background()
	log := &commitRequestLog{}
	state := &foldingCommitRecorder{fakeState: newFakeState(), log: log}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g := compileHookFoldGraph(t, false, true)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if _, err := eng.BuildTaskLease(ctx, leaseHookRoot(t, ctx, eng, queue, id, "fetch")); err != ErrSystemTaskHandled {
		t.Fatalf("BuildTaskLease(disabled) err = %v, want ErrSystemTaskHandled", err)
	}

	assertFoldedHookCommit(t, g, log)
	assertDownstreamDelivered(t, queue)

	node, err := state.GetNode(ctx, id, "fetch")
	if err != nil || node == nil || node.Status != types.NodeStatusSkipped || node.Port != "main" {
		t.Fatalf("fetch = %+v, %v; want skipped/main", node, err)
	}
}

// assertFallbackHookCommit pins the pre-fold contract for a store without the
// folding marker: the commit carries the advance intent and no arrivals.
func assertFallbackHookCommit(t *testing.T, log *commitRequestLog) {
	t.Helper()
	reqs := log.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("CommitNode requests = %d, want exactly 1 (the hook's commit)", len(reqs))
	}
	req := reqs[0]
	if len(req.Arrivals) != 0 {
		t.Fatalf("non-folding store received %d folded arrivals, want 0: it never "+
			"declared FoldsArrivalsIntoCommit", len(req.Arrivals))
	}
	if req.AdvanceTask == nil {
		t.Fatal("non-folding store received no AdvanceTask: downstream scheduling would be lost")
	}
	if req.AdvanceTask.Type != TaskTypeNodeAdvance {
		t.Fatalf("advance carrier type = %v, want node advance", req.AdvanceTask.Type)
	}
	if req.AdvanceTask.Port == nil || *req.AdvanceTask.Port != "main" {
		t.Fatalf("advance carrier port = %v, want main", req.AdvanceTask.Port)
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("fallback commit request is invalid: %v", err)
	}
}

func TestPinnedNodeKeepsAdvanceTaskOnNonFoldingBackend(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	log := &commitRequestLog{}
	state := &nonFoldingCommitRecorder{StateStore: inner, AtomicStateStore: inner, log: log}
	if _, ok := any(state).(ArrivalFoldingCommitter); ok {
		t.Fatal("nonFoldingCommitRecorder satisfies ArrivalFoldingCommitter: the explicit " +
			"FoldsArrivalsIntoCommit marker is not what gates the capability")
	}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g := compileHookFoldGraph(t, true, false)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if _, err := eng.BuildTaskLease(ctx, leaseHookRoot(t, ctx, eng, queue, id, "fetch")); err != ErrSystemTaskHandled {
		t.Fatalf("BuildTaskLease(pinned) err = %v, want ErrSystemTaskHandled", err)
	}

	assertFallbackHookCommit(t, log)
	// The fallback must still schedule downstream through AdvanceNode; an
	// advance intent that is emitted but never consumed is the same loss as no
	// carrier at all.
	assertDownstreamDelivered(t, queue)
}

func TestDisabledNodeKeepsAdvanceTaskOnNonFoldingBackend(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	log := &commitRequestLog{}
	state := &nonFoldingCommitRecorder{StateStore: inner, AtomicStateStore: inner, log: log}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g := compileHookFoldGraph(t, false, true)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if _, err := eng.BuildTaskLease(ctx, leaseHookRoot(t, ctx, eng, queue, id, "fetch")); err != ErrSystemTaskHandled {
		t.Fatalf("BuildTaskLease(disabled) err = %v, want ErrSystemTaskHandled", err)
	}

	assertFallbackHookCommit(t, log)
	assertDownstreamDelivered(t, queue)
}
