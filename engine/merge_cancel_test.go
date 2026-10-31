package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileMergeCancelGraph builds the smallest graph with real cancel work: two
// parallel branches join at a wait_any merge, the loser branch carries a node
// that can park (test.suspend), and the merge's successor must only run once
// the convergence happens. on_others defaults to cancel — the same default the
// Descriptor ships — so no parameter is set.
//
//	       start
//	     /       \
//	fast          slow (test.suspend)
//	     \       /
//	    merge (wait_any)
//	        |
//	       end
func compileMergeCancelGraph(t *testing.T) *graph.Graph {
	t.Helper()
	return compileMergeCancelGraphParams(t, map[string]any{"mode": "wait_any"})
}

// compileMergeCancelGraphParams compiles the same topology with the merge
// node's parameters supplied by the caller, so policy variants share it.
func compileMergeCancelGraphParams(t *testing.T, mergeParams map[string]any) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "merge-cancel",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "fast", Type: "test.echo"},
			{Name: "slow", Type: "test.suspend"},
			{Name: "merge", Type: "xflow.merge", Parameters: mergeParams},
			{Name: "end", Type: "test.echo"},
		},
		Connections: mergeCancelConnections(),
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func mergeCancelConnections() types.Connections {
	return types.Connections{
		"start": {"main": {Targets: []types.Connection{
			{Node: "fast", Input: "main"},
			{Node: "slow", Input: "main"},
		}}},
		"fast":  {"main": {Targets: []types.Connection{{Node: "merge", Input: "main"}}}},
		"slow":  {"main": {Targets: []types.Connection{{Node: "merge", Input: "main"}}}},
		"merge": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
	}
}

// recordingMergeCancelObserver records what the scan reports as unresolved.
type recordingMergeCancelObserver struct {
	mu    sync.Mutex
	nodes []string
}

func (o *recordingMergeCancelObserver) OnMergeCancelIncomplete(_ context.Context, node string, count int) {
	if count <= 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.nodes = append(o.nodes, node)
}

func (o *recordingMergeCancelObserver) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.nodes...)
}

// mergeCapableState is the fake plus the merge-cancel marker. *fakeState alone
// deliberately does not declare it (it keeps the pre-feature behaviour), so the
// capable path needs this explicit opt-in; embedding the pointer promotes every
// other method and the folding marker unchanged.
type mergeCapableState struct {
	*fakeState
}

func (s *mergeCapableState) CommitsMergeCancel() bool { return true }

// recordingLogger records Warn calls and ignores the rest; it exists only for
// the degrade path, which must warn exactly once.
type recordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *recordingLogger) Debug(string, ...any)  {}
func (l *recordingLogger) Debugf(string, ...any) {}
func (l *recordingLogger) Info(string, ...any)   {}
func (l *recordingLogger) Infof(string, ...any)  {}
func (l *recordingLogger) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *recordingLogger) Warnf(string, ...any)  {}
func (l *recordingLogger) Error(string, ...any)  {}
func (l *recordingLogger) Errorf(string, ...any) {}
func (l *recordingLogger) Panic(string, ...any)  {}
func (l *recordingLogger) Panicf(string, ...any) {}

func (l *recordingLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.warns)
}

// startMergeCancelGraph submits the fixture and parks slow on its signal
// BEFORE fast commits — the ordering matters: the first active arrival is what
// fires a wait_any merge, and slow parked first is what makes the race real.
// The state after this helper: slow Suspended, fast success, and the merge
// scheduled (its exec task is on the queue, delivered by fast's own flush) but
// NOT executed — the intent rides the merge's terminal commit, so the caller
// settles the merge to fire the cancellation.
func startMergeCancelGraph(t *testing.T, ctx context.Context, eng *Engine, state *fakeState, queue *fakeQueue, g *graph.Graph) types.ExecutionID {
	t.Helper()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v, want nil", err)
	}
	state.InitInDegrees(id, g)
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("root delivery = %+v, want exactly one task for start", tasks)
	}
	executeTask(t, eng, tasks[0])

	// start's folded commit schedules fast and slow.
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after start error = %v", err)
	}
	remaining := queue.Drain()
	if len(remaining) != 2 {
		t.Fatalf("after start: %d tasks (%v), want fast and slow", len(remaining), taskNames(remaining))
	}
	// Park slow FIRST. If fast ran first, its active arrival would fire the
	// merge while slow is still unstarted — a different scenario.
	for _, task := range remaining {
		if task.NodeName == "slow" {
			executeTask(t, eng, task)
		}
	}
	// Now fast: the merge fires on its active arrival.
	for _, task := range remaining {
		if task.NodeName == "fast" {
			executeTask(t, eng, task)
		}
	}
	suspended, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	if len(suspended) != 1 || suspended[0] != "slow" {
		t.Fatalf("suspended nodes = %v, want [slow]", suspended)
	}
	return id
}

// TestMergeCancel_ConvergesAfterFiring drives the whole convergence end to end
// over the engine path: slow parks, fast fires the merge, the merge's success
// commit carries the mergecancel intent, the scan cancels the parked slow, and
// the execution converges with slow canceled and the merge's successor run.
// The observer must report nothing: every closure node was settled.
func TestMergeCancel_ConvergesAfterFiring(t *testing.T) {
	ctx := context.Background()
	obs := &recordingMergeCancelObserver{}
	state := &mergeCapableState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":    &echoHandler{},
		"xflow.merge":  &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(t, state, queue, reg)
	eng.mergeCancelObserver = obs
	g := compileMergeCancelGraph(t)

	id := startMergeCancelGraph(t, ctx, eng, state.fakeState, queue, g)

	// Execute the merge: its terminal commit carries the mergecancel intent,
	// and the commit's own flush consumes it. settleAll then drives the merge's
	// successor to completion.
	settleAll(t, ctx, eng, queue, id)

	slow, err := state.GetNode(ctx, id, "slow")
	if err != nil || slow == nil {
		t.Fatalf("GetNode(slow) = (%v, %v)", slow, err)
	}
	if slow.Status != types.NodeStatusCanceled {
		t.Fatalf("slow status = %q, want canceled: the scan must retire the parked branch", slow.Status)
	}
	if slow.Error != mergeCancelReasonPrefix+"merge" {
		t.Fatalf("slow error = %q, want %q", slow.Error, mergeCancelReasonPrefix+"merge")
	}

	// slow is canceled, and its suspended waiter is retired.
	suspended, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	if len(suspended) != 0 {
		t.Fatalf("suspended nodes after cancel = %v, want none", suspended)
	}
	if got := obs.snapshot(); len(got) != 0 {
		t.Fatalf("incomplete = %v, want none: every closure node was settled", got)
	}

	merge, err := state.GetNode(ctx, id, "merge")
	if err != nil || merge == nil || merge.Status != types.NodeStatusSuccess {
		t.Fatalf("merge status = (%v, %v), want success", merge, err)
	}
	end, err := state.GetNode(ctx, id, "end")
	if err != nil || end == nil || end.Status != types.NodeStatusSuccess {
		t.Fatalf("end status = (%v, %v), want success: the merge's successor must run", end, err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution = %q, want success after convergence", snap.Status)
	}
}

// TestMergeCancel_DetachLeavesBranchParked is the on_others: detach variant of
// the same fixture: the merge still fires, but it commits no merge-cancel
// intent, so the parked branch keeps waiting and the execution never converges
// on its own.
func TestMergeCancel_DetachLeavesBranchParked(t *testing.T) {
	ctx := context.Background()
	obs := &recordingMergeCancelObserver{}
	state := &mergeCapableState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":    &echoHandler{},
		"xflow.merge":  &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(t, state, queue, reg)
	eng.mergeCancelObserver = obs
	g := compileMergeCancelGraphParams(t, map[string]any{"mode": "wait_any", "on_others": "detach"})

	id := startMergeCancelGraph(t, ctx, eng, state.fakeState, queue, g)

	slow, err := state.GetNode(ctx, id, "slow")
	if err != nil || slow == nil {
		t.Fatalf("GetNode(slow) = (%v, %v)", slow, err)
	}
	if slow.Status != types.NodeStatusSuspended {
		t.Fatalf("slow status = %q, want suspended: detach must not cancel it", slow.Status)
	}
	if got := obs.snapshot(); len(got) != 0 {
		t.Fatalf("incomplete = %v, want none: detach is not residue", got)
	}

	// Settle the merge and its successor. The parked branch still holds a
	// non-terminal row, so the execution must stay running.
	settleAll(t, ctx, eng, queue, id)
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("execution = %q, want running: detach leaves the parked branch", snap.Status)
	}
	slow, err = state.GetNode(ctx, id, "slow")
	if err != nil || slow == nil || slow.Status != types.NodeStatusSuspended {
		t.Fatalf("slow after settle = (%v, %v), want still suspended", slow, err)
	}
}

// TestMergeCancel_ClosureResidueIsReportedNotAttempted drives one synthetic
// intent straight through handleSystemTask over a closure whose nodes are in
// the states the design excludes: a terminal row, an unscheduled node, and a
// Waiting row. Only the Waiting row is residue — reported, never written; the
// unscheduled node is refused by the fence and no row is fabricated.
func TestMergeCancel_ClosureResidueIsReportedNotAttempted(t *testing.T) {
	ctx := context.Background()
	obs := &recordingMergeCancelObserver{}
	state := &mergeCapableState{fakeState: newFakeState()}
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	eng.mergeCancelObserver = obs
	g := compileMergeCancelGraph(t)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// A terminal ancestor: fast already succeeded.
	fastIdx := mustNodeIdx(t, g, "fast")
	if err := state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id, Name: "fast", NodeIdx: fastIdx,
		UnitIdx: g.UnitIndexForNode(fastIdx),
		Status:  types.NodeStatusSuccess, ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(fast) error = %v", err)
	}
	// A Waiting ancestor: slow is parked on a sub-workflow or expansion.
	slowIdx := mustNodeIdx(t, g, "slow")
	if err := state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id, Name: "slow", NodeIdx: slowIdx,
		UnitIdx: g.UnitIndexForNode(slowIdx),
		Status:  types.NodeStatusWaiting, ActivationID: 2,
	}); err != nil {
		t.Fatalf("UpsertNode(slow) error = %v", err)
	}

	mergeIdx := mustNodeIdx(t, g, "merge")
	handled, err := eng.handleSystemTask(ctx, &Task{
		ExecutionID: id, NodeName: "merge", NodeIdx: mergeIdx,
		UnitIdx: g.UnitIndexForNode(mergeIdx),
		Type:    TaskTypeMergeCancel, ActivationID: 3,
	}, false)
	if err != nil || !handled {
		t.Fatalf("handleSystemTask(merge-cancel) = (%v, %v), want handled without error", handled, err)
	}

	// start is in the closure but was never scheduled: the fence refuses, and
	// nothing may be written for it (裁定④: unscheduled roots are not canceled).
	if row, err := state.GetNode(ctx, id, "start"); err != nil || row != nil {
		t.Fatalf("GetNode(start) = (%v, %v), want no row: the unscheduled root must not be canceled", row, err)
	}
	if row, _ := state.GetNode(ctx, id, "fast"); row == nil || row.Status != types.NodeStatusSuccess {
		t.Fatalf("fast = %+v, want untouched success", row)
	}
	// slow is the one residue: reported, not written.
	if row, err := state.GetNode(ctx, id, "slow"); err != nil || row == nil || row.Status != types.NodeStatusWaiting {
		t.Fatalf("slow = (%v, %v), want untouched waiting", row, err)
	}
	if got := obs.snapshot(); len(got) != 1 || got[0] != "slow" {
		t.Fatalf("incomplete = %v, want exactly [slow]", got)
	}
}

// TestMergeCancel_CapabilityMissingDegradesToDetach: a store that folds
// arrivals but does not declare CommitsMergeCancel keeps the pre-feature
// behaviour — the merge fires, writes no cancel intent, warns once, and the
// parked branch stays suspended.
func TestMergeCancel_CapabilityMissingDegradesToDetach(t *testing.T) {
	ctx := context.Background()
	log := &recordingLogger{}
	state := newFakeState() // deliberately no CommitsMergeCancel marker
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":    &echoHandler{},
		"xflow.merge":  &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(t, state, queue, reg)
	eng.logger = log
	g := compileMergeCancelGraph(t)

	id := startMergeCancelGraph(t, ctx, eng, state, queue, g)

	// Execute the merge, which is where the degrade decision is made.
	settleAll(t, ctx, eng, queue, id)

	if got := log.warnCount(); got != 1 {
		t.Fatalf("degrade warnings = %d, want exactly one per engine", got)
	}
	if !eng.mergeCancelWarned.Load() {
		t.Fatal("mergeCancelWarned = false after the degrade path")
	}
	slow, err := state.GetNode(ctx, id, "slow")
	if err != nil || slow == nil || slow.Status != types.NodeStatusSuspended {
		t.Fatalf("slow = (%v, %v), want suspended: without the capability the merge detaches", slow, err)
	}
}

// TestMergeCancel_HandleSystemTaskRefusesBadIndex: the intent's NodeIdx is
// validated, so a corrupted intent fails loudly instead of being acked against
// nothing.
func TestMergeCancel_HandleSystemTaskRefusesBadIndex(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	g := compileMergeCancelGraph(t)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	handled, err := eng.handleSystemTask(ctx, &Task{
		ExecutionID: id, NodeName: "merge", NodeIdx: g.NodeCount(),
		UnitIdx: -1, Type: TaskTypeMergeCancel, ActivationID: 1,
	}, false)
	if !handled {
		t.Fatal("handled = false, want true: a bad index is a classified failure")
	}
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("err = %v, want an out-of-range error", err)
	}
}

// mergeCancelReadErrState makes one node's GetNode fail, standing in for a
// transient store error mid-scan.
type mergeCancelReadErrState struct {
	*mergeCapableState
	errNode string
}

func (s *mergeCancelReadErrState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	if name == s.errNode {
		return nil, errors.New("injected store read failure")
	}
	return s.mergeCapableState.GetNode(ctx, id, name)
}

// TestMergeCancel_TransientReadErrorIsReturned: a store error mid-scan must
// leave the intent unacknowledged so the outbox redelivers it. The error comes
// back with handled=true; a silent nil would ack the intent and strand every
// branch after the failing node.
func TestMergeCancel_TransientReadErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	obs := &recordingMergeCancelObserver{}
	state := &mergeCancelReadErrState{
		mergeCapableState: &mergeCapableState{fakeState: newFakeState()},
		errNode:           "slow",
	}
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	eng.mergeCancelObserver = obs
	g := compileMergeCancelGraph(t)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	mergeIdx := mustNodeIdx(t, g, "merge")
	handled, err := eng.handleSystemTask(ctx, &Task{
		ExecutionID: id, NodeName: "merge", NodeIdx: mergeIdx,
		UnitIdx: g.UnitIndexForNode(mergeIdx),
		Type:    TaskTypeMergeCancel, ActivationID: 1,
	}, false)
	if !handled {
		t.Fatal("handled = false, want true: the intent was understood but undeliverable")
	}
	if err == nil || !strings.Contains(err.Error(), "injected store read failure") {
		t.Fatalf("err = %v, want the store read failure returned for outbox retry", err)
	}
	if got := obs.snapshot(); len(got) != 0 {
		t.Fatalf("incomplete = %v, want none: a read error is not a settled node", got)
	}
}

// settleAll flushes and executes delivered tasks until the queue stays empty
// and the outbox holds nothing ready. Terminates because the fixture's only
// suspending node never resumes once canceled.
func settleAll(t *testing.T, ctx context.Context, eng *Engine, queue *fakeQueue, id types.ExecutionID) {
	t.Helper()
	for i := 0; i < 16; i++ {
		if err := eng.FlushOutbox(ctx, id); err != nil {
			t.Fatalf("FlushOutbox() error = %v", err)
		}
		tasks := queue.Drain()
		if len(tasks) == 0 {
			return
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
	t.Fatal("settleAll did not settle within 16 rounds; the fixture is looping")
}

func mustNodeIdx(t *testing.T, g *graph.Graph, name string) int {
	t.Helper()
	idx, ok := g.NodeIndex(name)
	if !ok {
		t.Fatalf("graph has no %q node", name)
	}
	return idx
}
