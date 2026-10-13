package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// subWorkflowLinkState — fakeState plus a real link capability
// ---------------------------------------------------------------------------

// subWorkflowLinkState decorates fakeState with the SubWorkflowLinkStore
// methods, implemented for real rather than stubbed: the dispatch tests need a
// store that actually parks the node and records the link, or the handler under
// test would be asserting against its own double. The store-level semantics
// (the fence, the preserving transition, the reclaim/retry survival) are pinned
// once, against both real backends, in backend/internal/statestoretest; what
// lives here is only enough of the contract for the engine path to run.
type subWorkflowLinkState struct {
	*fakeState
	links map[string]SubWorkflowLink // key: execID+"/"+nodeName
}

var _ SubWorkflowLinkStore = (*subWorkflowLinkState)(nil)

func newSubWorkflowLinkState() *subWorkflowLinkState {
	return &subWorkflowLinkState{
		fakeState: newFakeState(),
		links:     make(map[string]SubWorkflowLink),
	}
}

// BeginSubWorkflowWait fences on the documented lease identity and claimable
// statuses, keeps the lease on the parked node, and stores the link. A refused
// park mutates nothing — not the node, not the link — so a test can tell
// "refused" from "parked under a different identity".
func (s *subWorkflowLinkState) BeginSubWorkflowWait(_ context.Context, lease *TaskLease, link SubWorkflowLink, _ []OutboxEntry) (bool, error) {
	if lease == nil || link.ChildExecutionID == "" || link.ParentNode == "" || link.ParentNode != lease.Task.NodeName {
		return false, ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	node := s.nodes[key]
	if node == nil || (node.Status != types.NodeStatusRunning && node.Status != types.NodeStatusCommitting) ||
		node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken ||
		node.Attempt != lease.Attempt || node.ActivationID != lease.Task.ActivationID {
		return false, nil
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	copy.ChildExecutionID = link.ChildExecutionID
	s.nodes[key] = &copy
	s.links[key] = link
	return true, nil
}

func (s *subWorkflowLinkState) GetSubWorkflowLink(_ context.Context, id types.ExecutionID, nodeName string) (SubWorkflowLink, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	link, ok := s.links[string(id)+"/"+nodeName]
	return link, ok, nil
}

// ---------------------------------------------------------------------------
// rig
// ---------------------------------------------------------------------------

// subWorkflowRig is one parent execution waiting to have its single
// xflow.subworkflow node dispatched: the engine, the store it owns, the queue
// its deliveries land in, and the task the scheduler would have enqueued.
type subWorkflowRig struct {
	eng      *Engine
	state    *subWorkflowLinkState
	queue    *fakeQueue
	parentID types.ExecutionID
	task     *Task
}

// newSubWorkflowRig seeds a running parent execution of g with the given
// submission params and assembles the engine that will dispatch its node. The
// task is built the way the scheduler builds it — from the graph's own index —
// so the handler is exercised on the identity it will really see.
func newSubWorkflowRig(t *testing.T, g *graph.Graph, params map[string]any,
	resolver graph.SubWorkflowResolver, opts ...Option) *subWorkflowRig {
	t.Helper()
	state := newSubWorkflowLinkState()
	parentID := types.ExecutionID("exec-parent")
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:      parentID,
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		Params:  params,
		TraceID: "trace-parent",
	}); err != nil {
		t.Fatalf("seed parent execution: %v", err)
	}
	queue := &fakeQueue{}
	all := append([]Option{}, opts...)
	if resolver != nil {
		all = append(all, WithSubWorkflowResolver(resolver))
	}
	eng := New(state, queue, all...)
	idx, ok := g.NodeIndex("sub")
	if !ok {
		t.Fatal(`the rig graph has no node named "sub"`)
	}
	return &subWorkflowRig{
		eng:      eng,
		state:    state,
		queue:    queue,
		parentID: parentID,
		task: &Task{
			ExecutionID:  parentID,
			NodeName:     "sub",
			NodeIdx:      idx,
			UnitIdx:      g.UnitIndexForNode(idx),
			Type:         TaskTypeSubWorkflow,
			ActivationID: 1,
		},
	}
}

// dispatch delivers one task and requires the engine to have claimed it: the
// handled=false case would mean a runner is about to be offered a task type no
// handler can run.
func (r *subWorkflowRig) dispatch(t *testing.T, task *Task) {
	t.Helper()
	handled, err := r.eng.handleSystemTask(context.Background(), task, true)
	if err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}
	if !handled {
		t.Fatal("the engine did not claim its own sub-workflow task")
	}
}

func (r *subWorkflowRig) node(t *testing.T) *NodeSnapshot {
	t.Helper()
	node, err := r.state.GetNode(context.Background(), r.parentID, "sub")
	if err != nil || node == nil {
		t.Fatalf("read the sub node: node=%v err=%v", node, err)
	}
	return node
}

func (r *subWorkflowRig) link(t *testing.T) SubWorkflowLink {
	t.Helper()
	link, ok, err := r.state.GetSubWorkflowLink(context.Background(), r.parentID, "sub")
	if err != nil || !ok {
		t.Fatalf("read the sub-workflow link: ok=%v err=%v", ok, err)
	}
	return link
}

func (r *subWorkflowRig) child(t *testing.T, id types.ExecutionID) *ExecutionSnapshot {
	t.Helper()
	child, err := r.state.GetExecution(context.Background(), id)
	if err != nil || child == nil {
		t.Fatalf("read child execution %q: child=%v err=%v", id, child, err)
	}
	return child
}

// revoke simulates the lease sweeper's reclaim: the parked (or claimed) node
// goes back to Pending with its lease cleared, and the sweeper re-enqueues the
// same intent.
func (r *subWorkflowRig) revoke(t *testing.T) {
	t.Helper()
	revoked, err := r.state.RevokeLease(context.Background(), r.parentID, "sub", r.node(t).LeaseToken)
	if err != nil || !revoked {
		t.Fatalf("RevokeLease: revoked=%v err=%v", revoked, err)
	}
}

// subWorkflowParentWithRetry compiles the resolved parent with retry settings
// on its node, so a test can tell a retriable refusal (retry scheduled) from a
// permanent one (failed outright).
func subWorkflowParentWithRetry(t *testing.T) *graph.Graph {
	t.Helper()
	def := subWorkflowParentDef()
	def.Nodes[0].Retry = &types.RetrySettings{MaxAttempts: 3, InitialInterval: 1}
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	return g
}

// flakySubWorkflowResolver fails every resolution with a retriable error, the
// shape of a registry outage.
type flakySubWorkflowResolver struct{ err error }

func (f flakySubWorkflowResolver) ResolveSubWorkflow(context.Context, graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return graph.SubWorkflowTarget{}, f.err
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestSubWorkflowChildIDIsStableAndCollisionFree pins the derivation the whole
// idempotence story rests on: one (parent, node, activation, generation) maps
// to one id, forever, and changing any component of the key changes the id.
func TestSubWorkflowChildIDIsStableAndCollisionFree(t *testing.T) {
	base := subWorkflowChildID("exec-parent", "sub", 1, 1)
	if base != subWorkflowChildID("exec-parent", "sub", 1, 1) {
		t.Fatal("the derived child id is not deterministic")
	}
	if !strings.HasPrefix(string(base), "exec-sw-") {
		t.Fatalf("child id %q does not carry the derived-id prefix", base)
	}
	variants := map[string]types.ExecutionID{
		"parent":     subWorkflowChildID("exec-other", "sub", 1, 1),
		"node":       subWorkflowChildID("exec-parent", "other", 1, 1),
		"activation": subWorkflowChildID("exec-parent", "sub", 2, 1),
		"generation": subWorkflowChildID("exec-parent", "sub", 1, 2),
	}
	for field, got := range variants {
		if got == base {
			t.Fatalf("changing the %s did not change the derived child id", field)
		}
	}
}

// TestSubWorkflowDispatchStartsChildAndParksParent is the happy path end to
// end: the node is claimed, the child execution is created and delivered its
// roots, and the node parks at waiting under the very lease the completion will
// commit with.
func TestSubWorkflowDispatchStartsChildAndParksParent(t *testing.T) {
	resolver := subWorkflowStubResolver(t)
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, resolver)

	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want waiting", node.Status)
	}
	link := rig.link(t)
	wantChild := subWorkflowChildID(rig.parentID, "sub", 1, 1)
	if link.ChildExecutionID != wantChild {
		t.Fatalf("link child = %q, want the derived %q", link.ChildExecutionID, wantChild)
	}
	if link.ParentNode != "sub" || link.Generation != 1 || link.Depth != 1 {
		t.Fatalf("link = %+v, want node %q generation 1 depth 1", link, "sub")
	}
	if link.LeaseID != node.LeaseID || link.LeaseToken != node.LeaseToken ||
		link.Attempt != node.Attempt || link.ActivationID != node.ActivationID {
		t.Fatalf("the link's lease identity %+v does not match the parked node %+v", link, node)
	}
	// Two clocks, two meanings. The lease is the reclaim/notification budget and
	// is capped at the engine's wait TTL; the wait's end is the node's own
	// bound, which for a node declaring no timeout is the resolved engine
	// default that bounds every node type.
	if want := node.LeaseIssuedAt.Add(node.LeaseTTL); !link.WaitDeadline.After(want) {
		t.Fatalf("wait deadline = %s, want the node bound past the lease deadline %s", link.WaitDeadline, want)
	}
	if want := node.LeaseIssuedAt.Add(DefaultNodeTimeout); !link.WaitDeadline.Equal(want) {
		t.Fatalf("wait deadline = %s, want the resolved node timeout %s", link.WaitDeadline, want)
	}
	if node.LeaseTTL != subWorkflowWaitTTL {
		t.Fatalf("wait lease TTL = %s, want the %s notification budget", node.LeaseTTL, subWorkflowWaitTTL)
	}
	// The parent side of the drill-down: the parked node names its child, both
	// on the record and through Inspect, which is how an operator finds the
	// child execution to inspect next.
	if node.ChildExecutionID != wantChild {
		t.Fatalf("parked node child = %q, want the derived %q", node.ChildExecutionID, wantChild)
	}
	detail, err := rig.eng.Inspect(context.Background(), rig.parentID, "sub")
	if err != nil {
		t.Fatalf("Inspect(parent): %v", err)
	}
	if len(detail.Nodes) != 1 || detail.Nodes[0].ChildExecutionID != wantChild {
		t.Fatalf("Inspected parent nodes = %+v, want sub carrying child %q", detail.Nodes, wantChild)
	}
	if detail.ParentNode != "" {
		t.Fatalf("Inspected parent shows ParentNode %q, want empty on a root execution", detail.ParentNode)
	}
	// The child side of the same drill-down, and the field that tells a
	// sub-workflow child (ParentNode) apart from a rerun (RerunFrom).
	childDetail, err := rig.eng.Inspect(context.Background(), wantChild, "start")
	if err != nil {
		t.Fatalf("Inspect(child): %v", err)
	}
	if childDetail.ParentID != rig.parentID || childDetail.ParentNode != "sub" || childDetail.RerunFrom != "" {
		t.Fatalf("Inspected child linkage = (%q, %q, %q), want (%q, %q, empty)",
			childDetail.ParentID, childDetail.ParentNode, childDetail.RerunFrom, rig.parentID, "sub")
	}

	child := rig.child(t, wantChild)
	if child.Status != types.ExecutionStatusRunning {
		t.Fatalf("child status = %s, want running", child.Status)
	}
	if child.ParentID != rig.parentID || child.ParentNode != "sub" || child.Depth != 1 {
		t.Fatalf("child linkage = (%q, %q, %d), want (%q, %q, 1)",
			child.ParentID, child.ParentNode, child.Depth, rig.parentID, "sub")
	}
	if child.Graph != resolver.target.Graph {
		t.Fatal("the child runs a different graph than the resolved target")
	}
	if child.TraceID != "trace-parent" {
		t.Fatalf("child trace = %q, want the parent's trace", child.TraceID)
	}
	if child.Params != nil {
		t.Fatalf("child params = %v, want nil for a node with no input map", child.Params)
	}

	tasks := rig.queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("delivered %d child tasks, want the single root", len(tasks))
	}
	if tasks[0].ExecutionID != wantChild || tasks[0].NodeName != "start" || tasks[0].Type != TaskTypeNodeExec {
		t.Fatalf("child root task = (%q, %q, %d), want (%q, %q, %d)",
			tasks[0].ExecutionID, tasks[0].NodeName, tasks[0].Type, wantChild, "start", TaskTypeNodeExec)
	}
}

// TestSubWorkflowRedeliveryLeavesTheParkedWaitAlone pins the duplicate
// delivery: the node is already waiting under a live lease, so the second
// delivery claims nothing, creates nothing and re-delivers nothing.
func TestSubWorkflowRedeliveryLeavesTheParkedWaitAlone(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	rig.queue.Drain()

	rig.dispatch(t, rig.task)

	if len(rig.state.executions) != 2 {
		t.Fatalf("a redelivery left %d executions, want the parent and its one child", len(rig.state.executions))
	}
	if second := rig.link(t); second != first {
		t.Fatalf("the redelivery rewrote the link: %+v -> %+v", first, second)
	}
	if tasks := rig.queue.Drain(); len(tasks) != 0 {
		t.Fatalf("the redelivery re-delivered %d child tasks", len(tasks))
	}
}

// TestSubWorkflowReclaimResumesTheSameChild is the crash-after-create window:
// the sweeper revoked the wait and re-enqueued the same intent, and the
// redispatch must find the child rather than create a second one, re-parking
// the node under the new claim.
func TestSubWorkflowReclaimResumesTheSameChild(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	rig.queue.Drain()

	rig.revoke(t)
	rig.dispatch(t, rig.task)

	second := rig.link(t)
	if second.ChildExecutionID != first.ChildExecutionID {
		t.Fatalf("the reclaim started a second child %q; the wait already owned %q",
			second.ChildExecutionID, first.ChildExecutionID)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("the reclaim re-parked under the revoked lease identity")
	}
	if second.Attempt != first.Attempt+1 {
		t.Fatalf("parked attempt = %d, want %d: a reclaim is the next claim of the same activation",
			second.Attempt, first.Attempt+1)
	}
	if len(rig.state.executions) != 2 {
		t.Fatalf("the reclaim left %d executions, want the parent and its one child", len(rig.state.executions))
	}
	if tasks := rig.queue.Drain(); len(tasks) != 0 {
		t.Fatalf("resuming a live child re-delivered %d of its tasks", len(tasks))
	}
}

// TestSubWorkflowRetryStartsAFreshChildGeneration pins the retry semantics: a
// retry carries the next generation, so it starts its own child instead of
// resuming the one the failed attempt left behind — which is what a reclaim,
// carrying the same generation, must keep doing.
func TestSubWorkflowRetryStartsAFreshChildGeneration(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	rig.queue.Drain()

	rig.revoke(t)
	retryTask := *rig.task
	retryTask.Generation = 2 // what scheduleRetry stamps on the retry intent
	rig.dispatch(t, &retryTask)

	second := rig.link(t)
	if second.Generation != 2 {
		t.Fatalf("parked generation = %d, want 2", second.Generation)
	}
	if second.ChildExecutionID == first.ChildExecutionID {
		t.Fatal("the retry resumed the previous generation's child")
	}
	if len(rig.state.executions) != 3 {
		t.Fatalf("executions = %d, want the parent and both generations' children", len(rig.state.executions))
	}
	if rig.child(t, first.ChildExecutionID).Status != types.ExecutionStatusRunning {
		t.Fatal("the previous generation's child was disturbed by the retry")
	}
	if tasks := rig.queue.Drain(); len(tasks) != 1 {
		t.Fatalf("the fresh generation delivered %d child tasks, want its one root", len(tasks))
	}
}

// TestSubWorkflowReclaimKeepsTheWaitGeneration is the reclaim-after-retry
// window. The retry moved the wait on to generation 2, then the sweeper
// reclaimed that wait. The redelivery carries the generation the lease
// persisted (ReclaimLease rebuilds the task from ExpiredLease.Generation), and
// the dispatch must resume generation 2's child: falling back to generation 1's
// id would re-park the node on the child the retry abandoned, and that child's
// conclusion would then decide the node.
func TestSubWorkflowReclaimKeepsTheWaitGeneration(t *testing.T) {
	g := subWorkflowParentWithRetry(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	rig.queue.Drain()

	rig.revoke(t)
	retryTask := *rig.task
	retryTask.Generation = 2 // what scheduleRetry stamps on the retry intent
	rig.dispatch(t, &retryTask)
	second := rig.link(t)
	rig.queue.Drain()

	// Exactly what the lease sweeper redelivers: the task ReclaimLease rebuilt
	// from the expired lease, generation included.
	rig.revoke(t)
	reclaimed := *rig.task
	reclaimed.Generation = 2
	rig.dispatch(t, &reclaimed)

	third := rig.link(t)
	if third.ChildExecutionID != second.ChildExecutionID {
		t.Fatalf("the reclaim re-parked on child %q, want generation 2's %q",
			third.ChildExecutionID, second.ChildExecutionID)
	}
	if third.ChildExecutionID == first.ChildExecutionID {
		t.Fatal("the reclaim fell back to the child the retry abandoned")
	}
	if third.Generation != 2 {
		t.Fatalf("reclaimed wait generation = %d, want 2 kept", third.Generation)
	}
	if len(rig.state.executions) != 3 {
		t.Fatalf("executions = %d, want the parent and both generations' children", len(rig.state.executions))
	}
	if tasks := rig.queue.Drain(); len(tasks) != 0 {
		t.Fatalf("resuming a live child re-delivered %d of its tasks", len(tasks))
	}
}

// TestSubWorkflowReclaimServesTheGenerationOnTheLease is the crash-window
// repro: generation 2's child exists, the process dies before the park is
// written, and the link the sweeper's redelivery reads still names generation
// 1's child. The link cannot tell the redelivery which generation is live — it
// is exactly the stale record the crash left behind — so the generation has to
// travel with the task. It is persisted on the lease, rebuilt by the sweeper,
// and served as-is: inferring from the link instead would re-derive generation
// 1's child id and commit the node from the terminal verdict of the child the
// retry already abandoned, leaving generation 2's child orphaned.
func TestSubWorkflowReclaimServesTheGenerationOnTheLease(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowParentWithRetry(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))

	// Generation 1: park on the first child, then conclude it failed so the
	// node's retry mints generation 2.
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	child1 := first.ChildExecutionID
	rig.queue.Drain()
	concludeChild(t, rig, child1, types.ExecutionStatusFailed, "boom", nil)
	notifyCompletion(t, rig, child1)

	// Generation 2: dispatch the retry's task, then consume the delayed intent
	// it was rebuilt from. The backoff is not this test's subject, and leaving
	// the intent pending would put a second task in the queue when the reclaim
	// below flushes the outbox — the redelivery is what this test reads.
	retryTask := *rig.task
	retryTask.Generation = 2 // what scheduleRetry stamps on the retry intent
	rig.dispatch(t, &retryTask)
	pending, err := rig.state.ListOutbox(ctx, rig.parentID, time.Now().UTC().Add(time.Minute), 16)
	if err != nil {
		t.Fatalf("ListOutbox (retry intent): %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("recorded %d retry intents, want 1", len(pending))
	}
	if err := rig.state.AckOutbox(ctx, rig.parentID, pending[0].ID); err != nil {
		t.Fatalf("AckOutbox (retry intent): %v", err)
	}
	second := rig.link(t)
	child2 := second.ChildExecutionID
	if child2 == child1 {
		t.Fatal("the retry did not start a new child")
	}
	rig.queue.Drain()

	// The crash window: generation 2's child exists, the park that would have
	// named it does not. The link the sweeper reads is generation 1's.
	key := string(rig.parentID) + "/sub"
	rig.state.links[key] = first

	// The sweeper: it finds the wait's expired lease and rebuilds the task.
	// Passing a `before` beyond the wait TTL makes the parked wait due now.
	expired, err := rig.state.ListExpiredLeases(ctx, time.Now().UTC().Add(subWorkflowWaitTTL+time.Minute))
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	var waitLease ExpiredLease
	for _, candidate := range expired {
		if candidate.ExecutionID == rig.parentID && candidate.NodeName == "sub" {
			waitLease = candidate
		}
	}
	if waitLease.LeaseToken == "" {
		t.Fatal("the sweeper did not see the parked wait's lease as expired")
	}
	if _, err := rig.eng.ReclaimLease(ctx, waitLease); err != nil {
		t.Fatalf("ReclaimLease: %v", err)
	}
	reclaimedTasks := rig.queue.Drain()
	if len(reclaimedTasks) != 1 {
		t.Fatalf("reclaim delivered %d tasks, want exactly the redelivery", len(reclaimedTasks))
	}
	if reclaimedTasks[0].Generation != 2 {
		t.Fatalf("reclaimed task generation = %d, want 2: the sweeper rebuilds the task "+
			"from the lease, so the lease is where the generation must survive",
			reclaimedTasks[0].Generation)
	}
	rig.dispatch(t, reclaimedTasks[0])

	// The redelivery must resume generation 2's child rather than conclude the
	// node from the generation 1 child the crash left named in the link.
	third := rig.link(t)
	if third.ChildExecutionID != child2 {
		t.Fatalf("the reclaim re-parked on child %q, want generation 2's %q",
			third.ChildExecutionID, child2)
	}
	if node := rig.node(t); node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want waiting: generation 2's child is still running", node.Status)
	}
	if child := rig.child(t, child2); child.Status != types.ExecutionStatusRunning {
		t.Fatalf("generation 2's child = %s, want still running: the redelivery must resume it, not abandon it", child.Status)
	}
	if len(rig.state.executions) != 3 {
		t.Fatalf("executions = %d, want the parent and both generations' children", len(rig.state.executions))
	}
}

// TestSubWorkflowWaitDeadlineSurvivesAReclaim pins the other half of the wait's
// bound: a re-park keeps the deadline the wait was parked with. The new lease
// starts its own clock, and adopting it would let a wait outlive its node
// timeout for as long as the sweeper keeps reclaiming.
func TestSubWorkflowWaitDeadlineSurvivesAReclaim(t *testing.T) {
	def := subWorkflowParentDef()
	def.Nodes[0].Timeout = time.Minute
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	rig.queue.Drain()

	// Shorten the parked wait's end, so a renewed deadline would be visibly
	// later than the kept one.
	key := string(rig.parentID) + "/sub"
	shortened := first
	shortened.WaitDeadline = time.Now().UTC().Add(30 * time.Second).Truncate(time.Millisecond)
	rig.state.links[key] = shortened

	rig.revoke(t)
	rig.dispatch(t, rig.task)

	second := rig.link(t)
	if !second.WaitDeadline.Equal(shortened.WaitDeadline) {
		t.Fatalf("wait deadline after a reclaim = %s, want the kept %s", second.WaitDeadline, shortened.WaitDeadline)
	}
	if node := rig.node(t); node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want waiting: the wait has not outlived its bound", node.Status)
	}
}

// TestSubWorkflowWaitTimeoutFailsTheNodeAndCancelsTheChild is the bound's other
// end: a child that outlives the node's timeout ends the wait. The engine owns
// this node type, so no runner is there to enforce the timeout — the reclaim
// redelivery, the one clock a parked node has, is where the expiry is
// observed, and the child it was waiting on must be stopped with it.
func TestSubWorkflowWaitTimeoutFailsTheNodeAndCancelsTheChild(t *testing.T) {
	def := subWorkflowParentDef()
	def.Nodes[0].Timeout = time.Minute
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)
	rig.queue.Drain()

	// Age the wait past its bound, then let the sweeper reclaim it: the
	// redelivery is what carries the news that the bound has passed.
	key := string(rig.parentID) + "/sub"
	expired := first
	expired.WaitDeadline = time.Now().UTC().Add(-time.Second)
	rig.state.links[key] = expired

	rig.revoke(t)
	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed after the wait's node timeout", node.Status)
	}
	if !strings.Contains(node.Error, "node timeout") {
		t.Fatalf("node error = %q, want the timeout reason", node.Error)
	}
	if child := rig.child(t, first.ChildExecutionID); child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child status = %s, want canceled: the wait that gave up must stop it", child.Status)
	}
}

// TestSubWorkflowStartWithoutResolverFailsTheNodePermanently: an engine with no
// resolver cannot run the node, so the dispatch must fail it through the normal
// pipeline — visibly, with the reason — rather than treat it as a no-op.
func TestSubWorkflowStartWithoutResolverFailsTheNodePermanently(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, nil)

	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed", node.Status)
	}
	if !strings.Contains(node.Error, "does not support sub-workflow nodes") {
		t.Fatalf("node error = %q, want the missing-resolver reason", node.Error)
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want the parent alone: nothing may outlive the refusal", len(rig.state.executions))
	}
	exec := rig.child(t, rig.parentID)
	if exec.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed", exec.Status)
	}
}

// TestSubWorkflowTransientStartFailureRetriesAtTheNextGeneration is the
// retryable refusal: a registry outage is retried, and the retry intent is the
// engine-owned task type carrying the next generation, so the retry can never
// re-enter the failed generation's child.
func TestSubWorkflowTransientStartFailureRetriesAtTheNextGeneration(t *testing.T) {
	g := subWorkflowParentWithRetry(t)
	rig := newSubWorkflowRig(t, g, nil, flakySubWorkflowResolver{err: errors.New("registry unavailable")})

	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusPending {
		t.Fatalf("node status = %s, want the retry's pending reset", node.Status)
	}
	// The retry is a durable delayed intent (the backoff has not elapsed), so
	// the assertion reads the outbox rather than the queue fakeQueue would only
	// see once the backoff is due.
	entries, err := rig.state.ListOutbox(context.Background(), rig.parentID, time.Now().UTC().Add(time.Minute), 16)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("recorded %d retry intents, want 1", len(entries))
	}
	if entries[0].Task.Type != TaskTypeSubWorkflow {
		t.Fatalf("retry task type = %d, want TaskTypeSubWorkflow", entries[0].Task.Type)
	}
	if entries[0].Task.Generation != 2 {
		t.Fatalf("retry generation = %d, want 2: the retry starts a fresh child", entries[0].Task.Generation)
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want the parent alone: the failure happened before any child existed", len(rig.state.executions))
	}
}

// TestSubWorkflowDepthOverflowFailsWithoutRetry pins the runtime bound and its
// classification: the child would be one level past the limit, which no retry
// can change, so even a node that declares retries fails outright.
func TestSubWorkflowDepthOverflowFailsWithoutRetry(t *testing.T) {
	g := subWorkflowParentWithRetry(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t), WithMaxSubWorkflowDepth(2))
	snap := rig.child(t, rig.parentID)
	snap.Depth = 2 // the parent already sits at the limit; its child would be the third level

	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed", node.Status)
	}
	if !strings.Contains(node.Error, "exceeds the limit") {
		t.Fatalf("node error = %q, want the depth bound's reason", node.Error)
	}
	if tasks := rig.queue.Drain(); len(tasks) != 0 {
		t.Fatalf("a permanent refusal scheduled %d retries", len(tasks))
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want the parent alone: no child may be created past the limit", len(rig.state.executions))
	}
}

// TestSubWorkflowHashMismatchIsRefused pins the pin itself: a resolver that
// serves a different revision than the compiled binding must be refused, not
// silently run — otherwise "pinned at registration" would decay into "whatever
// is current at dispatch".
func TestSubWorkflowHashMismatchIsRefused(t *testing.T) {
	g := resolvedSubWorkflowParent(t) // compiled against the stub's sha256:child
	stray := stubSubWorkflowResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:other",
		Graph:          subWorkflowChildGraph(t),
	}}
	rig := newSubWorkflowRig(t, g, nil, stray)

	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed", node.Status)
	}
	if !strings.Contains(node.Error, "pins") {
		t.Fatalf("node error = %q, want the binding-mismatch reason", node.Error)
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want the parent alone: a mismatched revision must not run", len(rig.state.executions))
	}
}

// TestSubWorkflowInputEvaluatesIntoChildParams pins the boundary contract of
// parameters.input: it is evaluated by the engine, against the parent node's
// own boundary environment ($input from the submission params), and the result
// is the child's submission params — recursively, preserving types.
func TestSubWorkflowInputEvaluatesIntoChildParams(t *testing.T) {
	def := subWorkflowParentDef()
	def.Nodes[0].Parameters["input"] = map[string]any{
		"order_id": "${{ $input.order_id }}",
		"greeting": "${{ 'hello ' + $input.customer }}",
		"amount":   "${{ 40 + 2 }}",
		"literal":  "unchanged",
		"nested":   map[string]any{"matches": "${{ $input.order_id == 'o-1' }}"},
	}
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, g, map[string]any{"order_id": "o-1", "customer": "ada"}, subWorkflowStubResolver(t))

	rig.dispatch(t, rig.task)

	child := rig.child(t, rig.link(t).ChildExecutionID)
	params := child.Params
	if params == nil {
		t.Fatal("the child was created without the evaluated input map")
	}
	if params["order_id"] != "o-1" {
		t.Fatalf("order_id = %#v, want the parent's submission param", params["order_id"])
	}
	if params["greeting"] != "hello ada" {
		t.Fatalf("greeting = %#v, want the interpolated template", params["greeting"])
	}
	switch n := params["amount"].(type) {
	case int:
		if n != 42 {
			t.Fatalf("amount = %v, want 42", n)
		}
	case float64:
		if n != 42 {
			t.Fatalf("amount = %v, want 42", n)
		}
	default:
		t.Fatalf("amount = %#v (%T), want the number 42", params["amount"], params["amount"])
	}
	if params["literal"] != "unchanged" {
		t.Fatalf("literal = %#v, want the untouched string", params["literal"])
	}
	nested, ok := params["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %#v (%T), want the evaluated map", params["nested"], params["nested"])
	}
	if nested["matches"] != true {
		t.Fatalf("nested.matches = %#v, want true", nested["matches"])
	}
}
