package engine

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// #1: retryBackoff int64 overflow — large multiplier must not produce negative
// ---------------------------------------------------------------------------

func TestRetryBackoff_HugeMultiplierClampsToCapNotNegative(t *testing.T) {
	settings := &types.RetrySettings{
		InitialInterval: 1000, // 1s
		Multiplier:      1e18, // absurdly large
	}
	d := retryBackoff(5, settings, "exec-overflow", "node")
	if d <= 0 {
		t.Fatalf("retryBackoff with huge multiplier = %v, want positive (capped)", d)
	}
	if d > retryBackoffCap+retryBackoffCap/5 {
		t.Fatalf("retryBackoff with huge multiplier = %v, exceeds cap+jitter", d)
	}
}

func TestRetryBackoff_MaxFloat64DoesNotOverflow(t *testing.T) {
	settings := &types.RetrySettings{
		InitialInterval: 1000,
		Multiplier:      math.MaxFloat64,
	}
	d := retryBackoff(1, settings, "exec-maxfloat", "node")
	if d <= 0 {
		t.Fatalf("retryBackoff with MaxFloat64 multiplier = %v, want positive", d)
	}
	if d > retryBackoffCap+retryBackoffCap/5 {
		t.Fatalf("retryBackoff with MaxFloat64 exceeds cap+jitter: %v", d)
	}
}

// ---------------------------------------------------------------------------
// #2: FlushOutbox head-of-line blocking — poison entry must not starve others
// ---------------------------------------------------------------------------

// poisonQueue fails Enqueue for specific node names.
type poisonQueue struct {
	fakeQueue
	poison map[string]bool
}

func (q *poisonQueue) Enqueue(_ context.Context, t *Task) error {
	if q.poison[t.NodeName] {
		return errors.New("poison: enqueue failed")
	}
	q.fakeQueue.mu.Lock()
	defer q.fakeQueue.mu.Unlock()
	q.fakeQueue.tasks = append(q.fakeQueue.tasks, t)
	return nil
}

func (q *poisonQueue) EnqueueDelayed(ctx context.Context, t *Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

func TestFlushOutbox_PoisonEntryDoesNotBlockHealthyEntries(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	pq := &poisonQueue{poison: map[string]bool{"poison_node": true}}
	eng := New(state, pq)

	id := types.ExecutionID("exec-hol-test")
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "hol-test",
		Nodes: []types.NodeDef{
			{Name: "poison_node", Type: "test.echo"},
			{Name: "healthy_node", Type: "test.echo"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}

	// Manually insert two outbox entries: poison first, healthy second.
	// Entry IDs are sorted alphabetically by ListOutbox, so name them
	// to guarantee ordering: "a_poison" < "b_healthy".
	state.mu.Lock()
	state.atomicOutbox[id] = map[string]OutboxEntry{
		"a_poison": {
			ID:   "a_poison",
			Task: Task{ExecutionID: id, NodeName: "poison_node", NodeIdx: 0, Type: TaskTypeNodeExec},
		},
		"b_healthy": {
			ID:   "b_healthy",
			Task: Task{ExecutionID: id, NodeName: "healthy_node", NodeIdx: 1, Type: TaskTypeNodeExec},
		},
	}
	state.mu.Unlock()

	// FlushOutbox should process healthy_node despite poison_node failure.
	err = eng.FlushOutbox(ctx, id)
	// The overall FlushOutbox returns an error (from the poison entry).
	if err == nil {
		t.Fatal("FlushOutbox() expected error from poison entry, got nil")
	}

	// The healthy entry should have been delivered (Acked from outbox).
	tasks := pq.fakeQueue.Drain()
	found := false
	for _, task := range tasks {
		if task.NodeName == "healthy_node" {
			found = true
		}
	}
	if !found {
		t.Fatal("healthy_node task was not delivered — head-of-line blocking persists")
	}

	// Poison entry should remain in the outbox (not Acked).
	remaining := listAtomicOutbox(t, state, id, time.Now().Add(time.Hour))
	if len(remaining) != 1 || remaining[0].ID != "a_poison" {
		t.Fatalf("expected only poison entry remaining, got %d entries", len(remaining))
	}
}

// ---------------------------------------------------------------------------
// #4: Legacy signal path AcquireResumeLock — !acquired returns error not nil
// ---------------------------------------------------------------------------

func TestDeliverSignalLegacy_ResumeLockContendedReturnsError(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "signal-lock-test",
		Nodes: []types.NodeDef{{Name: "wait", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("exec-signal-lock")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}
	eng.graphs[id] = g

	// Set up the node as suspended waiting for signal "go".
	state.mu.Lock()
	state.suspended[string(id)+"/wait"] = &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}}
	state.nodes[string(id)+"/wait"] = &NodeSnapshot{
		ExecutionID:  id,
		Name:         "wait",
		NodeIdx:      0,
		Status:       types.NodeStatusSuspended,
		ActivationID: 1,
	}
	// Pre-acquire the resume lock to simulate contention.
	state.resumed[string(id)+"/wait"] = true
	state.mu.Unlock()

	err = eng.DeliverSignal(ctx, id, "go", map[string]any{"v": 1})
	if err == nil {
		t.Fatal("DeliverSignal() expected error when resume lock contended, got nil")
	}
	if tasks := queue.Drain(); len(tasks) != 0 {
		t.Fatalf("expected no tasks enqueued (lock held by other), got %d", len(tasks))
	}
}

// ---------------------------------------------------------------------------
// #5: Legacy path RevokeLease+Enqueue failure — returns error (not silent)
// ---------------------------------------------------------------------------

func TestReclaimLease_EnqueueFailureReturnsError(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	failQueue := &toggleOutboxQueue{err: errors.New("queue down")}
	_ = New(state, failQueue) // unused; we need a non-atomic state for legacy path

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "reclaim-enqueue-fail",
		Nodes: []types.NodeDef{{Name: "step", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := types.ExecutionID("exec-reclaim-fail")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}

	// Place the node in Running with a known lease token.
	token := LeaseToken("tok-reclaim")
	state.mu.Lock()
	state.nodes[string(id)+"/step"] = &NodeSnapshot{
		ExecutionID:   id,
		Name:          "step",
		NodeIdx:       0,
		Status:        types.NodeStatusRunning,
		LeaseID:       "lease-1",
		LeaseToken:    token,
		LeaseIssuedAt: time.Now().Add(-time.Hour),
		LeaseTTL:      time.Minute,
		ActivationID:  1,
	}
	state.mu.Unlock()

	// ReclaimLease — RevokeLease succeeds but Enqueue fails.
	// fakeState does NOT implement AtomicStateStore... wait, it does.
	// We need to use a state that does NOT implement AtomicStateStore to hit
	// the legacy path. Let's use a wrapper.
	legacyState := &nonAtomicState{fakeState: state}
	eng2 := New(legacyState, failQueue)
	eng2.graphs[id] = g

	reclaimed, err := eng2.ReclaimLease(ctx, ExpiredLease{
		ExecutionID:  id,
		NodeName:     "step",
		NodeIdx:      0,
		LeaseID:      "lease-1",
		LeaseToken:   token,
		IssuedAt:     time.Now().Add(-time.Hour),
		TTL:          time.Minute,
		ActivationID: 1,
	})
	if err == nil {
		t.Fatal("ReclaimLease() expected error when Enqueue fails after RevokeLease, got nil")
	}
	if !reclaimed {
		t.Fatal("ReclaimLease() expected reclaimed=true (revoke succeeded)")
	}
	// Verify the node is now Pending (revoke succeeded).
	node, _ := legacyState.GetNode(ctx, id, "step")
	if node == nil || node.Status != types.NodeStatusPending {
		t.Fatalf("node status = %v, want Pending (revoke succeeded)", node)
	}
}

// nonAtomicState wraps fakeState but does NOT implement AtomicStateStore,
// forcing engine code to take the legacy (non-atomic) path.
type nonAtomicState struct {
	*fakeState
}

// Ensure nonAtomicState does NOT satisfy AtomicStateStore by not embedding
// the atomic methods. We re-expose only the StateStore interface methods.
func (s *nonAtomicState) CreateExecution(ctx context.Context, e *ExecutionSnapshot) error {
	return s.fakeState.CreateExecution(ctx, e)
}
func (s *nonAtomicState) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	return s.fakeState.UpdateExecutionStatus(ctx, id, status, errMsg)
}
func (s *nonAtomicState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	return s.fakeState.GetExecution(ctx, id)
}
func (s *nonAtomicState) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	return s.fakeState.LoadGraph(ctx, id)
}
func (s *nonAtomicState) UpsertNode(ctx context.Context, n *NodeSnapshot) error {
	return s.fakeState.UpsertNode(ctx, n)
}
func (s *nonAtomicState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	return s.fakeState.GetNode(ctx, id, name)
}
func (s *nonAtomicState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	return s.fakeState.AcquireTaskLease(ctx, lease)
}
func (s *nonAtomicState) ClaimTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	return s.fakeState.ClaimTaskLease(ctx, lease)
}
func (s *nonAtomicState) ListExpiredLeases(ctx context.Context, before time.Time) ([]ExpiredLease, error) {
	return s.fakeState.ListExpiredLeases(ctx, before)
}
func (s *nonAtomicState) RevokeLease(ctx context.Context, id types.ExecutionID, name string, token LeaseToken) (bool, error) {
	return s.fakeState.RevokeLease(ctx, id, name, token)
}
func (s *nonAtomicState) DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	return s.fakeState.DecrementInDegree(ctx, id, nodeIdx, portActive)
}
func (s *nonAtomicState) CheckCompletion(ctx context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
	return s.fakeState.CheckCompletion(ctx, id, totalNodes)
}
func (s *nonAtomicState) SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	return s.fakeState.SuspendOrConsume(ctx, id, name, spec)
}
func (s *nonAtomicState) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) (string, *types.SignalPayload, error) {
	return s.fakeState.DeliverSignal(ctx, id, name, data)
}
func (s *nonAtomicState) ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	return s.fakeState.ResuspendAtomic(ctx, id, nodeName, oldSignalName, newSignalName, spec)
}
func (s *nonAtomicState) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error) {
	return s.fakeState.RevokeSignal(ctx, id, signalName)
}
func (s *nonAtomicState) AcquireResumeLock(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
	return s.fakeState.AcquireResumeLock(ctx, id, name)
}
func (s *nonAtomicState) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
	return s.fakeState.ListSuspendedNodes(ctx, id)
}
func (s *nonAtomicState) CreateSubExecution(ctx context.Context, sub *SubExecution) error {
	return s.fakeState.CreateSubExecution(ctx, sub)
}
func (s *nonAtomicState) CompleteSubExecution(ctx context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	return s.fakeState.CompleteSubExecution(ctx, parentExecID, parentNode, childExecID, status, result)
}
func (s *nonAtomicState) GetSubExecutionResults(ctx context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
	return s.fakeState.GetSubExecutionResults(ctx, parentExecID, parentNode)
}
func (s *nonAtomicState) PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	return s.fakeState.PutOutput(ctx, id, name, data)
}
func (s *nonAtomicState) GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	return s.fakeState.GetOutput(ctx, id, name)
}
func (s *nonAtomicState) PublishExecutionEvent(ctx context.Context, event ExecutionEvent) error {
	return s.fakeState.PublishExecutionEvent(ctx, event)
}
func (s *nonAtomicState) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan ExecutionEvent, error) {
	return s.fakeState.WatchExecution(ctx, id)
}

// ---------------------------------------------------------------------------
// H1 (commit.go): a fatal error on a cyclic node must finalize the execution
// atomically via CyclicComplete (finalStatus=failed, finalError set) in the
// SAME fenced commit as the terminal node write — not via a separate,
// crash-exposed UpdateExecutionStatus that could leave the execution stuck
// Running behind a terminal Failed node.
// ---------------------------------------------------------------------------

func TestCyclicFatalNodeError_FinalizesExecutionAtomically(t *testing.T) {
	g := cyclicReviewGraph(t, "cyclic-fatal", 10)
	state := newFakeState()
	queue := &flakyQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{"round": 1})
	if err != nil {
		t.Fatal(err)
	}
	// Watch the finalization event so we can assert the fatal error is recorded.
	events, err := state.WatchExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	startLease, err := eng.BuildTaskLease(ctx, queue.drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 1}}}); err != nil {
		t.Fatal(err)
	}
	reviewTask := queue.drain()[0]
	reviewLease, err := eng.BuildTaskLease(ctx, reviewTask)
	if err != nil {
		t.Fatal(err)
	}

	// review returns a fatal (system) error. Default OnError="stop" → ExecFatal,
	// which the cyclic commit path folds into a single fenced CyclicComplete.
	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Error: errors.New("boom")}); err != nil {
		t.Fatalf("fatal commit error = %v", err)
	}

	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed (finalized atomically with the fatal node)", snap.Status)
	}
	node, err := state.GetNode(ctx, id, "review")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil || node.Status != types.NodeStatusFailed {
		t.Fatalf("review node = %+v, want status failed", node)
	}
	if got := drainFailedEventError(t, events); got != "boom" {
		t.Fatalf("finalization event error = %q, want %q", got, "boom")
	}
	if extra := queue.drain(); len(extra) != 0 {
		t.Fatalf("no downstream expected after fatal abort, got %v", taskNames(extra))
	}

	// Idempotent crash recovery: a redelivery of the same lease observes the
	// duplicate terminal with the execution already failed — never stuck Running.
	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Error: errors.New("boom")}); err != nil {
		t.Fatalf("replay commit error = %v", err)
	}
	snap2, _ := state.GetExecution(ctx, id)
	if snap2.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status after replay = %s, want failed", snap2.Status)
	}
}

// TestCyclicFatalNodeError_CancelAwareFinalization asserts the fenced fatal
// finalization honors a concurrent Cancel: the execution stays canceling (the
// cancel is not clobbered to failed) while the node write still lands terminal.
func TestCyclicFatalNodeError_CancelAwareFinalization(t *testing.T) {
	g := cyclicReviewGraph(t, "cyclic-fatal-cancel", 10)
	state := newFakeState()
	queue := &flakyQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{"round": 1})
	if err != nil {
		t.Fatal(err)
	}
	startLease, err := eng.BuildTaskLease(ctx, queue.drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 1}}}); err != nil {
		t.Fatal(err)
	}
	reviewLease, err := eng.BuildTaskLease(ctx, queue.drain()[0])
	if err != nil {
		t.Fatal(err)
	}

	// A concurrent Cancel moved the execution to canceling before the fatal
	// commit lands.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatal(err)
	}

	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Error: errors.New("boom")}); err != nil {
		t.Fatalf("fatal commit during cancel error = %v", err)
	}
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusCanceling {
		t.Fatalf("execution status = %s, want canceling preserved (cancel not clobbered by fatal finalization)", snap.Status)
	}
	node, _ := state.GetNode(ctx, id, "review")
	if node == nil || node.Status != types.NodeStatusFailed {
		t.Fatalf("review node = %+v, want failed (fenced node write still lands)", node)
	}
}

func drainFailedEventError(t *testing.T, events <-chan ExecutionEvent) string {
	t.Helper()
	for {
		select {
		case ev := <-events:
			if ev.Status == types.ExecutionStatusFailed {
				return ev.Error
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for failed execution event")
			return ""
		}
	}
}

// ---------------------------------------------------------------------------
// M4 (types.go): SubExecution JSON tags must be lowercase so the expansion Lua
// (completeExpandedSubExecutionLua) can read child.status / child.result.
// Result must NOT be omitempty so an empty-object batch result round-trips.
// ---------------------------------------------------------------------------

func TestSubExecution_JSONTagsAreLowercase(t *testing.T) {
	sub := SubExecution{
		ParentExecID: "parent-1",
		ParentNode:   "fanout",
		ChildExecID:  "child-1",
		BatchIndex:   3,
		Status:       types.ExecutionStatusSuccess,
		Result:       map[string]any{"k": "v"},
	}
	raw, err := json.Marshal(sub)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"parent_exec_id", "parent_node", "child_exec_id", "batch_index", "status", "result"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("marshaled SubExecution missing lowercase key %q; got %s", key, raw)
		}
	}
	// The pre-fix capitalized keys broke the expansion Lua's child.status /
	// child.result reads (status transition silently never fired).
	for _, bad := range []string{"Status", "Result", "ParentExecID", "BatchIndex", "ParentNode", "ChildExecID"} {
		if _, ok := m[bad]; ok {
			t.Fatalf("marshaled SubExecution has capitalized key %q; expansion Lua reads lowercase", bad)
		}
	}
	// Result must round-trip even when empty (NOT omitempty): {} stays present.
	rawEmpty, err := json.Marshal(SubExecution{Result: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	var me map[string]json.RawMessage
	if err := json.Unmarshal(rawEmpty, &me); err != nil {
		t.Fatal(err)
	}
	if _, ok := me["result"]; !ok {
		t.Fatalf("empty Result must marshal as a present key, got %s", rawEmpty)
	}
}
