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

// flakyQueue is a TaskQueue whose Enqueue can be toggled to fail, modelling a
// process crash / queue outage between the fenced terminal commit and the
// downstream task delivery. It records successfully delivered tasks so a test
// can assert what actually reached the queue.
type flakyQueue struct {
	mu    sync.Mutex
	tasks []*Task
	fail  bool
}

func (q *flakyQueue) setFail(b bool) {
	q.mu.Lock()
	q.fail = b
	q.mu.Unlock()
}

func (q *flakyQueue) Enqueue(_ context.Context, t *Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.fail {
		return errors.New("enqueue crashed")
	}
	q.tasks = append(q.tasks, t)
	return nil
}

func (q *flakyQueue) EnqueueDelayed(ctx context.Context, t *Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

func (q *flakyQueue) drain() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := q.tasks
	q.tasks = nil
	return tasks
}

func cyclicReviewGraph(t *testing.T, name string, maxDepth int) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    name,
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: maxDepth},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": {Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestCyclic_DownstreamSurvivesEnqueueCrashAndRecovers is the core regression
// for #7: a crash (enqueue failure) between the cyclic terminal commit and the
// downstream delivery must NOT lose the downstream task. The intent is
// persisted durably in the outbox during the same fenced commit and is
// recovered when the runner redelivers the (now duplicate-terminal) lease.
func TestCyclic_DownstreamSurvivesEnqueueCrashAndRecovers(t *testing.T) {
	g := cyclicReviewGraph(t, "cyclic-crash", 10)
	state := newFakeState()
	queue := &flakyQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{"round": 1})
	if err != nil {
		t.Fatal(err)
	}

	// Drive start → review so review is the node whose reject-branch loops back.
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

	// Simulate a crash during downstream delivery: the terminal commit + outbox
	// write succeed atomically, but the follow-up enqueue fails.
	queue.setFail(true)
	err = eng.CommitTaskResult(ctx, reviewLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 2}, Port: "reject"}})
	if err == nil {
		t.Fatal("expected transient error when downstream delivery fails")
	}
	if delivered := queue.drain(); len(delivered) != 0 {
		t.Fatalf("no downstream task should be delivered after crash, got %v", taskNames(delivered))
	}

	// The downstream intent must be durably persisted, not lost.
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 durable downstream intent after crash, got %d: %+v", len(entries), entries)
	}
	if entries[0].Task.NodeName != "start" || entries[0].Task.ActivationID != reviewTask.ActivationID+1 {
		t.Fatalf("durable intent = %s@%d, want start@%d", entries[0].Task.NodeName, entries[0].Task.ActivationID, reviewTask.ActivationID+1)
	}

	// Recovery: the queue heals and the runner redelivers the same lease. The
	// commit is now a duplicate-terminal, and the persisted intent is flushed.
	queue.setFail(false)
	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 2}, Port: "reject"}}); err != nil {
		t.Fatalf("recovery commit error = %v", err)
	}
	recovered := queue.drain()
	if len(recovered) != 1 || recovered[0].NodeName != "start" {
		t.Fatalf("expected recovered start task, got %v", taskNames(recovered))
	}
	if recovered[0].ActivationID != reviewTask.ActivationID+1 {
		t.Fatalf("recovered activation = %d, want %d", recovered[0].ActivationID, reviewTask.ActivationID+1)
	}
}

// TestCyclic_DuplicateCommitDoesNotDoubleEnqueueDownstream verifies the
// at-least-once commit path does not double-schedule the cyclic downstream: a
// redelivered (duplicate-terminal) commit re-flushes the outbox but the intent
// was already delivered and acked, so exactly one downstream task is produced.
func TestCyclic_DuplicateCommitDoesNotDoubleEnqueueDownstream(t *testing.T) {
	g := cyclicReviewGraph(t, "cyclic-dup", 10)
	state := newFakeState()
	queue := &flakyQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, map[string]any{"round": 1}); err != nil {
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

	// First commit delivers the reject-branch downstream.
	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 2}, Port: "reject"}}); err != nil {
		t.Fatal(err)
	}
	first := queue.drain()
	if len(first) != 1 || first[0].NodeName != "start" {
		t.Fatalf("expected one downstream start task, got %v", taskNames(first))
	}

	// Redelivered duplicate commit must not enqueue a second copy.
	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 2}, Port: "reject"}}); err != nil {
		t.Fatal(err)
	}
	if dup := queue.drain(); len(dup) != 0 {
		t.Fatalf("duplicate commit must not re-enqueue downstream, got %v", taskNames(dup))
	}
}

// TestCyclic_TerminalBranchCompletesExecutionAtomically verifies that a cyclic
// node whose active branch has no downstream finalizes the execution status in
// the same fenced commit (no separate, crash-exposed status write).
func TestCyclic_TerminalBranchCompletesExecutionAtomically(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cyclic-complete",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "finish", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "finish", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &flakyQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	startLease, err := eng.BuildTaskLease(ctx, queue.drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}); err != nil {
		t.Fatal(err)
	}
	finishLease, err := eng.BuildTaskLease(ctx, queue.drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	// finish has no outgoing edge → completing it must finalize the execution.
	if err := eng.CommitTaskResult(ctx, finishLease, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}); err != nil {
		t.Fatal(err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", snap.Status)
	}
	if extra := queue.drain(); len(extra) != 0 {
		t.Fatalf("no downstream expected after terminal branch, got %v", taskNames(extra))
	}
}
