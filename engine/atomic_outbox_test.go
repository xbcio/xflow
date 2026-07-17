package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

var errOutboxQueueUnavailable = errors.New("task queue unavailable")

type toggleOutboxQueue struct {
	fakeQueue
	err error
}

func (q *toggleOutboxQueue) Enqueue(ctx context.Context, task *Task) error {
	if q.err != nil {
		return q.err
	}
	return q.fakeQueue.Enqueue(ctx, task)
}

func (q *toggleOutboxQueue) EnqueueDelayed(ctx context.Context, task *Task, _ time.Duration) error {
	return q.Enqueue(ctx, task)
}

type ackFailureState struct {
	*fakeState
	ackFailures int
}

func (s *ackFailureState) AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error {
	if s.ackFailures > 0 {
		s.ackFailures--
		return errors.New("outbox acknowledgement lost")
	}
	return s.fakeState.AckOutbox(ctx, id, entryID)
}

func compileAtomicOutboxGraph(t *testing.T, retry *types.RetrySettings) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "atomic-outbox",
		Settings: &types.WorkflowSettings{Retry: retry},
		Nodes:    []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func listAtomicOutbox(t *testing.T, state *fakeState, id types.ExecutionID, before time.Time) []OutboxEntry {
	t.Helper()
	entries, err := state.ListOutbox(context.Background(), id, before, 16)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	return entries
}

func makeFakeOutboxReady(state *fakeState, id types.ExecutionID) {
	state.mu.Lock()
	defer state.mu.Unlock()
	for entryID, entry := range state.atomicOutbox[id] {
		entry.AvailableAt = time.Time{}
		state.atomicOutbox[id][entryID] = entry
	}
}

// runFakeTasksWithOutbox advances a fake backend's durable clock explicitly.
// Production dispatchers wait until AvailableAt; unit tests use this helper to
// model that passage without sleeping and without weakening the outbox's
// delayed-delivery contract.
func runFakeTasksWithOutbox(t *testing.T, eng *Engine, queue *fakeQueue, state *fakeState, id types.ExecutionID, maxSteps int) {
	t.Helper()
	for step := 0; step < maxSteps; step++ {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			if entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Hour)); len(entries) == 0 {
				return
			}
			makeFakeOutboxReady(state, id)
			if err := eng.FlushOutbox(context.Background(), id); err != nil {
				t.Fatalf("FlushOutbox() error = %v", err)
			}
			tasks = queue.Drain()
			if len(tasks) == 0 {
				return
			}
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
	t.Fatalf("workflow did not settle after %d queue/outbox steps", maxSteps)
}

func TestEngineSubmitKeepsInitialOutboxWhenQueueIsUnavailable(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{err: errOutboxQueueUnavailable}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), map[string]any{"request": "r-1"})
	if err != nil {
		t.Fatalf("Submit() error = %v, want durable success", err)
	}
	if id == "" {
		t.Fatal("Submit() returned an empty execution ID")
	}

	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second))
	if len(entries) != 1 || entries[0].Task.NodeName != "start" {
		t.Fatalf("initial outbox entries = %+v, want one start task", entries)
	}
	if entries[0].ID != initialOutboxID(id, "start", 0) {
		t.Fatalf("initial outbox ID = %q, want %q", entries[0].ID, initialOutboxID(id, "start", 0))
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after recovery error = %v", err)
	}
	queued := queue.Drain()
	if len(queued) != 1 || queued[0].NodeName != "start" {
		t.Fatalf("recovered queue = %v, want one start task", taskNames(queued))
	}
	if entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second)); len(entries) != 0 {
		t.Fatalf("outbox after acknowledgement = %+v, want empty", entries)
	}
}

func TestEngineRetryOutboxSurvivesQueueOutage(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)
	g := compileAtomicOutboxGraph(t, &types.RetrySettings{MaxAttempts: 2, InitialInterval: 1})

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{Error: errors.New("transient")}); err != nil {
		t.Fatalf("CommitTaskResult() error = %v", err)
	}

	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Hour))
	if len(entries) != 1 || entries[0].ID != retryOutboxID(id, "start", 0, lease.Attempt) {
		t.Fatalf("retry outbox entries = %+v, want retry task for attempt %d", entries, lease.Attempt)
	}
	if entries[0].AvailableAt.IsZero() {
		t.Fatal("retry outbox must retain its delayed availability")
	}

	makeFakeOutboxReady(state, id)
	queue.err = errOutboxQueueUnavailable
	if err := eng.FlushOutbox(ctx, id); !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("FlushOutbox() error = %v, want queue outage", err)
	}
	if entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second)); len(entries) != 1 {
		t.Fatalf("retry entry lost during queue outage: %+v", entries)
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after recovery error = %v", err)
	}
	queued := queue.Drain()
	if len(queued) != 1 || queued[0].NodeName != "start" || queued[0].Type != TaskTypeNodeExec {
		t.Fatalf("recovered retry task = %+v", queued)
	}
	node, err := state.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.Status != types.NodeStatusPending {
		t.Fatalf("node after durable retry = %+v, want pending", node)
	}
}

func TestEngineReleaseTaskLeaseOutboxPreservesResumeTask(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)
	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	task := queue.Drain()[0]
	task.Type = TaskTypeNodeResume
	task.ActivationID = 7
	task.AutoDepth = 3
	task.Payload = &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval",
		Data:      map[string]any{"approved": true},
	}
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	queue.err = errOutboxQueueUnavailable
	released, err := eng.ReleaseTaskLease(ctx, lease)
	if !released || !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("ReleaseTaskLease() released=%v err=%v, want true/queue outage", released, err)
	}
	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second))
	if len(entries) != 1 || entries[0].ID != requeueOutboxID(id, "start", 7, lease.LeaseID) {
		t.Fatalf("release outbox entries = %+v", entries)
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after release recovery error = %v", err)
	}
	queued := queue.Drain()
	if len(queued) != 1 {
		t.Fatalf("requeued task count = %d, want 1", len(queued))
	}
	got := queued[0]
	if got.Type != TaskTypeNodeResume || got.ActivationID != 7 || got.AutoDepth != 3 || got.Payload == nil || got.Payload.Name != "approval" || got.Payload.Data["approved"] != true {
		t.Fatalf("requeued task = %+v, want original resume metadata and payload", got)
	}
}

func TestEngineReclaimLeaseOutboxSurvivesQueueOutage(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue, WithDefaultLeaseTTL(time.Minute))
	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	task := queue.Drain()[0]
	if _, err := eng.BuildTaskLease(ctx, task); err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	expired, err := state.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil || len(expired) != 1 {
		t.Fatalf("ListExpiredLeases() entries=%+v err=%v, want one", expired, err)
	}

	queue.err = errOutboxQueueUnavailable
	reclaimed, err := eng.ReclaimLease(ctx, expired[0])
	if !reclaimed || !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("ReclaimLease() reclaimed=%v err=%v, want true/queue outage", reclaimed, err)
	}
	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second))
	if len(entries) != 1 || entries[0].ID != requeueOutboxID(id, "start", 0, expired[0].LeaseID) {
		t.Fatalf("reclaim outbox entries = %+v", entries)
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after reclaim recovery error = %v", err)
	}
	queued := queue.Drain()
	if len(queued) != 1 || queued[0].Type != TaskTypeNodeExec || queued[0].NodeName != "start" {
		t.Fatalf("reclaimed queue = %+v", queued)
	}
}

func TestEngineFlushOutboxRetriesAfterAcknowledgementLoss(t *testing.T) {
	ctx := context.Background()
	state := &ackFailureState{fakeState: newFakeState(), ackFailures: 1}
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v, want durable success despite lost ack", err)
	}
	if first := queue.Drain(); len(first) != 1 || first[0].NodeName != "start" {
		t.Fatalf("first delivery = %+v, want start task", first)
	}
	if entries := listAtomicOutbox(t, state.fakeState, id, time.Now().Add(time.Second)); len(entries) != 1 {
		t.Fatalf("outbox after lost acknowledgement = %+v, want retained entry", entries)
	}

	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() retry error = %v", err)
	}
	if duplicate := queue.Drain(); len(duplicate) != 1 || duplicate[0].NodeName != "start" {
		t.Fatalf("duplicate delivery = %+v, want start task", duplicate)
	}
	if entries := listAtomicOutbox(t, state.fakeState, id, time.Now().Add(time.Second)); len(entries) != 0 {
		t.Fatalf("outbox after retry acknowledgement = %+v, want empty", entries)
	}
}

func TestEngineSuspendPreSignalOutboxSurvivesQueueOutage(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)
	g := compileAtomicOutboxGraph(t, nil)

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	root := queue.Drain()
	if len(root) != 1 {
		t.Fatalf("root tasks = %+v, want one", root)
	}
	state.mu.Lock()
	state.signals[string(id)+"/approval"] = map[string]any{"by": "early"}
	state.mu.Unlock()
	lease, err := eng.BuildTaskLease(ctx, root[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	queue.err = errOutboxQueueUnavailable
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{Suspend: &types.SuspendSpec{
		Mode:    types.ModeSignal,
		Signals: []string{"approval"},
	}}); !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("CommitTaskResult() error = %v, want queue outage", err)
	}
	node, err := state.GetNode(ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusSuspended {
		t.Fatalf("node after failed continuation delivery = %+v err=%v, want suspended", node, err)
	}
	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second))
	if len(entries) != 1 || entries[0].Task.Type != TaskTypeNodeResume || entries[0].Task.Payload == nil || entries[0].Task.Payload.Name != "approval" {
		t.Fatalf("suspend continuation outbox = %+v, want durable approval resume", entries)
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after queue recovery error = %v", err)
	}
	resumes := queue.Drain()
	if len(resumes) != 1 || resumes[0].Type != TaskTypeNodeResume || resumes[0].Payload == nil || resumes[0].Payload.Data["by"] != "early" {
		t.Fatalf("recovered resume = %+v, want pre-delivered signal", resumes)
	}
}

func TestEngineLoopSplitJSONBatchesUseDurableSystemTasks(t *testing.T) {
	ctx := context.Background()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "durable-loop-split",
		Options: &types.WorkflowOptions{ExperimentalExpand: true},
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.loop"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"loop": {"main": []types.Connection{{Node: "done", Input: "main"}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	root := queue.Drain()
	if len(root) != 1 {
		t.Fatalf("root tasks = %+v, want one", root)
	}
	lease, err := eng.BuildTaskLease(ctx, root[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	queue.err = errOutboxQueueUnavailable
	result := TaskResult{Output: &types.Output{Data: map[string]any{
		"_loop":   true,
		"batches": []any{[]any{"from-json"}},
	}}}
	if err := eng.CommitTaskResult(ctx, lease, result); !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("CommitTaskResult() error = %v, want queue outage", err)
	}
	node, err := state.GetNode(ctx, id, "loop")
	if err != nil || node == nil || node.Status != types.NodeStatusWaiting {
		t.Fatalf("loop node after failed batch handoff = %+v err=%v, want waiting", node, err)
	}
	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second))
	if len(entries) != 1 || entries[0].Task.Type != TaskTypeNodeBatch {
		t.Fatalf("batch outbox = %+v, want one durable batch task", entries)
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after queue recovery error = %v", err)
	}
	batches := queue.Drain()
	if len(batches) != 1 || batches[0].Type != TaskTypeNodeBatch {
		t.Fatalf("delivered batches = %+v, want one internal batch task", batches)
	}
	if _, err := eng.BuildTaskLease(ctx, batches[0]); !errors.Is(err, ErrSystemTaskHandled) {
		t.Fatalf("BuildTaskLease(batch) error = %v, want ErrSystemTaskHandled", err)
	}
	downstream := queue.Drain()
	if len(downstream) != 1 || downstream[0].NodeName != "done" || downstream[0].Type != TaskTypeNodeExec {
		t.Fatalf("downstream after internal batch = %+v, want done execute task", downstream)
	}
}

type outboxObserverState struct {
	*fakeState
	deadLettered int
}

func newOutboxObserverState() *outboxObserverState {
	return &outboxObserverState{fakeState: newFakeState()}
}

func (s *outboxObserverState) RecordOutboxFailure(_ context.Context, id types.ExecutionID, entryID string, maxAttempts int) (OutboxDeliveryFailure, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := s.atomicOutbox[id]
	entry, ok := entries[entryID]
	if !ok {
		return OutboxDeliveryFailure{}, nil
	}
	if maxAttempts <= 0 {
		maxAttempts = DefaultOutboxMaxDeliveryAttempts
	}
	entry.Attempts++
	result := OutboxDeliveryFailure{Attempts: entry.Attempts}
	if entry.Attempts >= maxAttempts {
		delete(entries, entryID)
		if len(entries) == 0 {
			delete(s.atomicOutbox, id)
		}
		s.deadLettered++
		result.DeadLettered = true
		return result, nil
	}
	entries[entryID] = entry
	return result, nil
}

func (s *outboxObserverState) OutboxMetrics(_ context.Context) (OutboxMetricsSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending := 0
	for _, entries := range s.atomicOutbox {
		pending += len(entries)
	}
	return OutboxMetricsSnapshot{Pending: pending, DeadLettered: s.deadLettered}, nil
}

type outboxObserverRecorder struct {
	retries     []int
	deadLetters int
	replayed    []DeadLetterReplayOutcome
	pending     []OutboxMetricsSnapshot
	operations  []string
}

func (r *outboxObserverRecorder) OnOutboxRetry(_ context.Context, attempt int) {
	r.retries = append(r.retries, attempt)
}

func (r *outboxObserverRecorder) OnOutboxDeadLetter(context.Context) {
	r.deadLetters++
}

func (r *outboxObserverRecorder) OnOutboxReplayed(_ context.Context, outcome DeadLetterReplayOutcome) {
	r.replayed = append(r.replayed, outcome)
}

func (r *outboxObserverRecorder) OnOutboxPending(_ context.Context, pending, deadLettered int, oldestAge time.Duration) {
	r.pending = append(r.pending, OutboxMetricsSnapshot{
		Pending:         pending,
		DeadLettered:    deadLettered,
		OldestPendingAt: time.Now().Add(-oldestAge),
	})
}

func (r *outboxObserverRecorder) OnOutboxError(_ context.Context, operation string, _ error) {
	r.operations = append(r.operations, operation)
}

func TestEngineFlushOutboxNotifiesRetryDeadLetterAndBacklogObservers(t *testing.T) {
	ctx := context.Background()
	state := newOutboxObserverState()
	queue := &toggleOutboxQueue{err: errOutboxQueueUnavailable}
	observer := &outboxObserverRecorder{}
	eng := New(state, queue,
		WithOutboxObserver(observer),
		WithOutboxMaxDeliveryAttempts(2),
	)
	id := types.ExecutionID("outbox-observer")
	entry := OutboxEntry{
		ID:   "root/outbox-observer/start/0",
		Task: Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, []OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	if err := eng.FlushOutbox(ctx, id); !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("first FlushOutbox() error = %v, want queue outage", err)
	}
	if got := observer.retries; len(got) != 1 || got[0] != 1 {
		t.Fatalf("retry observations = %v, want [1]", got)
	}
	if observer.deadLetters != 0 {
		t.Fatalf("dead-letter observations after first failure = %d, want 0", observer.deadLetters)
	}

	if err := eng.FlushOutbox(ctx, id); !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("second FlushOutbox() error = %v, want queue outage", err)
	}
	if observer.deadLetters != 1 {
		t.Fatalf("dead-letter observations = %d, want 1", observer.deadLetters)
	}
	if len(observer.operations) != 2 {
		t.Fatalf("outbox error observations = %v, want two delivery errors", observer.operations)
	}

	NewOutboxDispatcher(eng, time.Hour).drain(ctx)
	if len(observer.pending) != 1 {
		t.Fatalf("backlog observations = %d, want 1", len(observer.pending))
	}
	got := observer.pending[0]
	if got.Pending != 0 || got.DeadLettered != 1 {
		t.Fatalf("backlog observation = %+v, want pending=0 dead_lettered=1", got)
	}
}
