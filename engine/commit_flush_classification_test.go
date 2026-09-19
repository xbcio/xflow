package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// A commit whose state transition applied but whose downstream outbox could not
// be delivered must keep the classification the transition earned. Reporting
// CommitOutcomeTransientError instead told service/control that no commit
// happened, and that caller releases the runner's leased capacity only for the
// four outcomes ReleasesLeasedCapacity returns true for — so an applied commit
// whose flush failed held its assignment's capacity until a retry happened to
// flush successfully, or forever once the runner stopped retrying.
//
// These tests pin the classification, not the delivery: the error still
// propagates so the runner re-reports, and the re-report lands on the node
// state fence and is idempotent.

func compileFlushClassificationGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "flush-classification",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// submitFlushClassificationWorkflow admits the two-node graph and hands back a
// lease for "start" with the queue healthy again, so the caller can break it
// deliberately right before the commit under test.
func submitFlushClassificationWorkflow(t *testing.T, ctx context.Context, eng *Engine, queue *toggleOutboxQueue) *TaskLease {
	t.Helper()
	id, err := eng.Submit(ctx, compileFlushClassificationGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	_ = id

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("initial queue = %v, want one start task", taskNames(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	return lease
}

func TestEngineCommitKeepsAcceptedWhenOutboxFlushFails(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	lease := submitFlushClassificationWorkflow(t, ctx, eng, queue)

	// The advance task the commit produces is what the flush below delivers, so
	// the queue must be healthy up to this point and broken only for the flush.
	queue.err = errOutboxQueueUnavailable
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})

	if err == nil {
		t.Fatal("CommitTaskResultWithOutcome() error = nil, want the flush failure")
	}
	if !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v, want the queue outage", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q — the transition applied despite the flush failure", outcome, CommitOutcomeAccepted)
	}
	if !outcome.ReleasesLeasedCapacity() {
		t.Fatalf("ReleasesLeasedCapacity(%q) = false; the caller would hold the runner's capacity forever", outcome)
	}
}

func TestEngineCommitKeepsDuplicateTerminalWhenOutboxFlushFails(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	lease := submitFlushClassificationWorkflow(t, ctx, eng, queue)

	// First commit applies the terminal write; its flush fails, so the advance
	// entry stays in the outbox.
	queue.err = errOutboxQueueUnavailable
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	}); !errors.Is(err, errOutboxQueueUnavailable) {
		t.Fatalf("first commit error = %v, want the queue outage", err)
	}

	// The runner re-reports the same lease. The node is already terminal, so
	// this is the duplicate arm — which also has pending outbox work to replay,
	// and also fails to deliver it. DuplicateTerminal is one of the four
	// outcomes that must release capacity, so degrading it loses the release
	// just as the accepted arm did.
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if err == nil {
		t.Fatal("duplicate commit error = nil, want the replay failure")
	}
	if outcome != CommitOutcomeDuplicateTerminal {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeDuplicateTerminal)
	}
	if !outcome.ReleasesLeasedCapacity() {
		t.Fatalf("ReleasesLeasedCapacity(%q) = false", outcome)
	}
}

func TestEngineCommitKeepsAcceptedWhenFlushFailsOnCyclicPath(t *testing.T) {
	ctx := context.Background()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "flush-classification-cyclic",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("initial queue = %v, want one task", taskNames(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	// AllowCycles routes the commit through commitLegacyNodeWithClassification,
	// a separate producer with the same degradation.
	queue.err = errOutboxQueueUnavailable
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if err == nil {
		t.Fatal("cyclic commit error = nil, want the flush failure")
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeAccepted)
	}
	if !outcome.ReleasesLeasedCapacity() {
		t.Fatalf("ReleasesLeasedCapacity(%q) = false", outcome)
	}
}

// groupFlushFailureState adds the one thing the shared group fixture lacks: a
// durable downstream intent for the flush to fail on. The fixture's CommitGroup
// finalizes the execution in the same transition (ExecutionDone), and commitGroup
// returns before its flush in that case — correctly, since a finished execution
// has no downstream work. Holding the execution open is what makes the group
// commit's delivery step reachable.
type groupFlushFailureState struct {
	*fakeGroupLeaseState
}

func (s *groupFlushFailureState) CommitGroup(ctx context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	res, err := s.fakeGroupLeaseState.CommitGroup(ctx, req)
	if err != nil || res.Outcome != CommitOutcomeAccepted {
		return res, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putAtomicOutboxLocked(req.ExecutionID, "group-downstream", Task{
		ExecutionID: req.ExecutionID,
		NodeName:    "D",
		Type:        TaskTypeNodeExec,
	}, time.Now().UTC())
	res.ExecutionDone = false
	return res, nil
}

func TestEngineCommitGroupKeepsAcceptedWhenOutboxFlushFails(t *testing.T) {
	ctx := context.Background()
	eng, g, execID := setupGroupLeaseTest(t)
	state := &groupFlushFailureState{fakeGroupLeaseState: eng.state.(*fakeGroupLeaseState)}
	eng.state = state
	eng.queue = &toggleOutboxQueue{err: errOutboxQueueUnavailable}

	gm := g.Groups()[0]
	task := &Task{
		ExecutionID:  execID,
		NodeName:     gm.Name,
		NodeIdx:      gm.EntryIdx,
		UnitIdx:      gm.UnitIdx,
		Type:         TaskTypeGroupExec,
		ActivationID: 0,
	}
	lease, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease() error = %v", err)
	}

	bo := gm.BoundaryOutputs[0]
	src := g.NodeAt(bo.Src.NodeIdx)
	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome: GroupOutcomeSuccess,
		Exits: []GroupExitResult{{
			NodeName: src.Name,
			Port:     bo.Src.Port,
			Data:     map[string]any{"result": "ok"},
		}},
	})

	if err == nil {
		t.Fatal("CommitGroupResult() error = nil, want the flush failure")
	}
	if !errors.Is(err, errGroupCommitFlushPending) {
		t.Fatalf("CommitGroupResult() error = %v, want errGroupCommitFlushPending", err)
	}
	// Without the sentinel this returned ("", err): an unclassified outcome
	// releases no capacity at all.
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeAccepted)
	}
	if !outcome.ReleasesLeasedCapacity() {
		t.Fatalf("ReleasesLeasedCapacity(%q) = false", outcome)
	}
}

// The suspend path is a third producer with the same degradation, in
// commitSuspendedTaskResult. Its durable claim already committed the suspension
// by the time the continuation flush runs, so the same rule applies.
func TestEngineCommitKeepsAcceptedWhenSuspendFlushFails(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileFlushClassificationGraph(t), nil)
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
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{Suspend: &types.SuspendSpec{
		Mode:    types.ModeSignal,
		Signals: []string{"approval"},
	}})
	if err == nil {
		t.Fatal("suspend commit error = nil, want the continuation flush failure")
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeAccepted)
	}
	if !outcome.ReleasesLeasedCapacity() {
		t.Fatalf("ReleasesLeasedCapacity(%q) = false", outcome)
	}
}

// The flush failure must not become invisible to the retry machinery: the
// caller still gets a non-nil error, and the pending outbox entry survives for
// the outbox dispatcher to deliver.
func TestEngineCommitKeepsPendingOutboxAfterFlushFailure(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	lease := submitFlushClassificationWorkflow(t, ctx, eng, queue)
	id := lease.Task.ExecutionID

	queue.err = errOutboxQueueUnavailable
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	}); err == nil {
		t.Fatal("commit error = nil, want the flush failure")
	}

	entries := listAtomicOutbox(t, state, id, time.Now().Add(time.Second))
	if len(entries) != 1 {
		t.Fatalf("outbox after failed flush = %+v, want the retained advance entry", entries)
	}

	queue.err = nil
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after recovery error = %v", err)
	}
	queued := queue.Drain()
	if len(queued) != 1 || queued[0].NodeName != "done" {
		t.Fatalf("recovered queue = %v, want the downstream done task", taskNames(queued))
	}
}
