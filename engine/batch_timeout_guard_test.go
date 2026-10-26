package engine

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// batchReasonState is fakeState with the status write both real backends have:
// the execution-level reason is recorded for any status (fakeState keeps it for
// failed only) and a terminal status is never overwritten. ExpireExecution reads
// the reason back to tell its own timeout from another one, so without this it
// would never see its write.
type batchReasonState struct {
	*fakeState
}

func (s *batchReasonState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	exec, ok := f.executions[id]
	if !ok || types.IsTerminalExecutionStatus(exec.Status) {
		return nil
	}
	exec.Status = status
	if errMsg != "" {
		exec.Error = errMsg
	}
	f.releaseConcurrencySlotLocked(id, status)
	f.publishLocked(ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

// batchGuardRig is a map node with three batches and a settings.timeout, driven
// through the path a remote runner takes: BuildSubgraphLease for each batch,
// then CommitSubgraphResult with whatever the runner reported. The stored
// execution deadline is pinned to an exact instant before the batches are
// leased, because the engine memoizes its first read of it.
type batchGuardRig struct {
	eng     *Engine
	state   *fakeState
	queue   *fakeQueue
	id      types.ExecutionID
	batches []*Task
}

func newBatchGuardRig(t *testing.T, onError string, bounded bool, execDeadline time.Time) *batchGuardRig {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "batch-timeout-guard",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map", OnError: onError, Parameters: mapBodyParamsForTest()},
			{Name: "next", Type: "test.echo"},
			{Name: "onerr", Type: "test.echo"},
		},
		Connections: types.Connections{
			"loop": {
				"main":  {Targets: []types.Connection{{Node: "next", Input: "main"}}},
				"error": {Targets: []types.Connection{{Node: "onerr", Input: "main"}}},
			},
		},
	}
	if bounded {
		def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &loopHandler{},
		"test.echo": &echoHandler{},
	}}
	eng := newTestEngine(t, &batchReasonState{fakeState: state}, queue, reg)
	id, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if bounded {
		state.mu.Lock()
		state.executions[id].Deadline = execDeadline
		state.mu.Unlock()
	}
	return &batchGuardRig{eng: eng, state: state, queue: queue, id: id, batches: drainBatchTasks(t, eng, queue)}
}

// report commits one batch as a remote runner would and returns what the map
// node left behind. result.Output.Data is the runner's batch data, error the
// lease's error.
func (r *batchGuardRig) report(t *testing.T, i int, result TaskResult) CommitOutcome {
	t.Helper()
	lease, _, err := r.eng.BuildSubgraphLease(context.Background(), r.batches[i])
	if err != nil {
		t.Fatalf("BuildSubgraphLease(batch %d) error = %v", i, err)
	}
	outcome, err := r.eng.CommitSubgraphResult(context.Background(), lease, result)
	if err != nil {
		t.Fatalf("CommitSubgraphResult(batch %d) error = %v", i, err)
	}
	return outcome
}

// reportAll reports every batch; the one at timedOut carries the failure result,
// the others succeed. It returns the downstream nodes queued over the whole run.
func (r *batchGuardRig) reportAll(t *testing.T, timedOut int, failure TaskResult) (queued []string) {
	t.Helper()
	for i := range r.batches {
		result := TaskResult{Output: &types.Output{Data: map[string]any{"items": []any{"ok"}, "count": 1}}}
		if i == timedOut {
			result = failure
		}
		r.report(t, i, result)
		queued = append(queued, taskNames(r.queue.Drain())...)
	}
	return queued
}

// timedOutBatch is what a runner reports for a batch whose body ran out of time:
// the batch data carries the host-written marker, and the lease carries the
// failure. The error is plain text, because that is all that survives the trip.
func timedOutBatch() TaskResult {
	return TaskResult{
		Output: &types.Output{Data: map[string]any{
			"items":         []any{map[string]any{batchErrorKey: "timeout: deadline exceeded", "_index": 0}},
			"count":         1,
			"failed":        1,
			batchTimeoutKey: true,
		}},
		Error: errors.New("timeout: deadline exceeded"),
	}
}

// failedBatch is the same failure without the marker: an item that failed for
// any other reason, or a failure reported by a runner that predates the marker.
// That second case is the accepted degradation (Gap 2): such a runner's batch
// goes through failLoopSplit as it always did, and no compatibility shim guesses
// a timeout from the text.
func failedBatch() TaskResult {
	return TaskResult{
		Output: &types.Output{Data: map[string]any{
			"items":  []any{map[string]any{batchErrorKey: "boom", "_index": 0}},
			"count":  1,
			"failed": 1,
		}},
		Error: errors.New("boom"),
	}
}

func (r *batchGuardRig) requireExpired(t *testing.T, queued []string) {
	t.Helper()
	snap, err := r.state.GetExecution(context.Background(), r.id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("execution status = %q (error %q), want %q", snap.Status, snap.Error, types.ExecutionStatusTimeout)
	}
	if snap.Error != ExecutionDeadlineExceededError {
		t.Fatalf("execution error = %q, want %q", snap.Error, ExecutionDeadlineExceededError)
	}
	if len(queued) != 0 {
		t.Fatalf("downstream enqueued %v, want nothing past the execution deadline", queued)
	}
	// The map node must not have been committed by on_error: still waiting, the
	// way ExpireExecution leaves a node it did not own.
	node, err := r.state.GetNode(context.Background(), r.id, "loop")
	if err != nil || node == nil {
		t.Fatalf("GetNode(loop) = (%v, %v)", node, err)
	}
	if node.Status == types.NodeStatusFailed || node.Status == types.NodeStatusContinued || node.Status == types.NodeStatusSuccess {
		t.Fatalf("map node status = %q: it was committed instead of left to the expiry", node.Status)
	}
}

// TestBatchTimeoutPastTheExecutionDeadlineExpiresTheExecution: once the
// execution's own budget is gone, a batch that ran out of time is the
// execution's timeout. It must not reach failLoopSplit, whose ApplyOnError would
// end the execution as failed (stop), keep it running past its budget (continue)
// or finish it as a success through the error port.
func TestBatchTimeoutPastTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	for _, onError := range []string{"", "continue", "error_output", "main_output"} {
		t.Run(guardName(onError), func(t *testing.T) {
			// The timed-out batch reports FIRST, the others after: the verdict is
			// only known at the barrier, whichever batch reports last.
			rig := newBatchGuardRig(t, onError, true, time.Now().Add(-time.Minute))
			queued := rig.reportAll(t, 0, timedOutBatch())
			rig.requireExpired(t, queued)
		})
	}
}

// TestSettledBatchWithATimedOutItemPastTheExecutionDeadlineExpiresTheExecution:
// under continue_on_error a batch can settle successfully while one of its items
// ran out of time. The batch reports no error, but the marker is still there and
// the execution's budget is gone, so the barrier must still expire the execution.
func TestSettledBatchWithATimedOutItemPastTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	rig := newBatchGuardRig(t, "continue", true, time.Now().Add(-time.Minute))
	settled := TaskResult{Output: &types.Output{Data: map[string]any{
		"items":         []any{"ok", map[string]any{batchErrorKey: "timeout: deadline exceeded", "_index": 1}},
		"count":         2,
		"failed":        1,
		batchTimeoutKey: true,
	}}}
	queued := rig.reportAll(t, 0, settled)
	rig.requireExpired(t, queued)
}

// TestBatchTimeoutBeforeTheExecutionDeadlineStillAppliesOnError is the control:
// the batch's own budget ran out while the execution still has time, so the map
// node's on_error decides, exactly as before.
func TestBatchTimeoutBeforeTheExecutionDeadlineStillAppliesOnError(t *testing.T) {
	cases := []struct {
		onError    string
		wantExec   types.ExecutionStatus
		wantNode   types.NodeStatus
		wantQueued string
	}{
		{"", types.ExecutionStatusFailed, types.NodeStatusFailed, ""},
		{"continue", types.ExecutionStatusRunning, types.NodeStatusContinued, "next"},
		{"error_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "onerr"},
	}
	for _, tc := range cases {
		t.Run(guardName(tc.onError), func(t *testing.T) {
			rig := newBatchGuardRig(t, tc.onError, true, time.Now().Add(time.Hour))
			queued := rig.reportAll(t, 0, timedOutBatch())
			requireBatchOnError(t, rig, queued, tc.wantExec, tc.wantNode, tc.wantQueued)
		})
	}
}

// TestOtherBatchFailuresPastTheExecutionDeadlineStillApplyOnError: the guard is
// about the marker and nothing else. A batch that failed for any other reason,
// or whose runner sent no marker, keeps its on_error semantics even after the
// execution budget is gone; the maintenance sweep ends such an execution.
func TestOtherBatchFailuresPastTheExecutionDeadlineStillApplyOnError(t *testing.T) {
	rig := newBatchGuardRig(t, "continue", true, time.Now().Add(-time.Minute))
	queued := rig.reportAll(t, 0, failedBatch())
	requireBatchOnError(t, rig, queued, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
}

// TestBatchTimeoutOnAnUnboundedGraphNeverReadsTheExecutionSnapshot: a graph
// without settings.timeout has no execution deadline, so the marker costs one
// map lookup and not one snapshot read, and on_error decides.
func TestBatchTimeoutOnAnUnboundedGraphNeverReadsTheExecutionSnapshot(t *testing.T) {
	rig := newBatchGuardRig(t, "continue", false, time.Time{})
	g, err := rig.eng.loadGraph(context.Background(), rig.id)
	if err != nil || g == nil {
		t.Fatalf("loadGraph() = (%v, %v)", g, err)
	}
	handled, err := rig.eng.expireIfExecutionDeadlineReached(context.Background(), rig.id, g, time.Now().AddDate(100, 0, 0))
	if err != nil || handled {
		t.Fatalf("guard = (%v, %v), want (false, nil)", handled, err)
	}
	queued := rig.reportAll(t, 0, timedOutBatch())
	requireBatchOnError(t, rig, queued, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
}

// requireBatchOnError pins what failLoopSplit's ApplyOnError did to the map
// node: the execution and node statuses, and which single downstream node (if
// any) was scheduled.
func requireBatchOnError(t *testing.T, rig *batchGuardRig, queued []string, wantExec types.ExecutionStatus, wantNode types.NodeStatus, wantQueued string) {
	t.Helper()
	snap, err := rig.state.GetExecution(context.Background(), rig.id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != wantExec {
		t.Fatalf("execution status = %q, want %q", snap.Status, wantExec)
	}
	node, err := rig.state.GetNode(context.Background(), rig.id, "loop")
	if err != nil || node == nil {
		t.Fatalf("GetNode(loop) = (%v, %v)", node, err)
	}
	if node.Status != wantNode {
		t.Fatalf("map node status = %q, want %q", node.Status, wantNode)
	}
	var want []string
	if wantQueued != "" {
		want = []string{wantQueued}
	}
	if !slices.Equal(queued, want) {
		t.Fatalf("downstream enqueued %v, want %v", queued, want)
	}
}
