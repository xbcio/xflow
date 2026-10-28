package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/types"
)

// settleCall is one CompleteExpandedSubExecution call as the engine made it.
type settleCall struct {
	child  types.ExecutionID
	status types.ExecutionStatus
	result map[string]any
}

// recordingItemsState is itemsState plus a record of every batch settlement the
// engine attempted, and an optional injected failure of that commit. The call is
// recorded before the fence decides, so a stale batch shows up as a call that was
// rejected, which the tests tell apart through the sub-execution statuses.
type recordingItemsState struct {
	*itemsState
	callMu      sync.Mutex
	calls       []settleCall
	completeErr error
}

func newRecordingItemsState() *recordingItemsState {
	return &recordingItemsState{itemsState: newItemsState()}
}

func (s *recordingItemsState) CompleteExpandedSubExecution(ctx context.Context, lease *TaskLease, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, bool, []map[string]any, error) {
	s.callMu.Lock()
	s.calls = append(s.calls, settleCall{child: childExecID, status: status, result: cloneMap(result)})
	injected := s.completeErr
	s.callMu.Unlock()
	if injected != nil {
		return false, false, nil, injected
	}
	return s.itemsState.CompleteExpandedSubExecution(ctx, lease, childExecID, status, result)
}

func (s *recordingItemsState) recorded() []settleCall {
	s.callMu.Lock()
	defer s.callMu.Unlock()
	return append([]settleCall(nil), s.calls...)
}

// permanentReadFailure is what rstate.ResolveItems returns for a blob that is
// gone: marked permanent while keeping the cause matchable.
func permanentReadFailure() error {
	return fmt.Errorf("%w: %w", types.ErrPermanent, errors.New("gone"))
}

// failRun is an expanded map workflow whose store is a recordingItemsState and
// whose body is the echo executor.
type failRun struct {
	*itemsRun
	store *recordingItemsState
	exec  *echoBodyExecutor
}

// expandForFailure expands a map node over len(lengths) batches of the given
// sizes. adjust, when set, edits the map node before the workflow compiles.
func expandForFailure(t *testing.T, adjust func(*types.NodeDef), lengths ...int) *failRun {
	t.Helper()
	total := 0
	for _, n := range lengths {
		total += n
	}
	items := numberedItems(total)
	store := newRecordingItemsState()
	exec := newEchoBodyExecutor()
	run := expandShapedWith(t, store, &shapedMapHandler{items: items, batches: chunked(items, lengths...), batchSize: lengths[0]},
		adjust, WithBatchBodyExecutor(exec))
	return &failRun{itemsRun: run, store: store, exec: exec}
}

// resolveFailing makes every batch read from here on fail with err, bypassing
// the process cache so a batch that was already resolved is read again.
func (r *failRun) resolveFailing(err error) {
	r.store.resolveErr = err
	batchItemsCache.Purge()
}

// nodeState reads the map node and the execution as the test workflow left them.
func (r *failRun) nodeState(t *testing.T) (*NodeSnapshot, *ExecutionSnapshot) {
	t.Helper()
	node, err := r.state.GetNode(context.Background(), r.execID, "m")
	if err != nil || node == nil {
		t.Fatalf("GetNode(m) = %+v, err = %v", node, err)
	}
	snap, err := r.state.GetExecution(context.Background(), r.execID)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution = %+v, err = %v", snap, err)
	}
	return node, snap
}

// TestPermanentReadFailureSettlesTheBatchAsFailed: a batch whose stored items are
// permanently unreadable must not be retried and must not strand the map node.
// ExecuteBatch returns nil (the queue task is done), commits the batch as Failed
// with the reason under _error, and once every batch has reported the map node
// fails and the execution aborts under the default stop strategy.
func TestPermanentReadFailureSettlesTheBatchAsFailed(t *testing.T) {
	run := expandForFailure(t, nil, 2, 2)
	run.resolveFailing(permanentReadFailure())

	for i, task := range run.batches {
		if err := run.eng.ExecuteBatch(context.Background(), task); err != nil {
			t.Fatalf("ExecuteBatch(batch %d) = %v, want nil so the queue neither retries nor archives it", i, err)
		}
	}
	calls := run.store.recorded()
	if len(calls) != len(run.batches) {
		t.Fatalf("CompleteExpandedSubExecution calls = %d, want %d", len(calls), len(run.batches))
	}
	for i, call := range calls {
		reason, _ := call.result[batchErrorKey].(string)
		if call.status != types.ExecutionStatusFailed || !strings.HasPrefix(reason, "batch items unavailable: ") {
			t.Errorf("call %d = status %q result %v, want Failed with a batch items unavailable reason", i, call.status, call.result)
		}
	}
	if n := len(run.exec.requests()); n != 0 {
		t.Errorf("body ran %d times for batches whose items could not be read", n)
	}
	node, snap := run.nodeState(t)
	if node.Status != types.NodeStatusFailed || snap.Status != types.ExecutionStatusFailed {
		t.Errorf("map node %q, execution %q, want both failed", node.Status, snap.Status)
	}
	if !strings.Contains(node.Error, "batch items unavailable") {
		t.Errorf("map node error = %q, want it to carry the batch items unavailable reason", node.Error)
	}
	if downstream := run.queue.Drain(); len(downstream) != 0 {
		t.Errorf("downstream tasks %v fired on a failed map node", taskNames(downstream))
	}
}

// TestTransientReadFailureIsRetriedNotSettled: a read error that is not marked
// permanent keeps the old behaviour. ExecuteBatch returns it so the queue
// retries, and nothing is committed, so the map node is still waiting for the
// batch rather than failed by a store that may be back a moment later.
func TestTransientReadFailureIsRetriedNotSettled(t *testing.T) {
	run := expandForFailure(t, nil, 2, 2)
	transient := errors.New("store down")
	run.resolveFailing(transient)

	err := run.eng.ExecuteBatch(context.Background(), run.batches[0])
	if !errors.Is(err, transient) {
		t.Fatalf("ExecuteBatch = %v, want the store error returned for a retry", err)
	}
	if types.IsPermanent(err) {
		t.Fatalf("ExecuteBatch error %v is permanent; a transient read failure must stay retryable", err)
	}
	if calls := run.store.recorded(); len(calls) != 0 {
		t.Fatalf("a transient failure committed %d batch results, want none", len(calls))
	}
	node, snap := run.nodeState(t)
	if node.Status != types.NodeStatusWaiting || snap.Status == types.ExecutionStatusFailed {
		t.Errorf("map node %q, execution %q, want the node still waiting", node.Status, snap.Status)
	}
}

// TestOneUnreadableBatchFailsTheMapNode: only one batch's items are permanently
// unreadable while the others run normally. The map node still fails once every
// batch has reported, with the reason in its error, instead of finishing as a
// success that is missing one batch's results.
func TestOneUnreadableBatchFailsTheMapNode(t *testing.T) {
	run := expandForFailure(t, nil, 2, 2, 2)
	ctx := context.Background()

	for _, i := range []int{0, 2} {
		if err := run.eng.ExecuteBatch(ctx, run.batches[i]); err != nil {
			t.Fatalf("ExecuteBatch(batch %d) = %v", i, err)
		}
	}
	if node, _ := run.nodeState(t); node.Status != types.NodeStatusWaiting {
		t.Fatalf("map node is %q before its last batch reported, want waiting", node.Status)
	}
	run.resolveFailing(permanentReadFailure())
	if err := run.eng.ExecuteBatch(ctx, run.batches[1]); err != nil {
		t.Fatalf("ExecuteBatch(unreadable batch) = %v, want nil", err)
	}

	node, snap := run.nodeState(t)
	if node.Status != types.NodeStatusFailed || snap.Status != types.ExecutionStatusFailed {
		t.Errorf("map node %q, execution %q, want both failed", node.Status, snap.Status)
	}
	// The map node's verdict is read from the _error key, not from the status, so
	// the status of the settled batch needs its own assertion: a batch recorded as
	// Success would still fail the node here and leave the sub-execution lying.
	calls := run.store.recorded()
	if last := calls[len(calls)-1]; last.status != types.ExecutionStatusFailed {
		t.Errorf("unreadable batch was committed as %q, want Failed", last.status)
	}
	if !strings.Contains(node.Error, "batch items unavailable") || !strings.Contains(node.Error, "1 of 3 batches failed") {
		t.Errorf("map node error = %q, want 1 of 3 batches failed with the batch items unavailable reason", node.Error)
	}
	if got := len(run.exec.requests()); got != 2 {
		t.Errorf("body ran %d times, want 2: the two readable batches only", got)
	}
}

// supersedeParent gives the map node a token the batches do not hold, which is
// what a parent reclaimed into a newer generation looks like to the fence.
func (r *failRun) supersedeParent() {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	for key, node := range r.store.nodes {
		stale := *node
		stale.LeaseToken = "superseded"
		r.store.nodes[key] = &stale
	}
}

// TestSupersededBatchWithUnreadableItemsIsDropped: the stale check comes before
// the items are read, so a batch of a reclaimed parent neither costs a store read
// nor commits a failure into the new generation, even when its blob is gone.
func TestSupersededBatchWithUnreadableItemsIsDropped(t *testing.T) {
	run := expandForFailure(t, nil, 2, 2)
	run.supersedeParent()
	run.resolveFailing(permanentReadFailure())

	if err := run.eng.ExecuteBatch(context.Background(), run.batches[0]); err != nil {
		t.Fatalf("ExecuteBatch(superseded) = %v, want nil", err)
	}
	if run.store.resolveCalls != 0 {
		t.Errorf("ResolveItems calls = %d for a superseded batch, want 0", run.store.resolveCalls)
	}
	if calls := run.store.recorded(); len(calls) != 0 {
		t.Errorf("a superseded batch committed %d results, want none", len(calls))
	}
}

// TestFailBatchTaskIgnoresBatchesThatNoLongerCount: FailBatchTask is called by
// layers that did not check the fence first. A batch of a superseded parent is
// rejected by the fence and an inactive execution is skipped; both return nil and
// leave every sub-execution as it was.
func TestFailBatchTaskIgnoresBatchesThatNoLongerCount(t *testing.T) {
	ctx := context.Background()
	cause := errors.New("blob is gone")

	t.Run("superseded parent", func(t *testing.T) {
		run := expandForFailure(t, nil, 2, 2)
		run.supersedeParent()
		if err := run.eng.FailBatchTask(ctx, run.batches[0], cause); err != nil {
			t.Fatalf("FailBatchTask(superseded) = %v, want nil", err)
		}
		for _, sub := range run.store.subExecs[fakeExpansionSubKeyOf(run, "m")] {
			if sub.Status != types.ExecutionStatusRunning {
				t.Errorf("sub-execution %s is %q after a rejected failure, want it untouched", sub.ChildExecID, sub.Status)
			}
		}
	})
	t.Run("inactive execution", func(t *testing.T) {
		run := expandForFailure(t, nil, 2, 2)
		if err := run.state.UpdateExecutionStatus(ctx, run.execID, types.ExecutionStatusCanceled, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus: %v", err)
		}
		if err := run.eng.FailBatchTask(ctx, run.batches[0], cause); err != nil {
			t.Fatalf("FailBatchTask(inactive) = %v, want nil", err)
		}
		if calls := run.store.recorded(); len(calls) != 0 {
			t.Errorf("an inactive execution had %d batch results committed, want none", len(calls))
		}
	})
}

// fakeExpansionSubKeyOf is the fake state's key for the sub-executions of the
// named map node's current lease.
func fakeExpansionSubKeyOf(run *failRun, node string) string {
	n, _ := run.state.GetNode(context.Background(), run.execID, node)
	return fmt.Sprintf("%s/%s/%s", run.execID, node, n.LeaseID)
}

// TestFailBatchTaskSettlesLikeExecuteBatch: calling FailBatchTask directly on a
// readable batch has the same effect as ExecuteBatch giving up on it: the batch
// is committed Failed with the reason, and the map node fails once the rest
// arrive. A commit that itself fails is returned, so the caller can retry.
func TestFailBatchTaskSettlesLikeExecuteBatch(t *testing.T) {
	ctx := context.Background()
	cause := errors.New("blob is gone")

	t.Run("settles", func(t *testing.T) {
		run := expandForFailure(t, nil, 2, 2)
		if err := run.eng.ExecuteBatch(ctx, run.batches[0]); err != nil {
			t.Fatalf("ExecuteBatch(batch 0) = %v", err)
		}
		if err := run.eng.FailBatchTask(ctx, run.batches[1], cause); err != nil {
			t.Fatalf("FailBatchTask = %v", err)
		}
		calls := run.store.recorded()
		last := calls[len(calls)-1]
		if last.status != types.ExecutionStatusFailed || last.result[batchErrorKey] != "batch items unavailable: blob is gone" {
			t.Errorf("settlement = status %q result %v, want Failed with exactly the prefixed cause", last.status, last.result)
		}
		node, snap := run.nodeState(t)
		if node.Status != types.NodeStatusFailed || snap.Status != types.ExecutionStatusFailed {
			t.Errorf("map node %q, execution %q, want both failed", node.Status, snap.Status)
		}
	})
	t.Run("commit error is returned", func(t *testing.T) {
		run := expandForFailure(t, nil, 2, 2)
		run.store.completeErr = errors.New("redis timeout")
		if err := run.eng.FailBatchTask(ctx, run.batches[0], cause); !errors.Is(err, run.store.completeErr) {
			t.Fatalf("FailBatchTask = %v, want the commit error so the caller retries", err)
		}
		if node, _ := run.nodeState(t); node.Status != types.NodeStatusWaiting {
			t.Errorf("map node %q after a failed commit, want it still waiting", node.Status)
		}
	})
	t.Run("malformed task is an error", func(t *testing.T) {
		run := expandForFailure(t, nil, 2, 2)
		if err := run.eng.FailBatchTask(ctx, &Task{}, cause); err == nil {
			t.Fatal("FailBatchTask accepted a task with no payload")
		}
	})
}

// TestUnreadableBatchFailsTheMapNodeEvenWithContinueOnError pins what a batch
// with no items array does at the barrier. The batch result is just
// {_error: ...}: flattenBatchResults keeps it as one results slot rather than
// dropping it, and failedBatchErrors reads the top-level _error, which does not
// depend on continue_on_error (that setting only decides when a batch whose
// ITEMS failed counts as failed, in BatchResultForCommit). So the map node fails
// and nothing is swallowed. Under error_output the failure is routed with the
// results array intact, where the unreadable batch shows up as that one slot.
func TestUnreadableBatchFailsTheMapNodeEvenWithContinueOnError(t *testing.T) {
	ctx := context.Background()
	continueOnError := func(node *types.NodeDef) { node.Parameters["continue_on_error"] = true }

	t.Run("stop", func(t *testing.T) {
		run := expandForFailure(t, continueOnError, 2, 2)
		if err := run.eng.ExecuteBatch(ctx, run.batches[0]); err != nil {
			t.Fatalf("ExecuteBatch(batch 0) = %v", err)
		}
		run.resolveFailing(permanentReadFailure())
		if err := run.eng.ExecuteBatch(ctx, run.batches[1]); err != nil {
			t.Fatalf("ExecuteBatch(unreadable batch) = %v", err)
		}
		node, snap := run.nodeState(t)
		if node.Status != types.NodeStatusFailed || snap.Status != types.ExecutionStatusFailed {
			t.Errorf("continue_on_error=true: map node %q, execution %q, want both failed", node.Status, snap.Status)
		}
	})
	t.Run("error_output", func(t *testing.T) {
		run := expandForFailure(t, func(node *types.NodeDef) {
			continueOnError(node)
			node.OnError = string(types.OnErrorOutput)
		}, 2, 2)
		if err := run.eng.ExecuteBatch(ctx, run.batches[0]); err != nil {
			t.Fatalf("ExecuteBatch(batch 0) = %v", err)
		}
		run.resolveFailing(permanentReadFailure())
		if err := run.eng.ExecuteBatch(ctx, run.batches[1]); err != nil {
			t.Fatalf("ExecuteBatch(unreadable batch) = %v", err)
		}
		node, _ := run.nodeState(t)
		if node.Port != "error" || !strings.Contains(node.Error, "batch items unavailable") {
			t.Fatalf("map node port %q error %q, want the failure routed to the error port", node.Port, node.Error)
		}
		results, _ := node.Output["results"].([]any)
		if len(results) != 3 {
			t.Fatalf("results = %v, want 2 items from the readable batch plus 1 slot for the unreadable one", results)
		}
		slot, _ := results[2].(map[string]any)
		if _, marked := slot[batchErrorKey]; !marked {
			t.Errorf("results[2] = %v, want the unreadable batch kept as a slot carrying _error", results[2])
		}
	})
}
