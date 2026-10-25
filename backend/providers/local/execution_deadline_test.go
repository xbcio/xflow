package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// deadlineBase is the fixed instant the deadline tests measure from. The index
// only compares the before it is handed, so no test here reads the wall clock
// or sleeps.
var deadlineBase = time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)

// newDeadlineExecution creates a running execution carrying the given deadline
// (the zero time means the workflow declared no settings.timeout).
func newDeadlineExecution(t *testing.T, state *memoryState, id types.ExecutionID, deadline time.Time) {
	t.Helper()
	if err := state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution(%s) error = %v", id, err)
	}
}

// listedDeadlines returns the IDs ListDeadlineExceededExecutions reports at
// before, in the order it reports them.
func listedDeadlines(t *testing.T, state *memoryState, before time.Time, limit int) []types.ExecutionID {
	t.Helper()
	got, err := state.ListDeadlineExceededExecutions(context.Background(), before, limit)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions() error = %v", err)
	}
	ids := make([]types.ExecutionID, 0, len(got))
	for _, item := range got {
		ids = append(ids, item.ExecutionID)
	}
	return ids
}

func requireDeadlineIDs(t *testing.T, got []types.ExecutionID, want ...types.ExecutionID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("listed = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listed = %v, want %v", got, want)
		}
	}
}

func TestMemoryDeadlineZeroIsNeverRegistered(t *testing.T) {
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-no-deadline", time.Time{})

	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase.AddDate(100, 0, 0), 8))
	state.mu.Lock()
	tracked := len(state.deadlines)
	state.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("deadlines tracked = %d, want 0 for a zero Deadline", tracked)
	}
}

func TestMemoryDeadlineNotYetDueIsNotReturned(t *testing.T) {
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-future", deadlineBase.Add(time.Second))

	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8))
}

// TestMemoryDeadlineDueIsReturnedAtTheBoundary pins the inclusive boundary: a
// deadline equal to before is due, and it comes back with the registered
// instant and no namespace (the memory backend has none).
func TestMemoryDeadlineDueIsReturnedAtTheBoundary(t *testing.T) {
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-due", deadlineBase)

	got, err := state.ListDeadlineExceededExecutions(context.Background(), deadlineBase, 8)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions() error = %v", err)
	}
	if len(got) != 1 || got[0].ExecutionID != "exec-due" {
		t.Fatalf("listed = %+v, want exec-due", got)
	}
	if !got[0].Deadline.Equal(deadlineBase) {
		t.Fatalf("Deadline = %v, want %v", got[0].Deadline, deadlineBase)
	}
	if got[0].Namespace != "" {
		t.Fatalf("Namespace = %q, want empty", got[0].Namespace)
	}
}

func TestMemoryDeadlineOrdersByDeadlineAndBoundsThePage(t *testing.T) {
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-c", deadlineBase.Add(2*time.Second))
	newDeadlineExecution(t, state, "exec-b", deadlineBase.Add(time.Second))
	newDeadlineExecution(t, state, "exec-a", deadlineBase.Add(time.Second))
	newDeadlineExecution(t, state, "exec-e", deadlineBase.Add(4*time.Second))
	newDeadlineExecution(t, state, "exec-d", deadlineBase.Add(3*time.Second))
	before := deadlineBase.Add(time.Hour)

	if got := listedDeadlines(t, state, before, 0); len(got) != 0 {
		t.Fatalf("limit 0 listed %v, want none", got)
	}
	if got := listedDeadlines(t, state, before, -1); len(got) != 0 {
		t.Fatalf("limit -1 listed %v, want none", got)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, before, 2), "exec-a", "exec-b")
	requireDeadlineIDs(t, listedDeadlines(t, state, before, 5),
		"exec-a", "exec-b", "exec-c", "exec-d", "exec-e")
	requireDeadlineIDs(t, listedDeadlines(t, state, before, 50),
		"exec-a", "exec-b", "exec-c", "exec-d", "exec-e")
}

// TestMemoryDeadlineTerminalStatusWithdrawsTheEntry covers both terminal
// writers. The store has two: the public status write and the finish the
// commit paths run under their own lock. Each must withdraw the entry, or a
// concluded execution stays listed until a list repairs it.
func TestMemoryDeadlineTerminalStatusWithdrawsTheEntry(t *testing.T) {
	statuses := []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
	}
	writers := map[string]func(state *memoryState, id types.ExecutionID, status types.ExecutionStatus) error{
		"status write": func(state *memoryState, id types.ExecutionID, status types.ExecutionStatus) error {
			return state.UpdateExecutionStatus(context.Background(), id, status, "")
		},
		"finish": func(state *memoryState, id types.ExecutionID, status types.ExecutionStatus) error {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.finishExecutionLocked(id, state.executions[id], status, "")
			return nil
		},
	}
	for writerName, write := range writers {
		for _, status := range statuses {
			t.Run(writerName+"/"+string(status), func(t *testing.T) {
				state := newMemoryState()
				newDeadlineExecution(t, state, "exec-terminal", deadlineBase)
				requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), "exec-terminal")

				if err := write(state, "exec-terminal", status); err != nil {
					t.Fatalf("terminal write error = %v", err)
				}
				state.mu.Lock()
				_, tracked := state.deadlines["exec-terminal"]
				state.mu.Unlock()
				if tracked {
					t.Fatalf("%s left the deadline entry registered", status)
				}
				requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase.Add(time.Hour), 8))
			})
		}
	}
}

// TestMemoryDeadlineCommitNodeCompletionWithdrawsTheEntry drives the finish
// writer through the real commit that concludes an execution, rather than by
// calling it directly.
func TestMemoryDeadlineCommitNodeCompletionWithdrawsTheEntry(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("exec-commit-deadline")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: nodeStateGraph(t), Status: types.ExecutionStatusRunning,
		Deadline: deadlineBase,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), id)

	lease := privateOutputLease(id, "stateful", "lease-1", "token-1", engine.TaskTypeNodeExec)
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	committed, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "stateful",
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !committed.ExecutionDone {
		t.Fatal("CommitNode() did not complete the one-unit execution")
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase.Add(time.Hour), 8))
}

// TestMemoryDeadlineTimeoutKeepsTheEntryUntilAck pins the one terminal status
// that does not withdraw: the cascade that stops the execution's nodes runs
// after the timeout write, so the entry must stay discoverable until the
// caller confirms it finished.
func TestMemoryDeadlineTimeoutKeepsTheEntryUntilAck(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-timeout", deadlineBase)

	if err := state.UpdateExecutionStatus(ctx, "exec-timeout", types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), "exec-timeout")
	// Listing is not an acknowledgement.
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), "exec-timeout")

	if err := state.AckDeadlineExpired(ctx, "exec-timeout"); err != nil {
		t.Fatalf("AckDeadlineExpired() error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8))
}

func TestMemoryDeadlineAckIsIdempotentAndScoped(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-one", deadlineBase)
	newDeadlineExecution(t, state, "exec-two", deadlineBase)

	for i := 0; i < 2; i++ {
		if err := state.AckDeadlineExpired(ctx, "exec-one"); err != nil {
			t.Fatalf("AckDeadlineExpired(exec-one) #%d error = %v", i, err)
		}
	}
	if err := state.AckDeadlineExpired(ctx, "exec-unknown"); err != nil {
		t.Fatalf("AckDeadlineExpired(unknown) error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), "exec-two")
}

// TestMemoryDeadlineCancelingStaysListed: canceling is not terminal, and
// ExpireExecution answers a canceling execution with ExpireOutcomeCanceling, so
// the entry has to survive for the cancel's own terminal write to withdraw.
func TestMemoryDeadlineCancelingStaysListed(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-canceling", deadlineBase)

	if err := state.UpdateExecutionStatus(ctx, "exec-canceling", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), "exec-canceling")

	if err := state.UpdateExecutionStatus(ctx, "exec-canceling", types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8))
}

// TestMemoryDeadlineIndexRepairsStaleEntries covers the registration whose
// withdrawal was skipped, and the one whose execution is gone: List must not
// report either and must drop them.
func TestMemoryDeadlineIndexRepairsStaleEntries(t *testing.T) {
	state := newMemoryState()
	newDeadlineExecution(t, state, "exec-stale", deadlineBase)
	newDeadlineExecution(t, state, "exec-gone", deadlineBase)
	newDeadlineExecution(t, state, "exec-live", deadlineBase)
	// Simulate a lost withdrawal by rewriting the status directly.
	state.mu.Lock()
	state.executions["exec-stale"].snap.Status = types.ExecutionStatusSuccess
	delete(state.executions, "exec-gone")
	state.mu.Unlock()

	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), "exec-live")
	state.mu.Lock()
	_, staleTracked := state.deadlines["exec-stale"]
	_, goneTracked := state.deadlines["exec-gone"]
	state.mu.Unlock()
	if staleTracked || goneTracked {
		t.Fatalf("stale entries were not repaired by the list: stale=%v gone=%v", staleTracked, goneTracked)
	}
}

// TestMemoryDeadlineExpiresAQueuedExecution: a queued execution carries its
// deadline, so it is reported when due; writing it timeout takes it out of the
// line without promoting it, and the entry stays until acknowledged.
func TestMemoryDeadlineExpiresAQueuedExecution(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "deadline-queued",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	admission := engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeQueue,
	}

	holderSnap, holderEntries := sweepSubmission(t, g)
	if _, err := state.CreateExecutionWithConcurrency(ctx, holderSnap, holderEntries, admission); err != nil {
		t.Fatalf("holder submission error = %v", err)
	}
	queuedSnap, queuedEntries := sweepSubmission(t, g)
	queuedSnap.Deadline = deadlineBase
	res, err := state.CreateExecutionWithConcurrency(ctx, queuedSnap, queuedEntries, admission)
	if err != nil {
		t.Fatalf("queued submission error = %v", err)
	}
	if !res.Queued {
		t.Fatalf("over-limit submission = %+v, want it parked", res)
	}

	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), queuedSnap.ID)

	if err := state.UpdateExecutionStatus(ctx, queuedSnap.ID, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
	}
	if pos, err := state.ConcurrencyQueuePosition(ctx, queuedSnap.ID); err != nil || pos != 0 {
		t.Fatalf("ConcurrencyQueuePosition() = %d, %v after the timeout, want it out of the line", pos, err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), queuedSnap.ID)

	// Releasing the holder must not promote the timed-out execution.
	if err := state.UpdateExecutionStatus(ctx, holderSnap.ID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(holder success) error = %v", err)
	}
	snap, err := state.GetExecution(ctx, queuedSnap.ID)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if snap == nil || snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("timed-out queued execution after the holder released = %+v, want it still timeout", snap)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), queuedSnap.ID)

	if err := state.AckDeadlineExpired(ctx, queuedSnap.ID); err != nil {
		t.Fatalf("AckDeadlineExpired() error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8))
}

// TestMemoryDeadlineRejectedStatusWritesKeepTheIndex pins what a write the
// store refuses does to the deadline index: nothing. The store refuses by
// returning nil without applying anything, so the index must be read as it was
// before the call. A timeout execution awaiting its acknowledgement and a
// canceling one both stay listed however many late writes bounce off them; an
// execution that already concluded was withdrawn when it concluded, and a late
// write must neither bring it back nor trip over the missing entry.
//
// Two writers are covered. The status write is refused by its CAS guard. The
// finish the commit paths run under their own lock is refused by its terminal
// guard, which has no canceling arm (finishing a canceling execution is
// accepted), so the canceling rows apply to the status write only.
func TestMemoryDeadlineRejectedStatusWritesKeepTheIndex(t *testing.T) {
	ctx := context.Background()
	const lateReason = "late write"
	rows := []struct {
		from      types.ExecutionStatus
		write     types.ExecutionStatus
		listed    bool
		viaFinish bool
	}{
		{types.ExecutionStatusTimeout, types.ExecutionStatusSuccess, true, true},
		{types.ExecutionStatusTimeout, types.ExecutionStatusFailed, true, true},
		{types.ExecutionStatusTimeout, types.ExecutionStatusCanceled, true, true},
		{types.ExecutionStatusCanceling, types.ExecutionStatusSuccess, true, false},
		{types.ExecutionStatusCanceling, types.ExecutionStatusFailed, true, false},
		{types.ExecutionStatusCanceling, types.ExecutionStatusTimeout, true, false},
		{types.ExecutionStatusSuccess, types.ExecutionStatusFailed, false, true},
		{types.ExecutionStatusSuccess, types.ExecutionStatusTimeout, false, true},
		{types.ExecutionStatusFailed, types.ExecutionStatusSuccess, false, true},
		{types.ExecutionStatusCanceled, types.ExecutionStatusTimeout, false, true},
	}
	writers := []struct {
		name  string
		write func(state *memoryState, id types.ExecutionID, status types.ExecutionStatus) error
	}{
		{"status write", func(state *memoryState, id types.ExecutionID, status types.ExecutionStatus) error {
			return state.UpdateExecutionStatus(ctx, id, status, lateReason)
		}},
		{"finish", func(state *memoryState, id types.ExecutionID, status types.ExecutionStatus) error {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.finishExecutionLocked(id, state.executions[id], status, lateReason)
			return nil
		}},
	}
	for _, row := range rows {
		for _, writer := range writers {
			if writer.name == "finish" && !row.viaFinish {
				continue
			}
			t.Run(string(row.from)+"/"+writer.name+"/"+string(row.write), func(t *testing.T) {
				state := newMemoryState()
				newDeadlineExecution(t, state, "exec-rejected", deadlineBase)
				reason := ""
				if row.from == types.ExecutionStatusTimeout {
					reason = engine.ExecutionDeadlineExceededError
				}
				if err := state.UpdateExecutionStatus(ctx, "exec-rejected", row.from, reason); err != nil {
					t.Fatalf("UpdateExecutionStatus(%s) error = %v", row.from, err)
				}
				wantListed := []types.ExecutionID{}
				if row.listed {
					wantListed = append(wantListed, "exec-rejected")
				}
				// The precondition: the row starts from the index it claims to.
				requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), wantListed...)

				if err := writer.write(state, "exec-rejected", row.write); err != nil {
					t.Fatalf("refused %s write error = %v, want it ignored without an error", row.write, err)
				}
				snap, err := state.GetExecution(ctx, "exec-rejected")
				if err != nil || snap == nil {
					t.Fatalf("GetExecution() = %+v, %v", snap, err)
				}
				if snap.Status != row.from || snap.Error != reason {
					t.Fatalf("after the refused write: status %q error %q, want %q / %q (nothing applied)",
						snap.Status, snap.Error, row.from, reason)
				}
				requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), wantListed...)
				state.mu.Lock()
				_, tracked := state.deadlines["exec-rejected"]
				state.mu.Unlock()
				if tracked != row.listed {
					t.Fatalf("deadline entry tracked = %v after the refused write, want %v", tracked, row.listed)
				}
			})
		}
	}
}

// TestMemoryDeadlineRejectedCommitKeepsTheEntryOfATimedOutExecution drives the
// finish writer through the real commit instead of calling it: a node that held
// its lease when the execution timed out commits afterwards, and the commit is
// refused as ExecutionInactive. Only the timeout's own acknowledgement may
// withdraw the entry; a commit that was turned away must leave it.
func TestMemoryDeadlineRejectedCommitKeepsTheEntryOfATimedOutExecution(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("exec-commit-after-timeout")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: nodeStateGraph(t), Status: types.ExecutionStatusRunning,
		Deadline: deadlineBase,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	lease := privateOutputLease(id, "stateful", "lease-1", "token-1", engine.TaskTypeNodeExec)
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), id)

	committed, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "stateful",
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if committed.Outcome != engine.CommitOutcomeExecutionInactive || committed.Applied || committed.ExecutionDone {
		t.Fatalf("CommitNode() after the timeout = %+v, want a refused ExecutionInactive commit", committed)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v, %v", snap, err)
	}
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("after the refused commit: status %q error %q, want timeout / %q",
			snap.Status, snap.Error, engine.ExecutionDeadlineExceededError)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8), id)

	if err := state.AckDeadlineExpired(ctx, id); err != nil {
		t.Fatalf("AckDeadlineExpired() error = %v", err)
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, deadlineBase, 8))
}
