package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// runExecutionTiming pins the execution-level wall clocks that inspect exposes
// as ExecutionDetail.created_at / updated_at. The editor derives a finished
// execution's duration from exactly these two values, so a backend that drops
// them (or never advances updated_at) makes every completed run report either
// no duration or a zero one, with nothing failing at the API layer.
func runExecutionTiming(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	id := types.ExecutionID("exec-contract-timing")
	before := time.Now().UTC().Add(-time.Minute)
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v), want a snapshot", snap, err)
	}
	if snap.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt is zero: created_at is the left edge of the inspect duration")
	}
	if snap.UpdatedAt.IsZero() {
		t.Fatalf("UpdatedAt is zero: even an untouched execution was last updated at creation")
	}
	if snap.CreatedAt.Before(before) || snap.CreatedAt.After(time.Now().UTC()) {
		t.Fatalf("CreatedAt = %v, want within [%v, now]", snap.CreatedAt, before)
	}
	if snap.UpdatedAt.Before(snap.CreatedAt) {
		t.Fatalf("UpdatedAt = %v before CreatedAt = %v", snap.UpdatedAt, snap.CreatedAt)
	}
	created := snap.CreatedAt
	previous := snap.UpdatedAt
	// Millisecond-resolution backends (Redis stores UnixMilli) need the clock to
	// actually move between the two writes; a non-decreasing check alone would
	// pass on a backend that never stamps a transition at all.
	time.Sleep(5 * time.Millisecond)
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}
	snap, err = state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() after transition = (%v, %v), want a snapshot", snap, err)
	}
	if !snap.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v after a status transition, want %v: creation time is immutable", snap.CreatedAt, created)
	}
	if !snap.UpdatedAt.After(previous) {
		t.Fatalf("UpdatedAt = %v after a status transition, want after %v: updated_at is the "+
			"right edge of the inspect duration", snap.UpdatedAt, previous)
	}
}

// runNodeTiming pins the write-once node timing facts: started_at is the first
// lease the node ever received and finished_at is its first terminal write.
// Retries and cyclic reactivations may replace the status and attempt, never
// these two values — restamping them would make a retried node's inspect
// duration cover only its last attempt, and a duplicate terminal write would
// move an end line that already happened.
func runNodeTiming(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	id := types.ExecutionID("exec-contract-node-timing")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	const name = "start"
	firstStart := time.Now().UTC().Add(-3 * time.Minute).Truncate(time.Millisecond)
	retryStart := firstStart.Add(time.Minute)
	finished := firstStart.Add(2 * time.Minute)
	laterFinished := finished.Add(time.Minute)

	base := func(status types.NodeStatus, activation, attempt int) *engine.NodeSnapshot {
		return &engine.NodeSnapshot{
			ExecutionID: id, Name: name, NodeIdx: 0, UnitIdx: 0,
			Status: status, ActivationID: activation, Attempt: attempt,
		}
	}
	write := func(n *engine.NodeSnapshot) {
		t.Helper()
		if err := state.UpsertNode(ctx, n); err != nil {
			t.Fatalf("UpsertNode(%s, activation %d) error = %v", n.Status, n.ActivationID, err)
		}
	}
	read := func() *engine.NodeSnapshot {
		t.Helper()
		got, err := state.GetNode(ctx, id, name)
		if err != nil || got == nil {
			t.Fatalf("GetNode() = (%v, %v), want a snapshot", got, err)
		}
		return got
	}

	first := base(types.NodeStatusRunning, 1, 1)
	first.StartedAt = firstStart
	write(first)
	got := read()
	if !got.StartedAt.Equal(firstStart) {
		t.Fatalf("StartedAt = %v, want %v (the first lease issue time)", got.StartedAt, firstStart)
	}
	if !got.FinishedAt.IsZero() {
		t.Fatalf("FinishedAt = %v on a running node, want zero", got.FinishedAt)
	}

	// A retry is a new attempt, not a new start; the attempt counter must still
	// refresh, so the timing copy cannot be a wholesale snapshot copy.
	retry := base(types.NodeStatusRunning, 1, 2)
	retry.StartedAt = retryStart
	write(retry)
	got = read()
	if got.Attempt != 2 {
		t.Fatalf("Attempt = %d after retry, want 2", got.Attempt)
	}
	if !got.StartedAt.Equal(firstStart) {
		t.Fatalf("StartedAt = %v after retry, want the first lease's %v", got.StartedAt, firstStart)
	}

	done := base(types.NodeStatusSuccess, 1, 3)
	done.StartedAt = retryStart
	done.FinishedAt = finished
	write(done)
	got = read()
	if !got.StartedAt.Equal(firstStart) {
		t.Fatalf("StartedAt = %v after terminal commit, want %v", got.StartedAt, firstStart)
	}
	if !got.FinishedAt.Equal(finished) {
		t.Fatalf("FinishedAt = %v, want %v", got.FinishedAt, finished)
	}

	// Cyclic re-entry: a higher activation is allowed to overwrite the terminal
	// status, but the first start and first finish already happened.
	reentered := base(types.NodeStatusSuccess, 2, 4)
	reentered.StartedAt = retryStart
	reentered.FinishedAt = laterFinished
	write(reentered)
	got = read()
	if !got.StartedAt.Equal(firstStart) {
		t.Fatalf("StartedAt = %v after reactivation, want the first ever %v", got.StartedAt, firstStart)
	}
	if !got.FinishedAt.Equal(finished) {
		t.Fatalf("FinishedAt = %v after reactivation, want the first terminal %v", got.FinishedAt, finished)
	}
}

// runNodeTimingThroughLeases drives the same write-once facts through the
// transitions that actually produce them: AcquireTaskLease is the only writer
// of the start line and a terminal CommitNode is the only writer of the end
// line. The UpsertNode case above takes both values from the test, so a backend
// that re-stamps on a re-claim or a duplicate terminal commit stays green there
// while violating the contract in production.
func runNodeTimingThroughLeases(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatalf("%T does not implement AtomicStateStore", state)
	}
	id := types.ExecutionID("exec-contract-node-timing-lease")
	g := ContractGraph()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	startIdx, _ := g.NodeIndex("start")
	unitIdx := g.UnitIndexForNode(startIdx)
	firstIssued := time.Now().UTC().Add(-3 * time.Minute).Truncate(time.Millisecond)
	// A short TTL makes the next claim's IssuedAt fall past the previous
	// deadline, which is the only condition under which a running lease may be
	// re-claimed by the same (or any) owner.
	acquire := func(leaseID, token string, issued time.Time, activation int) {
		t.Helper()
		lease := &engine.TaskLease{
			LeaseID:    engine.LeaseID(leaseID),
			LeaseToken: engine.LeaseToken(token),
			Attempt:    1,
			IssuedAt:   issued,
			TTL:        time.Second,
			Task: engine.Task{
				ExecutionID: id, NodeName: "start", NodeIdx: startIdx,
				UnitIdx: unitIdx, Type: engine.TaskTypeNodeExec, ActivationID: activation,
			},
		}
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease(%s, activation %d): acquired=%v err=%v",
				leaseID, activation, acquired, err)
		}
	}
	read := func() *engine.NodeSnapshot {
		t.Helper()
		got, err := state.GetNode(ctx, id, "start")
		if err != nil || got == nil {
			t.Fatalf("GetNode() = (%v, %v), want a snapshot", got, err)
		}
		return got
	}
	commit := func() {
		t.Helper()
		// The commit fences on the current lease identity, so read it from the
		// store: a re-claim advances the attempt and, on a cyclic re-entry, the
		// activation. Production uses exactly the lease the acquire returned.
		cur := read()
		res, err := atomic.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "start", NodeIdx: startIdx, UnitIdx: unitIdx,
			ActivationID: cur.ActivationID,
			LeaseID:      cur.LeaseID, LeaseToken: cur.LeaseToken, Attempt: cur.Attempt,
			Status: types.NodeStatusSuccess,
		})
		if err != nil || res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(activation %d, attempt %d) = (%+v, %v), want accepted and applied",
				cur.ActivationID, cur.Attempt, res, err)
		}
	}

	acquire("lease-1", "token-1", firstIssued, 1)
	if got := read(); got.StartedAt.UnixMilli() != firstIssued.UnixMilli() {
		t.Fatalf("StartedAt = %v after the first lease, want %v", got.StartedAt, firstIssued)
	} else if !got.FinishedAt.IsZero() {
		t.Fatalf("FinishedAt = %v on a running node, want zero", got.FinishedAt)
	}
	// Retry through the lease path: the attempt advances, the start line does not.
	acquire("lease-2", "token-2", firstIssued.Add(2*time.Second), 1)
	if got := read(); got.Attempt != 2 {
		t.Fatalf("Attempt = %d after re-claim, want 2", got.Attempt)
	} else if got.StartedAt.UnixMilli() != firstIssued.UnixMilli() {
		t.Fatalf("StartedAt = %v after re-claim, want the first lease's %v", got.StartedAt, firstIssued)
	}
	time.Sleep(5 * time.Millisecond)
	commit()
	got := read()
	if got.FinishedAt.IsZero() {
		t.Fatalf("FinishedAt is zero after the terminal commit: the end line is the " +
			"commit's, not the caller's")
	}
	finished := got.FinishedAt
	// Cyclic re-entry at a higher activation runs the node again but must not
	// rewrite history. The memory backend rebuilds the snapshot on acquire,
	// which is exactly where the end line used to be dropped.
	acquire("lease-3", "token-3", time.Now().UTC().Add(time.Minute), 2)
	if got := read(); got.StartedAt.UnixMilli() != firstIssued.UnixMilli() {
		t.Fatalf("StartedAt = %v after reactivation, want %v", got.StartedAt, firstIssued)
	} else if !got.FinishedAt.Equal(finished) {
		t.Fatalf("FinishedAt = %v after reactivation, want the first terminal %v",
			got.FinishedAt, finished)
	}
	// A fresh end line would be stamped with the backend's clock, so leave a
	// clear gap: a mutation that restamps must produce a different instant.
	time.Sleep(5 * time.Millisecond)
	commit()
	if got := read(); !got.FinishedAt.Equal(finished) {
		t.Fatalf("FinishedAt = %v after the second terminal commit, want the first %v",
			got.FinishedAt, finished)
	}
}

// runExecutionTimingThroughTerminalCommit pins the clock write of the commit
// that finalizes an execution. That transition happens inside the backend's
// commit transition and never passes through UpdateExecutionStatus, so a
// backend that stamps updated_at only in the explicit status writer reports
// every normally completed run with a zero duration.
func runExecutionTimingThroughTerminalCommit(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatalf("%T does not implement AtomicStateStore", state)
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "contract-terminal-timing",
		Nodes: []types.NodeDef{{Name: "only", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id := types.ExecutionID("exec-contract-terminal-timing")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	created, err := state.GetExecution(ctx, id)
	if err != nil || created == nil {
		t.Fatalf("GetExecution() = (%v, %v), want a snapshot", created, err)
	}
	idx, _ := g.NodeIndex("only")
	lease := &engine.TaskLease{
		LeaseID: "lease-terminal", LeaseToken: "token-terminal", Attempt: 1,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: idx,
			UnitIdx: g.UnitIndexForNode(idx), Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease: acquired=%v err=%v", acquired, err)
	}
	// Millisecond-resolution backends need the clock to move between creation
	// and completion for the strictly-after assertion to mean anything.
	time.Sleep(5 * time.Millisecond)
	res, err := atomic.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "only", NodeIdx: idx, UnitIdx: lease.Task.UnitIdx,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess,
	})
	if err != nil || !res.ExecutionDone {
		t.Fatalf("CommitNode() = (%+v, %v), want the single-unit execution finalized", res, err)
	}
	done, err := state.GetExecution(ctx, id)
	if err != nil || done == nil {
		t.Fatalf("GetExecution() after commit = (%v, %v), want a snapshot", done, err)
	}
	if !done.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("CreatedAt = %v after completion, want the creation value %v",
			done.CreatedAt, created.CreatedAt)
	}
	if !done.UpdatedAt.After(created.CreatedAt) {
		t.Fatalf("UpdatedAt = %v after the terminal commit, want after CreatedAt = %v: "+
			"the commit finalizes the execution inside the backend, so it is the only "+
			"place this transition can stamp the clock", done.UpdatedAt, created.CreatedAt)
	}
}
