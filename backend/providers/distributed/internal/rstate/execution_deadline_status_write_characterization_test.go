package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// These two tests CHARACTERIZE what updateExecutionStatusLua does today on an
// execution that does not exist. They pin the current behaviour, which is a
// hole, not a contract: whoever closes it (an existence guard in the script, or
// a stricter settled rule in executionDeadline) flips the assertions marked
// "the hole" in the same change.

// TestCharacterizeStatusWriteOnAnAbsentExecution: UpdateExecutionStatus on an ID
// that was never created is not refused. The script has no existence guard, so
// it creates a bare status key (and the clock and error beside it) with none of
// the keys a creation writes with it. To the deadline read that bare key is
// "status present, deadline_ms absent", which it settles as "no time budget".
func TestCharacterizeStatusWriteOnAnAbsentExecution(t *testing.T) {
	ctx := context.Background()
	srv, rdb, state := newMiniredisStore(t)
	id := types.ExecutionID("never-created")

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ConcurrencyQueueTimeoutError); err != nil {
		t.Fatalf("UpdateExecutionStatus on an absent execution: %v", err)
	}

	// The hole: a status key exists for an execution nothing ever created.
	statusKey := execKey(namespace.Default, id, "status")
	if got, err := srv.Get(statusKey); err != nil || got != string(types.ExecutionStatusTimeout) {
		t.Fatalf("status key after a write to an absent execution = (%q, %v), want a bare %q (the hole)", got, err, types.ExecutionStatusTimeout)
	}
	for _, suffix := range []string{"deadline_ms", "graph", "created_at_ms"} {
		if srv.Exists(execKey(namespace.Default, id, suffix)) {
			t.Fatalf("%s exists after a write to an absent execution: the bare write is no longer bare", suffix)
		}
	}

	// What a replica concludes from it: settled, and no budget.
	deadline, settled := newReplica(rdb).executionDeadline(ctx, id)
	if !settled || !deadline.IsZero() {
		t.Fatalf("executionDeadline on the bare key = (%v, settled=%v), want (zero, true): the hole", deadline, settled)
	}
}

// TestCharacterizeQueueExpiryBeforeCreationLands walks the one real path that
// reaches the bare write. A queued submission takes its place in line
// (admitWorkflowSlot) BEFORE createExecutionAdmitted writes the execution, and
// the expiry sweep acts on the line, not on the execution. With the line entry
// due inside that window (queue_timeout is only validated as >= 0, and a
// creator that stalls or dies after admission gets there with any value) the
// sweep expires the member, which writes a bare terminal status for an
// execution that does not exist yet.
//
// A replica that reads in the window settles that as "no time budget" and
// remembers it; the creation then lands with the real deadline, and the first
// write that replica makes for the execution (here the promotion a release would
// trigger) renews the status with the default retention, 48 hours short of the
// deadline.
func TestCharacterizeQueueExpiryBeforeCreationLands(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	const wait = time.Millisecond
	g := queueTTLGraph(t, "characterize-expiry", wait)
	_ = admitForSweep(t, ctx, creator, g, 1) // fills the only slot

	queued := engine.NewExecutionID()
	deadline := time.Now().Add(replicaBudget)
	sweeper := newReplica(rdb)
	reader := newReplica(rdb)
	statusKey := execKey(namespace.Default, queued, "status")

	var (
		swept           int
		sweepErr        error
		bareInWindow    bool
		readerDeadline  time.Time
		readerSettled   bool
		windowCompleted bool
	)
	hook := &interleaveHook{
		// Just before the creation's transaction writes the status.
		matches: func(cmd redis.Cmder) bool {
			args := cmd.Args()
			return cmd.Name() == "set" && len(args) > 1 && args[1] == statusKey
		},
		run: func() {
			time.Sleep(10 * time.Millisecond) // past the 1ms line deadline
			swept, sweepErr = sweeper.ExpireQueuedExecutions(ctx, 0)
			bareInWindow = srv.Exists(statusKey)
			readerDeadline, readerSettled = reader.executionDeadline(ctx, queued)
			if readerSettled {
				reader.learnExecDeadline(queued, readerDeadline)
			}
			windowCompleted = true
		},
	}
	rdb.AddHook(hook)

	if _, err := creator.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: queued, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}, []engine.OutboxEntry{{
		ID:   "root/" + string(queued) + "/start/0",
		Task: engine.Task{ExecutionID: queued, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}}, engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1, Mode: types.ConcurrencyModeQueue, QueueTimeout: wait,
	}); err != nil {
		t.Fatalf("CreateExecutionWithConcurrency: %v", err)
	}
	if !windowCompleted {
		t.Fatal("the window between admission and creation was never observed")
	}

	if sweepErr != nil || swept != 1 {
		t.Fatalf("expiry in the window = (%d, %v), want (1, nil): the line member is due and its execution does not exist yet", swept, sweepErr)
	}
	if !bareInWindow {
		t.Fatal("no status key in the window: the expiry no longer writes for an execution that was not created (the hole is closed)")
	}
	if !readerSettled || !readerDeadline.IsZero() {
		t.Fatalf("reader in the window = (%v, settled=%v), want (zero, true): the hole", readerDeadline, readerSettled)
	}
	requireMemo(t, reader, queued, true) // remembered as "no budget"

	// The creation overwrites the bare terminal status: the expired execution
	// comes back queued, with its real deadline.
	if got, _ := srv.Get(statusKey); got != string(types.ExecutionStatusQueued) {
		t.Fatalf("status after the creation landed = %q, want %q", got, types.ExecutionStatusQueued)
	}
	snap, err := creator.GetExecution(ctx, queued)
	if err != nil || snap == nil || snap.Deadline.UnixMilli() != deadline.UnixMilli() {
		t.Fatalf("GetExecution after the creation = (%+v, %v), want the deadline %v", snap, err, deadline)
	}

	// The consequence: the reader's first write renews the status with the base
	// retention, far short of the deadline the execution is entitled to.
	if err := reader.promoteExecution(ctx, namespace.Default, queued); err != nil {
		t.Fatalf("promoteExecution: %v", err)
	}
	if got := srv.TTL(statusKey); got > replicaDefaultRetention {
		t.Fatalf("status TTL after the reader's write = %v, want at most the %v base retention (the hole): "+
			"the poisoned memo should have shrunk it", got, replicaDefaultRetention)
	}
}
