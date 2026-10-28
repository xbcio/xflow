package rstate

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// execKeysOf returns every key that carries the execution's hash tag, sorted.
func execKeysOf(srv *miniredis.Miniredis, id types.ExecutionID) []string {
	tag := "{" + string(id) + "}"
	var out []string
	for _, key := range srv.Keys() {
		if strings.Contains(key, tag) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// TestStatusWriteOnAnAbsentExecutionWritesNothing: UpdateExecutionStatus on an ID
// that was never created is refused and leaves no key behind, whatever status it
// carries. A bare status key, with none of the keys a creation writes beside
// it, would read to executionDeadline as "status present, deadline_ms absent",
// which it settles as "no time budget"; the existence guard in
// updateExecutionStatusLua is what keeps that state from being written. The local
// backend ignores a status write for a missing execution the same way.
//
// Every status is tried, not only the terminal ones: a guard that covers one
// status leaves the others writing.
func TestStatusWriteOnAnAbsentExecutionWritesNothing(t *testing.T) {
	ctx := context.Background()
	srv, rdb, state := newMiniredisStore(t)

	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusPending, types.ExecutionStatusQueued, types.ExecutionStatusRunning,
		types.ExecutionStatusPaused, types.ExecutionStatusCanceling, types.ExecutionStatusCanceled,
		types.ExecutionStatusSuccess, types.ExecutionStatusFailed, types.ExecutionStatusTimeout,
	} {
		id := types.ExecutionID("never-created-" + string(status))
		// A non-empty error exercises the error key the script writes beside the status.
		if err := state.UpdateExecutionStatus(ctx, id, status, engine.ConcurrencyQueueTimeoutError); err != nil {
			t.Fatalf("UpdateExecutionStatus(%s) on an absent execution: %v", status, err)
		}
		if keys := execKeysOf(srv, id); len(keys) != 0 {
			t.Fatalf("UpdateExecutionStatus(%s) on an absent execution left %v, want nothing", status, keys)
		}

		// What a replica concludes from the empty state: nothing settled, so
		// nothing remembered, and a creation that lands later is not shadowed.
		replica := newReplica(rdb)
		if deadline, settled := replica.executionDeadline(ctx, id); settled {
			t.Fatalf("executionDeadline(%s) on an absent execution = (%v, settled=true), want unsettled", status, deadline)
		}
		_ = replica.getExecTTL(ctx, id)
		requireMemo(t, replica, id, false)
	}
}

// TestQueueExpiryBeforeCreationLandsKeepsTheRetention walks the one real path that
// used to reach a status write for an absent execution. A queued submission takes
// its place in line (admitWorkflowSlot) BEFORE createExecutionAdmitted writes the
// execution, and the expiry sweep acts on the line, not on the execution. With the
// line entry due inside that window (queue_timeout is only validated as >= 0, and
// a creator that stalls or dies after admission gets there with any value) the
// sweep expires the member and tries to write a terminal status for an execution
// that does not exist yet.
//
// That write is refused now. The point of the walk is what the refusal protects:
// a replica that reads in the window finds nothing to settle, remembers nothing,
// and so the creation that lands afterwards keeps its real deadline all the way
// into the first write that replica makes for the execution (here the promotion a
// release would trigger), which renews the status for the full budget instead of
// the replica's default retention, 48 hours short of the deadline.
func TestQueueExpiryBeforeCreationLandsKeepsTheRetention(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	const wait = time.Millisecond
	g := queueTTLGraph(t, "absent-expiry", wait)
	_ = admitForSweep(t, ctx, creator, g, 1) // fills the only slot

	queued := engine.NewExecutionID()
	deadline := time.Now().Add(replicaBudget)
	sweeper := newReplica(rdb)
	reader := newReplica(rdb)
	statusKey := execKey(namespace.Default, queued, "status")

	var (
		swept           int
		sweepErr        error
		statusInWindow  bool
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
			statusInWindow = srv.Exists(statusKey)
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

	// The member was due, so the sweep took it out of the line and tried to write
	// its terminal status. The count also includes a write the script refused;
	// it is here as evidence the member was handled, not as a promise.
	if sweepErr != nil || swept != 1 {
		t.Fatalf("expiry in the window = (%d, %v), want (1, nil): the line member is due and its execution does not exist yet", swept, sweepErr)
	}
	if statusInWindow {
		t.Fatal("a status key exists in the window: the expiry wrote for an execution that was not created")
	}
	if readerSettled {
		t.Fatalf("reader in the window settled (%v): there was nothing to settle", readerDeadline)
	}
	requireMemo(t, reader, queued, false)

	// The creation lands as it always did, with its real deadline.
	if got, _ := srv.Get(statusKey); got != string(types.ExecutionStatusQueued) {
		t.Fatalf("status after the creation landed = %q, want %q", got, types.ExecutionStatusQueued)
	}
	snap, err := creator.GetExecution(ctx, queued)
	if err != nil || snap == nil || snap.Deadline.UnixMilli() != deadline.UnixMilli() {
		t.Fatalf("GetExecution after the creation = (%+v, %v), want the deadline %v", snap, err, deadline)
	}

	// The reader's first write renews the status for the budget it learns from
	// deadline_ms, not for its base retention.
	if err := reader.promoteExecution(ctx, namespace.Default, queued); err != nil {
		t.Fatalf("promoteExecution: %v", err)
	}
	requireStatusOutlivesDeadline(t, srv, queued, deadline, "the reader's promotion")
}
