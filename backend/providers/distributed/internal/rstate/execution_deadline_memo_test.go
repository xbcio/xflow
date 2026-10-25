package rstate

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// interleaveHook runs a function once, at a chosen point of the first command
// (or pipeline) that matches: just before it is sent, or right after it has been
// applied. It is how these tests put another replica's write into the window a
// real race needs, without sleeping or relying on scheduling.
type interleaveHook struct {
	matches func(redis.Cmder) bool
	after   bool // run once the matching command has been applied, not before
	run     func()
	fired   atomic.Bool
}

func (h *interleaveHook) fire() {
	if h.fired.CompareAndSwap(false, true) {
		h.run()
	}
}

func (h *interleaveHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *interleaveHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		hit := h.matches(cmd)
		if hit && !h.after {
			h.fire()
		}
		err := next(ctx, cmd)
		if hit && h.after {
			h.fire()
		}
		return err
	}
}

func (h *interleaveHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		hit := false
		for _, cmd := range cmds {
			if h.matches(cmd) {
				hit = true
				break
			}
		}
		if hit && !h.after {
			h.fire()
		}
		err := next(ctx, cmds)
		if hit && h.after {
			h.fire()
		}
		return err
	}
}

// requireMemo fails unless the replica's deadline memo holds (or, with
// wantKnown false, does not hold) an entry for the execution, and returns it.
func requireMemo(t *testing.T, s *Store, id types.ExecutionID, wantKnown bool) time.Time {
	t.Helper()
	s.ttlMu.RLock()
	memo, known := s.execDeadlines[id]
	s.ttlMu.RUnlock()
	if known != wantKnown {
		t.Fatalf("deadline memo holds an entry for %q = %v (%v), want %v", id, known, memo, wantKnown)
	}
	return memo
}

// TestRetentionSurvivesAReadBeforeTheExecutionExists is the memo-poisoning half of
// B1. A replica can be asked for an execution's retention before the execution
// exists: a queued submission takes its place in line first and writes the
// execution after, so a release on another replica may promote it in between.
// "deadline_ms is absent" is then not the answer "no time budget", and a replica
// that took it as one stayed on the default retention for that execution for as
// long as it ran, shrinking the status back on every write.
func TestRetentionSurvivesAReadBeforeTheExecutionExists(t *testing.T) {
	ctx := context.Background()

	for name, write := range map[string]func(t *testing.T, s *Store, id types.ExecutionID){
		"PauseExecution": func(t *testing.T, s *Store, id types.ExecutionID) {
			if err := s.PauseExecution(ctx, id); err != nil {
				t.Fatalf("PauseExecution: %v", err)
			}
		},
		"UpdateExecutionStatus/paused": func(t *testing.T, s *Store, id types.ExecutionID) {
			if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusPaused, ""); err != nil {
				t.Fatalf("UpdateExecutionStatus(paused): %v", err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv, rdb, creator := newMiniredisStore(t)
			id := types.ExecutionID("early-read-" + types.ExecutionID(name[:5]))
			reader := newReplica(rdb)

			if got := reader.getExecTTL(ctx, id); got != replicaDefaultRetention {
				t.Fatalf("getExecTTL of an execution that does not exist = %v, want the %v default", got, replicaDefaultRetention)
			}
			requireMemo(t, reader, id, false)

			deadline := createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())
			write(t, reader, id)
			requireStatusOutlivesDeadline(t, srv, id, deadline, name+" on the replica that asked first")
		})
	}
}

// TestReadRacingCreationDoesNotPoisonTheMemo puts the creation exactly between
// the replica's read of deadline_ms and its check that the execution exists. Read
// in that order (or as two round trips) the pair says "no deadline_ms" and "the
// execution is there", which is the signature of an execution with no time
// budget, and the replica would remember it as one. The read is one transaction
// so the pair is one snapshot.
func TestReadRacingCreationDoesNotPoisonTheMemo(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("read-races-creation")
	reader := newReplica(rdb)

	var deadline time.Time
	hook := &interleaveHook{
		matches: isDeadlineGet,
		after:   true,
		run: func() {
			deadline = createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())
		},
	}
	rdb.AddHook(hook)

	reader.getExecTTL(ctx, id)
	if !hook.fired.Load() {
		t.Fatal("the creation never landed inside the read; the test proves nothing")
	}
	if memo, known := func() (time.Time, bool) {
		reader.ttlMu.RLock()
		defer reader.ttlMu.RUnlock()
		m, k := reader.execDeadlines[id]
		return m, k
	}(); known && memo.IsZero() {
		t.Fatal("the replica memoized \"no time budget\" for an execution created during its read")
	}

	if err := reader.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution: %v", err)
	}
	requireStatusOutlivesDeadline(t, srv, id, deadline, "a write on the replica whose read raced the creation")
}

// TestSameReplicaReadBeforeCreationKeepsTheCreatorsDeadline: on the replica that
// creates the execution, a read that came first must not leave the memo holding
// anything but the deadline the creation wrote, whether the read finished before
// the creation started or the creation landed inside it.
func TestSameReplicaReadBeforeCreationKeepsTheCreatorsDeadline(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(replicaBudget)

	t.Run("read finished before the creation", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("same-replica-before")
		state.getExecTTL(ctx, id)
		createWithDeadline(t, state, ctx, id, deadline)
		if memo := requireMemo(t, state, id, true); memo.UnixMilli() != deadline.UnixMilli() {
			t.Fatalf("memoized deadline = %v, want the creator's %v", memo, deadline)
		}
	})

	t.Run("creation landed inside the read", func(t *testing.T) {
		_, rdb, state := newMiniredisStore(t)
		id := types.ExecutionID("same-replica-inside")
		hook := &interleaveHook{
			matches: isDeadlineGet,
			after:   true,
			run:     func() { createWithDeadline(t, state, ctx, id, deadline) },
		}
		rdb.AddHook(hook)
		state.getExecTTL(ctx, id)
		if !hook.fired.Load() {
			t.Fatal("the creation never landed inside the read; the test proves nothing")
		}
		if memo := requireMemo(t, state, id, true); memo.UnixMilli() != deadline.UnixMilli() {
			t.Fatalf("memoized deadline = %v, want the creator's %v", memo, deadline)
		}
	})
}

// TestReaderDoesNotOverwriteAMemoTheCreatorFilled: what a creation path records
// is authoritative for the replica that wrote it. A reader that learned its answer
// from Redis a moment earlier must keep what it finds in the memo, not replace it.
func TestReaderDoesNotOverwriteAMemoTheCreatorFilled(t *testing.T) {
	ctx := context.Background()
	_, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("reader-vs-creator")
	createWithDeadline(t, creator, ctx, id, time.Now().Add(replicaBudget))

	reader := newReplica(rdb)
	authoritative := time.Now().Add(72 * time.Hour) // what a creation path on this replica just wrote
	hook := &interleaveHook{
		matches: isDeadlineGet,
		after:   true,
		run:     func() { reader.rememberExecDeadline(id, authoritative) },
	}
	rdb.AddHook(hook)

	got := reader.getExecTTL(ctx, id)
	if !hook.fired.Load() {
		t.Fatal("the creation path never ran inside the read; the test proves nothing")
	}
	if memo := requireMemo(t, reader, id, true); memo.UnixMilli() != authoritative.UnixMilli() {
		t.Fatalf("memoized deadline = %v, want the creation path's %v: the read overwrote it", memo, authoritative)
	}
	requireTTLNearDeadline(t, got, authoritative, "getExecTTL after the creation path filled the memo")
}

// TestQueuedExecutionPromotedBeforeItsCreationLandsKeepsItsRetention is the
// reachable form of the race: CreateExecutionWithConcurrency takes the execution's
// place in line first and writes the execution after, and the deadline index
// registration is the last step before that write. A release on another replica
// promotes the member in that window, which asks the replica for the retention of
// an execution that does not exist yet. Once the creation lands, the release that
// really promotes it must still write a retention that covers the deadline.
func TestQueuedExecutionPromotedBeforeItsCreationLandsKeepsItsRetention(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	const wait = time.Minute
	g := queueTTLGraph(t, "memo-queue-race", wait)
	holder := admitForSweep(t, ctx, creator, g, 1)
	other := newReplica(rdb)

	queued := engine.NewExecutionID()
	deadline := time.Now().Add(replicaBudget)
	indexKey := executionDeadlineIndexKey(namespace.Default)
	hook := &interleaveHook{
		matches: func(cmd redis.Cmder) bool {
			args := cmd.Args()
			return cmd.Name() == "zadd" && len(args) > 1 && args[1] == indexKey
		},
		run: func() {
			if err := other.promoteExecution(ctx, namespace.Default, queued); err != nil {
				t.Errorf("promoteExecution before the creation landed: %v", err)
			}
		},
	}
	rdb.AddHook(hook)

	res, err := creator.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: queued, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}, []engine.OutboxEntry{{
		ID:   "root/" + string(queued) + "/start/0",
		Task: engine.Task{ExecutionID: queued, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}}, engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1, Mode: types.ConcurrencyModeQueue, QueueTimeout: wait,
	})
	if err != nil || !res.Queued {
		t.Fatalf("queued submission = (%+v, %v), want it parked", res, err)
	}
	if !hook.fired.Load() {
		t.Fatal("the other replica never reached the execution before its creation landed; the test proves nothing")
	}

	// The release that really promotes it, on the replica that asked too early.
	if err := other.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(holder): %v", err)
	}
	if got, err := rdb.Get(ctx, execKey(namespace.Default, queued, "status")).Result(); err != nil || got != string(types.ExecutionStatusRunning) {
		t.Fatalf("queued execution status = (%q, %v), want it promoted to running; the test proves nothing otherwise", got, err)
	}
	requireStatusOutlivesDeadline(t, srv, queued, deadline, "the promotion on the replica that asked first")
}

// TestMemoHoldsOnlySettledAnswers pins what is and is not worth remembering. An
// execution with no time budget is settled after one read, so it costs one round
// trip per replica; an execution that does not exist is not, so it is asked about
// again every time and never remembered, and nothing panics for it.
func TestMemoHoldsOnlySettledAnswers(t *testing.T) {
	ctx := context.Background()

	t.Run("execution without a budget", func(t *testing.T) {
		_, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("memo-no-budget")
		createWithDeadline(t, creator, ctx, id, time.Time{})
		hook := &deadlineReadHook{}
		rdb.AddHook(hook)
		other := newReplica(rdb)
		for i := 0; i < 5; i++ {
			if got := other.getExecTTL(ctx, id); got != replicaDefaultRetention {
				t.Fatalf("getExecTTL = %v, want the %v default", got, replicaDefaultRetention)
			}
		}
		if got := hook.reads.Load(); got != 1 {
			t.Fatalf("5 calls issued %d deadline reads, want exactly 1", got)
		}
		if memo := requireMemo(t, other, id, true); !memo.IsZero() {
			t.Fatalf("memo = %v, want the zero deadline for \"no budget\"", memo)
		}
	})

	t.Run("execution that does not exist", func(t *testing.T) {
		_, rdb, _ := newMiniredisStore(t)
		id := types.ExecutionID("memo-never-created")
		hook := &deadlineReadHook{}
		rdb.AddHook(hook)
		other := newReplica(rdb)
		for i := 0; i < 3; i++ {
			if got := other.getExecTTL(ctx, id); got != replicaDefaultRetention {
				t.Fatalf("getExecTTL of an absent execution = %v, want the %v default", got, replicaDefaultRetention)
			}
		}
		if got := hook.reads.Load(); got != 3 {
			t.Fatalf("3 calls issued %d deadline reads, want 3: an absent execution is not remembered", got)
		}
		requireMemo(t, other, id, false)
	})
}
