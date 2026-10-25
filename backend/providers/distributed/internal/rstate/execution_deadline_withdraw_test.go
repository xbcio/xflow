package rstate

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// inIndex reads the index ZSET directly. It must not go through
// ListDeadlineExceededExecutions: List repairs concluded entries by itself, so
// asking it whether a terminal writer withdrew the entry would pass even if the
// writer never did.
func inIndex(t *testing.T, rdb *redis.Client, ns namespace.Namespace, id types.ExecutionID) bool {
	t.Helper()
	_, err := rdb.ZScore(context.Background(), executionDeadlineIndexKey(ns), string(id)).Result()
	if errors.Is(err, redis.Nil) {
		return false
	}
	if err != nil {
		t.Fatalf("ZSCORE %s: %v", executionDeadlineIndexKey(ns), err)
	}
	return true
}

func requireIndexed(t *testing.T, rdb *redis.Client, id types.ExecutionID) {
	t.Helper()
	if !inIndex(t, rdb, namespace.Default, id) {
		t.Fatalf("%q is not in the deadline index before its terminal write; the test proves nothing", id)
	}
}

func requireWithdrawn(t *testing.T, rdb *redis.Client, id types.ExecutionID, writer string) {
	t.Helper()
	if inIndex(t, rdb, namespace.Default, id) {
		t.Fatalf("%s left %q in the deadline index after concluding the execution", writer, id)
	}
}

// TestDeadlineIndexTerminalStatusWithdrawsTheEntry covers the writers that end
// an execution through UpdateExecutionStatus. timeout is the exception and has
// its own test.
func TestDeadlineIndexTerminalStatusWithdrawsTheEntry(t *testing.T) {
	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusSuccess, types.ExecutionStatusFailed, types.ExecutionStatusCanceled,
	} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			_, rdb, state := newMiniredisStore(t)
			id := types.ExecutionID("withdraw-" + string(status))
			createWithDeadline(t, state, ctx, id, indexBase.Add(time.Second))
			requireIndexed(t, rdb, id)

			if err := state.UpdateExecutionStatus(ctx, id, status, ""); err != nil {
				t.Fatalf("UpdateExecutionStatus(%s): %v", status, err)
			}
			requireWithdrawn(t, rdb, id, "UpdateExecutionStatus("+string(status)+")")
		})
	}
}

// TestDeadlineIndexEveryTerminalWriterWithdraws drives each remaining writer
// that concludes an execution without UpdateExecutionStatus. They share one
// hook (afterExecutionTerminal); a writer that kept the old slot-release call
// would leave a stale entry behind, and only the next List would clean it up.
func TestDeadlineIndexEveryTerminalWriterWithdraws(t *testing.T) {
	ctx := context.Background()
	deadline := indexBase.Add(time.Second)

	t.Run("CommitNode", func(t *testing.T) {
		_, rdb, state := newMiniredisStore(t)
		id := types.ExecutionID("withdraw-commit-node")
		g := errorDetailsRedisGraph(t, "only")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		requireIndexed(t, rdb, id)
		idx, _ := g.NodeIndex("only")
		lease := privateRedisLease(id, "only", idx, "lease-wd", "token-wd")
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
		}
		lease.Attempt = 1
		res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "only", NodeIdx: idx,
			ActivationID: lease.Task.ActivationID, LeaseID: lease.LeaseID,
			LeaseToken: lease.LeaseToken, Attempt: lease.Attempt, Status: types.NodeStatusSuccess,
		})
		if err != nil || !res.ExecutionDone {
			t.Fatalf("CommitNode = (%+v, %v), want the execution finalized", res, err)
		}
		requireWithdrawn(t, rdb, id, "CommitNode")
	})

	t.Run("CommitGroup", func(t *testing.T) {
		state, _ := newGroupRetentionStore(t, time.Hour)
		rdb := state.rdb.(*redis.Client)
		id := types.ExecutionID("withdraw-commit-group")
		g := groupRetentionGraph(t, types.GroupModeDurable, 0)
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: g, Deadline: deadline,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		requireIndexed(t, rdb, id)
		lease := &engine.GroupLease{
			LeaseID: "L-wd", LeaseToken: "tok-wd", Attempt: 1, ExecutionID: id,
			GroupUnitIdx: g.Groups()[0].UnitIdx, GroupID: "edge",
			IssuedAt: time.Now().UTC(), TTL: time.Minute,
		}
		if ok, err := state.AcquireGroupLease(ctx, lease); err != nil || !ok {
			t.Fatalf("AcquireGroupLease = %v, %v", ok, err)
		}
		commitGroupToTerminal(t, state, lease)
		requireWithdrawn(t, rdb, id, "CommitGroup")
	})

	t.Run("CheckCompletion success", func(t *testing.T) {
		_, rdb, state := newMiniredisStore(t)
		id := types.ExecutionID("withdraw-check-success")
		createWithDeadline(t, state, ctx, id, deadline)
		requireIndexed(t, rdb, id)
		if err := rdb.Set(ctx, nodeStatusKey(namespace.Default, id, "start"), string(types.NodeStatusSuccess), 0).Err(); err != nil {
			t.Fatal(err)
		}
		if done, failed, err := state.CheckCompletion(ctx, id, 1); err != nil || !done || failed {
			t.Fatalf("CheckCompletion = (%v, %v, %v), want (true, false, nil)", done, failed, err)
		}
		requireWithdrawn(t, rdb, id, "CheckCompletion(success)")
	})

	t.Run("CheckCompletion failed", func(t *testing.T) {
		_, rdb, state := newMiniredisStore(t)
		id := types.ExecutionID("withdraw-check-failed")
		createWithDeadline(t, state, ctx, id, deadline)
		requireIndexed(t, rdb, id)
		if err := rdb.Set(ctx, nodeStatusKey(namespace.Default, id, "start"), string(types.NodeStatusFailed), 0).Err(); err != nil {
			t.Fatal(err)
		}
		if done, failed, err := state.CheckCompletion(ctx, id, 1); err != nil || !done || !failed {
			t.Fatalf("CheckCompletion = (%v, %v, %v), want (true, true, nil)", done, failed, err)
		}
		requireWithdrawn(t, rdb, id, "CheckCompletion(failed)")
	})

	t.Run("SeedExecution", func(t *testing.T) {
		_, rdb, state := newMiniredisStore(t)
		id := types.ExecutionID("withdraw-seed")
		resp, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
			Execution: &engine.ExecutionSnapshot{
				ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
			},
			Units: []engine.SeedUnit{{
				UnitIdx: 0, NodeName: "start", NodeIdx: 0, Status: types.NodeStatusSuccess,
				Port: "main", Output: map[string]any{"k": "v"},
			}},
		})
		if err != nil {
			t.Fatalf("SeedExecution: %v", err)
		}
		snap, err := state.GetExecution(ctx, id)
		if err != nil || snap == nil || snap.Status != types.ExecutionStatusSuccess {
			t.Fatalf("seed response %+v, execution %+v (%v): the seed was meant to resolve the whole graph", resp, snap, err)
		}
		requireWithdrawn(t, rdb, id, "SeedExecution")
	})

	t.Run("SeedExecutionFromEntry", func(t *testing.T) {
		_, rdb, state := newMiniredisStore(t)
		req := timeoutSeedRequest(t, "withdraw-entry", "withdraw-entry-key", time.Minute)
		resp, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil || resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
		}
		snap, err := state.GetExecution(ctx, resp.ExecutionID)
		if err != nil || snap == nil || snap.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution %+v (%v): the admission was meant to finalize it", snap, err)
		}
		requireWithdrawn(t, rdb, resp.ExecutionID, "SeedExecutionFromEntry")
	})
}

// TestDeadlineIndexExpiresAQueuedExecution pins what ExpireExecution relies on
// for an execution still waiting for a slot: it is listed once due, the timeout
// write takes it out of the line without promoting it, and the entry stays for
// the acknowledgement.
func TestDeadlineIndexExpiresAQueuedExecution(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := queueTTLGraph(t, "deadline-queued", time.Hour)
	admission := engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeQueue, QueueTimeout: time.Hour,
	}
	entry := func(id types.ExecutionID) []engine.OutboxEntry {
		return []engine.OutboxEntry{{
			ID:   "root/" + string(id) + "/start/0",
			Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		}}
	}
	holder, queued := types.ExecutionID("deadline-holder"), types.ExecutionID("deadline-queued-exec")
	if _, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: holder, Graph: g, Status: types.ExecutionStatusRunning,
	}, entry(holder), admission); err != nil {
		t.Fatalf("holder admission: %v", err)
	}
	res, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: queued, Graph: g, Status: types.ExecutionStatusRunning, Deadline: indexBase.Add(time.Second),
	}, entry(queued), admission)
	if err != nil || !res.Queued {
		t.Fatalf("queued submission = (%+v, %v), want it parked", res, err)
	}
	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Minute), 10), queued)
	if pos, err := state.ConcurrencyQueuePosition(ctx, queued); err != nil || pos != 1 {
		t.Fatalf("queue position = (%d, %v), want 1", pos, err)
	}

	if err := state.UpdateExecutionStatus(ctx, queued, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout): %v", err)
	}
	if pos, err := state.ConcurrencyQueuePosition(ctx, queued); err != nil || pos != 0 {
		t.Fatalf("queue position after the timeout = (%d, %v), want 0: it must leave the line", pos, err)
	}
	if !inIndex(t, rdb, namespace.Default, queued) {
		t.Fatal("the timeout write withdrew the entry; it must stay until AckDeadlineExpired")
	}

	// Releasing the holder must not promote an execution that already timed out.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("holder UpdateExecutionStatus(success): %v", err)
	}
	snap, err := state.GetExecution(ctx, queued)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("queued execution after the holder finished = (%+v, %v), want it still timeout", snap, err)
	}
}

// failDeadlineIndexWrites is a go-redis hook that fails ZADDs against the
// deadline index, standing in for a registration the server did not take.
type failDeadlineIndexWrites struct{ failures atomic.Int32 }

func (h *failDeadlineIndexWrites) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failDeadlineIndexWrites) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "zadd") && len(cmd.Args()) > 1 {
			if key, _ := cmd.Args()[1].(string); strings.HasSuffix(key, ":exec_deadlines") {
				h.failures.Add(1)
				return errors.New("injected: deadline index unavailable")
			}
		}
		return next(ctx, cmd)
	}
}

func (h *failDeadlineIndexWrites) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestDeadlineIndexRegistersBeforeTheWrite pins register-before-write: when the
// registration fails the execution must not exist, on every creation path. An
// execution with a deadline and no entry is undiscoverable.
func TestDeadlineIndexRegistersBeforeTheWrite(t *testing.T) {
	ctx := context.Background()
	deadline := indexBase.Add(time.Second)

	t.Run("CreateExecution", func(t *testing.T) {
		srv, rdb, state := newMiniredisStore(t)
		hook := &failDeadlineIndexWrites{}
		rdb.AddHook(hook)
		id := types.ExecutionID("register-first-create")
		err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
		})
		if err == nil {
			t.Fatal("CreateExecution succeeded although the deadline could not be registered")
		}
		if hook.failures.Load() == 0 {
			t.Fatal("the hook never saw a ZADD against the deadline index; the test proves nothing")
		}
		if srv.Exists(execKey(namespace.Default, id, "status")) {
			t.Fatal("the execution was written although its registration failed")
		}
	})

	t.Run("SeedExecution", func(t *testing.T) {
		srv, rdb, state := newMiniredisStore(t)
		hook := &failDeadlineIndexWrites{}
		rdb.AddHook(hook)
		id := types.ExecutionID("register-first-seed")
		if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{Execution: &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
		}}); err == nil {
			t.Fatal("SeedExecution succeeded although the deadline could not be registered")
		}
		if hook.failures.Load() == 0 {
			t.Fatal("the hook never saw a ZADD against the deadline index; the test proves nothing")
		}
		if srv.Exists(execKey(namespace.Default, id, "status")) {
			t.Fatal("the execution was written although its registration failed")
		}
	})

	t.Run("SeedExecutionFromEntry", func(t *testing.T) {
		srv, rdb, state := newMiniredisStore(t)
		hook := &failDeadlineIndexWrites{}
		rdb.AddHook(hook)
		req := timeoutSeedRequest(t, "register-first-entry", "register-first-entry-key", time.Minute)
		if _, err := state.SeedExecutionFromEntry(ctx, req); err == nil {
			t.Fatal("SeedExecutionFromEntry succeeded although the deadline could not be registered")
		}
		if hook.failures.Load() == 0 {
			t.Fatal("the hook never saw a ZADD against the deadline index; the test proves nothing")
		}
		id := engine.DeterministicExecutionID(req.AdmissionKey)
		if srv.Exists(execKey(namespace.Default, id, "status")) || srv.Exists(admissionKey(namespace.Default, id)) {
			t.Fatal("the execution or its admission was written although the registration failed")
		}
	})
}

// TestDeadlineIndexRepairsStaleEntries covers the entries a crash between
// "register" and "write" (or a lost ZREM) leaves behind: List returns none of
// them and removes them.
func TestDeadlineIndexRepairsStaleEntries(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	key := executionDeadlineIndexKey(namespace.Default)

	// An entry whose execution was never written.
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(indexBase.UnixMilli()), Member: "never-written"}).Err(); err != nil {
		t.Fatal(err)
	}
	// An entry whose execution concluded without the writer withdrawing it.
	createWithDeadline(t, state, ctx, "concluded-leftover", indexBase.Add(time.Second))
	if err := rdb.Set(ctx, execKey(namespace.Default, "concluded-leftover", "status"), string(types.ExecutionStatusSuccess), 0).Err(); err != nil {
		t.Fatal(err)
	}
	// And one live execution that must survive the repair.
	createWithDeadline(t, state, ctx, "live", indexBase.Add(2*time.Second))

	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Minute), 10), "live")
	members, err := rdb.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != "live" {
		t.Fatalf("index after the repair = %v, want only [live]", members)
	}
}
