package rstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The retention a replica that did not create an execution falls back to, and
// the one newMiniredisStore builds its stores with. Every budget below is far
// above it, so any write that resolves the default instead of the deadline
// leaves a TTL the assertions can tell apart.
const replicaDefaultRetention = time.Minute

// replicaBudget is how long the executions below are allowed to run: much longer
// than the default retention, which is the only case the deadline retention
// matters for.
const replicaBudget = 48 * time.Hour

// newReplica is another process serving the same Redis: no in-memory TTL
// override, no deadline memo, nothing the creating replica knew.
func newReplica(rdb redis.UniversalClient) *Store {
	return New(rdb, nil, replicaDefaultRetention)
}

// createLongBudgetExecution creates a running execution with a replicaBudget
// deadline on the creating replica and returns the deadline. It checks the
// creation itself got the extended retention, so a failure further down cannot
// be a creation that never extended anything.
func createLongBudgetExecution(t *testing.T, ctx context.Context, srv *miniredis.Miniredis, creator *Store, id types.ExecutionID, g *graph.Graph) time.Time {
	t.Helper()
	deadline := time.Now().Add(replicaBudget)
	if err := creator.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution(%q): %v", id, err)
	}
	requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, id, "status")), deadline, "status after creation")
	return deadline
}

func requireStatusOutlivesDeadline(t *testing.T, srv *miniredis.Miniredis, id types.ExecutionID, deadline time.Time, what string) {
	t.Helper()
	requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, id, "status")), deadline, "status after "+what)
}

// twoNodeGraph is a DAG whose first node can commit without finishing the
// execution.
func twoNodeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "replica-two-node",
		Nodes: []types.NodeDef{{Name: "a", Type: "test.echo"}, {Name: "b", Type: "test.echo"}},
		Connections: types.Connections{
			"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile(two-node): %v", err)
	}
	return g
}

// TestStatusRetentionSurvivesAnotherReplica is B1. The status key's TTL is the
// execution's lifetime, and every writer that renews it resolves the TTL through
// getExecTTL. A replica that never created the execution has no in-memory
// override, so before the deadline was folded into getExecTTL each of these
// writes shrank the status back to the default retention while deadline_ms and
// the index member stayed: the execution then vanished and was never timed out.
//
// Each case runs on its own fresh replica so none of them inherits what an
// earlier one learned.
func TestStatusRetentionSurvivesAnotherReplica(t *testing.T) {
	ctx := context.Background()

	t.Run("PauseExecution", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("replica-pause")
		deadline := createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())
		if err := newReplica(rdb).PauseExecution(ctx, id); err != nil {
			t.Fatalf("PauseExecution: %v", err)
		}
		requireStatusOutlivesDeadline(t, srv, id, deadline, "PauseExecution")
	})

	t.Run("ResumeExecution", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("replica-resume")
		deadline := createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())
		if err := creator.PauseExecution(ctx, id); err != nil {
			t.Fatalf("PauseExecution on the creator: %v", err)
		}
		if err := newReplica(rdb).ResumeExecution(ctx, id); err != nil {
			t.Fatalf("ResumeExecution: %v", err)
		}
		requireStatusOutlivesDeadline(t, srv, id, deadline, "ResumeExecution")
	})

	for _, status := range []types.ExecutionStatus{types.ExecutionStatusPaused, types.ExecutionStatusCanceling} {
		t.Run("UpdateExecutionStatus/"+string(status), func(t *testing.T) {
			srv, rdb, creator := newMiniredisStore(t)
			id := types.ExecutionID("replica-update-" + string(status))
			deadline := createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())
			if err := newReplica(rdb).UpdateExecutionStatus(ctx, id, status, ""); err != nil {
				t.Fatalf("UpdateExecutionStatus(%s): %v", status, err)
			}
			requireStatusOutlivesDeadline(t, srv, id, deadline, "UpdateExecutionStatus("+string(status)+")")
			// The clock annotates the status and must live as long as it does.
			requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, id, "updated_at_ms")), deadline, "updated_at_ms")
		})
	}

	t.Run("QueuePromotion", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		const wait = time.Minute
		g := queueTTLGraph(t, "replica-promotion", wait)
		holder := admitForSweep(t, ctx, creator, g, 1)

		queued := engine.NewExecutionID()
		deadline := time.Now().Add(replicaBudget)
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
		requireStatusOutlivesDeadline(t, srv, queued, deadline, "the queued creation")

		// A different replica finishes the holder; its release promotes the queued
		// execution, and the promotion rewrites that execution's status key.
		if err := newReplica(rdb).UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(holder): %v", err)
		}
		if got, err := rdb.Get(ctx, execKey(namespace.Default, queued, "status")).Result(); err != nil || got != string(types.ExecutionStatusRunning) {
			t.Fatalf("queued execution status = (%q, %v), want it promoted to running; the test proves nothing otherwise", got, err)
		}
		requireStatusOutlivesDeadline(t, srv, queued, deadline, "the promotion")
	})

	t.Run("AcquireTaskLease", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("replica-acquire")
		g := errorDetailsRedisGraph(t, "only")
		deadline := createLongBudgetExecution(t, ctx, srv, creator, id, g)
		lease := privateRedisLease(id, "only", 0, "lease-acq", "token-acq")
		if _, acquired, err := newReplica(rdb).AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
		}
		requireOutlivesDeadline(t, srv.TTL(nodeStatusKey(namespace.Default, id, "only")), deadline, "node status after AcquireTaskLease")
	})

	t.Run("CommitNode", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("replica-commit")
		g := twoNodeGraph(t)
		deadline := createLongBudgetExecution(t, ctx, srv, creator, id, g)
		idx, _ := g.NodeIndex("a")
		lease := privateRedisLease(id, "a", idx, "lease-commit", "token-commit")
		other := newReplica(rdb)
		if _, acquired, err := other.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
		}
		lease.Attempt = 1
		res, err := other.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "a", NodeIdx: idx,
			ActivationID: lease.Task.ActivationID, LeaseID: lease.LeaseID,
			LeaseToken: lease.LeaseToken, Attempt: lease.Attempt, Status: types.NodeStatusSuccess,
		})
		if err != nil || res.ExecutionDone {
			t.Fatalf("CommitNode = (%+v, %v), want a non-terminal commit", res, err)
		}
		requireOutlivesDeadline(t, srv.TTL(nodeStatusKey(namespace.Default, id, "a")), deadline, "node status after CommitNode")
		requireOutlivesDeadline(t, srv.TTL(remainingNodesKey(namespace.Default, id)), deadline, "remaining counter after CommitNode")
	})

	t.Run("PutOutput", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("replica-put-output")
		deadline := createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())
		if err := newReplica(rdb).PutOutput(ctx, id, "start", map[string]any{"x": 1}); err != nil {
			t.Fatalf("PutOutput: %v", err)
		}
		requireOutlivesDeadline(t, srv.TTL(outputKey(namespace.Default, id, "start")), deadline, "output after PutOutput")
	})
}

// TestPausedThenResumedExecutionStillTimesOut is the end-to-end failure B1
// produced: pause and resume on other replicas, let two default retentions pass,
// and the execution must still be there and still be due. Before the fix its
// status key was gone while deadline_ms and the index member lived on, and the
// sweep dropped the member as "missing", so the execution could never time out.
func TestPausedThenResumedExecutionStillTimesOut(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("replica-pause-resume-timeout")
	deadline := createLongBudgetExecution(t, ctx, srv, creator, id, testDurableGraph())

	if err := newReplica(rdb).PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution: %v", err)
	}
	if err := newReplica(rdb).ResumeExecution(ctx, id); err != nil {
		t.Fatalf("ResumeExecution: %v", err)
	}
	srv.FastForward(2 * replicaDefaultRetention)

	reader := newReplica(rdb)
	snap, err := reader.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution after two default retentions = (%+v, %v), want the execution to still exist", snap, err)
	}
	if snap.Deadline.UnixMilli() != deadline.UnixMilli() {
		t.Fatalf("Deadline = %v, want %v", snap.Deadline, deadline)
	}
	due, err := reader.ListDeadlineExceededExecutions(ctx, deadline.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions: %v", err)
	}
	requireListed(t, due, id)
}

// deadlineReadHook counts GETs of any execution's deadline_ms key and can fail
// the first few of them. Counting at the client is the point: getExecTTL sits on
// the hot path of every state write, and an extra Redis round trip per call
// would show up here and nowhere else.
type deadlineReadHook struct {
	reads    atomic.Int64
	failures int64 // the first N reads fail with an injected error
}

func (h *deadlineReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *deadlineReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if cmd.Name() == "get" && len(args) > 1 && strings.HasSuffix(fmt.Sprint(args[1]), ":deadline_ms") {
			if h.reads.Add(1) <= h.failures {
				return errors.New("injected deadline read failure")
			}
		}
		return next(ctx, cmd)
	}
}

func (h *deadlineReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestGetExecTTLReadsTheDeadlineOncePerReplica pins the cost of folding the
// deadline into getExecTTL: one read per replica per execution, remembered
// afterwards. An execution with no time budget is remembered too, otherwise
// every write to every timeout-less execution would pay the extra GET.
func TestGetExecTTLReadsTheDeadlineOncePerReplica(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		deadline time.Time
	}{
		{"with a deadline", time.Now().Add(replicaBudget)},
		{"without a deadline", time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rdb, creator := newMiniredisStore(t)
			id := types.ExecutionID("replica-reads")
			createWithDeadline(t, creator, ctx, id, tc.deadline)

			hook := &deadlineReadHook{}
			rdb.AddHook(hook)
			other := newReplica(rdb)
			var ttl time.Duration
			for i := 0; i < 5; i++ {
				ttl = other.getExecTTL(ctx, id)
			}
			if got := hook.reads.Load(); got != 1 {
				t.Fatalf("5 getExecTTL calls on one replica read deadline_ms %d times, want exactly 1", got)
			}
			if tc.deadline.IsZero() {
				if ttl != replicaDefaultRetention {
					t.Fatalf("getExecTTL without a deadline = %v, want the %v default", ttl, replicaDefaultRetention)
				}
				return
			}
			requireOutlivesDeadline(t, ttl, tc.deadline, "getExecTTL on another replica")
		})
	}
}

// TestCreationPathsRememberTheDeadline: the replica that creates an execution
// already holds its deadline, so its own getExecTTL must not read it back. A zero
// deadline is remembered as "no budget" the same way.
func TestCreationPathsRememberTheDeadline(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(replicaBudget)
	for _, tc := range []struct {
		name   string
		create func(t *testing.T, state *Store) types.ExecutionID
	}{
		{"CreateExecution", func(t *testing.T, state *Store) types.ExecutionID {
			createWithDeadline(t, state, ctx, "remember-create", deadline)
			return "remember-create"
		}},
		{"CreateExecution without a deadline", func(t *testing.T, state *Store) types.ExecutionID {
			createWithDeadline(t, state, ctx, "remember-create-none", time.Time{})
			return "remember-create-none"
		}},
		{"SeedExecution", func(t *testing.T, state *Store) types.ExecutionID {
			if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{Execution: &engine.ExecutionSnapshot{
				ID: "remember-seed", Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
			}}); err != nil {
				t.Fatalf("SeedExecution: %v", err)
			}
			return "remember-seed"
		}},
		{"SeedExecutionFromEntry", func(t *testing.T, state *Store) types.ExecutionID {
			req := timeoutSeedRequest(t, "remember-entry", "remember-entry-key", replicaBudget)
			resp, err := state.SeedExecutionFromEntry(ctx, req)
			if err != nil || resp.State != engine.AdmissionStateAccepted {
				t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
			}
			return resp.ExecutionID
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rdb, state := newMiniredisStore(t)
			hook := &deadlineReadHook{}
			rdb.AddHook(hook)
			id := tc.create(t, state)
			hook.reads.Store(0)
			for i := 0; i < 5; i++ {
				state.getExecTTL(ctx, id)
			}
			if got := hook.reads.Load(); got != 0 {
				t.Fatalf("the creating replica read deadline_ms %d times after creation, want 0: it already knows the deadline", got)
			}
		})
	}
}

// TestFailedDeadlineReadIsNotRemembered: a read that fails says nothing about the
// execution, so it must not be remembered as "no time budget". If it were, one
// transient Redis error would leave this replica unable to extend that
// execution's retention for as long as the process runs.
func TestFailedDeadlineReadIsNotRemembered(t *testing.T) {
	ctx := context.Background()
	_, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("replica-read-fails")
	deadline := time.Now().Add(replicaBudget)
	createWithDeadline(t, creator, ctx, id, deadline)

	hook := &deadlineReadHook{failures: 1}
	rdb.AddHook(hook)
	other := newReplica(rdb)

	if got := other.getExecTTL(ctx, id); got != replicaDefaultRetention {
		t.Fatalf("getExecTTL with the deadline unreadable = %v, want the %v base retention", got, replicaDefaultRetention)
	}
	requireOutlivesDeadline(t, other.getExecTTL(ctx, id), deadline, "getExecTTL after the read recovered")
	other.getExecTTL(ctx, id)
	if got := hook.reads.Load(); got != 2 {
		t.Fatalf("deadline_ms was read %d times over 3 calls, want 2 (the failure is retried, the success is remembered)", got)
	}
}
