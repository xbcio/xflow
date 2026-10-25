//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestRedisClusterSlotSafety exercises the cluster-sensitive transitions of the
// distributed backend against a REAL Redis Cluster.
//
// The package's unit tests pin the hash-tag scheme statically (a representative
// set of execution keys carries {id}, every entry-activation script touches
// only workflow-digest keys) and run against miniredis or a single-node Redis. Neither can fail with
// CROSSSLOT: a single-node server has no slots, and miniredis does not enforce
// them. The failure mode they cannot see is a Lua script or multi-key command
// whose KEYS span slots — the backend works in every other test and returns
// "CROSSSLOT Keys in request don't hash to the same slot" only on a real
// cluster, which is exactly how a hash-tag regression would reach production.
//
// The test is opt-in: it skips unless XFLOW_TEST_REDIS_CLUSTER_ADDRS names a
// cluster (comma-separated host:port seeds), because CI has no cluster. It
// clears xflow:* keys before and after itself, so point it at a DEDICATED
// cluster only — never at a shared Redis:
//
//	XFLOW_TEST_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
//	  go test ./test/integration/ -tags integration -run TestRedisClusterSlotSafety -count=1
//
// The operations are the ones whose scripts or batches key by execution or
// workflow: create + outbox, route-checked lease acquire, a commit that folds
// downstream arrivals, the single-MGET output fan-in, entry-activation
// desired-state/assignment, the outbox ready-index and dead-letter replay, and
// suspend/signal. Each is driven through the public engine interfaces exactly
// as the engine drives it; a CROSSSLOT anywhere fails the operation and the
// subtest.
func TestRedisClusterSlotSafety(t *testing.T) {
	addrs := redisClusterAddrs(t)

	// Build the backend through the production factory so the test also covers
	// the path that actually runs in a cluster deployment: RedisConfig ->
	// redis.NewUniversalClient with IsClusterMode pinned (see redisconfig.go).
	// The legacy single-address path would silently build a non-cluster client
	// for a one-seed configuration and never follow MOVED; this asserts the
	// factory really produces a cluster client.
	b, err := distributed.New("", nil,
		distributed.WithRedisConfig(distributed.RedisConfig{
			Mode:  distributed.RedisModeCluster,
			Addrs: addrs,
		}),
		distributed.WithConsumer(false),
		distributed.WithExecTTL(time.Minute),
	)
	if err != nil {
		t.Fatalf("distributed.New(cluster): %v", err)
	}
	rdb, ok := b.RedisClient().(*redis.ClusterClient)
	if !ok {
		t.Fatalf("distributed.New(cluster) built %T, want *redis.ClusterClient: a non-cluster client "+
			"would answer MOVED as a hard error instead of following it", b.RedisClient())
	}
	ctx := context.Background()
	requireHealthyCluster(t, ctx, rdb)

	// A dedicated cluster is assumed (documented above); each run starts and
	// ends with an empty xflow namespace so a leftover execution from an
	// earlier failure cannot crowd this run's discovery page.
	clearClusterXflowKeys(t, rdb)
	t.Cleanup(func() { clearClusterXflowKeys(t, rdb) })

	state := b.State()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatalf("%T does not implement engine.AtomicStateStore", state)
	}
	routeChecked, ok := state.(engine.RouteCheckedLeaseAcquirer)
	if !ok {
		t.Fatalf("%T does not implement engine.RouteCheckedLeaseAcquirer", state)
	}
	batchOutput, ok := state.(engine.BatchOutputReader)
	if !ok {
		t.Fatalf("%T does not implement engine.BatchOutputReader", state)
	}
	outboxLeaser, ok := state.(engine.OutboxLeaser)
	if !ok {
		t.Fatalf("%T does not implement engine.OutboxLeaser", state)
	}
	outboxReleaser, ok := state.(engine.OutboxReleaser)
	if !ok {
		t.Fatalf("%T does not implement engine.OutboxReleaser", state)
	}
	outboxFailures, ok := state.(engine.OutboxFailureRecorder)
	if !ok {
		t.Fatalf("%T does not implement engine.OutboxFailureRecorder", state)
	}
	deadLetters, ok := state.(engine.DeadLetterStore)
	if !ok {
		t.Fatalf("%T does not implement engine.DeadLetterStore", state)
	}

	runID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	execID := func(name string) types.ExecutionID {
		return types.ExecutionID("cluster-verify-" + runID + "-" + name)
	}

	t.Run("create and outbox", func(t *testing.T) {
		id := execID("create")
		g := clusterVerifyTwoNodeGraph(t)
		entry := engine.OutboxEntry{
			ID:   string(id) + "/start/1",
			Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1},
		}
		if err := atomic.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{entry}); err != nil {
			t.Fatalf("CreateExecutionWithOutbox: %v", err)
		}

		// A ready-discovery read and the per-execution ordered set are separate
		// keys of one execution; both must stay inside the {id} slot.
		entries, err := atomic.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("ListOutbox: %v", err)
		}
		if len(entries) != 1 || entries[0].ID != entry.ID {
			t.Fatalf("ListOutbox = %+v, want the one created entry %q", entries, entry.ID)
		}
		claimed, err := outboxLeaser.LeaseOutbox(ctx, id, time.Now().UTC(), 10)
		if err != nil {
			t.Fatalf("LeaseOutbox: %v", err)
		}
		if len(claimed) != 1 || claimed[0].ID != entry.ID {
			t.Fatalf("LeaseOutbox = %+v, want the one created entry %q", claimed, entry.ID)
		}
		if err := outboxReleaser.ReleaseOutbox(ctx, id, claimed[0]); err != nil {
			t.Fatalf("ReleaseOutbox: %v", err)
		}
		if err := atomic.AckOutbox(ctx, id, entry.ID); err != nil {
			t.Fatalf("AckOutbox: %v", err)
		}
	})

	t.Run("route-checked lease", func(t *testing.T) {
		id := execID("lease")
		g := clusterVerifyTwoNodeGraph(t)
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		idx, _ := g.NodeIndex("start")
		lease := clusterVerifyLease(id, "start", idx, g.UnitIndexForNode(idx))
		res, err := routeChecked.AcquireTaskLeaseChecked(ctx, lease, engine.LeaseAcquireOptions{})
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked: %v", err)
		}
		if !res.Acquired || !res.ExecutionFound {
			t.Fatalf("AcquireTaskLeaseChecked = %+v, want an acquired lease on a found execution", res)
		}
		node, err := state.GetNode(ctx, id, "start")
		if err != nil || node == nil {
			t.Fatalf("GetNode(start) = %+v, %v", node, err)
		}
		lease.Attempt = node.Attempt

		// Fenced commit of the leased node, then prove the route check refuses a
		// terminal node in an acyclic graph. Both decisions and their writes are
		// one Lua transition over keys of this execution.
		commit := clusterVerifyCommitRequest(lease, types.NodeStatusSuccess)
		if res, err := atomic.CommitNode(ctx, commit); err != nil {
			t.Fatalf("CommitNode(start): %v", err)
		} else if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(start) = %+v, want an applied accepted commit", res)
		}
		again := clusterVerifyLease(id, "start", idx, g.UnitIndexForNode(idx))
		res, err = routeChecked.AcquireTaskLeaseChecked(ctx, again, engine.LeaseAcquireOptions{})
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked(terminal node): %v", err)
		}
		if res.Acquired || !res.ExecutionFound {
			t.Fatalf("AcquireTaskLeaseChecked(terminal node) = %+v, want a refusal on a found execution", res)
		}

		// The execution-liveness arm: a live-but-terminal execution refuses a
		// route that would otherwise be acquirable.
		if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, "cluster verify"); err != nil {
			t.Fatalf("UpdateExecutionStatus: %v", err)
		}
		endIdx, _ := g.NodeIndex("end")
		terminal := clusterVerifyLease(id, "end", endIdx, g.UnitIndexForNode(endIdx))
		res, err = routeChecked.AcquireTaskLeaseChecked(ctx, terminal, engine.LeaseAcquireOptions{})
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked(terminal execution): %v", err)
		}
		if res.Acquired || !res.ExecutionFound || res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("AcquireTaskLeaseChecked(terminal execution) = %+v, want refusal with success status", res)
		}
	})

	t.Run("commit with folded arrivals", func(t *testing.T) {
		id := execID("arrivals")
		g := clusterVerifyFanInGraph(t)
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		joinIdx, _ := g.NodeIndex("join")
		joinUnit := g.UnitIndexForNode(joinIdx)
		// The fan-in has two inbound edges. Arrival folding is the case where one
		// commit touches the source node state, the destination's in-degree,
		// active-input and schedule counters, and the outbox write of the
		// resolved intent — all in one script, all under {id}.
		for _, source := range []string{"left", "right"} {
			srcIdx, _ := g.NodeIndex(source)
			lease := clusterVerifyLease(id, source, srcIdx, g.UnitIndexForNode(srcIdx))
			res, err := routeChecked.AcquireTaskLeaseChecked(ctx, lease, engine.LeaseAcquireOptions{})
			if err != nil || !res.Acquired {
				t.Fatalf("AcquireTaskLeaseChecked(%s) = %+v, %v", source, res, err)
			}
			node, err := state.GetNode(ctx, id, source)
			if err != nil || node == nil {
				t.Fatalf("GetNode(%s) = %+v, %v", source, node, err)
			}
			lease.Attempt = node.Attempt
			req := clusterVerifyCommitRequest(lease, types.NodeStatusSuccess)
			req.Arrivals = []engine.DownstreamArrival{{
				NodeName:     "join",
				NodeIdx:      joinIdx,
				UnitIdx:      joinUnit,
				ArrivalCount: 1,
				ActiveCount:  1,
			}}
			committed, err := atomic.CommitNode(ctx, req)
			if err != nil {
				t.Fatalf("CommitNode(%s, folded arrival): %v", source, err)
			}
			if committed.Outcome != engine.CommitOutcomeAccepted || !committed.Applied {
				t.Fatalf("CommitNode(%s) = %+v, want an applied accepted commit", source, committed)
			}
			if len(committed.Skipped) != 0 {
				t.Fatalf("CommitNode(%s).Skipped = %v, want none: the incoming port was active", source, committed.Skipped)
			}
			entries, err := atomic.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
			if err != nil {
				t.Fatalf("ListOutbox(%s): %v", source, err)
			}
			if source == "left" {
				if len(entries) != 0 {
					t.Fatalf("after the first arrival %d intents were scheduled, want 0 until the in-degree reaches zero", len(entries))
				}
				continue
			}
			if len(entries) != 1 || entries[0].Task.NodeName != "join" || entries[0].Task.Type != engine.TaskTypeNodeExec {
				t.Fatalf("after the folded arrivals ListOutbox = %+v, want exactly the join execute intent", entries)
			}
		}
	})

	t.Run("batch output MGET", func(t *testing.T) {
		id := execID("outputs")
		if err := state.PutOutput(ctx, id, "alpha", map[string]any{"value": "a"}); err != nil {
			t.Fatalf("PutOutput(alpha): %v", err)
		}
		if err := state.PutOutput(ctx, id, "beta", map[string]any{"value": "b"}); err != nil {
			t.Fatalf("PutOutput(beta): %v", err)
		}
		// GetOutputs is a single MGET over per-name keys. Redis Cluster rejects a
		// multi-key MGET whose keys hash to different slots, so this call is the
		// end-to-end assertion that the outputs of one execution share {id}; a
		// per-name GET fallback would not exercise the batch.
		got, err := batchOutput.GetOutputs(ctx, id, []string{"alpha", "beta", "alpha", "missing"})
		if err != nil {
			t.Fatalf("GetOutputs: %v", err)
		}
		if len(got) != 2 || got["alpha"]["value"] != "a" || got["beta"]["value"] != "b" {
			t.Fatalf("GetOutputs = %#v, want alpha=a and beta=b", got)
		}
	})

	t.Run("entry activation", func(t *testing.T) {
		store := b.NewEntryActivationStore(time.Minute)
		revisions, ok := store.(engine.EntryActivationRevisionStore)
		if !ok {
			t.Fatalf("%T does not implement engine.EntryActivationRevisionStore", store)
		}
		ns := namespace.Namespace("cluster-verify")
		wf := types.WorkflowID("wf-" + runID)
		key := engine.EntryActivationKey{
			Namespace:       ns,
			WorkflowID:      wf,
			WorkflowVersion: "v1",
			EntryUnitID:     "unit-a",
		}
		activation := engine.EntryActivation{
			Namespace:        ns,
			WorkflowID:       wf,
			WorkflowVersion:  "v1",
			EntryUnitID:      "unit-a",
			NodeType:         "http.request",
			PackageHash:      "pkg-a",
			Desired:          true,
			RegistryRevision: 3,
		}

		// The revision watermark, the activation record and the per-workflow
		// index are three keys. They must hash to one slot, or every Upsert and
		// assignment script fails on the cluster while passing on miniredis.
		if err := revisions.AdvanceWorkflowRevision(ctx, ns, wf, 2); err != nil {
			t.Fatalf("AdvanceWorkflowRevision: %v", err)
		}
		below := activation
		below.RegistryRevision = 1
		if err := store.Upsert(ctx, below); err != nil {
			t.Fatalf("Upsert(below watermark): %v", err)
		}
		if _, found, err := store.Get(ctx, key); err != nil {
			t.Fatalf("Get(after below-watermark upsert): %v", err)
		} else if found {
			t.Fatalf("Upsert below the workflow watermark wrote a record; it must be a silent no-op")
		}
		if err := store.Upsert(ctx, activation); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		rec, found, err := store.Get(ctx, key)
		if err != nil || !found {
			t.Fatalf("Get = %+v, %v, want the upserted activation", rec, err)
		}
		if rec.PackageHash != "pkg-a" || rec.NodeType != "http.request" {
			t.Fatalf("Get = %+v, want the desired-state fields round-tripped", rec)
		}
		list, err := store.List(ctx, ns)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(list) != 1 || list[0].EntryUnitID != "unit-a" {
			t.Fatalf("List = %+v, want exactly the one activation", list)
		}

		if assigned, err := store.Assign(ctx, key, "runner-a", "session-a", 1, time.Now().Add(time.Minute)); err != nil {
			t.Fatalf("Assign: %v", err)
		} else if !assigned {
			t.Fatal("Assign(gen 1) = false, want the first claim to win")
		}
		if assigned, err := store.Assign(ctx, key, "runner-b", "session-b", 1, time.Now().Add(time.Minute)); err != nil {
			t.Fatalf("Assign(same gen): %v", err)
		} else if assigned {
			t.Fatal("Assign(same gen) = true, want a first-writer-wins refusal")
		}
		if renewed, err := store.Renew(ctx, key, 1, time.Now().Add(2*time.Minute)); err != nil {
			t.Fatalf("Renew: %v", err)
		} else if !renewed {
			t.Fatal("Renew(gen 1) = false, want the owner's renewal granted")
		}
		if err := store.Fence(ctx, key, 2); err != nil {
			t.Fatalf("Fence: %v", err)
		}
		// Fence raises the generation floor to at least 2, and Assign is
		// strictly monotonic, so the reassignment must be generation 3.
		if assigned, err := store.Assign(ctx, key, "runner-c", "session-c", 3, time.Now().Add(time.Minute)); err != nil {
			t.Fatalf("Assign(gen 3 after fence): %v", err)
		} else if !assigned {
			t.Fatal("Assign(gen 3 after fence) = false, want the fenced generation reassignable")
		}
	})

	t.Run("outbox ready index and dead letter", func(t *testing.T) {
		id := execID("deadletter")
		// The entry ID's leading segment is the intent the replay guard branches
		// on; an unrecognised prefix fails closed as metadata_missing by design.
		entry := engine.OutboxEntry{
			ID:   fmt.Sprintf("execute/%s/finish/1", id),
			Task: engine.Task{ExecutionID: id, NodeName: "finish", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1},
		}
		if err := atomic.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: clusterVerifyTwoNodeGraph(t), Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{entry}); err != nil {
			t.Fatalf("CreateExecutionWithOutbox: %v", err)
		}
		// The ready index is a namespace-scoped ZSET updated outside the
		// per-execution Lua; discovery reads it and then the execution's own
		// keys. Both halves must survive a cluster round trip.
		ids, err := atomic.ListOutboxExecutions(ctx, 100)
		if err != nil {
			t.Fatalf("ListOutboxExecutions: %v", err)
		}
		if !clusterVerifyContainsExec(ids, id) {
			t.Fatalf("ListOutboxExecutions = %v, want it to contain %q", ids, id)
		}

		// Force the entry into dead-letter storage. The dead set, dead bodies,
		// per-entry metadata, the ready index and the replay receipt are all
		// execution-scoped keys written by Lua transitions.
		for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
			if _, err := outboxFailures.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
				t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
			}
		}
		dead, err := deadLetters.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
		if err != nil {
			t.Fatalf("ListDeadLetters: %v", err)
		}
		if len(dead.Entries) != 1 || dead.Entries[0].ID != entry.ID {
			t.Fatalf("ListDeadLetters = %+v, want the one dead entry %q", dead.Entries, entry.ID)
		}
		ready, err := atomic.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("ListOutbox(dead): %v", err)
		}
		if len(ready) != 0 {
			t.Fatalf("ListOutbox(dead) = %+v, want no ready entries", ready)
		}

		// The replay guard is activation-safe and fails closed: it reads the
		// target node's status and activation from keys derived inside the
		// script, so seed the running node the entry targets (the same shape
		// the shared replay contract uses).
		if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
			ExecutionID:  id,
			Name:         "finish",
			NodeIdx:      0,
			Status:       types.NodeStatusRunning,
			ActivationID: 1,
		}); err != nil {
			t.Fatalf("UpsertNode(finish): %v", err)
		}

		replayed, err := deadLetters.ReplayDeadLetter(ctx, engine.ReplayDeadLetterRequest{
			ExecutionID: id,
			EntryID:     entry.ID,
			RequestID:   "replay-" + runID,
			Operator:    "cluster-verify",
			Reason:      "cluster slot safety verification",
		})
		if err != nil {
			t.Fatalf("ReplayDeadLetter: %v", err)
		}
		if replayed.Outcome != engine.ReplayReplayed || replayed.AuditID == "" {
			t.Fatalf("ReplayDeadLetter = %+v, want replayed with a receipt", replayed)
		}
		ready, err = atomic.ListOutbox(ctx, id, time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("ListOutbox(replayed): %v", err)
		}
		if len(ready) != 1 || ready[0].ID != entry.ID {
			t.Fatalf("ListOutbox(replayed) = %+v, want the replayed entry back in the ready set", ready)
		}

		// Drain the execution: the terminating empty claim is what removes it
		// from the ready index.
		claimed, err := outboxLeaser.LeaseOutbox(ctx, id, time.Now().UTC(), 10)
		if err != nil {
			t.Fatalf("LeaseOutbox(drain): %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("LeaseOutbox(drain) = %+v, want the replayed entry", claimed)
		}
		if err := atomic.AckOutbox(ctx, id, claimed[0].ID); err != nil {
			t.Fatalf("AckOutbox(drain): %v", err)
		}
		if _, err := outboxLeaser.LeaseOutbox(ctx, id, time.Now().UTC(), 10); err != nil {
			t.Fatalf("LeaseOutbox(terminating empty claim): %v", err)
		}
		ids, err = atomic.ListOutboxExecutions(ctx, 100)
		if err != nil {
			t.Fatalf("ListOutboxExecutions(after drain): %v", err)
		}
		if clusterVerifyContainsExec(ids, id) {
			t.Fatalf("ListOutboxExecutions(after drain) = %v, still contains %q", ids, id)
		}
	})

	t.Run("suspend and signal", func(t *testing.T) {
		id := execID("suspend")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: clusterVerifyTwoNodeGraph(t), Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}

		// Pre-delivered signal: the payload key, waiter index and suspended set
		// are read/written together when the node checks in.
		if resume, payload, err := state.DeliverSignal(ctx, id, "approval-a", map[string]any{"by": "lead"}); err != nil {
			t.Fatalf("DeliverSignal(pre): %v", err)
		} else if resume != "" || payload != nil {
			t.Fatalf("DeliverSignal(pre) = (%q, %+v), want the signal stored", resume, payload)
		}
		payload, err := state.SuspendOrConsume(ctx, id, "wait-a", &types.SuspendSpec{Signals: []string{"approval-a"}})
		if err != nil {
			t.Fatalf("SuspendOrConsume(pre-delivered): %v", err)
		}
		if payload == nil || payload.Name != "approval-a" || payload.Data["by"] != "lead" {
			t.Fatalf("SuspendOrConsume(pre-delivered) = %+v, want the stored payload", payload)
		}

		// Parked waiter: registration touches the waiter key, the suspended
		// set and the timeout ZSET (all under {id}); delivery resumes it.
		if payload, err := state.SuspendOrConsume(ctx, id, "wait-b", &types.SuspendSpec{
			Signals: []string{"approval-b"},
			Timeout: time.Minute,
		}); err != nil {
			t.Fatalf("SuspendOrConsume(park): %v", err)
		} else if payload != nil {
			t.Fatalf("SuspendOrConsume(park) = %+v, want the node parked", payload)
		}
		suspended, err := state.ListSuspendedNodes(ctx, id)
		if err != nil {
			t.Fatalf("ListSuspendedNodes: %v", err)
		}
		if !clusterVerifyContainsString(suspended, "wait-b") {
			t.Fatalf("ListSuspendedNodes = %v, want it to contain wait-b", suspended)
		}
		resume, payload, err := state.DeliverSignal(ctx, id, "approval-b", map[string]any{"by": "cluster"})
		if err != nil {
			t.Fatalf("DeliverSignal(waiter): %v", err)
		}
		if resume != "wait-b" || payload != nil {
			t.Fatalf("DeliverSignal(waiter) = (%q, %+v), want wait-b resumed", resume, payload)
		}
		if suspended, err = state.ListSuspendedNodes(ctx, id); err != nil {
			t.Fatalf("ListSuspendedNodes(after resume): %v", err)
		}
		if clusterVerifyContainsString(suspended, "wait-b") {
			t.Fatalf("ListSuspendedNodes(after resume) = %v, want wait-b removed", suspended)
		}

		if acquired, err := state.AcquireResumeLock(ctx, id, "wait-b"); err != nil {
			t.Fatalf("AcquireResumeLock: %v", err)
		} else if !acquired {
			t.Fatal("AcquireResumeLock(first) = false, want true")
		}
		if acquired, err := state.AcquireResumeLock(ctx, id, "wait-b"); err != nil {
			t.Fatalf("AcquireResumeLock(second): %v", err)
		} else if acquired {
			t.Fatal("AcquireResumeLock(second) = true, want the lock held")
		}
	})

	t.Run("execution deadline index", func(t *testing.T) {
		clusterVerifyDeadlineIndex(t, ctx, addrs, rdb, state)
	})
}

// redisClusterAddrs parses XFLOW_TEST_REDIS_CLUSTER_ADDRS and skips when it is
// unset. The variable is its own opt-in: unlike the single-node Redis gates,
// there is no localhost default, because pointing this test at an ordinary
// Redis would exercise nothing and pointing it at a shared cluster would clear
// keys that are not ours.
func redisClusterAddrs(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("XFLOW_TEST_REDIS_CLUSTER_ADDRS")
	if raw == "" {
		t.Skip("XFLOW_TEST_REDIS_CLUSTER_ADDRS not set; skipping real Redis Cluster test " +
			"(e.g. 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002)")
	}
	addrs := make([]string, 0, 3)
	for _, part := range strings.Split(raw, ",") {
		if addr := strings.TrimSpace(part); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		t.Fatalf("XFLOW_TEST_REDIS_CLUSTER_ADDRS=%q contains no addresses", raw)
	}
	return addrs
}

// requireHealthyCluster fails unless the cluster reports full slot coverage and
// at least two masters. Two masters make the run meaningful: with one node a
// slot mismatch can still fail with CROSSSLOT, but MOVED routing never happens,
// so half of the deployment behaviour under test would be absent.
func requireHealthyCluster(t *testing.T, ctx context.Context, rdb *redis.ClusterClient) {
	t.Helper()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis cluster at %v is unreachable: %v", rdb.Options().Addrs, err)
	}
	info, err := rdb.ClusterInfo(ctx).Result()
	if err != nil {
		t.Fatalf("CLUSTER INFO: %v", err)
	}
	if !strings.Contains(info, "cluster_state:ok") {
		t.Fatalf("CLUSTER INFO reports a non-ok state:\n%s", info)
	}
	slots, err := rdb.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatalf("CLUSTER SLOTS: %v", err)
	}
	covered := 0
	masters := map[string]struct{}{}
	for _, slot := range slots {
		covered += slot.End - slot.Start + 1
		if len(slot.Nodes) > 0 {
			masters[slot.Nodes[0].Addr] = struct{}{}
		}
	}
	if covered != 16384 {
		t.Fatalf("cluster covers %d slots, want 16384", covered)
	}
	if len(masters) < 2 {
		t.Fatalf("cluster has %d master(s), want at least 2 for meaningful slot routing: %v", len(masters), masters)
	}
	t.Logf("redis cluster: seeder %v, %d masters, 16384 slots covered", rdb.Options().Addrs, len(masters))
}

// clearClusterXflowKeys removes the test backend's key namespace from every
// master. Per-key DEL on purpose: a multi-key DEL whose keys span slots is
// itself a CROSSSLOT and would fail for a reason unrelated to the backend.
func clearClusterXflowKeys(t *testing.T, rdb *redis.ClusterClient) {
	t.Helper()
	ctx := context.Background()
	err := rdb.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		var cursor uint64
		for {
			keys, next, err := node.Scan(ctx, cursor, "xflow:*", 500).Result()
			if err != nil {
				return err
			}
			for _, key := range keys {
				if err := node.Del(ctx, key).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	})
	if err != nil {
		t.Fatalf("clear xflow:* from cluster: %v", err)
	}
}

// clusterVerifyTwoNodeGraph is the smallest connected acyclic graph: the create
// path sees a real edge, and the second node is available for the
// execution-liveness arm of the route-checked lease.
func clusterVerifyTwoNodeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "cluster-verify-two-node",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.start"}, {Name: "end", Type: "test.echo"}},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "end", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile two-node graph: %v", err)
	}
	return g
}

// clusterVerifyFanInGraph gives the arrival-folding commit two inbound edges to
// one destination, which is what makes the in-degree counter, rather than a
// single arrival, decide when the join is scheduled.
func clusterVerifyFanInGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "cluster-verify-fan-in",
		Nodes: []types.NodeDef{
			{Name: "left", Type: "test.start"},
			{Name: "right", Type: "test.start"},
			{Name: "join", Type: "test.join"},
		},
		Connections: types.Connections{
			"left":  {"main": types.PortConnections{Targets: []types.Connection{{Node: "join", Input: "main"}}}},
			"right": {"main": types.PortConnections{Targets: []types.Connection{{Node: "join", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile fan-in graph: %v", err)
	}
	return g
}

// clusterVerifyLease builds a first-generation lease. The attempt is filled in
// from the stored node after the acquire, because the store, not the caller,
// assigns it.
func clusterVerifyLease(id types.ExecutionID, node string, nodeIdx, unitIdx int) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID:    engine.LeaseID(fmt.Sprintf("lease/%s/%s", id, node)),
		LeaseToken: engine.LeaseToken(fmt.Sprintf("token/%s/%s", id, node)),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     node,
			NodeIdx:      nodeIdx,
			UnitIdx:      unitIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
}

func clusterVerifyCommitRequest(lease *engine.TaskLease, status types.NodeStatus) engine.CommitNodeRequest {
	return engine.CommitNodeRequest{
		ExecutionID:  lease.Task.ExecutionID,
		NodeName:     lease.Task.NodeName,
		NodeIdx:      lease.Task.NodeIdx,
		UnitIdx:      lease.Task.UnitIdx,
		ActivationID: lease.Task.ActivationID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       status,
	}
}

func clusterVerifyContainsExec(ids []types.ExecutionID, want types.ExecutionID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func clusterVerifyContainsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// clusterVerifyDeadlineIndex drives the execution-deadline index against the
// real cluster. The index (xflow:ns:<ns>:exec_deadlines) is the one
// namespace-wide key the deadline feature adds and, unlike every key of an
// execution, it carries no {id} hash tag. It therefore must never be a KEYS
// entry of a per-execution Lua script, and the scripts that run beside it
// (updateExecutionStatusLua, checkCompletionLua) must keep refusing a missing
// execution without touching anything. Only a real cluster fails these with
// CROSSSLOT, so every step names the keys it touches.
func clusterVerifyDeadlineIndex(t *testing.T, ctx context.Context, addrs []string, rdb *redis.ClusterClient, state engine.StateStore) {
	t.Helper()
	expirer, ok := state.(engine.ExecutionDeadlineExpirer)
	if !ok {
		t.Fatalf("%T does not implement engine.ExecutionDeadlineExpirer", state)
	}
	ns := namespace.Namespace("cluster-verify-deadline")
	ctx = namespace.WithNamespace(ctx, ns)
	runID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	indexKey := fmt.Sprintf("xflow:ns:%s:exec_deadlines", ns)
	execKey := func(id types.ExecutionID, suffix string) string {
		return fmt.Sprintf("xflow:ns:%s:exec:{%s}:%s", ns, id, suffix)
	}
	check := func(op string, err error, keys ...string) {
		t.Helper()
		if err == nil {
			return
		}
		if strings.Contains(err.Error(), "CROSSSLOT") {
			t.Fatalf("%s: CROSSSLOT over keys %q: %v", op, keys, err)
		}
		t.Fatalf("%s (keys %q): %v", op, keys, err)
	}

	// Twelve executions whose {id} keys spread over the slots, plus the index
	// key. The run is meaningless if every key happened to share one slot.
	const total = 12
	base := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Millisecond)
	far := base.Add(time.Hour)
	ids := make([]types.ExecutionID, total)
	for i := range ids {
		ids[i] = types.ExecutionID(fmt.Sprintf("cluster-verify-dl-%s-%02d", runID, i))
	}
	indexSlot, err := rdb.ClusterKeySlot(ctx, indexKey).Result()
	check("CLUSTER KEYSLOT", err, indexKey)
	slots := map[int64]struct{}{}
	apart := 0
	for _, id := range ids {
		slot, err := rdb.ClusterKeySlot(ctx, execKey(id, "status")).Result()
		check("CLUSTER KEYSLOT", err, execKey(id, "status"))
		slots[slot] = struct{}{}
		if slot != indexSlot {
			apart++
		}
	}
	if len(slots) < 2 || apart == 0 {
		t.Fatalf("the %d execution keys hash to %d slot(s), %d apart from the index slot %d: "+
			"the run would not exercise cross-slot routing", total, len(slots), apart, indexSlot)
	}
	graphs := clusterVerifyTwoNodeGraph(t)
	deadlineOf := func(i int) time.Time { return base.Add(time.Duration(i) * time.Second) }
	listAt := func(before time.Time, limit int) []engine.DeadlineExpiredExecution {
		t.Helper()
		got, err := expirer.ListDeadlineExceededExecutions(ctx, before, limit)
		check("ListDeadlineExceededExecutions", err, indexKey)
		var mine []engine.DeadlineExpiredExecution
		for _, item := range got {
			if item.Namespace == ns {
				mine = append(mine, item)
			}
		}
		return mine
	}
	requireListed := func(what string, got []engine.DeadlineExpiredExecution, want ...types.ExecutionID) {
		t.Helper()
		have := make([]types.ExecutionID, 0, len(got))
		for _, item := range got {
			have = append(have, item.ExecutionID)
		}
		if !slices.Equal(have, want) {
			t.Fatalf("%s: listed %v, want %v", what, have, want)
		}
	}
	inIndex := func(id types.ExecutionID) bool {
		t.Helper()
		_, err := rdb.ZScore(ctx, indexKey, string(id)).Result()
		if errors.Is(err, redis.Nil) {
			return false
		}
		check("ZSCORE", err, indexKey)
		return true
	}
	exists := func(key string) bool {
		t.Helper()
		n, err := rdb.Exists(ctx, key).Result()
		check("EXISTS", err, key)
		return n > 0
	}
	statusOf := func(id types.ExecutionID) types.ExecutionStatus {
		t.Helper()
		snap, err := state.GetExecution(ctx, id)
		check("GetExecution", err, execKey(id, "status"))
		if snap == nil {
			t.Fatalf("GetExecution(%q) = nil, want the execution", id)
		}
		return snap.Status
	}
	setStatus := func(id types.ExecutionID, status types.ExecutionStatus, reason string) {
		t.Helper()
		check("UpdateExecutionStatus("+string(status)+")",
			state.UpdateExecutionStatus(ctx, id, status, reason),
			execKey(id, "status"), execKey(id, "error"), indexKey)
	}
	succeedNodes := func(id types.ExecutionID) {
		t.Helper()
		for idx, name := range []string{"start", "end"} {
			check("UpsertNode("+name+")", state.UpsertNode(ctx, &engine.NodeSnapshot{
				ExecutionID: id, Name: name, NodeIdx: idx, Status: types.NodeStatusSuccess, ActivationID: 1,
			}), execKey(id, "node:"+name+":status"))
		}
	}

	// 1. Registration. Each create writes its {id} keys in one transaction and
	// its member into the shared index with an ordinary ZADD beforehand.
	for i, id := range ids {
		check("CreateExecution", state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: graphs, Status: types.ExecutionStatusRunning, Deadline: deadlineOf(i),
		}), execKey(id, "status"), execKey(id, "deadline_ms"), indexKey)
	}
	if n, err := rdb.ZCard(ctx, indexKey).Result(); err != nil || n != total {
		t.Fatalf("ZCARD %s = %d, %v, want %d members", indexKey, n, err, total)
	}
	got := listAt(far, 100)
	requireListed("all due", got, ids...)
	for i, item := range got {
		if item.Deadline.UnixMilli() != deadlineOf(i).UnixMilli() {
			t.Fatalf("listed %q Deadline = %v, want %v", item.ExecutionID, item.Deadline, deadlineOf(i))
		}
	}
	requireListed("not yet due", listAt(base.Add(-time.Millisecond), 100))
	requireListed("boundary is inclusive", listAt(deadlineOf(2), 100), ids[0], ids[1], ids[2])
	requireListed("page bound", listAt(far, 5), ids[:5]...)
	// 2. Withdrawal. success, failed and canceled drop the member in the
	// terminal write (an ordinary ZREM beside the Lua), and so does the commit
	// path that finalizes an execution from its node statuses. timeout keeps the
	// member until AckDeadlineExpired.
	setStatus(ids[0], types.ExecutionStatusSuccess, "")
	setStatus(ids[1], types.ExecutionStatusFailed, "cluster verify failure")
	setStatus(ids[2], types.ExecutionStatusCanceled, "")
	for i, want := range []types.ExecutionStatus{
		types.ExecutionStatusSuccess, types.ExecutionStatusFailed, types.ExecutionStatusCanceled,
	} {
		if got := statusOf(ids[i]); got != want {
			t.Fatalf("status of %q = %q, want %q", ids[i], got, want)
		}
		if inIndex(ids[i]) {
			t.Fatalf("%q is still in %s after %s", ids[i], indexKey, want)
		}
	}
	succeedNodes(ids[4])
	if done, failed, err := state.CheckCompletion(ctx, ids[4], 2); err != nil || !done || failed {
		check("CheckCompletion(complete)", err, execKey(ids[4], "status"), execKey(ids[4], "node:start:status"))
		t.Fatalf("CheckCompletion(%q) = (%v, %v), want (true, false)", ids[4], done, failed)
	}
	if got := statusOf(ids[4]); got != types.ExecutionStatusSuccess || inIndex(ids[4]) {
		t.Fatalf("after CheckCompletion %q has status %q (indexed=%v), want success withdrawn", ids[4], got, inIndex(ids[4]))
	}
	setStatus(ids[3], types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError)
	if got := statusOf(ids[3]); got != types.ExecutionStatusTimeout || !inIndex(ids[3]) {
		t.Fatalf("after timeout %q has status %q (indexed=%v), want timeout kept in the index", ids[3], got, inIndex(ids[3]))
	}
	requireListed("timeout stays until ack", listAt(far, 100), append([]types.ExecutionID{ids[3]}, ids[5:]...)...)
	for range 2 {
		check("AckDeadlineExpired", expirer.AckDeadlineExpired(ctx, ids[3]), indexKey)
	}
	check("AckDeadlineExpired(unknown)", expirer.AckDeadlineExpired(ctx, types.ExecutionID("cluster-verify-dl-"+runID+"-unknown")), indexKey)
	requireListed("after ack", listAt(far, 100), ids[5:]...)
	// 3. Absent executions. Both Lua scripts must refuse a missing execution
	// without writing anything. A never-created id has no keys at all; ids[8] is
	// an execution whose status key is gone while its node keys, graph cache and
	// index member survive, which is the shape that would otherwise make the
	// scripts resurrect a bare status key.
	ghost := types.ExecutionID("cluster-verify-dl-" + runID + "-ghost")
	setStatus(ghost, types.ExecutionStatusRunning, "")
	setStatus(ghost, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError)
	if done, failed, err := state.CheckCompletion(ctx, ghost, 2); err != nil || done || failed {
		check("CheckCompletion(ghost)", err, execKey(ghost, "status"))
		t.Fatalf("CheckCompletion(ghost) = (%v, %v), want (false, false)", done, failed)
	}
	if snap, err := state.GetExecution(ctx, ghost); err != nil || snap != nil {
		check("GetExecution(ghost)", err, execKey(ghost, "status"))
		t.Fatalf("GetExecution(ghost) = %+v, want nil", snap)
	}
	var leaked []string
	check("SCAN ghost keys", rdb.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		keys, _, err := node.Scan(ctx, 0, execKey(ghost, "*"), 1000).Result()
		leaked = append(leaked, keys...)
		return err
	}))
	if len(leaked) != 0 {
		t.Fatalf("writes for the missing execution %q left keys behind: %v", ghost, leaked)
	}

	succeedNodes(ids[8])
	check("DEL status", rdb.Del(ctx, execKey(ids[8], "status")).Err(), execKey(ids[8], "status"))
	if done, failed, err := state.CheckCompletion(ctx, ids[8], 2); err != nil || done || failed {
		check("CheckCompletion(status gone)", err, execKey(ids[8], "status"), execKey(ids[8], "node:start:status"))
		t.Fatalf("CheckCompletion(status gone) = (%v, %v), want (false, false)", done, failed)
	}
	setStatus(ids[8], types.ExecutionStatusRunning, "")
	setStatus(ids[8], types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError)
	for _, suffix := range []string{"status", "error"} {
		if exists(execKey(ids[8], suffix)) {
			t.Fatalf("%s was written for an execution whose status key is gone", execKey(ids[8], suffix))
		}
	}
	// The orphaned member is repaired by the read that finds it (a ZREM and a
	// re-check, all on the index key).
	requireListed("orphaned member repaired", listAt(far, 100), ids[5], ids[6], ids[7], ids[9], ids[10], ids[11])
	if inIndex(ids[8]) {
		t.Fatalf("%s still holds %q after a listing found its execution gone", indexKey, ids[8])
	}
	// 4. canceling fence. canceling is not terminal: the member stays, success
	// is refused by the fence in updateExecutionStatusLua, CheckCompletion
	// refuses to finalize it, and only canceled concludes it.
	cancelID := ids[9]
	setStatus(cancelID, types.ExecutionStatusCanceling, "")
	if got := statusOf(cancelID); got != types.ExecutionStatusCanceling || !inIndex(cancelID) {
		t.Fatalf("after canceling %q has status %q (indexed=%v), want canceling and still indexed", cancelID, got, inIndex(cancelID))
	}
	setStatus(cancelID, types.ExecutionStatusSuccess, "")
	if got := statusOf(cancelID); got != types.ExecutionStatusCanceling {
		t.Fatalf("success over canceling left %q in status %q, want the fence to keep canceling", cancelID, got)
	}
	succeedNodes(cancelID)
	if done, failed, err := state.CheckCompletion(ctx, cancelID, 2); err != nil || done || failed {
		check("CheckCompletion(canceling)", err, execKey(cancelID, "status"), execKey(cancelID, "node:start:status"))
		t.Fatalf("CheckCompletion(canceling) = (%v, %v), want (false, false)", done, failed)
	}
	if got := statusOf(cancelID); got != types.ExecutionStatusCanceling || !inIndex(cancelID) {
		t.Fatalf("CheckCompletion changed canceling %q to %q (indexed=%v)", cancelID, got, inIndex(cancelID))
	}
	setStatus(cancelID, types.ExecutionStatusCanceled, "")
	if got := statusOf(cancelID); got != types.ExecutionStatusCanceled || inIndex(cancelID) {
		t.Fatalf("after canceled %q has status %q (indexed=%v), want canceled and withdrawn", cancelID, got, inIndex(cancelID))
	}

	// 5. Retention read path. A second backend is another replica: its deadline
	// memo is empty, so its first status write reads deadline_ms back through
	// the EXISTS+GET transaction of executionDeadline (two keys of one {id}
	// slot). That read logs and degrades to the default retention on any
	// failure, so a CROSSSLOT would be invisible except for the TTL it leaves:
	// the status key would carry the one-minute default instead of outliving
	// the deadline by the grace hour.
	replica, err := distributed.New("", nil,
		distributed.WithRedisConfig(distributed.RedisConfig{Mode: distributed.RedisModeCluster, Addrs: addrs}),
		distributed.WithConsumer(false),
		distributed.WithExecTTL(time.Minute),
	)
	if err != nil {
		t.Fatalf("distributed.New(cluster replica): %v", err)
	}
	stopReplica, err := replica.StartBinding(engine.New(replica.State(), replica.Queue()))
	if err != nil {
		t.Fatalf("replica StartBinding: %v", err)
	}
	t.Cleanup(stopReplica)
	retained := ids[10]
	statusKey := execKey(retained, "status")
	before, err := rdb.PTTL(ctx, statusKey).Result()
	check("PTTL", err, statusKey)
	if before < 2*time.Hour {
		t.Fatalf("PTTL %s = %v after create, want retention past the deadline (over 2h)", statusKey, before)
	}
	check("replica UpdateExecutionStatus", replica.State().UpdateExecutionStatus(ctx, retained, types.ExecutionStatusRunning, ""),
		statusKey, execKey(retained, "deadline_ms"))
	after, err := rdb.PTTL(ctx, statusKey).Result()
	check("PTTL", err, statusKey)
	if after < 2*time.Hour {
		t.Fatalf("PTTL %s = %v after a write from a replica with an empty memo, want retention past the deadline (over 2h): "+
			"the EXISTS+GET deadline read did not run or did not return", statusKey, after)
	}
}
