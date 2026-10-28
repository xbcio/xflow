package rstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// requireTTLNearDeadline fails unless the key's TTL is the deadline plus the
// grace to within a couple of minutes, in both directions. requireOutlivesDeadline
// only bounds it from below, which a TTL that overshoots by hours or days would
// also satisfy.
//
// The grace is written as a literal hour on purpose. Every other helper derives
// its expectation from deadlineGrace, so a change to that constant moves the
// expectation with it and nothing notices; this one is the fixed reference.
func requireTTLNearDeadline(t *testing.T, got time.Duration, deadline time.Time, what string) {
	t.Helper()
	want := time.Until(deadline) + time.Hour
	if diff := got - want; diff < -2*time.Minute || diff > 2*time.Minute {
		t.Fatalf("%s TTL = %v, want about %v (the deadline plus one hour of grace)", what, got, want)
	}
}

// TestRetentionPastDeadlineAddsOneHourOfGrace pins the grace itself, against a
// literal rather than the constant: a deadline past the retention gets the time
// left to it plus an hour, and one already behind us is still covered for the
// part of the hour that remains.
func TestRetentionPastDeadlineAddsOneHourOfGrace(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		deadline time.Time
		want     time.Duration
	}{
		{"deadline ahead", now.Add(3 * time.Hour), 4 * time.Hour},
		{"deadline just passed", now.Add(-10 * time.Minute), 50 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := retentionPastDeadline(time.Minute, tc.deadline)
			if diff := got - tc.want; diff < -time.Minute || diff > time.Minute {
				t.Fatalf("retentionPastDeadline(1m) = %v, want about %v", got, tc.want)
			}
		})
	}
}

// TestRetentionMagnitudeFollowsTheDeadline pins the size of the extension, not
// only that there is one. Stores in the field keep the 24h default, so the case
// that matters is a time budget past that: the keys must live to the deadline
// plus the grace, no longer and no shorter. A budget inside the retention must
// leave the retention alone.
func TestRetentionMagnitudeFollowsTheDeadline(t *testing.T) {
	ctx := context.Background()
	const defaultRetention = 24 * time.Hour

	type creator func(t *testing.T, state *Store, timeout time.Duration) (types.ExecutionID, time.Time)
	paths := map[string]creator{
		"CreateExecution": func(t *testing.T, state *Store, timeout time.Duration) (types.ExecutionID, time.Time) {
			deadline := time.Now().Add(timeout)
			createWithDeadline(t, state, ctx, "mag-create", deadline)
			return "mag-create", deadline
		},
		"SeedExecution": func(t *testing.T, state *Store, timeout time.Duration) (types.ExecutionID, time.Time) {
			deadline := time.Now().Add(timeout)
			if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{Execution: &engine.ExecutionSnapshot{
				ID: "mag-seed", Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
			}}); err != nil {
				t.Fatalf("SeedExecution: %v", err)
			}
			return "mag-seed", deadline
		},
		"SeedExecutionFromEntry": func(t *testing.T, state *Store, timeout time.Duration) (types.ExecutionID, time.Time) {
			deadline := time.Now().Add(timeout)
			resp, err := state.SeedExecutionFromEntry(ctx, timeoutSeedRequest(t, "mag-entry", "mag-entry-key", timeout))
			if err != nil || resp.State != engine.AdmissionStateAccepted {
				t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
			}
			return resp.ExecutionID, deadline
		},
	}

	for name, create := range paths {
		t.Run(name+"/budget past the retention", func(t *testing.T) {
			srv, state := queueTTLStore(t, defaultRetention)
			id, deadline := create(t, state, 48*time.Hour)
			for _, suffix := range []string{"status", "deadline_ms"} {
				requireTTLNearDeadline(t, srv.TTL(execKey(namespace.Default, id, suffix)), deadline, suffix)
			}
		})
		t.Run(name+"/budget inside the retention", func(t *testing.T) {
			srv, state := queueTTLStore(t, defaultRetention)
			id, _ := create(t, state, 3*time.Hour)
			if got := srv.TTL(execKey(namespace.Default, id, "status")); got != defaultRetention {
				t.Fatalf("status TTL = %v, want the untouched %v: a budget that fits the retention must not lengthen it", got, defaultRetention)
			}
			// Nothing was lengthened, so nothing is recorded as this execution's
			// retention override: the entry would only pin the default in memory
			// until the execution concludes.
			state.ttlMu.RLock()
			_, recorded := state.execTTLs[id]
			state.ttlMu.RUnlock()
			if recorded {
				t.Fatal("a budget that fits the retention was recorded as a per-execution retention override")
			}
		})
	}
}

// TestRedeliveredEntryAdmissionMemoizesTheStoredDeadline: a redelivered admission
// loses the SetNX on deadline_ms, so the deadline it computed from its own clock
// is later than the stored one. Remembering that value would overstate the
// execution's lifetime on whichever replica took the redelivery, so the memo must
// hold the stored deadline there, whether the replica already knew it or reads it
// for the first time.
func TestRedeliveredEntryAdmissionMemoizesTheStoredDeadline(t *testing.T) {
	ctx := context.Background()
	_, rdb, first := newMiniredisStore(t)
	req := timeoutSeedRequest(t, "entry-redeliver", "entry-redeliver-key", replicaBudget)
	resp, err := first.SeedExecutionFromEntry(ctx, req)
	if err != nil || resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
	}
	storedMs, err := rdb.Get(ctx, execKey(namespace.Default, resp.ExecutionID, "deadline_ms")).Int64()
	if err != nil {
		t.Fatalf("read the stored deadline: %v", err)
	}

	time.Sleep(5 * time.Millisecond) // the redelivery's clock reads later than the first admission's
	for name, store := range map[string]*Store{"same replica": first, "other replica": newReplica(rdb)} {
		t.Run(name, func(t *testing.T) {
			dup, err := store.SeedExecutionFromEntry(ctx, req)
			if err != nil || !dup.Duplicate {
				t.Fatalf("redelivered SeedExecutionFromEntry = (%+v, %v), want a duplicate", dup, err)
			}
			store.getExecTTL(ctx, resp.ExecutionID) // fills the memo on a replica that had none
			store.ttlMu.RLock()
			memo, known := store.execDeadlines[resp.ExecutionID]
			store.ttlMu.RUnlock()
			if !known || memo.UnixMilli() != storedMs {
				t.Fatalf("memoized deadline = (%v, known=%v), want the stored %d", memo, known, storedMs)
			}
		})
	}
}

// TestSuspendRenewsTheDeadlineKey: parking a node renews the execution's keys,
// and deadline_ms is one of them. It is written once, at creation, so without
// the renewal it lapses on its own TTL while the status lives on; GetExecution
// then reads a zero Deadline and the expiry is reported as never due.
//
// Creation already gave deadline_ms the full retention, so the test first runs
// its TTL down, which is what a key written long ago looks like by the time the
// execution parks.
func TestSuspendRenewsTheDeadlineKey(t *testing.T) {
	ctx := context.Background()
	srv, rdb, state := newMiniredisStore(t)
	deadline := time.Now().Add(3 * time.Hour)
	id := types.ExecutionID("renew-deadline-key")
	g := errorDetailsRedisGraph(t, "wait")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	lease := privateRedisLease(id, "wait", 0, "lease-renew", "token-renew")
	lease.Task.ActivationID = 1
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease claimed=%v err=%v", claimed, err)
	}

	deadlineKey := execKey(namespace.Default, id, "deadline_ms")
	if err := rdb.Expire(ctx, deadlineKey, 2*time.Minute).Err(); err != nil {
		t.Fatalf("shorten deadline_ms: %v", err)
	}
	if payload, _, err := state.SuspendTaskLease(ctx, lease, nil, false, false, &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"}, Timeout: 30 * time.Second,
	}, ""); err != nil || payload != nil {
		t.Fatalf("SuspendTaskLease payload=%+v err=%v, want parked", payload, err)
	}

	requireOutlivesDeadline(t, srv.TTL(deadlineKey), deadline, "deadline_ms after the park")

	// Past the default retention and the shortened TTL, well before the deadline.
	srv.FastForward(10 * time.Minute)
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution after the wait = (%+v, %v), want the execution", snap, err)
	}
	if snap.Deadline.UnixMilli() != deadline.UnixMilli() {
		t.Fatalf("Deadline after the wait = %v, want %v: deadline_ms lapsed while the status lived on", snap.Deadline, deadline)
	}
}

// landAfterIndexZRem runs land once, right after a ZREM against the given index
// key has been applied: the instant between List's repair removing a "missing"
// member and its re-check.
type landAfterIndexZRem struct {
	key  string
	land func()
	once sync.Once
}

func (h *landAfterIndexZRem) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *landAfterIndexZRem) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		if cmd.Name() == "zrem" && len(args) > 1 && args[1] == h.key {
			h.once.Do(h.land)
		}
		return err
	}
}

func (h *landAfterIndexZRem) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestDeadlineIndexRepairRestoresAnExecutionThatLandsMidRepair: registration
// precedes the status write, so List can find a member whose execution is about
// to be created. The repair removes the member and checks again; an execution
// that landed in between must be put back with its original score, or it is
// stranded with a deadline and no entry. A member whose execution never lands
// must stay removed.
func TestDeadlineIndexRepairRestoresAnExecutionThatLandsMidRepair(t *testing.T) {
	ctx := context.Background()
	srv, rdb, state := newMiniredisStore(t)
	key := executionDeadlineIndexKey(namespace.Default)
	score := float64(indexBase.UnixMilli())
	for _, id := range []string{"late", "ghost"} {
		if err := rdb.ZAdd(ctx, key, redis.Z{Score: score, Member: id}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	rdb.AddHook(&landAfterIndexZRem{key: key, land: func() {
		if err := srv.Set(execKey(namespace.Default, "late", "status"), string(types.ExecutionStatusRunning)); err != nil {
			t.Fatalf("land the late execution: %v", err)
		}
	}})

	// Both members were missing when List read them, so neither is returned now.
	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Minute), 10))

	if got, err := rdb.ZScore(ctx, key, "late").Result(); err != nil || got != score {
		t.Fatalf("index score of the execution that landed mid-repair = (%v, %v), want it restored with %v", got, err, score)
	}
	if inIndex(t, rdb, namespace.Default, "ghost") {
		t.Fatal("the member whose execution never landed was restored")
	}
	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Minute), 10), "late")
}

// TestAckDeadlineExpiredOnlyTouchesItsNamespace: the same execution ID can exist
// in two namespaces, and acknowledging one must leave the other's entry, or that
// execution is never expired.
func TestAckDeadlineExpiredOnlyTouchesItsNamespace(t *testing.T) {
	_, rdb, state := newMiniredisStore(t)
	other := namespace.WithNamespace(context.Background(), "tenant-ack")
	createWithDeadline(t, state, other, "shared-id", indexBase.Add(time.Second))
	createWithDeadline(t, state, context.Background(), "shared-id", indexBase.Add(time.Second))
	if !inIndex(t, rdb, "tenant-ack", "shared-id") || !inIndex(t, rdb, namespace.Default, "shared-id") {
		t.Fatal("precondition: the execution is not indexed in both namespaces")
	}

	if err := state.AckDeadlineExpired(other, "shared-id"); err != nil {
		t.Fatalf("AckDeadlineExpired: %v", err)
	}
	if inIndex(t, rdb, "tenant-ack", "shared-id") {
		t.Fatal("the acknowledged namespace still holds the entry")
	}
	if !inIndex(t, rdb, namespace.Default, "shared-id") {
		t.Fatal("acknowledging in one namespace removed the default namespace's entry for the same ID")
	}
}

// TestCancelCleanupDropsTheSuspendedSetTimeoutKeepsIt pins both halves of the
// difference between the two terminal cleanups. Cancel cascades first and writes
// canceled last, so its cleanup removes the suspended-nodes set like it always
// has. Timeout writes first and cascades after, and the cascade lists that set.
func TestCancelCleanupDropsTheSuspendedSetTimeoutKeepsIt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		status    types.ExecutionStatus
		wantKept  bool
		errorText string
	}{
		{types.ExecutionStatusCanceled, false, ""},
		{types.ExecutionStatusTimeout, true, engine.ExecutionDeadlineExceededError},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			_, rdb, state := newMiniredisStore(t)
			id := types.ExecutionID("cleanup-" + string(tc.status))
			parkWaitingNode(t, state, ctx, id, time.Now().Add(time.Hour), types.ModeSignal)
			setKey := suspendedNodesKey(namespace.Default, id)
			if n, _ := rdb.Exists(ctx, setKey).Result(); n != 1 {
				t.Fatal("precondition: the park left no suspended-nodes set")
			}

			if err := state.UpdateExecutionStatus(ctx, id, tc.status, tc.errorText); err != nil {
				t.Fatalf("UpdateExecutionStatus(%s): %v", tc.status, err)
			}
			n, err := rdb.Exists(ctx, setKey).Result()
			if err != nil {
				t.Fatal(err)
			}
			if kept := n == 1; kept != tc.wantKept {
				t.Fatalf("suspended-nodes set present after %s = %v, want %v", tc.status, kept, tc.wantKept)
			}
		})
	}
}

// TestListDeadlineExceededRespectsOwnedNamespaces: a process scoped to its own
// namespaces must not expire another deployment's executions even when they
// share the Redis. The registry would otherwise hand it every namespace.
func TestListDeadlineExceededRespectsOwnedNamespaces(t *testing.T) {
	_, _, state := newMiniredisStore(t)
	in := namespace.WithNamespace(context.Background(), "tenant-owned")
	out := namespace.WithNamespace(context.Background(), "tenant-foreign")
	createWithDeadline(t, state, in, "owned-due", indexBase.Add(time.Second))
	createWithDeadline(t, state, out, "foreign-due", indexBase.Add(2*time.Second))
	before := indexBase.Add(time.Minute)

	requireListed(t, listDue(t, state, context.Background(), before, 10), "owned-due", "foreign-due")

	state.ConfigureOwnedNamespaces([]namespace.Namespace{"tenant-owned"})
	got := listDue(t, state, context.Background(), before, 10)
	requireListed(t, got, "owned-due")
	if got[0].Namespace != "tenant-owned" {
		t.Fatalf("Namespace = %q, want tenant-owned", got[0].Namespace)
	}
}
