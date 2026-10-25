package rstate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// TestDeadlineMemoIsDroppedWhenTheExecutionEnds: the memo is per-process state,
// so whatever concludes an execution on this replica has to take its entry with
// it. Both the terminal status write and the rollback of a failed creation do.
func TestDeadlineMemoIsDroppedWhenTheExecutionEnds(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(replicaBudget)

	t.Run("terminal status", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("memo-evict-terminal")
		createWithDeadline(t, state, ctx, id, deadline)
		requireMemo(t, state, id, true)
		if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(success): %v", err)
		}
		requireMemo(t, state, id, false)
	})

	t.Run("evictExecutionCaches", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("memo-evict-direct")
		createWithDeadline(t, state, ctx, id, deadline)
		requireMemo(t, state, id, true)
		state.evictExecutionCaches(id)
		requireMemo(t, state, id, false)
	})

	t.Run("cleanupCreatedExecution", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("memo-evict-cleanup")
		snap := &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
		}
		if err := state.CreateExecution(ctx, snap); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		requireMemo(t, state, id, true)
		state.cleanupCreatedExecution(ctx, snap)
		requireMemo(t, state, id, false)
	})

	// The same rollback reached the way production reaches it: the Redis half of
	// the creation lands, the SQL half is refused, and the creation unwinds.
	t.Run("SQL write refused", func(t *testing.T) {
		_, rdb, _ := newMiniredisStore(t)
		state := New(rdb, &refusingCreateStore{}, replicaDefaultRetention)
		id := types.ExecutionID("memo-evict-sql-refused")
		err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
		})
		if err == nil {
			t.Fatal("CreateExecution succeeded although the SQL write was refused; the test proves nothing")
		}
		requireMemo(t, state, id, false)
	})
}

// refusingCreateStore fails the SQL half of a creation. The embedded interface
// is nil on purpose: nothing else is reached on this path, and a call that is
// reached fails loudly.
type refusingCreateStore struct{ store.Store }

func (*refusingCreateStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	return errors.New("sql create refused")
}

// TestDeadlineMemoStaysWithinItsBound: a replica that never concludes the
// executions it touches would otherwise keep one entry per execution for as long
// as it runs. At the bound the memo is dropped whole and refills from the
// executions still in flight.
func TestDeadlineMemoStaysWithinItsBound(t *testing.T) {
	deadline := time.Now().Add(replicaBudget)

	for name, add := range map[string]func(s *Store, id types.ExecutionID){
		"rememberExecDeadline": func(s *Store, id types.ExecutionID) { s.rememberExecDeadline(id, deadline) },
		"learnExecDeadline":    func(s *Store, id types.ExecutionID) { s.learnExecDeadline(id, deadline) },
	} {
		t.Run(name, func(t *testing.T) {
			_, _, state := newMiniredisStore(t)
			var last types.ExecutionID
			for i := 0; i < execDeadlineMemoCap+10; i++ {
				last = types.ExecutionID("bound-" + time.Duration(i).String())
				add(state, last)
				state.ttlMu.RLock()
				size := len(state.execDeadlines)
				state.ttlMu.RUnlock()
				if size > execDeadlineMemoCap {
					t.Fatalf("memo holds %d entries after %d inserts, want at most %d", size, i+1, execDeadlineMemoCap)
				}
			}
			// The newest entry survives the drop; it is what the caller just learned.
			requireMemo(t, state, last, true)
		})
	}
}

// TestRetentionSurvivesAnotherReplicaInANonDefaultNamespace: every key the funnel
// reads carries the namespace of the context it is asked under. Reading the
// default namespace's keys instead finds no execution at all, so a replica
// serving a tenant would take the "absent" answer and write the default
// retention over a status key that has to outlive its deadline.
func TestRetentionSurvivesAnotherReplicaInANonDefaultNamespace(t *testing.T) {
	const ns = namespace.Namespace("tenant-memo")
	ctx := namespace.WithNamespace(context.Background(), ns)

	statusTTL := func(srv interface{ TTL(string) time.Duration }, id types.ExecutionID) time.Duration {
		return srv.TTL(execKey(ns, id, "status"))
	}

	t.Run("PauseExecution", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("ns-pause")
		deadline := time.Now().Add(replicaBudget)
		createWithDeadline(t, creator, ctx, id, deadline)
		requireOutlivesDeadline(t, statusTTL(srv, id), deadline, "status after creation in the namespace")

		if err := newReplica(rdb).PauseExecution(ctx, id); err != nil {
			t.Fatalf("PauseExecution: %v", err)
		}
		requireOutlivesDeadline(t, statusTTL(srv, id), deadline, "status after PauseExecution in the namespace")
	})

	t.Run("UpdateExecutionStatus", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("ns-update")
		deadline := time.Now().Add(replicaBudget)
		createWithDeadline(t, creator, ctx, id, deadline)
		if err := newReplica(rdb).UpdateExecutionStatus(ctx, id, types.ExecutionStatusPaused, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(paused): %v", err)
		}
		requireOutlivesDeadline(t, statusTTL(srv, id), deadline, "status after UpdateExecutionStatus in the namespace")
	})

	t.Run("an execution in the namespace is not found under the default one", func(t *testing.T) {
		_, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("ns-isolated")
		createWithDeadline(t, creator, ctx, id, time.Now().Add(replicaBudget))
		other := newReplica(rdb)
		if got := other.getExecTTL(context.Background(), id); got != replicaDefaultRetention {
			t.Fatalf("getExecTTL under the default namespace = %v, want the %v default: the execution lives elsewhere", got, replicaDefaultRetention)
		}
		requireMemo(t, other, id, false)
	})
}

// TestDurableModeMarkerOutlivesALongBudgetExecution: the per-execution mode
// marker is written with twice the retention of the keys it qualifies, and that
// retention already includes the deadline. A durable execution with a time budget
// past the default retention therefore keeps its marker for longer than any
// status key another replica can write for it (a write reaches at most the
// deadline plus the grace), whichever replica renews what.
//
// The transient mode is deliberately not covered: its marker follows the graph's
// TransientTTL, which bounds how long a transient run is expected to last.
func TestDurableModeMarkerOutlivesALongBudgetExecution(t *testing.T) {
	ctx := context.Background()

	requireMarkerCovers := func(t *testing.T, srv interface{ TTL(string) time.Duration }, id types.ExecutionID, deadline time.Time, what string) {
		t.Helper()
		marker := srv.TTL(transientMarkKey(namespace.Default, id))
		status := srv.TTL(execKey(namespace.Default, id, "status"))
		if marker <= 0 {
			t.Fatalf("%s: the mode marker has no TTL (%v); it should exist and expire", what, marker)
		}
		if marker < status {
			t.Fatalf("%s: marker TTL %v is shorter than the status TTL %v: its absence would stop meaning \"not transient\"", what, marker, status)
		}
		requireOutlivesDeadline(t, marker, deadline, "mode marker "+what)
	}

	creators := map[string]func(t *testing.T, state *Store) (types.ExecutionID, time.Time){
		"CreateExecution": func(t *testing.T, state *Store) (types.ExecutionID, time.Time) {
			deadline := time.Now().Add(replicaBudget)
			createWithDeadline(t, state, ctx, "marker-create", deadline)
			return "marker-create", deadline
		},
		"SeedExecution": func(t *testing.T, state *Store) (types.ExecutionID, time.Time) {
			deadline := time.Now().Add(replicaBudget)
			if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{Execution: &engine.ExecutionSnapshot{
				ID: "marker-seed", Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
			}}); err != nil {
				t.Fatalf("SeedExecution: %v", err)
			}
			return "marker-seed", deadline
		},
		"SeedExecutionFromEntry": func(t *testing.T, state *Store) (types.ExecutionID, time.Time) {
			deadline := time.Now().Add(replicaBudget)
			resp, err := state.SeedExecutionFromEntry(ctx, timeoutSeedRequest(t, "marker-entry", "marker-entry-key", replicaBudget))
			if err != nil || resp.State != engine.AdmissionStateAccepted {
				t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
			}
			return resp.ExecutionID, deadline
		},
	}

	for name, create := range creators {
		t.Run(name, func(t *testing.T) {
			srv, rdb, creator := newMiniredisStore(t)
			id, deadline := create(t, creator)
			requireMarkerCovers(t, srv, id, deadline, "after creation")

			// An entry-seeded execution is born terminal, so there is nothing for
			// another replica to pause; its marker is checked at creation only.
			if name == "SeedExecutionFromEntry" {
				return
			}
			if err := newReplica(rdb).PauseExecution(ctx, id); err != nil {
				t.Fatalf("PauseExecution on another replica: %v", err)
			}
			requireStatusOutlivesDeadline(t, srv, id, deadline, "PauseExecution")
			requireMarkerCovers(t, srv, id, deadline, "after a pause on another replica")
		})
	}

	t.Run("CommitNode on another replica", func(t *testing.T) {
		srv, rdb, creator := newMiniredisStore(t)
		id := types.ExecutionID("marker-commit")
		g := twoNodeGraph(t)
		deadline := createLongBudgetExecution(t, ctx, srv, creator, id, g)
		requireMarkerCovers(t, srv, id, deadline, "after creation")

		idx, _ := g.NodeIndex("a")
		lease := privateRedisLease(id, "a", idx, "lease-marker", "token-marker")
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
		requireMarkerCovers(t, srv, id, deadline, "after a commit on another replica")
	})
}

// TestCreationReplacesAStaleMemo: what a creation path writes is authoritative, so
// it replaces whatever the memo held. The case is an ID reused after an earlier
// execution under it was only ever read on this replica: the replica memoized the
// old deadline, never concluded that execution (so nothing evicted the entry), and
// now creates a new one under the same ID. Keeping the old value would size every
// later status write from a deadline that no longer exists.
func TestCreationReplacesAStaleMemo(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("memo-reused-id")
	stale := time.Now().Add(replicaBudget)
	createWithDeadline(t, creator, ctx, id, stale)

	replica := newReplica(rdb)
	replica.getExecTTL(ctx, id)
	if memo := requireMemo(t, replica, id, true); memo.UnixMilli() != stale.UnixMilli() {
		t.Fatalf("precondition: memoized deadline = %v, want the stored %v", memo, stale)
	}

	srv.FlushAll() // the old execution is gone from Redis; this replica never learned of it
	fresh := time.Now().Add(72 * time.Hour)
	createWithDeadline(t, replica, ctx, id, fresh)

	if memo := requireMemo(t, replica, id, true); memo.UnixMilli() != fresh.UnixMilli() {
		t.Fatalf("memoized deadline after the re-creation = %v, want the new %v: the creation path kept the stale entry", memo, fresh)
	}
}

// roundTripRecorder records, per round trip to Redis, the names of the commands it
// carried: a single command is one entry, a pipeline or transaction is one entry
// for the whole batch.
type roundTripRecorder struct {
	mu    sync.Mutex
	trips [][]string
}

func (r *roundTripRecorder) record(names []string) {
	r.mu.Lock()
	r.trips = append(r.trips, names)
	r.mu.Unlock()
}

func (r *roundTripRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *roundTripRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		r.record([]string{cmd.Name()})
		return next(ctx, cmd)
	}
}

func (r *roundTripRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		names := make([]string, 0, len(cmds))
		for _, cmd := range cmds {
			names = append(names, cmd.Name())
		}
		r.record(names)
		return next(ctx, cmds)
	}
}

// TestDeadlineReadIsOneTransactionOnOneSlot pins the shape of the memo-miss read:
// one round trip, one MULTI/EXEC, the existence check ahead of the deadline read.
// One transaction is what makes the pair a single snapshot (two round trips can
// straddle a creation), and it is also the only form that stays on one slot of a
// Redis Cluster, which the shared {id} hash tag guarantees for both keys.
func TestDeadlineReadIsOneTransactionOnOneSlot(t *testing.T) {
	ctx := context.Background()
	_, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("memo-one-trip")
	createWithDeadline(t, creator, ctx, id, time.Now().Add(replicaBudget))

	rec := &roundTripRecorder{}
	rdb.AddHook(rec)
	newReplica(rdb).execDeadline(ctx, id)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := []string{"multi", "exists", "get", "exec"}
	if len(rec.trips) != 1 || !slices.Equal(rec.trips[0], want) {
		t.Fatalf("a memo miss made the round trips %v, want exactly one carrying %v", rec.trips, want)
	}

	status, deadlineKey := execKey(namespace.Default, id, "status"), execKey(namespace.Default, id, "deadline_ms")
	if hashTag(status) == "" || hashTag(status) != hashTag(deadlineKey) {
		t.Fatalf("status key %q and deadline key %q do not share a hash tag, so a transaction over both is not one cluster slot", status, deadlineKey)
	}
}

// hashTag returns the part of the key Redis Cluster hashes: the text between the
// first "{" and the next "}", when that is non-empty.
func hashTag(key string) string {
	open := strings.Index(key, "{")
	if open < 0 {
		return ""
	}
	end := strings.Index(key[open+1:], "}")
	if end <= 0 {
		return ""
	}
	return key[open+1 : open+1+end]
}
