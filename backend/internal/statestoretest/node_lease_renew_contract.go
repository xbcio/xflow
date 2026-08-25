package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// NodeLeaseRenewStore is a backend that can hand out node leases, renew them,
// and report which ones the sweeper considers expired.
type NodeLeaseRenewStore interface {
	engine.StateStore
	engine.NodeLeaseRenewer
}

// RunNodeLeaseRenewContract pins the renewal semantics a node lease must have
// for a long-running handler to survive its own execution.
//
// Without renewal, DefaultLeaseTTL is a hard ceiling on how long a node may
// legitimately run: BuildTaskLease always stamps the lease with the engine's
// default TTL, and types.NodeDef.Timeout ("zero means no limit") is read
// nowhere in engine, node, or runner. An http.request member with a
// user-supplied options.timeout above the TTL therefore gets reclaimed and
// redelivered while its runner is healthily working — two runners then execute
// the same node.
func RunNodeLeaseRenewContract(t *testing.T, newStore func(*testing.T) NodeLeaseRenewStore) {
	ctx := context.Background()

	seed := func(t *testing.T) (NodeLeaseRenewStore, types.ExecutionID, *engine.TaskLease) {
		t.Helper()
		s := newStore(t)
		id := types.ExecutionID("e-" + t.Name())
		if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("create execution: %v", err)
		}
		lease := &engine.TaskLease{
			LeaseID:    "L-1",
			LeaseToken: "T1",
			IssuedAt:   time.Now().UTC().Add(-90 * time.Second),
			TTL:        time.Minute,
			Task: engine.Task{
				ExecutionID: id,
				NodeName:    "start",
				NodeIdx:     0,
				Type:        engine.TaskTypeNodeExec,
			},
		}
		if _, acquired, err := s.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("acquire node lease: acquired=%v err=%v", acquired, err)
		}
		return s, id, lease
	}

	// isExpired reports whether the sweeper would reclaim this node right now.
	isExpired := func(t *testing.T, s NodeLeaseRenewStore, id types.ExecutionID) bool {
		t.Helper()
		expired, err := s.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("ListExpiredLeases: %v", err)
		}
		for i := range expired {
			if expired[i].ExecutionID == id && expired[i].NodeName == "start" {
				return true
			}
		}
		return false
	}

	// The whole point: a lease that was already past its deadline must stop
	// being reclaimable once its owner renews it. Asserting only that
	// RenewTaskLease returns true would pass on a backend that updates the
	// metadata but leaves the expiry index alone — and the sweeper reads the
	// index, so such a backend still hands the node to a second runner.
	t.Run("RenewPushesTheDeadlinePastTheSweeper", func(t *testing.T) {
		s, id, lease := seed(t)
		if !isExpired(t, s, id) {
			t.Fatal("a lease issued 90s ago with a 60s TTL must be expired before renewal — " +
				"the test cannot prove renewal works if the starting state is not reclaimable")
		}

		renewed, err := s.RenewTaskLease(ctx, id, "start", lease.LeaseToken, time.Now().UTC().Add(5*time.Minute))
		if err != nil {
			t.Fatalf("RenewTaskLease: %v", err)
		}
		if !renewed {
			t.Fatal("RenewTaskLease returned false for the live owner's token")
		}

		if isExpired(t, s, id) {
			t.Fatal("the node is still reported as expired after renewal — the sweeper reads the " +
				"expiry index, so a renewal that only rewrites metadata leaves the healthy " +
				"runner's work item up for grabs")
		}
	})

	// A renewed lease must still be the same lease: renewal is not a re-acquire,
	// so the token and attempt counter must survive it. A backend that bumped
	// attempt would make the owner's own commit fail its fence check.
	t.Run("RenewPreservesTokenAndAttempt", func(t *testing.T) {
		s, id, lease := seed(t)
		before, err := s.GetNode(ctx, id, "start")
		if err != nil || before == nil {
			t.Fatalf("GetNode before renew: %v", err)
		}

		if renewed, err := s.RenewTaskLease(ctx, id, "start", lease.LeaseToken, time.Now().UTC().Add(5*time.Minute)); err != nil || !renewed {
			t.Fatalf("RenewTaskLease: renewed=%v err=%v", renewed, err)
		}

		after, err := s.GetNode(ctx, id, "start")
		if err != nil || after == nil {
			t.Fatalf("GetNode after renew: %v", err)
		}
		if after.LeaseToken != before.LeaseToken {
			t.Errorf("lease token changed on renew: %q -> %q", before.LeaseToken, after.LeaseToken)
		}
		if after.Attempt != before.Attempt {
			t.Errorf("attempt changed on renew: %d -> %d — the owner's commit would fail its fence",
				before.Attempt, after.Attempt)
		}
		if after.Status != types.NodeStatusRunning {
			t.Errorf("status = %v after renew, want running", after.Status)
		}
	})

	// The sweeper is not the only thing that judges a lease dead. A competing
	// AcquireTaskLease for the same node checks the stored deadline itself and
	// takes the node when it has passed — so a renewal that satisfies the
	// expiry scan but leaves the stored deadline behind still loses the node to
	// the next runner that tries to claim it.
	t.Run("RenewBlocksACompetingAcquire", func(t *testing.T) {
		s, id, lease := seed(t)
		if renewed, err := s.RenewTaskLease(ctx, id, "start", lease.LeaseToken, time.Now().UTC().Add(5*time.Minute)); err != nil || !renewed {
			t.Fatalf("RenewTaskLease: renewed=%v err=%v", renewed, err)
		}

		rival := &engine.TaskLease{
			LeaseID:    "L-2",
			LeaseToken: "T2",
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task:       lease.Task,
		}
		_, acquired, err := s.AcquireTaskLease(ctx, rival)
		if err != nil {
			t.Fatalf("rival AcquireTaskLease: %v", err)
		}
		if acquired {
			t.Fatal("a rival acquired the node after the owner renewed — the acquire path reads " +
				"the stored deadline, so a renewal that only moved the expiry index still " +
				"hands the running node to a second runner")
		}
	})

	t.Run("RenewFencedByToken", func(t *testing.T) {
		s, id, _ := seed(t)
		renewed, err := s.RenewTaskLease(ctx, id, "start", "WRONG", time.Now().UTC().Add(5*time.Minute))
		if err != nil {
			t.Fatalf("RenewTaskLease with a stale token: %v", err)
		}
		if renewed {
			t.Fatal("a stale token renewed the live owner's lease — a dead runner's last " +
				"in-flight renewal would keep the node unreclaimable forever")
		}
		if !isExpired(t, s, id) {
			t.Fatal("the rejected renewal still moved the deadline")
		}
	})

	// A map node holding a Waiting lease while its batches run on remote runners
	// is exactly the case where renewal matters most: the batches can legitimately
	// take far longer than one node's TTL, and ListExpiredLeases reclaims Waiting
	// nodes just like Running ones. A renewal gate that accepted only "running"
	// would answer Renewed=false here — and the runner reads that as "you lost the
	// lease", cancelling a healthy fan-out instead of extending it.
	t.Run("RenewExtendsAWaitingExpansionParent", func(t *testing.T) {
		s, id, lease := seed(t)
		node, err := s.GetNode(ctx, id, "start")
		if err != nil || node == nil {
			t.Fatalf("GetNode: %v", err)
		}
		node.Status = types.NodeStatusWaiting
		if err := s.UpsertNode(ctx, node); err != nil {
			t.Fatalf("move node to waiting: %v", err)
		}
		if !isExpired(t, s, id) {
			t.Fatal("a Waiting node past its deadline must be reclaimable before renewal — " +
				"otherwise this subtest proves nothing")
		}

		renewed, err := s.RenewTaskLease(ctx, id, "start", lease.LeaseToken, time.Now().UTC().Add(5*time.Minute))
		if err != nil {
			t.Fatalf("RenewTaskLease on a waiting parent: %v", err)
		}
		if !renewed {
			t.Fatal("renewal refused a Waiting expansion parent — its batches are still " +
				"executing, and the sweeper reclaims Waiting leases, so refusing here " +
				"means the fan-out is reclaimed mid-flight or cancelled by its own runner")
		}
		if isExpired(t, s, id) {
			t.Fatal("the waiting parent is still reported as expired after renewal")
		}
	})

	// Once the lease is gone, renewal must not resurrect it. Otherwise a
	// renewal racing a reclaim would restore a deadline for a lease whose work
	// item has already been requeued to another runner.
	t.Run("RenewRejectedAfterLeaseRevoked", func(t *testing.T) {
		s, id, lease := seed(t)
		atomic, ok := s.(engine.AtomicStateStore)
		if !ok {
			t.Skip("backend does not implement AtomicStateStore")
		}
		revoked, err := atomic.RevokeLeaseWithOutbox(ctx, id, "start", lease.LeaseToken, engine.OutboxEntry{
			ID: "requeue/" + string(id), Task: lease.Task})
		if err != nil || !revoked {
			t.Fatalf("revoke: revoked=%v err=%v", revoked, err)
		}

		renewed, err := s.RenewTaskLease(ctx, id, "start", lease.LeaseToken, time.Now().UTC().Add(5*time.Minute))
		if err != nil {
			t.Fatalf("RenewTaskLease after revoke: %v", err)
		}
		if renewed {
			t.Fatal("renewal revived a revoked lease — its task is already queued for another runner")
		}
	})
}
