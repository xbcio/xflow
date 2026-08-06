package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// A released assignment is one the control plane gave up on without deciding
// its work is finished: ReleaseLeased with RemoveSeen=false, which is what a
// stale-token commit produces (core.go's removeSeen is false for every outcome
// that is not accepted/duplicate-terminal/execution-inactive).
//
// The seen set is enqueue's dedup guard, so leaving a mark behind for an
// assignment the plane no longer owns turns it into a permanent black hole:
// EnqueueAssignment reports "duplicate", Dispatcher.HandleTask treats that as
// success and drops the task, and nothing ever re-queues it. Observed in
// production shape as a map expansion stuck forever at 5 of 6 batches, with the
// sixth left as state=released and still present in seen.
func TestReleasedAssignmentCanBeRequeued(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-release", 1)
	assignment := redisDirectoryTestAssignment("exec-release/node/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	lease := &engine.TaskLease{
		LeaseID:    "lease-release",
		LeaseToken: "token-release",
		Attempt:    1,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	// RemoveSeen=false is the stale-token path: the commit did not apply, so
	// the plane keeps the seen mark rather than declaring the work finished.
	if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		RemoveSeen:   false,
	}); err != nil {
		t.Fatalf("ReleaseLeased() error = %v", err)
	}

	// The engine re-dispatches the same task — same AssignmentID, because
	// BuildAssignmentID carries no generation. This must reach the queue.
	enqueued, err := directory.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() after release error = %v", err)
	}
	if !enqueued {
		t.Fatal("EnqueueAssignment() after a released lease returned duplicate; the " +
			"assignment is now unreachable — Dispatcher.HandleTask drops a duplicate " +
			"silently, so the task never runs again and its parent waits forever")
	}

	// It must also be claimable, not merely present: an enqueue that restores
	// the queue entry but leaves the state at "released" would still be dead.
	if _, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1)); err != nil {
		t.Fatalf("ClaimForRunner() after requeue error = %v", err)
	} else if !ok {
		t.Fatal("ClaimForRunner() found nothing after the released assignment was " +
			"re-enqueued; it is queued but unclaimable")
	}
}

// A still-leased assignment must NOT be re-enqueued. Enqueue's dedup is what
// keeps a duplicate dispatch — a retry, a racing outbox flush — from handing
// the same task to a second runner while the first is running it. Relaxing the
// released case must not relax this one.
func TestLeasedAssignmentIsStillDeduplicated(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-leased", 1)
	assignment := redisDirectoryTestAssignment("exec-leased/node/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	lease := &engine.TaskLease{
		LeaseID:    "lease-leased",
		LeaseToken: "token-leased",
		Attempt:    1,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	enqueued, err := directory.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() while leased error = %v", err)
	}
	if enqueued {
		t.Fatal("EnqueueAssignment() re-queued an assignment a runner currently " +
			"holds a lease on; the task would run twice concurrently")
	}
}

// A queued assignment must not be re-queued either — that is the ordinary
// duplicate-dispatch case, and re-queueing would move it to the back of the
// line on every retry.
func TestQueuedAssignmentIsStillDeduplicated(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	assignment := redisDirectoryTestAssignment("exec-queued/node/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	enqueued, err := directory.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() while queued error = %v", err)
	}
	if enqueued {
		t.Fatal("EnqueueAssignment() re-queued an already-queued assignment")
	}
}

// The in-memory directory must make the same call. It backs embedded
// deployments and most of this repo's tests, so a divergence here means the
// behaviour under test is not the behaviour in production.
func TestMemoryDirectoryReleasedAssignmentCanBeRequeued(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()

	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-mem-release",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	assignment := redisDirectoryTestAssignment("exec-mem-release/node/activation-1")
	if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil || !enqueued {
		t.Fatalf("EnqueueAssignment() = %v, %v; want true, nil", enqueued, err)
	}
	claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() = %v, %v; want a claim", ok, err)
	}
	lease := &engine.TaskLease{
		LeaseID:    "lease-mem",
		LeaseToken: "token-mem",
		Attempt:    1,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		RemoveSeen:   false,
	}); err != nil {
		t.Fatalf("ReleaseLeased() error = %v", err)
	}

	enqueued, err := directory.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() after release error = %v", err)
	}
	if !enqueued {
		t.Fatal("in-memory EnqueueAssignment() after a released lease returned " +
			"duplicate; the assignment is unreachable")
	}
}

// The in-memory guard against re-queueing live work, matching the Redis case.
func TestMemoryDirectoryLeasedAssignmentIsStillDeduplicated(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()

	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-mem-leased",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	assignment := redisDirectoryTestAssignment("exec-mem-leased/node/activation-1")
	if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil || !enqueued {
		t.Fatalf("EnqueueAssignment() = %v, %v; want true, nil", enqueued, err)
	}

	// Queued: still live, must dedupe.
	if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil || enqueued {
		t.Fatalf("EnqueueAssignment() while queued = %v, %v; want false, nil", enqueued, err)
	}

	claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() = %v, %v; want a claim", ok, err)
	}

	// Claimed but not yet finalized: still live, must dedupe.
	if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil || enqueued {
		t.Fatalf("EnqueueAssignment() while claimed = %v, %v; want false, nil", enqueued, err)
	}

	lease := &engine.TaskLease{
		LeaseID:    "lease-mem-leased",
		LeaseToken: "token-mem-leased",
		Attempt:    1,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	// Leased: a runner is executing it, must dedupe.
	if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil || enqueued {
		t.Fatalf("EnqueueAssignment() while leased = %v, %v; want false, nil", enqueued, err)
	}
}

// This is the case the old seen-only guard was really defending, and the one
// relaxing it could break: a stale-token report arrives for generation 1 AFTER
// the sweeper already reclaimed and re-dispatched the task as generation 2.
// Releasing generation 1 must not make the live generation 2 re-queueable —
// that would hand the same work to a second runner.
//
// The state check is what fences this, and it fences it more precisely than the
// seen mark did: ReleaseLeased is token-matched, so a release carrying the old
// token cannot touch a newer generation's state at all.
func TestStaleReleaseDoesNotRequeueALiveNewGeneration(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-gen", 2)
	assignment := redisDirectoryTestAssignment("exec-gen/node/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	genOneClaim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)

	genOne := &engine.TaskLease{
		LeaseID:    "lease-gen-1",
		LeaseToken: "token-gen-1",
		Attempt:    1,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, genOneClaim.ClaimID, genOne); err != nil {
		t.Fatalf("FinalizeClaim(gen 1) error = %v", err)
	}

	// The sweeper reclaims generation 1 and the engine re-dispatches, producing
	// generation 2 with a fresh token. This is the live work.
	if _, err := directory.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: assignment.AssignmentID,
		LeaseID:      genOne.LeaseID,
		LeaseToken:   genOne.LeaseToken,
	}); err != nil {
		t.Fatalf("ReleaseExpiredLease() error = %v", err)
	}
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	genTwoClaim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	genTwo := &engine.TaskLease{
		LeaseID:    "lease-gen-2",
		LeaseToken: "token-gen-2",
		Attempt:    2,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, genTwoClaim.ClaimID, genTwo); err != nil {
		t.Fatalf("FinalizeClaim(gen 2) error = %v", err)
	}

	// Now generation 1's late stale-token report lands.
	if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      genOne.LeaseID,
		LeaseToken:   genOne.LeaseToken,
		RemoveSeen:   false,
	}); err != nil {
		t.Fatalf("ReleaseLeased(stale gen 1) error = %v", err)
	}

	if enqueued, err := directory.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("EnqueueAssignment() after stale release error = %v", err)
	} else if enqueued {
		t.Fatal("a stale generation's release made the LIVE generation re-queueable; " +
			"the same task would now be dispatched to a second runner while the " +
			"first is still running it")
	}
}
