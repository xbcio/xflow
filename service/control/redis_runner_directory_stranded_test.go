package control

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

// seedStrandedLeasedAssignment finalizes a claim so the assignment is 'leased'
// with its per-runner index and finalized handoff written, then deletes the
// per-assignment lease-metadata key. Deleting it is exactly what its TTL does
// once the lease lapses, and it is the deterministic form of the shape the
// reaper exists for: state 'leased', metadata unrecoverable.
func seedStrandedLeasedAssignment(t *testing.T, ctx context.Context, rdb *redis.Client, directory *RedisRunnerDirectory, session RunnerSession, assignment Assignment, lease *engine.TaskLease) {
	t.Helper()

	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	expireStrandedLeaseMeta(t, ctx, rdb, directory, assignment.AssignmentID)
}

// expireStrandedLeaseMeta removes the per-assignment lease-metadata key. That
// is exactly what its TTL does once the lease lapses, and it is the
// deterministic form of the shape the reaper exists for.
//
// It has to be the last step for a given runner: the next claim poll replays the
// runner's leases and would itself release the now-stranded one.
func expireStrandedLeaseMeta(t *testing.T, ctx context.Context, rdb *redis.Client, directory *RedisRunnerDirectory, assignmentID AssignmentID) {
	t.Helper()

	if err := rdb.Del(ctx, directory.keys.assignmentLeaseMetaKey(string(assignmentID))).Err(); err != nil {
		t.Fatalf("expire lease metadata: %v", err)
	}
}

func assertStrandedReapLeftLeaseCount(t *testing.T, server *miniredis.Miniredis, directory *RedisRunnerDirectory, runnerID, want string) {
	t.Helper()

	if got := server.HGet(directory.keys.runnerLeaseCount, runnerID); got != want {
		t.Fatalf("runner:lease-count[%s] = %q, want %q", runnerID, got, want)
	}
}

// TestRedisRunnerDirectoryReapsStrandedLeasedAssignment is the primary path:
// the per-runner index names the assignment, its metadata is gone, and the
// reaper releases it, returns the runner's capacity, prunes the index and
// settles the finalized handoff record the release deliberately keeps.
func TestRedisRunnerDirectoryReapsStrandedLeasedAssignment(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-stranded"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-stranded/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-stranded", time.Minute)
	seedStrandedLeasedAssignment(t, ctx, rdb, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)
	claimID := server.HGet(directory.keys.handoffAssignment, assignmentID)

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1", reap.Released)
	}
	// inspected == released: the pass found one candidate of its shape and
	// released it. The pair is what makes that readable — the released count
	// alone cannot say whether this pass found one record or walked a hundred to
	// release the same one.
	if reap.Inspected != 1 {
		t.Fatalf("inspected = %d, want 1: the one stranded assignment is one candidate", reap.Inspected)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != "" {
		t.Fatalf("assignment state = %q, want released", got)
	}
	assertStrandedReapLeftLeaseCount(t, server, directory, runnerID, "0")
	if members, err := rdb.SMembers(ctx, directory.keys.runnerLeasedAssignmentsKey(runnerID)).Result(); err != nil {
		t.Fatalf("read leased index: %v", err)
	} else if len(members) != 0 {
		t.Fatalf("leased index = %v, want pruned", members)
	}
	if claimID != "" {
		if got := server.HGet(directory.keys.handoffState, claimID); got != "" {
			t.Fatalf("finalized handoff state = %q, want settled", got)
		}
	}
}

// TestRedisRunnerDirectoryStrandedReapLeavesLiveLeaseAlone is the safety
// property the whole reaper rests on: a leased assignment whose metadata is
// still present is live work another runner may be executing, and the reaper
// must not touch it. Delete the metadata-exists guard and this turns red.
func TestRedisRunnerDirectoryStrandedReapLeavesLiveLeaseAlone(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-live"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-live/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-live", time.Hour)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("released = %d, want 0: the lease metadata is still present", reap.Released)
	}
	// Inspected 2 with released 0 is the divergence case, and it is also the
	// honest reading of what this pass examines: the walk yielded the assignment
	// as a candidate and the handoff ledger still marks its debt finalized, so
	// two records were examined and neither was released. An assignment both
	// enumerations reach is counted once per enumeration — the counter measures
	// candidates examined, not distinct assignments — which is why the released
	// count is the only side of the pair with a one-to-one meaning.
	if reap.Inspected != 2 {
		t.Fatalf("inspected = %d, want 2: the walked assignment and its finalized ledger record", reap.Inspected)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != redisAssignmentLeased {
		t.Fatalf("assignment state = %q, want untouched %q", got, redisAssignmentLeased)
	}
	assertStrandedReapLeftLeaseCount(t, server, directory, runnerID, "1")
}

// TestRedisRunnerDirectoryStrandedReapReachesLedgerWithoutRunner covers the
// second enumeration. The owning runner is no longer registered, so the
// per-runner index is never consulted; only the handoff ledger still names the
// assignment, and the reaper must reach the release through it.
func TestRedisRunnerDirectoryStrandedReapReachesLedgerWithoutRunner(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-ledger"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-ledger/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-ledger", time.Minute)
	seedStrandedLeasedAssignment(t, ctx, rdb, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)
	claimID := server.HGet(directory.keys.handoffAssignment, assignmentID)
	if claimID == "" {
		t.Fatal("finalize did not record a handoff claim for the assignment")
	}

	// The runner is gone from the live registry, so pass A has no runner to walk.
	if err := rdb.HDel(ctx, directory.keys.runnerSession, runnerID).Err(); err != nil {
		t.Fatalf("deregister runner: %v", err)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1 via the ledger enumeration", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != "" {
		t.Fatalf("assignment state = %q, want released", got)
	}
	if got := server.HGet(directory.keys.handoffState, claimID); got != "" {
		t.Fatalf("handoff state = %q, want settled", got)
	}
}

// TestRedisRunnerDirectoryStrandedReapPrunesBogusIndexEntry checks the owner
// re-verification: an index entry that names an assignment owned by someone
// else is pruned and never becomes a candidate for the runner whose index it
// is. `assignment:runner` is the authority — the index is only a hint.
func TestRedisRunnerDirectoryStrandedReapPrunesBogusIndexEntry(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const ownerID = "runner-owner"
	const otherID = "runner-other"
	owner := registerRedisDirectoryRunner(t, ctx, directory, ownerID, 1)
	registerRedisDirectoryRunner(t, ctx, directory, otherID, 1)

	assignment := redisDirectoryTestAssignment("exec-owner/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-owner", time.Minute)
	seedStrandedLeasedAssignment(t, ctx, rdb, directory, owner, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	// A stale index write under the wrong runner. Trusting it would make the
	// other runner walk a lease it never held.
	if err := rdb.SAdd(ctx, directory.keys.runnerLeasedAssignmentsKey(otherID), assignmentID).Err(); err != nil {
		t.Fatalf("seed bogus index entry: %v", err)
	}

	candidates, err := directory.strandedLeaseCandidates(ctx, otherID)
	if err != nil {
		t.Fatalf("strandedLeaseCandidates() error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("strandedLeaseCandidates(%s) = %v, want none: the assignment belongs to %s", otherID, candidates, ownerID)
	}
	if members, err := rdb.SMembers(ctx, directory.keys.runnerLeasedAssignmentsKey(otherID)).Result(); err != nil {
		t.Fatalf("read other runner index: %v", err)
	} else if len(members) != 0 {
		t.Fatalf("bogus index entry survived: %v", members)
	}

	// The true owner's pass still releases exactly once.
	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1 (only the true owner's lease)", reap.Released)
	}
}

// TestRedisRunnerDirectoryStrandedReapFallsBackToFullScanWhenIndexShort covers
// the count validation. The per-runner index can only be written by a control
// plane that has it, so a live lease missing from it is a control plane that
// predates the index; the count still reflects it, and the reaper must find it
// through the full hash rather than trust the short index.
func TestRedisRunnerDirectoryStrandedReapFallsBackToFullScanWhenIndexShort(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-short"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-short/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-short", time.Minute)
	seedStrandedLeasedAssignment(t, ctx, rdb, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	if err := rdb.SRem(ctx, directory.keys.runnerLeasedAssignmentsKey(runnerID), assignmentID).Err(); err != nil {
		t.Fatalf("drop index entry: %v", err)
	}

	// Asserted on the enumeration directly: the end-to-end reap would also find
	// the assignment through the handoff ledger and hide a missing fallback.
	candidates, err := directory.strandedLeaseCandidates(ctx, runnerID)
	if err != nil {
		t.Fatalf("strandedLeaseCandidates() error = %v", err)
	}
	if len(candidates) != 1 || candidates[0] != assignmentID {
		t.Fatalf("strandedLeaseCandidates() = %v, want [%s] via the count-shortfall scan", candidates, assignmentID)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != "" {
		t.Fatalf("assignment state = %q, want released", got)
	}
	assertStrandedReapLeftLeaseCount(t, server, directory, runnerID, "0")
}

// TestRedisRunnerDirectoryStrandedReapIsBoundedAndIdempotent keeps the pass
// proportional to its limit and safe to repeat: a second call over an already
// drained directory releases nothing.
func TestRedisRunnerDirectoryStrandedReapIsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))

	// One stranded lease per runner, so the bound is exercised across runners
	// rather than through repeated single-runner claims.
	const runnerCount = 3
	for i := 0; i < runnerCount; i++ {
		runnerID := "runner-bounded-" + strconv.Itoa(i)
		session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)
		assignment := redisDirectoryTestAssignment(AssignmentID("exec-b" + strconv.Itoa(i) + "/node/a"))
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-b"+strconv.Itoa(i), time.Minute)
		seedStrandedLeasedAssignment(t, ctx, rdb, directory, session, assignment, lease)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 2)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 2 {
		t.Fatalf("released = %d, want exactly the limit of 2", reap.Released)
	}

	reap, err = directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("second ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("second released = %d, want the remaining 1", reap.Released)
	}

	reap, err = directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("third ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("third released = %d, want 0 on a drained directory", reap.Released)
	}
	for i := 0; i < runnerCount; i++ {
		assertStrandedReapLeftLeaseCount(t, server, directory, "runner-bounded-"+strconv.Itoa(i), "0")
	}
}
