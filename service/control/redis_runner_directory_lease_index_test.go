package control

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

// assignmentStateReadHook counts the reads that reach the assignment-state hash,
// which is the directory's whole assignment table. Every read of that hash on
// the poll path is a read that grows with the size of the directory instead of
// with the number of leases the polling runner holds, so the tests below pin the
// poll to the second shape.
type assignmentStateReadHook struct {
	mu      sync.Mutex
	key     string
	hgetall int
}

func (h *assignmentStateReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *assignmentStateReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.observe(cmd)
		return next(ctx, cmd)
	}
}

func (h *assignmentStateReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.observe(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *assignmentStateReadHook) observe(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	key, ok := args[1].(string)
	if !ok || key != h.key || strings.ToLower(cmd.Name()) != "hgetall" {
		return
	}
	h.mu.Lock()
	h.hgetall++
	h.mu.Unlock()
}

func (h *assignmentStateReadHook) reset() {
	h.mu.Lock()
	h.hgetall = 0
	h.mu.Unlock()
}

func (h *assignmentStateReadHook) fullHashReads() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hgetall
}

type redisDirectorySeededLease struct {
	assignmentID AssignmentID
	leaseID      engine.LeaseID
	leaseToken   engine.LeaseToken
}

// seedRedisDirectoryLeases enqueues count assignments, claims each for the
// session, and finalizes them into live leases. The leases already seeded are
// reported as active so each poll claims a new assignment rather than replaying
// one the runner is already running.
func seedRedisDirectoryLeases(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession, prefix string, count int) []redisDirectorySeededLease {
	t.Helper()

	seeded := make([]redisDirectorySeededLease, 0, count)
	for i := 0; i < count; i++ {
		assignmentID := AssignmentID(fmt.Sprintf("%s/activation-%d", prefix, i))
		assignment := redisDirectoryTestAssignment(assignmentID)
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

		req := redisDirectoryClaimRequest(session, 1)
		for _, held := range seeded {
			req.ActiveLeaseIDs = append(req.ActiveLeaseIDs, string(held.leaseID))
		}
		claim, ok, err := directory.ClaimForRunner(ctx, req)
		if err != nil || !ok {
			t.Fatalf("ClaimForRunner() seeding %q: ok=%v err=%v", assignmentID, ok, err)
		}
		if claim.ClaimID == "" {
			t.Fatalf("ClaimForRunner() seeding %q replayed lease %+v, want a fresh claim", assignmentID, claim.Lease)
		}

		held := redisDirectorySeededLease{
			assignmentID: assignmentID,
			leaseID:      engine.LeaseID("lease-" + string(assignmentID)),
			leaseToken:   engine.LeaseToken("token-" + string(assignmentID)),
		}
		if err := directory.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{
			LeaseID:    held.leaseID,
			LeaseToken: held.leaseToken,
			Task:       assignment.Task,
			NodeType:   assignment.Routing.NodeType,
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
		}); err != nil {
			t.Fatalf("FinalizeClaim(%q) error = %v", assignmentID, err)
		}
		seeded = append(seeded, held)
	}
	return seeded
}

func requireRedisSet(t *testing.T, ctx context.Context, rdb *redis.Client, key string) []string {
	t.Helper()

	members, err := rdb.SMembers(ctx, key).Result()
	if err != nil {
		t.Fatalf("SMembers(%q) error = %v", key, err)
	}
	return members
}

func TestRedisRunnerDirectoryPollReplaysLeaseWithoutReadingTheWholeAssignmentState(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := &assignmentStateReadHook{key: directory.keys.assignmentState}
	rdb.AddHook(hook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	noisy := registerRedisDirectoryRunner(t, ctx, directory, "runner-2", 4)
	held := seedRedisDirectoryLeases(t, ctx, directory, session, "exec-1/runner-1", 1)
	seedRedisDirectoryLeases(t, ctx, directory, noisy, "exec-1/runner-2", 4)

	indexKey := directory.keys.runnerLeasedAssignmentsKey("runner-1")
	if indexed := requireRedisSet(t, ctx, rdb, indexKey); len(indexed) != 1 || indexed[0] != string(held[0].assignmentID) {
		t.Fatalf("leased assignment index = %v, want [%s]", indexed, held[0].assignmentID)
	}
	states, err := rdb.HGetAll(ctx, directory.keys.assignmentState).Result()
	if err != nil {
		t.Fatalf("HGetAll(assignment state) error = %v", err)
	}
	if len(states) != 5 {
		t.Fatalf("assignment state holds %d assignments, want 5", len(states))
	}

	hook.reset()
	replay, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want a lease replay", ok, err)
	}
	if replay.Lease == nil || replay.Lease.LeaseID != held[0].leaseID {
		t.Fatalf("replay = %+v, want lease %q", replay, held[0].leaseID)
	}
	if reads := hook.fullHashReads(); reads != 0 {
		t.Fatalf("poll read the whole assignment-state hash %d time(s) while replaying 1 of the %d assignments in it, want 0", reads, len(states))
	}
}

func TestRedisRunnerDirectoryPollRebuildsTheLeaseIndexWhenItIsShort(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := &assignmentStateReadHook{key: directory.keys.assignmentState}
	rdb.AddHook(hook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	held := seedRedisDirectoryLeases(t, ctx, directory, session, "exec-1/runner-1", 1)

	indexKey := directory.keys.runnerLeasedAssignmentsKey("runner-1")
	// A lease finalized by a control plane that predates the index is absent from
	// it while the count every version maintains still accounts for it.
	if _, err := rdb.SRem(ctx, indexKey, string(held[0].assignmentID)).Result(); err != nil {
		t.Fatalf("SRem(%q) error = %v", indexKey, err)
	}
	count, err := rdb.HGet(ctx, directory.keys.runnerLeaseCount, "runner-1").Result()
	if err != nil || count != "1" {
		t.Fatalf("runner lease count = %q err=%v, want 1", count, err)
	}

	hook.reset()
	replay, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want the lease missing from the index", ok, err)
	}
	if replay.Lease == nil || replay.Lease.LeaseID != held[0].leaseID {
		t.Fatalf("replay = %+v, want lease %q", replay, held[0].leaseID)
	}
	if reads := hook.fullHashReads(); reads == 0 {
		t.Fatal("poll trusted the short index instead of reading the whole assignment-state hash to rebuild it")
	}
	if indexed := requireRedisSet(t, ctx, rdb, indexKey); len(indexed) != 1 || indexed[0] != string(held[0].assignmentID) {
		t.Fatalf("leased assignment index after rebuild = %v, want [%s]", indexed, held[0].assignmentID)
	}

	// The rebuilt index is trusted again, so the next poll stays bounded.
	hook.reset()
	next := redisDirectoryClaimRequest(session, 1)
	next.ActiveLeaseIDs = []string{string(held[0].leaseID)}
	if _, _, err := directory.ClaimForRunner(ctx, next); err != nil {
		t.Fatalf("ClaimForRunner() after rebuild error = %v", err)
	}
	if reads := hook.fullHashReads(); reads != 0 {
		t.Fatalf("poll after the rebuild read the whole assignment-state hash %d time(s), want 0", reads)
	}
}

func TestRedisRunnerDirectoryPollFindsLeaseMissingFromIndexDespiteAStaleIndexEntry(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := &assignmentStateReadHook{key: directory.keys.assignmentState}
	rdb.AddHook(hook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 2)
	seeded := seedRedisDirectoryLeases(t, ctx, directory, session, "exec-1/runner-1", 2)
	released, live := seeded[0], seeded[1]

	if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		AssignmentID: released.assignmentID,
		LeaseID:      released.leaseID,
		LeaseToken:   released.leaseToken,
	}); err != nil {
		t.Fatalf("ReleaseLeased() error = %v", err)
	}

	indexKey := directory.keys.runnerLeasedAssignmentsKey("runner-1")
	// A release leaves its index entry behind for a poll to prune, and dropping a
	// live lease from the index is what a control plane that predates the index
	// leaves behind.
	if _, err := rdb.SRem(ctx, indexKey, string(live.assignmentID)).Result(); err != nil {
		t.Fatalf("SRem(%q) error = %v", indexKey, err)
	}

	// The fixture is exactly the misleading shape: the index names as many
	// entries as the runner's lease count while one of them is stale and a live
	// lease is missing, so a completeness check that compared the two sizes would
	// accept the index and never look for the missing lease.
	if indexed := requireRedisSet(t, ctx, rdb, indexKey); len(indexed) != 1 || indexed[0] != string(released.assignmentID) {
		t.Fatalf("leased assignment index = %v, want [%s]", indexed, released.assignmentID)
	}
	count, err := rdb.HGet(ctx, directory.keys.runnerLeaseCount, "runner-1").Result()
	if err != nil || count != "1" {
		t.Fatalf("runner lease count = %q err=%v, want 1", count, err)
	}

	hook.reset()
	replay, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want the lease missing from the index", ok, err)
	}
	if replay.Lease == nil || replay.Lease.LeaseID != live.leaseID {
		t.Fatalf("replay = %+v, want lease %q", replay, live.leaseID)
	}
	if reads := hook.fullHashReads(); reads == 0 {
		t.Fatal("poll accepted the index on its size alone instead of reading the whole assignment-state hash")
	}
}
