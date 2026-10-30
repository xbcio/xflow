package control

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// A runner with Concurrency > 1 keeps polling while one of its workers executes
// a lease. replayLease matches on (runner, session, state==leased), which is
// equally true of a lease this very process is running right now — so every
// idle worker's poll used to be handed the same live lease, and the task ran
// once per unit of concurrency. Not a failover edge case: the steady state.
//
// The runner is the only party that knows which leases it is actually
// executing, so it reports them and the directory skips exactly those. The
// genuine replay cases are untouched, and each has its own coverage:
//   - response loss: the runner never received the lease, so it never reports
//     it active (TestRedisRunnerDirectoryReplaysFullLeaseAfterRestartAndReregistration,
//     first half, replays under the same session)
//   - process restart: the in-flight set is empty in the new process (same
//     test, second half, after re-registration)
func TestRedisRunnerDirectoryDoesNotReplayALeaseTheRunnerIsExecuting(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 2)
	assignment := redisDirectoryTestAssignment("exec-1/node-active/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	lease := &engine.TaskLease{
		LeaseID:     "lease-active",
		LeaseToken:  "token-active",
		Attempt:     1,
		Task:        assignment.Task,
		Input:       &types.Input{Data: map[string]any{"request_id": "req-1"}},
		NodeType:    assignment.Routing.NodeType,
		NodeVersion: assignment.Routing.NodeVersion,
		IssuedAt:    time.Now().UTC(),
		TTL:         time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	// The runner's first worker is executing lease-active. Its second worker
	// polls, reporting the lease it already holds.
	req := redisDirectoryClaimRequest(session, 2)
	req.ActiveLeaseIDs = []string{string(lease.LeaseID)}
	again, ok, err := directory.ClaimForRunner(ctx, req)
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if ok {
		t.Fatalf("ClaimForRunner() handed back lease %q while the runner reported it in flight — "+
			"every idle worker gets the same task and the node executes once per unit of concurrency",
			again.Lease.LeaseID)
	}
}

// The complement: a lease the runner does NOT report as in flight must still
// replay under the same session. This is the response-loss case — the runner
// never received the lease, so it cannot report it — and it is the reason
// replay exists at all. Without this assertion the fix above could be
// "never replay", which strands the task until its TTL elapses.
func TestRedisRunnerDirectoryStillReplaysALeaseTheRunnerIsNotExecuting(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 2)
	assignment := redisDirectoryTestAssignment("exec-1/node-lost/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	lease := &engine.TaskLease{
		LeaseID: "lease-lost", LeaseToken: "token-lost", Attempt: 1,
		Task: assignment.Task, NodeType: assignment.Routing.NodeType,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	// The runner is executing some other lease, and never saw this one.
	req := redisDirectoryClaimRequest(session, 2)
	req.ActiveLeaseIDs = []string{"lease-something-else"}
	replay, ok, err := directory.ClaimForRunner(ctx, req)
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok || replay.Lease == nil {
		t.Fatal("a finalized lease the runner never received was not replayed — the task is " +
			"stranded until its TTL elapses")
	}
	if replay.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("replayed lease token = %q, want %q", replay.Lease.LeaseToken, lease.LeaseToken)
	}
}

// finalizeReplayTestLease leaves one finalized lease for runner-1 and returns
// its assignment ID and the key its metadata lives under, so a test can damage
// the stored record.
func finalizeReplayTestLease(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession, leaseID engine.LeaseID) (AssignmentID, string) {
	t.Helper()

	assignment := redisDirectoryTestAssignment(AssignmentID("exec-1/node-" + string(leaseID) + "/activation-1"))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	lease := &engine.TaskLease{
		LeaseID: leaseID, LeaseToken: engine.LeaseToken("token-" + string(leaseID)), Attempt: 1,
		Task: assignment.Task, NodeType: assignment.Routing.NodeType,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	return assignment.AssignmentID, directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID))
}

// The runner's own in-flight leases are the common case of a replay poll: every
// idle worker polls while the others execute. The stored lease ID answers that
// without decoding the assignment payload or the metadata, so a record the
// decode would reject must not surface as a poll error for a lease the runner
// is running. If the decode still ran, the poll error would stop the runner
// from claiming work at all.
func TestRedisRunnerDirectoryReplaySkipsDecodingALeaseTheRunnerIsExecuting(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 2)
	_, metaKey := finalizeReplayTestLease(t, ctx, directory, session, "lease-busy")
	if err := rdb.Set(ctx, metaKey, "{not json", redis.KeepTTL).Err(); err != nil {
		t.Fatalf("corrupt lease metadata: %v", err)
	}

	req := redisDirectoryClaimRequest(session, 2)
	req.ActiveLeaseIDs = []string{"lease-busy"}
	if _, ok, err := directory.ClaimForRunner(ctx, req); err != nil || ok {
		t.Fatalf("ClaimForRunner() = ok %v, err %v; want no claim and no error for a lease the runner is executing", ok, err)
	}

	// Not reported in flight, the same record has to be decoded, which proves
	// the poll above passed because it skipped the decode rather than because
	// the record was readable.
	req.ActiveLeaseIDs = nil
	_, _, err := directory.ClaimForRunner(ctx, req)
	if err == nil || !strings.Contains(err.Error(), "decode persisted lease") {
		t.Fatalf("ClaimForRunner() error = %v, want a decode error for a lease the runner is not executing", err)
	}
}

// A lease whose ID the directory does not hold next to its metadata cannot be
// recognised without decoding. It still has to be recognised: the decode path
// is what keeps an in-flight lease from being handed out twice.
func TestRedisRunnerDirectoryReplayFallsBackToDecodingWhenTheStoredLeaseIDIsMissing(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 2)
	finalizeReplayTestLease(t, ctx, directory, session, "lease-nohash")
	if err := rdb.Del(ctx, directory.keys.assignmentLeaseID).Err(); err != nil {
		t.Fatalf("drop stored lease ids: %v", err)
	}

	req := redisDirectoryClaimRequest(session, 2)
	req.ActiveLeaseIDs = []string{"lease-nohash"}
	if got, ok, err := directory.ClaimForRunner(ctx, req); err != nil || ok {
		t.Fatalf("ClaimForRunner() = %+v, ok %v, err %v; want no claim for a lease the runner is executing", got, ok, err)
	}

	req.ActiveLeaseIDs = nil
	got, ok, err := directory.ClaimForRunner(ctx, req)
	if err != nil || !ok || got.Lease == nil || got.Lease.LeaseID != "lease-nohash" {
		t.Fatalf("ClaimForRunner() = %+v, ok %v, err %v; want the lease replayed", got, ok, err)
	}
}

// An in-flight lease whose metadata has expired is still stranded: nothing can
// renew or report it, and it holds the runner's capacity. The skip must not
// hide it from the release that frees that capacity.
func TestRedisRunnerDirectoryReplayStillReleasesAStrandedLeaseTheRunnerReportsActive(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 2)
	assignmentID, metaKey := finalizeReplayTestLease(t, ctx, directory, session, "lease-stranded")
	if err := rdb.Del(ctx, metaKey).Err(); err != nil {
		t.Fatalf("expire lease metadata: %v", err)
	}

	req := redisDirectoryClaimRequest(session, 2)
	req.ActiveLeaseIDs = []string{"lease-stranded"}
	if _, _, err := directory.ClaimForRunner(ctx, req); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	state, err := rdb.HGet(ctx, directory.keys.assignmentState, string(assignmentID)).Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("read assignment state: %v", err)
	}
	if state == redisAssignmentLeased {
		t.Fatalf("assignment state = %q, want the stranded lease released", state)
	}
}
