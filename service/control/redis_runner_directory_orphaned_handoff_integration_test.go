package control

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// The two tests below drive the orphaned-handoff fix end to end against a real
// Redis. Every other test for this pass runs on miniredis, which implements only
// a subset of Lua, so it cannot show whether the scripts that actually decide a
// crashed owner's fate behave as written. Those are:
//
//   - redisTakeHandoffRecoveryLua, the resolver-token fence;
//   - the recovery re-arm inside redisRecoverExpiredClaimsLua, which is the only
//     thing that makes a dead owner's token takeable;
//   - redisSettleHandoffLua, which requeues or drops the wedged assignment.
//
//	XFLOW_TEST_REDIS_ADDR=127.0.0.1:6380 go test ./service/control -run TestRedisRunnerDirectoryRealRedisOrphanedHandoff

// TestRedisRunnerDirectoryRealRedisOrphanedHandoffReapE2E reproduces the
// production shape of the defect and runs it to resolution.
//
// A runner claims a collection-sink assignment, reaches the pre-Build crash
// fence (MarkClaimLeaseMayExist), and dies. That leaves the assignment parked as
// 'lease_may_exist' debt whose only resolver is the dead runner's own poll, so
// the assignment is wedged forever — which is what a collection sink pinned to a
// PID-scoped embedded runner experiences after any SAS restart. The fix is the
// control plane's orphaned-handoff reaper; this test proves it recovers the work
// on a real server.
func TestRedisRunnerDirectoryRealRedisOrphanedHandoffReapE2E(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis runner-directory test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	directory := newRealRedisRunnerDirectory(t, rdb)

	// The embedded runner that owns the collection sink. Its heartbeat is fresh,
	// so while it lives its debt must be left strictly alone.
	owner := registerRedisDirectoryRunnerAt(t, ctx, directory, "sas-host-40898", time.Now().UTC())

	assignment := redisDirectoryTestAssignment("exec-orphan/node-sink/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, owner, 1)

	// The owner dies after taking the claim but before it can resolve the
	// uncertainty: the claim is parked as lease_may_exist, waiting for an engine
	// answer only its own poll would ever ask for.
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist() error = %v", err)
	}

	// The token is still inside its first deadline, so it is not yet eligible for
	// anyone — including the reaper. The debt is parked, not takeable.
	if claims, err := directory.ListOrphanedHandoffs(ctx, 8); err != nil || len(claims) != 0 {
		t.Fatalf("ListOrphanedHandoffs with a fresh token = %d claims, err=%v; want none", len(claims), err)
	}

	// The process dies. Its heartbeat ages past the staleness window; only the
	// directory's own clock changes.
	directory.clock = func() time.Time {
		return time.Now().UTC().Add(DefaultOrphanedHandoffOwnerStale + time.Minute)
	}

	// The owner is gone, but its token is still inside its deadline, so nobody may
	// take it yet. This is the fence that keeps two resolvers off one claim.
	if claims, err := directory.ListOrphanedHandoffs(ctx, 8); err != nil || len(claims) != 0 {
		t.Fatalf("ListOrphanedHandoffs before the token aged out = %d claims, err=%v; want none", len(claims), err)
	}

	// The claim-expiry backstop ages the token out. In production this is the
	// passage of claimTTL; here both clocks it reads are moved into the past and
	// the same Lua runs. Aging the claim's own expiry is what puts the record in
	// the sweep's reach at all — it is driven by ZRANGEBYSCORE claim:expiry — and
	// the handoff deadline is what the lease_may_exist branch re-arms on. The
	// branch only re-arms: it deliberately does not requeue.
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{
		Score:  float64(expired),
		Member: string(claim.ClaimID),
	}).Err(); err != nil {
		t.Fatalf("age the claim expiry: %v", err)
	}
	if err := rdb.HSet(ctx, directory.keys.handoffRecoveryDeadline, string(claim.ClaimID), expired).Err(); err != nil {
		t.Fatalf("age the handoff recovery deadline: %v", err)
	}
	if err := directory.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatalf("ReclaimExpiredClaims() error = %v", err)
	}
	if ready, err := rdb.HGet(ctx, directory.keys.handoffRecoveryReady, string(claim.ClaimID)).Result(); err != nil || ready != "1" {
		t.Fatalf("recovery-ready = %q, err=%v; want the backstop to re-arm the debt", ready, err)
	}

	// The defect: the backstop re-armed the debt but left the assignment wedged. A
	// replacement runner polling the same directory gets nothing.
	replacement := registerRedisDirectoryRunnerAt(t, ctx, directory, "sas-host-40095", time.Now().UTC())
	if _, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(replacement, 1)); err != nil || ok {
		t.Fatalf("replacement claim before the reap ok=%v, err=%v; want the assignment still wedged", ok, err)
	}

	// The fix: the reaper takes the token, asks the engine authority, and settles.
	core := &Core{engine: &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable}, runners: directory}
	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ReapOrphanedHandoffs() error = %v", err)
	}
	if result != (ReapResult{Inspected: 1, Released: 1}) {
		t.Fatalf("result = %+v, want one inspected and one settled", result)
	}

	// The ledger record is gone and the assignment is claimable again — by any
	// runner, not just the dead one.
	if _, err := rdb.HGet(ctx, directory.keys.handoffState, string(claim.ClaimID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("handoff state after the reap err=%v; want the record settled", err)
	}
	reclaimed, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(replacement, 1))
	if err != nil || !ok {
		t.Fatalf("post-reap claim ok=%v, err=%v; want the requeued assignment", ok, err)
	}
	if reclaimed.Assignment.AssignmentID != assignment.AssignmentID || reclaimed.Handoff != nil {
		t.Fatalf("post-reap claim = %#v; want the requeued assignment with no handoff", reclaimed)
	}
}

// TestRedisRunnerDirectoryRealRedisOrphanedHandoffOwnerLiveness pins the one gate
// that decides whether the reaper is safe: it must reclaim debt only from an
// owner that is actually gone. The same takeable record — re-armed token and all
// — is offered to the pass twice, and the only difference between the two is
// whether the owner is still heartbeating.
func TestRedisRunnerDirectoryRealRedisOrphanedHandoffOwnerLiveness(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis runner-directory test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	directory := newRealRedisRunnerDirectory(t, rdb)

	owner := registerRedisDirectoryRunnerAt(t, ctx, directory, "sas-host-live", time.Now().UTC())
	assignment := redisDirectoryTestAssignment("exec-live/node-sink/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, owner, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist() error = %v", err)
	}
	// Put the token outside its deadline. Unlike the E2E above this is the runner's
	// own dispatch-failure path, used here only to make the record fully takeable
	// so that liveness is the only thing left deciding the outcome.
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable() error = %v", err)
	}

	// Live owner: takeable record, but the pass must keep its hands off.
	if claims, err := directory.ListOrphanedHandoffs(ctx, 8); err != nil || len(claims) != 0 {
		t.Fatalf("ListOrphanedHandoffs with a live owner = %d claims, err=%v; want none", len(claims), err)
	}

	// The same record, one dead heartbeat later, is exactly what the pass exists
	// to take.
	directory.clock = func() time.Time {
		return time.Now().UTC().Add(DefaultOrphanedHandoffOwnerStale + time.Minute)
	}
	claims, err := directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimID != claim.ClaimID || claims[0].Handoff == nil {
		t.Fatalf("claims = %#v; want the dead owner's handoff taken", claims)
	}
	if claims[0].Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("handoff state = %q, want %q", claims[0].Handoff.State, HandoffDebtLeaseMayExist)
	}
}

// TestRedisRunnerDirectoryRealRedisOrphanedHandoffMissingAssignmentState pins the
// one ledger shape the reaper could not settle in the wild.
//
// A handoff record can outlive the assignment-side state it points at: the
// control plane's live test-environment directory held a 'lease_may_exist'
// record whose claimsAssignment, claim expiry, claim runner and assignment data
// were all intact while assignment:state, assignment:claim and assignment:runner
// had lost the field entirely — no field at all, not a stale one, so it cannot be
// explained by a transition in this version. That is enough to strand the
// assignment, because redisSettleHandoffLua required assignmentState == 'claimed'
// AND assignmentClaim == claimID. It then answered 'unresolved' on every cadence
// forever: the pass found the orphan (candidates_total=2) and failed it
// (pass_total{outcome="error"}=2) with released_total pinned at 0, while the
// collection sink stayed wedged and sink_calls stayed 0.
//
// The fix lets ownership be proven from either direction of the claim index and
// lets a missing state stand, since a live lease would be written through that
// same field and would therefore be present.
func TestRedisRunnerDirectoryRealRedisOrphanedHandoffMissingAssignmentState(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis runner-directory test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	directory := newRealRedisRunnerDirectory(t, rdb)

	owner := registerRedisDirectoryRunnerAt(t, ctx, directory, "sas-host-40898", time.Now().UTC())
	assignment := redisDirectoryTestAssignment("exec-orphan-state/node-sink/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, owner, 1)

	// The owner dies after taking the claim but before it can resolve the
	// uncertainty, leaving the handoff record behind.
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist() error = %v", err)
	}

	// The shape the live directory actually held: the assignment-side state, its
	// reverse index and its runner field are gone, while the assignment's data,
	// session, and every claim-side key survive. The mark above fences on the
	// claim being active, so this loss has to land after the handoff record
	// exists — which is the only order the live ledger is consistent with.
	for _, key := range []string{directory.keys.assignmentState, directory.keys.assignmentClaim, directory.keys.assignmentRunner} {
		if err := rdb.HDel(ctx, key, string(assignment.AssignmentID)).Err(); err != nil {
			t.Fatalf("drop %s: %v", key, err)
		}
	}
	if n, err := rdb.HLen(ctx, directory.keys.claimsAssignment).Result(); err != nil || n != 1 {
		t.Fatalf("claimsAssignment len = %d, err=%v; want the forward mapping to survive", n, err)
	}

	directory.clock = func() time.Time {
		return time.Now().UTC().Add(DefaultOrphanedHandoffOwnerStale + time.Minute)
	}
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{
		Score:  float64(expired),
		Member: string(claim.ClaimID),
	}).Err(); err != nil {
		t.Fatalf("age the claim expiry: %v", err)
	}
	if err := rdb.HSet(ctx, directory.keys.handoffRecoveryDeadline, string(claim.ClaimID), expired).Err(); err != nil {
		t.Fatalf("age the handoff recovery deadline: %v", err)
	}
	if err := directory.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatalf("ReclaimExpiredClaims() error = %v", err)
	}

	core := &Core{engine: &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable}, runners: directory}
	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ReapOrphanedHandoffs() error = %v; want the inconsistent ledger settled", err)
	}
	if result != (ReapResult{Inspected: 1, Released: 1}) {
		t.Fatalf("result = %+v, want one inspected and one settled", result)
	}
	if _, err := rdb.HGet(ctx, directory.keys.handoffState, string(claim.ClaimID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("handoff state after the reap err=%v; want the record settled", err)
	}

	// The point of the fix: the assignment is back in the queue, so the sink it
	// was pinned to can run again instead of staying wedged behind dead debt.
	replacement := registerRedisDirectoryRunnerAt(t, ctx, directory, "sas-host-40095", time.Now().UTC())
	reclaimed, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(replacement, 1))
	if err != nil || !ok {
		t.Fatalf("post-reap claim ok=%v, err=%v; want the requeued assignment", ok, err)
	}
	if reclaimed.Assignment.AssignmentID != assignment.AssignmentID || reclaimed.Handoff != nil {
		t.Fatalf("post-reap claim = %#v; want the requeued assignment with no handoff", reclaimed)
	}
}

func registerRedisDirectoryRunnerAt(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, runnerID string, now time.Time) RunnerSession {
	t.Helper()

	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          now,
	})
	if err != nil {
		t.Fatalf("Register(%q) error = %v", runnerID, err)
	}
	return session
}
