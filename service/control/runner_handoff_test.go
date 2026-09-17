package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestMemoryDrainRetainsLeaseMayExistDebtUntilEngineResolution(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/node-a/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)

	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "drain-debt", "hash-debt", RunnerDesiredStateDraining)); err != nil {
		t.Fatalf("drain: %v", err)
	}

	snapshot, found, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found || snapshot.Drain == nil {
		t.Fatalf("RunnerControl = %+v, found=%v, err=%v", snapshot, found, err)
	}
	if got := snapshot.Drain; got.UnsettledDebt != 1 || got.HandoffDebt != 1 || got.LeaseMayExistDebt != 1 || got.ReplayableDebt != 0 {
		t.Fatalf("drain debt = %+v, want one lease_may_exist blocker", got)
	}
	if snapshot.Drain.ServerQuiescent {
		t.Fatal("unknown handoff must not be server quiescent")
	}

	recovery, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok || recovery.Handoff == nil || recovery.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("recovery claim = %#v, ok=%v, err=%v; want lease_may_exist debt", recovery, ok, err)
	}
	if err := directory.SettleClaimHandoff(ctx, recovery.ClaimID, HandoffDispositionRequeue); err != nil {
		t.Fatalf("SettleClaimHandoff(requeue): %v", err)
	}
	settled, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || settled.Drain == nil || settled.Drain.UnsettledDebt != 0 {
		t.Fatalf("settled drain projection = %+v, err=%v; want no debt", settled.Drain, err)
	}
}

func TestMemoryDrainResolverFinalizesCreatedLeaseWithoutNewAdmission(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/finalize/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	lease := &engine.TaskLease{
		LeaseID: "lease-debt", LeaseToken: "token-debt", Task: assignment.Task,
		Input: &types.Input{Data: map[string]any{"v": "original"}}, NodeType: assignment.Routing.NodeType,
	}
	if err := directory.RecordClaimLeaseCreated(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "drain-finalize", "hash-finalize", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}

	fake := &fakeControlEngine{recoverLease: lease}
	core := &Core{engine: fake, runners: directory, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask(resolve): %v", err)
	}
	if resp.Lease == nil || resp.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("resolved lease = %+v, want token %q", resp.Lease, lease.LeaseToken)
	}
	// Mark the recovered lease as in-flight so the drain recovery path cannot
	// replay the same finalized lease to another worker. The queued admission is
	// still blocked by the server-side drain gate.
	request := testClaimRequest(session, 1)
	request.ActiveLeaseIDs = []string{string(lease.LeaseID)}
	if newClaim, ok, err := directory.ClaimForRunner(ctx, request); err != nil || ok {
		t.Fatalf("draining new claim = %#v, ok=%v, err=%v; want blocked", newClaim, ok, err)
	}
	snapshot, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.ReplayableDebt != 1 || snapshot.Drain.UnsettledDebt != 1 {
		t.Fatalf("finalized debt projection = %+v, err=%v", snapshot.Drain, err)
	}
}

func TestCoreHandoffResolverRequeuesWhenNoLeaseExists(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/requeue/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable}
	core := &Core{engine: fake, runners: directory, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask(resolve no lease): %v", err)
	}
	// The resolver requeued the original assignment and the same poll loop may
	// immediately admit it again. Its empty lease identity proves this is a new
	// ordinary admission, not the unknown handoff being replayed.
	if resp.Lease == nil || resp.Lease.LeaseID != "" || resp.Lease.LeaseToken != "" {
		t.Fatalf("resolve no lease response = %+v, want a fresh ordinary admission", resp.Lease)
	}
}

func TestCoreDoesNotReplayRecordedLeaseWhenEngineCannotRecoverIt(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/no-replay/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	oldLease := &engine.TaskLease{LeaseID: "old-lease", LeaseToken: "old-token", Task: assignment.Task}
	if err := directory.RecordClaimLeaseCreated(ctx, claim.ClaimID, oldLease); err != nil {
		t.Fatal(err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	core := &Core{engine: &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable}, runners: directory, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask: %v", err)
	}
	if resp.Lease == nil || resp.Lease.LeaseToken == oldLease.LeaseToken {
		t.Fatalf("response = %+v; stale recorded token must not be replayed", resp.Lease)
	}
}

func TestHandoffResolverKeepsDebtOnAuthorityFailure(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/failure/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlEngine{recoverErr: errors.New("state unavailable")}
	core := &Core{engine: fake, runners: directory, pollWait: time.Second}
	if _, err := core.pollTask(ctx, protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}, TransportInfo{}); err == nil {
		t.Fatal("pollTask authority failure = nil, want error")
	}
	// The failure released the resolver token but did not remove the debt.
	retry, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok || retry.Handoff == nil {
		t.Fatalf("retry handoff = %#v, ok=%v, err=%v; want retained debt", retry, ok, err)
	}
}

func TestMemoryFinalizedHandoffSettlesOnlyMatchingLease(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/finalized/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	lease := &engine.TaskLease{LeaseID: "lease-final", LeaseToken: "token-final", Task: assignment.Task}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "memory-final", "memory-final-hash", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	if err := directory.SettleFinalizedHandoff(ctx, assignment.AssignmentID, lease.LeaseID, "wrong"); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.ReplayableDebt != 1 {
		t.Fatalf("wrong-token projection = %+v, err=%v; want retained finalized debt", snapshot.Drain, err)
	}
	if err := directory.SettleFinalizedHandoff(ctx, assignment.AssignmentID, lease.LeaseID, lease.LeaseToken); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err = directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.UnsettledDebt != 0 {
		t.Fatalf("settled projection = %+v, err=%v; want no debt", snapshot.Drain, err)
	}
}

func TestDrainMutationIncludesCurrentHandoffDebt(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		new   func() (RunnerDirectory, func())
		reg   func(t *testing.T, ctx context.Context, directory RunnerDirectory) RunnerSession
		enq   func(t *testing.T, ctx context.Context, directory RunnerDirectory, assignment Assignment)
		claim func(t *testing.T, ctx context.Context, directory RunnerDirectory, session RunnerSession) Claim
	}{
		{
			name: "memory",
			new:  func() (RunnerDirectory, func()) { return NewMemoryRunnerDirectory(), func() {} },
			reg: func(t *testing.T, ctx context.Context, directory RunnerDirectory) RunnerSession {
				return mustRegisterMemoryRunner(t, ctx, directory.(*MemoryRunnerDirectory), "runner-mutation", 1)
			},
			enq: func(t *testing.T, ctx context.Context, directory RunnerDirectory, assignment Assignment) {
				mustEnqueueAssignment(t, ctx, directory.(*MemoryRunnerDirectory), assignment)
			},
			claim: func(t *testing.T, ctx context.Context, directory RunnerDirectory, session RunnerSession) Claim {
				return mustClaimAssignment(t, ctx, directory.(*MemoryRunnerDirectory), session)
			},
		},
		{
			name: "redis",
			new: func() (RunnerDirectory, func()) {
				_, rdb := newRedisRunnerDirectoryTestClient(t)
				return NewRedisRunnerDirectory(rdb), func() { _ = rdb.Close() }
			},
			reg: func(t *testing.T, ctx context.Context, directory RunnerDirectory) RunnerSession {
				return registerRedisDirectoryRunner(t, ctx, directory.(*RedisRunnerDirectory), "runner-mutation", 1)
			},
			enq: func(t *testing.T, ctx context.Context, directory RunnerDirectory, assignment Assignment) {
				mustEnqueueRedisDirectoryAssignment(t, ctx, directory.(*RedisRunnerDirectory), assignment)
			},
			claim: func(t *testing.T, ctx context.Context, directory RunnerDirectory, session RunnerSession) Claim {
				return claimRedisDirectoryAssignment(t, ctx, directory.(*RedisRunnerDirectory), session, 1)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			directory, cleanup := tc.new()
			defer cleanup()
			session := tc.reg(t, ctx, directory)
			assignment := testAssignment("exec-debt/mutation/activation-1")
			tc.enq(t, ctx, directory, assignment)
			claim := tc.claim(t, ctx, directory, session)
			ledger := directory.(HandoffDebtDirectory)
			if err := ledger.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
				t.Fatal(err)
			}
			control := directory.(RunnerControlDirectory)
			snapshot, err := control.SetRunnerControl(ctx, controlRequest("runner-mutation", "drain", "mutation", "mutation-hash", RunnerDesiredStateDraining))
			if err != nil || snapshot.Drain == nil {
				t.Fatalf("SetRunnerControl = %+v, err=%v", snapshot, err)
			}
			if snapshot.Drain.UnsettledDebt != 1 || snapshot.Drain.LeaseMayExistDebt != 1 {
				t.Fatalf("drain response = %+v, want current ledger debt", snapshot.Drain)
			}
		})
	}
}

func TestRedisDrainRetainsAndResolvesHandoffDebt(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/redis/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "redis-debt", "redis-debt-hash", RunnerDesiredStateDraining)); err != nil {
		t.Fatalf("drain: %v", err)
	}

	snapshot, found, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found || snapshot.Drain == nil {
		t.Fatalf("RunnerControl = %+v, found=%v, err=%v", snapshot, found, err)
	}
	if got := snapshot.Drain; got.UnsettledDebt != 1 || got.HandoffDebt != 1 || got.LeaseMayExistDebt != 1 {
		t.Fatalf("drain debt = %+v, want one unresolved lease_may_exist", got)
	}

	recovery, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok || recovery.Handoff == nil || recovery.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("recovery = %#v, ok=%v, err=%v", recovery, ok, err)
	}
	if err := directory.SettleClaimHandoff(ctx, recovery.ClaimID, HandoffDispositionRequeue); err != nil {
		t.Fatalf("SettleClaimHandoff(requeue): %v", err)
	}
	settled, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || settled.Drain == nil || settled.Drain.UnsettledDebt != 0 {
		t.Fatalf("settled drain = %+v, err=%v; want no debt", settled.Drain, err)
	}
	if next, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1)); err != nil || ok {
		t.Fatalf("draining new claim = %#v, ok=%v, err=%v; want gate blocked", next, ok, err)
	}
}

func TestRedisHandoffExpiryNeverRequeuesUnknownLease(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Millisecond))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/expiry/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	// Force the reservation into the past instead of sleeping: Redis recovery
	// must mark it resolver-eligible, never return it as an ordinary queue task.
	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: string(claim.ClaimID)}).Err(); err != nil {
		t.Fatal(err)
	}
	// The handoff's resolver token is deliberately distinct from the claim TTL:
	// force both authoritative deadlines into the past so this test exercises
	// expiry recovery rather than depending on scheduler timing.
	if err := rdb.HSet(ctx, directory.keys.handoffRecoveryDeadline, string(claim.ClaimID), "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := directory.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatalf("ReclaimExpiredClaims: %v", err)
	}
	recovery, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok || recovery.Handoff == nil || recovery.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("expired unknown handoff = %#v, ok=%v, err=%v", recovery, ok, err)
	}
	if err := directory.SettleClaimHandoff(ctx, recovery.ClaimID, HandoffDispositionDrop); err != nil {
		t.Fatalf("settle: %v", err)
	}
}

func TestRedisHandoffReregistrationRebindsRecovery(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	first := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/rebind/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, first, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	second := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	if first.SessionID == second.SessionID {
		t.Fatal("re-register did not fence old session")
	}
	if _, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(first, 1)); !errors.Is(err, ErrRunnerSessionStale) || ok {
		t.Fatalf("old session recovery = ok=%v err=%v; want stale", ok, err)
	}
	recovery, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(second, 1))
	if err != nil || !ok || recovery.Handoff == nil {
		t.Fatalf("rebound recovery = %#v, ok=%v, err=%v", recovery, ok, err)
	}
}

func TestMemoryHandoffReregistrationRebindsRecovery(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	first := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := testAssignment("exec-debt/memory-rebind/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, first)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	second := mustRegisterMemoryRunner(t, ctx, directory, "runner-debt", 1)
	if first.SessionID == second.SessionID {
		t.Fatal("re-register did not fence old session")
	}
	if _, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(first, 1)); !errors.Is(err, ErrRunnerSessionStale) || ok {
		t.Fatalf("old session recovery = ok=%v err=%v; want stale", ok, err)
	}
	recovery, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(second, 1))
	if err != nil || !ok || recovery.Handoff == nil {
		t.Fatalf("new session recovery = %#v, ok=%v, err=%v", recovery, ok, err)
	}
}

func TestRedisFinalizedHandoffSettlesOnlyMatchingLease(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/redis-finalized/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{LeaseID: "lease-final", LeaseToken: "token-final", Task: assignment.Task}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "redis-final", "redis-final-hash", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	if err := directory.SettleFinalizedHandoff(ctx, assignment.AssignmentID, lease.LeaseID, "wrong"); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.ReplayableDebt != 1 {
		t.Fatalf("wrong-token projection = %+v, err=%v; want retained finalized debt", snapshot.Drain, err)
	}
	if err := directory.SettleFinalizedHandoff(ctx, assignment.AssignmentID, lease.LeaseID, lease.LeaseToken); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err = directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.UnsettledDebt != 0 {
		t.Fatalf("settled projection = %+v, err=%v; want no debt", snapshot.Drain, err)
	}
}

func TestRedisExpiredLeaseSettlementTracksEngineOutcome(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/expired/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{LeaseID: "lease-expired", LeaseToken: "token-expired", Task: assignment.Task}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "expired-drain", "expired-hash", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	outcome, err := directory.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: assignment.AssignmentID, LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken,
	})
	if err != nil || outcome != ExpiredDirectoryLeaseReleased {
		t.Fatalf("ReleaseExpiredLease = %q, %v", outcome, err)
	}
	snapshot, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.ReplayableDebt != 1 {
		t.Fatalf("pre-engine settlement = %+v, err=%v; debt must remain", snapshot.Drain, err)
	}
	if err := directory.SettleFinalizedHandoff(ctx, assignment.AssignmentID, lease.LeaseID, lease.LeaseToken); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err = directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.UnsettledDebt != 0 {
		t.Fatalf("post-engine settlement = %+v, err=%v; want no debt", snapshot.Drain, err)
	}
}

func TestRedisHandoffCreatedLeaseResolvesThroughCoreDuringDrain(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/redis-core/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	lease := &engine.TaskLease{
		LeaseID: "lease-core", LeaseToken: "token-core", Task: assignment.Task,
		Input: &types.Input{Data: map[string]any{"source": "handoff"}}, NodeType: assignment.Routing.NodeType,
	}
	if err := directory.RecordClaimLeaseCreated(ctx, claim.ClaimID, lease); err != nil {
		t.Fatal(err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "redis-core-drain", "redis-core-hash", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlEngine{recoverLease: lease}
	core := &Core{engine: fake, runners: directory, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask(resolve): %v", err)
	}
	if resp.Lease == nil || resp.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("resolved lease = %+v, want token %q", resp.Lease, lease.LeaseToken)
	}
	snapshot, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.ReplayableDebt != 1 || snapshot.Drain.UnsettledDebt != 1 {
		t.Fatalf("finalized projection = %+v, err=%v", snapshot.Drain, err)
	}
}

func TestRedisLegacyClaimIsConservativelyMigratedToHandoffDebt(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Millisecond))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/legacy/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	// Model a claim left by a pre-ledger control-plane binary: it has normal
	// claim/assignment state but no handoff fields. A new directory must fail
	// closed and route it through the resolver rather than requeueing it.
	for _, key := range []string{
		directory.keys.handoffState,
		directory.keys.handoffGeneration,
		directory.keys.handoffLeaseMeta,
		directory.keys.handoffClaim,
		directory.keys.handoffAssignment,
		directory.keys.handoffRunner,
		directory.keys.handoffSession,
		directory.keys.handoffLeaseID,
		directory.keys.handoffLeaseToken,
		directory.keys.handoffRecoveryReady,
		directory.keys.handoffRecoveryDeadline,
	} {
		if err := rdb.HDel(ctx, key, string(claim.ClaimID), string(assignment.AssignmentID)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: string(claim.ClaimID)}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := directory.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatal(err)
	}
	recovery, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok || recovery.Handoff == nil || recovery.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("legacy recovery = %#v, ok=%v, err=%v; want migrated handoff debt", recovery, ok, err)
	}
}

func TestRedisDrainReceiptPreservesFirstHandoffProjection(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-debt", 1)
	assignment := redisDirectoryTestAssignment("exec-debt/receipt/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	req := controlRequest("runner-debt", "drain", "debt-receipt", "debt-receipt-hash", RunnerDesiredStateDraining)
	first, err := directory.SetRunnerControl(ctx, req)
	if err != nil || first.Drain == nil || first.Drain.UnsettledDebt != 1 || first.Drain.LeaseMayExistDebt != 1 {
		t.Fatalf("first drain = %+v, err=%v", first, err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatal(err)
	}
	if err := directory.SettleClaimHandoff(ctx, claim.ClaimID, HandoffDispositionDrop); err != nil {
		t.Fatal(err)
	}
	current, _, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || current.Drain == nil || current.Drain.UnsettledDebt != 0 {
		t.Fatalf("current drain = %+v, err=%v; debt should now be settled", current, err)
	}
	replayed, err := directory.SetRunnerControl(ctx, req)
	if err != nil || replayed.Drain == nil || replayed.Drain.UnsettledDebt != 1 || replayed.Drain.LeaseMayExistDebt != 1 {
		t.Fatalf("receipt replay = %+v, err=%v; want original debt projection", replayed, err)
	}
}

type ledgerFailingFinalizeDirectory struct {
	*MemoryRunnerDirectory
	failures int
}

func (d *ledgerFailingFinalizeDirectory) FinalizeClaim(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	if d.failures > 0 {
		d.failures--
		return errors.New("simulated directory finalization failure")
	}
	return d.MemoryRunnerDirectory.FinalizeClaim(ctx, claimID, lease)
}

func TestCoreFinalizeFailureLeavesDrainRecoverableDebt(t *testing.T) {
	ctx := context.Background()
	underlying := NewMemoryRunnerDirectory()
	directory := &ledgerFailingFinalizeDirectory{MemoryRunnerDirectory: underlying, failures: 1}
	session := mustRegisterMemoryRunner(t, ctx, underlying, "runner-debt", 1)
	assignment := testAssignment("exec-debt/finalize-failure/activation-1")
	mustEnqueueAssignment(t, ctx, underlying, assignment)
	lease := &engine.TaskLease{
		LeaseID: "lease-finalize-failure", LeaseToken: "token-finalize-failure", Task: assignment.Task,
		Input: &types.Input{Data: map[string]any{"source": "first-build"}}, NodeType: assignment.Routing.NodeType,
	}
	fake := &fakeControlEngine{buildLease: lease, recoverLease: lease}
	core := &Core{engine: fake, runners: directory, pollWait: time.Second}
	request := protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}
	if _, err := core.pollTask(ctx, request, TransportInfo{}); err == nil {
		t.Fatal("first poll = nil error, want failed finalization")
	}
	if _, err := underlying.SetRunnerControl(ctx, controlRequest("runner-debt", "drain", "finalize-failure-drain", "finalize-failure-hash", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := underlying.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.UnsettledDebt != 1 || snapshot.Drain.HandoffDebt != 1 {
		t.Fatalf("post-failure drain = %+v, err=%v; want recoverable blocker", snapshot.Drain, err)
	}
	resp, err := core.pollTask(ctx, request, TransportInfo{})
	if err != nil {
		t.Fatalf("recovery poll: %v", err)
	}
	if resp.Lease == nil || resp.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("recovery lease = %+v, want original token %q", resp.Lease, lease.LeaseToken)
	}
	snapshot, _, err = underlying.RunnerControl(ctx, session.RunnerID)
	if err != nil || snapshot.Drain == nil || snapshot.Drain.HandoffDebt != 0 || snapshot.Drain.ReplayableDebt != 1 {
		t.Fatalf("post-recovery drain = %+v, err=%v; want finalized debt", snapshot.Drain, err)
	}
}
