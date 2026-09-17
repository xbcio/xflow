package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestRedisRunnerControlDrainGateReplayResumeAndReceipts(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := newRedisRunnerControlTestDirectory(rdb, time.Unix(101, 0).UTC())
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 2)
	leased := redisDirectoryTestAssignment("exec-1/leased/activation-1")
	queued := redisDirectoryTestAssignment("exec-1/queued/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, leased)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	lease := &engine.TaskLease{
		LeaseID:    "lease-lost",
		LeaseToken: "token-lost",
		Task:       leased.Task,
		NodeType:   leased.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim: %v", err)
	}
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, queued)

	drainReq := controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining)
	first, err := directory.SetRunnerControl(ctx, drainReq)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if first.DesiredState != RunnerDesiredStateDraining || first.Generation != 1 {
		t.Fatalf("drain = %+v, want draining generation 1", first)
	}
	replayReq := redisDirectoryClaimRequest(session, 2)
	replay, ok, err := directory.ClaimForRunner(ctx, replayReq)
	if err != nil || !ok || replay.Lease == nil || replay.Lease.LeaseID != lease.LeaseID {
		t.Fatalf("draining replay = %#v, %v, %v; want %q", replay, ok, err, lease.LeaseID)
	}

	blockedReq := redisDirectoryClaimRequest(session, 2)
	blockedReq.ActiveLeaseIDs = []string{string(lease.LeaseID)}
	if queuedClaim, ok, err := directory.ClaimForRunner(ctx, blockedReq); err != nil || ok {
		t.Fatalf("draining queue claim = %#v, %v, %v; want blocked", queuedClaim, ok, err)
	}

	resume, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "resume", "request-b", "hash-b", RunnerDesiredStateActive))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resume.DesiredState != RunnerDesiredStateActive || resume.Generation != 2 {
		t.Fatalf("resume = %+v, want active generation 2", resume)
	}
	old, err := directory.SetRunnerControl(ctx, drainReq)
	if err != nil {
		t.Fatalf("old receipt replay: %v", err)
	}
	if old.Generation != first.Generation || old.DesiredState != RunnerDesiredStateDraining {
		t.Fatalf("old receipt = %+v, want original drain response", old)
	}
	current, found, err := directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found || current.DesiredState != RunnerDesiredStateActive || current.Generation != 2 {
		t.Fatalf("current = %+v, found=%v, err=%v; old receipt re-applied drain", current, found, err)
	}
	conflict := drainReq
	conflict.RequestHash = "different"
	if _, err := directory.SetRunnerControl(ctx, conflict); !errors.Is(err, ErrRunnerControlRequestConflict) {
		t.Fatalf("receipt conflict = %v, want ErrRunnerControlRequestConflict", err)
	}
}

func TestRedisRunnerControlReregistrationPreservesDrain(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	first := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	second := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)
	if first.SessionID == second.SessionID {
		t.Fatal("re-registration did not replace session")
	}
	current, found, err := directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found || current.DesiredState != RunnerDesiredStateDraining {
		t.Fatalf("current = %+v, found=%v, err=%v; drain must survive re-registration", current, found, err)
	}
}

func TestRedisRunnerControlDrainCompletionRequiresLocalObservation(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := newRedisRunnerControlTestDirectory(rdb, time.Unix(101, 0).UTC())
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)
	drain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	requireRunnerDrainProjection(t, drain, true, false, RunnerDrainPhaseQuiescing)

	if err := directory.Heartbeat(ctx, redisQuietDrainHeartbeat(session, drain.Generation, time.Unix(101, 0))); err != nil {
		t.Fatalf("quiet draining heartbeat: %v", err)
	}
	current, found, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found {
		t.Fatalf("current control = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, true, RunnerDrainPhaseComplete)

	if err := directory.Heartbeat(ctx, HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1, InFlight: 0, Now: time.Unix(102, 0),
	}); err != nil {
		t.Fatalf("heartbeat without observation: %v", err)
	}
	current, found, err = directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found {
		t.Fatalf("current control after omitted observation = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, false, RunnerDrainPhaseQuiescing)
}

func TestRedisRunnerControlDrainObservationRequiresQuietRecovery(t *testing.T) {
	cases := []struct {
		name        string
		inFlight    int
		observation *protocol.RunnerDrainObservation
	}{
		{name: "omitted"},
		{name: "wrong generation", observation: &protocol.RunnerDrainObservation{Generation: 2, RecoveryOnly: true}},
		{name: "work in flight", inFlight: 1, observation: &protocol.RunnerDrainObservation{Generation: 1, RecoveryOnly: true}},
		{name: "active activation", observation: &protocol.RunnerDrainObservation{Generation: 1, RecoveryOnly: true, ActiveActivations: 1}},
		{name: "ordinary polling not stopped", observation: &protocol.RunnerDrainObservation{Generation: 1, RecoveryOnly: false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, rdb := newRedisRunnerDirectoryTestClient(t)
			directory := newRedisRunnerControlTestDirectory(rdb, time.Unix(101, 0).UTC())
			session := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)
			drain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining))
			if err != nil {
				t.Fatalf("drain: %v", err)
			}
			if drain.Generation != 1 {
				t.Fatalf("drain generation = %d, want 1", drain.Generation)
			}
			if err := directory.Heartbeat(ctx, HeartbeatRequest{
				RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1, InFlight: tc.inFlight,
				Now: time.Unix(101, 0), DrainObservation: tc.observation,
			}); err != nil {
				t.Fatalf("heartbeat: %v", err)
			}
			current, found, err := directory.RunnerControl(ctx, session.RunnerID)
			if err != nil || !found {
				t.Fatalf("current control = %+v, found=%v, err=%v", current, found, err)
			}
			requireRunnerDrainProjection(t, current, true, false, RunnerDrainPhaseQuiescing)
		})
	}
}

func TestRedisRunnerControlDrainObservationFencesGenerationAndSession(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := newRedisRunnerControlTestDirectory(rdb, time.Unix(101, 0).UTC())
	firstSession := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)
	firstDrain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if err := directory.Heartbeat(ctx, redisQuietDrainHeartbeat(firstSession, firstDrain.Generation, time.Unix(101, 0))); err != nil {
		t.Fatalf("first quiet heartbeat: %v", err)
	}
	current, found, err := directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("first control = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, true, RunnerDrainPhaseComplete)

	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "resume", "request-b", "hash-b", RunnerDesiredStateActive)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	secondDrain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-c", "hash-c", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if secondDrain.Generation <= firstDrain.Generation {
		t.Fatalf("second drain generation = %d, want newer than %d", secondDrain.Generation, firstDrain.Generation)
	}
	if err := directory.Heartbeat(ctx, redisQuietDrainHeartbeat(firstSession, firstDrain.Generation, time.Unix(102, 0))); err != nil {
		t.Fatalf("old-generation heartbeat: %v", err)
	}
	current, found, err = directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("control after old generation = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, false, RunnerDrainPhaseQuiescing)

	secondSession := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)
	if secondSession.SessionID == firstSession.SessionID {
		t.Fatal("re-registration did not replace the session")
	}
	if err := directory.Heartbeat(ctx, redisQuietDrainHeartbeat(firstSession, secondDrain.Generation, time.Unix(103, 0))); !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("old-session heartbeat error = %v, want ErrRunnerSessionStale", err)
	}
	current, found, err = directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("control after re-registration = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, false, RunnerDrainPhaseQuiescing)

	if err := directory.Heartbeat(ctx, redisQuietDrainHeartbeat(secondSession, secondDrain.Generation, time.Unix(104, 0))); err != nil {
		t.Fatalf("replacement-session quiet heartbeat: %v", err)
	}
	current, found, err = directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("final control = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, true, RunnerDrainPhaseComplete)
}

func redisQuietDrainHeartbeat(session RunnerSession, generation uint64, now time.Time) HeartbeatRequest {
	return HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1, InFlight: 0, Now: now,
		DrainObservation: &protocol.RunnerDrainObservation{
			Generation: generation, RecoveryOnly: true, ActiveActivations: 0,
		},
	}
}

func newRedisRunnerControlTestDirectory(rdb redis.Cmdable, now time.Time) *RedisRunnerDirectory {
	return NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClock(func() time.Time {
		return now
	}))
}

func TestRedisRunnerControlAuditRecordsOnlyDesiredStateTransitions(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryControlAuditMaxLen(2))
	registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)

	drain := controlRequest("runner-a", "drain", "request-drain", "hash-drain", RunnerDesiredStateDraining)
	first, err := directory.SetRunnerControl(ctx, drain)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	entries, err := rdb.XRange(ctx, directory.keys.runnerControlAudit, "-", "+").Result()
	if err != nil {
		t.Fatalf("read audit after drain: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("audit entries after drain = %d, want 1", len(entries))
	}
	if got := entries[0].Values["runner_id"]; got != "runner-a" {
		t.Fatalf("audit runner_id = %v, want runner-a", got)
	}
	if got := entries[0].Values["desired_state"]; got != string(RunnerDesiredStateDraining) {
		t.Fatalf("audit desired_state = %v, want draining", got)
	}
	if got := entries[0].Values["previous_desired_state"]; got != string(RunnerDesiredStateActive) {
		t.Fatalf("audit previous_desired_state = %v, want active", got)
	}
	if got := entries[0].Values["generation"]; got != "1" {
		t.Fatalf("audit generation = %v, want 1", got)
	}
	if got := entries[0].Values["result"]; got != "stored" {
		t.Fatalf("audit result = %v, want stored", got)
	}
	if _, found := entries[0].Values["request_hash"]; found {
		t.Fatal("audit event must not contain request_hash")
	}

	if _, err := directory.SetRunnerControl(ctx, drain); err != nil {
		t.Fatalf("replay drain: %v", err)
	}
	noTransition := controlRequest("runner-a", "drain", "request-already-draining", "hash-already-draining", RunnerDesiredStateDraining)
	if got, err := directory.SetRunnerControl(ctx, noTransition); err != nil {
		t.Fatalf("store already-draining receipt: %v", err)
	} else if got.Generation != first.Generation {
		t.Fatalf("already-draining generation = %d, want %d", got.Generation, first.Generation)
	}
	if got, err := rdb.XLen(ctx, directory.keys.runnerControlAudit).Result(); err != nil {
		t.Fatalf("audit length after replays: %v", err)
	} else if got != 1 {
		t.Fatalf("audit length after replays = %d, want 1", got)
	}
	if _, err := rdb.HGet(ctx, directory.keys.runnerControlReceiptHash, runnerControlReceiptID(noTransition)).Result(); err != nil {
		t.Fatalf("already-draining receipt was not stored: %v", err)
	}

	resume := controlRequest("runner-a", "resume", "request-resume", "hash-resume", RunnerDesiredStateActive)
	current, err := directory.SetRunnerControl(ctx, resume)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	entries, err = rdb.XRange(ctx, directory.keys.runnerControlAudit, "-", "+").Result()
	if err != nil {
		t.Fatalf("read audit after resume: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("audit entries after resume = %d, want 2", len(entries))
	}
	last := entries[1].Values
	if got := last["desired_state"]; got != string(current.DesiredState) {
		t.Fatalf("resume audit desired_state = %v, want %s", got, current.DesiredState)
	}
	if got := last["previous_desired_state"]; got != string(RunnerDesiredStateDraining) {
		t.Fatalf("resume audit previous_desired_state = %v, want draining", got)
	}
	if got := last["generation"]; got != "2" {
		t.Fatalf("resume audit generation = %v, want %d", got, current.Generation)
	}
	if got := last["request_id"]; got != resume.RequestID {
		t.Fatalf("resume audit request_id = %v, want %s", got, resume.RequestID)
	}

	for _, req := range []RunnerControlRequest{
		controlRequest("runner-a", "drain", "request-drain-2", "hash-drain-2", RunnerDesiredStateDraining),
		controlRequest("runner-a", "resume", "request-resume-2", "hash-resume-2", RunnerDesiredStateActive),
	} {
		if _, err := directory.SetRunnerControl(ctx, req); err != nil {
			t.Fatalf("transition %s: %v", req.RequestID, err)
		}
	}
	if got, err := rdb.XLen(ctx, directory.keys.runnerControlAudit).Result(); err != nil {
		t.Fatalf("bounded audit length: %v", err)
	} else if got > 2 {
		t.Fatalf("bounded audit length = %d, want at most 2", got)
	}
}

func TestRedisRunnerControlReceiptRetentionExpiresAndCleansAllFields(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryControlReceiptRetention(time.Second))
	registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1)

	original := controlRequest("runner-a", "control", "request-a", "hash-a", RunnerDesiredStateDraining)
	first, err := directory.SetRunnerControl(ctx, original)
	if err != nil {
		t.Fatalf("initial drain: %v", err)
	}
	receiptID := runnerControlReceiptID(original)
	if got, err := rdb.HGet(ctx, directory.keys.runnerControlReceiptStoredAt, receiptID).Result(); err != nil {
		t.Fatalf("read stored-at: %v", err)
	} else if got != "100000" {
		t.Fatalf("stored-at = %s, want 100000", got)
	}
	if got, err := rdb.HGet(ctx, directory.keys.runnerControlReceiptStatus, receiptID).Result(); err != nil {
		t.Fatalf("read receipt status: %v", err)
	} else if got != "200" {
		t.Fatalf("receipt status = %s, want 200", got)
	}
	if got, err := rdb.ZScore(ctx, directory.keys.runnerControlReceiptExpiry, receiptID).Result(); err != nil {
		t.Fatalf("read receipt expiry: %v", err)
	} else if got != 101000 {
		t.Fatalf("receipt expiry = %v, want 101000", got)
	}

	conflicting := original
	conflicting.RequestHash = "different-hash"
	conflicting.Now = time.Unix(100, 500_000_000).UTC()
	if _, err := directory.SetRunnerControl(ctx, conflicting); !errors.Is(err, ErrRunnerControlRequestConflict) {
		t.Fatalf("unexpired receipt conflict = %v, want ErrRunnerControlRequestConflict", err)
	}

	// The expired current receipt must be removed before it is checked, so the
	// same id can become a new mutation without relying on a separate cleanup.
	reused := original
	reused.DesiredState = RunnerDesiredStateActive
	reused.RequestHash = "hash-a-reused"
	reused.Now = time.Unix(102, 0).UTC()
	second, err := directory.SetRunnerControl(ctx, reused)
	if err != nil {
		t.Fatalf("reuse expired receipt id: %v", err)
	}
	if second.DesiredState != RunnerDesiredStateActive || second.Generation != first.Generation+1 {
		t.Fatalf("reused receipt snapshot = %+v, want active generation %d", second, first.Generation+1)
	}
	if got, err := rdb.HGet(ctx, directory.keys.runnerControlReceiptHash, receiptID).Result(); err != nil {
		t.Fatalf("read replaced receipt hash: %v", err)
	} else if got != reused.RequestHash {
		t.Fatalf("reused receipt hash = %s, want %s", got, reused.RequestHash)
	}

	// A different request later triggers the bounded sweep of stale receipts.
	stale := controlRequest("runner-a", "control", "stale-a", "hash-stale-a", RunnerDesiredStateActive)
	stale.Now = reused.Now
	if _, err := directory.SetRunnerControl(ctx, stale); err != nil {
		t.Fatalf("store stale receipt: %v", err)
	}
	staleReceiptID := runnerControlReceiptID(stale)
	fresh := controlRequest("runner-a", "control", "fresh-a", "hash-fresh-a", RunnerDesiredStateActive)
	fresh.Now = time.Unix(103, 500_000_000).UTC()
	if _, err := directory.SetRunnerControl(ctx, fresh); err != nil {
		t.Fatalf("store unexpired receipt: %v", err)
	}
	freshReceiptID := runnerControlReceiptID(fresh)

	cleanup := controlRequest("runner-a", "control", "cleanup-a", "hash-cleanup-a", RunnerDesiredStateActive)
	cleanup.Now = time.Unix(104, 0).UTC()
	if got, err := directory.SetRunnerControl(ctx, cleanup); err != nil {
		t.Fatalf("cleanup request: %v", err)
	} else if got.Generation != second.Generation {
		t.Fatalf("cleanup generation = %d, want %d", got.Generation, second.Generation)
	}
	for _, key := range []string{
		directory.keys.runnerControlReceiptHash,
		directory.keys.runnerControlReceiptDesired,
		directory.keys.runnerControlReceiptGeneration,
		directory.keys.runnerControlReceiptRequestedAt,
		directory.keys.runnerControlReceiptReason,
		directory.keys.runnerControlReceiptClaims,
		directory.keys.runnerControlReceiptLeases,
		directory.keys.runnerControlReceiptUnsettledDebt,
		directory.keys.runnerControlReceiptHandoffDebt,
		directory.keys.runnerControlReceiptLeaseMayExistDebt,
		directory.keys.runnerControlReceiptReplayableDebt,
		directory.keys.runnerControlReceiptPendingActivationCleanup,
		directory.keys.runnerControlReceiptDrainDeadline,
		directory.keys.runnerControlReceiptStoredAt,
		directory.keys.runnerControlReceiptStatus,
	} {
		if _, err := rdb.HGet(ctx, key, staleReceiptID).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("expired receipt remains in %q: %v", key, err)
		}
	}
	if _, err := rdb.ZScore(ctx, directory.keys.runnerControlReceiptExpiry, staleReceiptID).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expired receipt remains in expiry index: %v", err)
	}
	if _, err := rdb.HGet(ctx, directory.keys.runnerControlReceiptHash, freshReceiptID).Result(); err != nil {
		t.Fatalf("unexpired receipt was removed: %v", err)
	}
}
