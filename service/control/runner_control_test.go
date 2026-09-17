package control

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func controlRequest(runnerID, action, requestID, requestHash string, desired RunnerDesiredState) RunnerControlRequest {
	return RunnerControlRequest{
		RunnerID:     runnerID,
		DesiredState: desired,
		Actor:        "operator-a",
		Action:       action,
		Reason:       action + " reason",
		RequestID:    requestID,
		RequestHash:  requestHash,
		Now:          time.Unix(100, 0).UTC(),
	}
}

func newHistoricalMemoryRunnerControlDirectory(opts ...MemoryRunnerDirectoryOption) *MemoryRunnerDirectory {
	options := append([]MemoryRunnerDirectoryOption{
		WithMemoryRunnerDirectoryClock(func() time.Time { return time.Unix(101, 0).UTC() }),
	}, opts...)
	return NewMemoryRunnerDirectory(options...)
}

func finalizedControlLease(t *testing.T, ctx context.Context, directory *MemoryRunnerDirectory, session RunnerSession, assignment Assignment, leaseID string) {
	t.Helper()
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{
		LeaseID:    engine.LeaseID(leaseID),
		LeaseToken: engine.LeaseToken("token-" + leaseID),
		Task:       assignment.Task,
		NodeType:   assignment.Routing.NodeType,
	}); err != nil {
		t.Fatalf("FinalizeClaim: %v", err)
	}
}

func TestMemoryRunnerControlDrainBlocksNewClaimsButReplaysFinalizedLease(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 2)
	leased := testAssignment("exec-1/leased/activation-1")
	queued := testAssignment("exec-1/queued/activation-1")
	finalizedControlLease(t, ctx, directory, session, leased, "lease-lost")
	mustEnqueueAssignment(t, ctx, directory, queued)

	snapshot, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "drain-a", "hash-drain-a", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("SetRunnerControl(drain): %v", err)
	}
	if snapshot.DesiredState != RunnerDesiredStateDraining || snapshot.Generation != 1 {
		t.Fatalf("drain snapshot = %+v, want draining generation 1", snapshot)
	}
	if snapshot.Drain == nil || snapshot.Drain.ServerQuiescent {
		t.Fatalf("drain projection = %+v, must remain conservative until handoff debt exists", snapshot.Drain)
	}

	replay, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 2))
	if err != nil || !ok || replay.Lease == nil {
		t.Fatalf("draining replay = %#v, %v, %v; want finalized lease replay", replay, ok, err)
	}
	if replay.Lease.LeaseID != "lease-lost" {
		t.Fatalf("replayed lease = %q, want lease-lost", replay.Lease.LeaseID)
	}

	request := testClaimRequest(session, 2)
	request.ActiveLeaseIDs = []string{"lease-lost"}
	if claim, ok, err := directory.ClaimForRunner(ctx, request); err != nil || ok {
		t.Fatalf("draining queue claim = %#v, %v, %v; want no new queue claim", claim, ok, err)
	}
}

func TestMemoryRunnerControlResumeReopensClaimsAndHistoricalReceiptDoesNotReapply(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	mustEnqueueAssignment(t, ctx, directory, testAssignment("exec-1/queued/activation-1"))

	drainReq := controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining)
	first, err := directory.SetRunnerControl(ctx, drainReq)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	resume, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "resume", "request-b", "hash-b", RunnerDesiredStateActive))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resume.DesiredState != RunnerDesiredStateActive || resume.Generation != 2 {
		t.Fatalf("resume = %+v, want active generation 2", resume)
	}

	replayed, err := directory.SetRunnerControl(ctx, drainReq)
	if err != nil {
		t.Fatalf("historical receipt replay: %v", err)
	}
	if replayed.DesiredState != first.DesiredState || replayed.Generation != first.Generation {
		t.Fatalf("replayed receipt = %+v, want original %+v", replayed, first)
	}
	current, found, err := directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found || current.DesiredState != RunnerDesiredStateActive || current.Generation != 2 {
		t.Fatalf("current control = %+v, found=%v, err=%v; old receipt must not re-drain", current, found, err)
	}

	claim, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok || claim.Lease != nil {
		t.Fatalf("resumed queue claim = %#v, %v, %v; want normal claim", claim, ok, err)
	}
}

func TestMemoryRunnerControlReceiptBodyConflictAndReregistrationPreservesDrain(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory()
	firstSession := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	request := controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining)
	if _, err := directory.SetRunnerControl(ctx, request); err != nil {
		t.Fatalf("drain: %v", err)
	}
	request.RequestHash = "different-body"
	if _, err := directory.SetRunnerControl(ctx, request); !errors.Is(err, ErrRunnerControlRequestConflict) {
		t.Fatalf("same receipt different body error = %v, want ErrRunnerControlRequestConflict", err)
	}

	secondSession := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	if firstSession.SessionID == secondSession.SessionID {
		t.Fatal("re-registration did not replace session")
	}
	current, found, err := directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found || current.DesiredState != RunnerDesiredStateDraining {
		t.Fatalf("control after re-register = %+v, found=%v, err=%v; drain must survive session replacement", current, found, err)
	}
}

func TestMemoryRunnerControlReceiptRetentionExpiresAndAllowsReuse(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory(WithMemoryRunnerDirectoryControlReceiptRetention(time.Second))
	if directory.controlReceiptRetention != time.Second {
		t.Fatalf("configured receipt retention = %v, want %v", directory.controlReceiptRetention, time.Second)
	}
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	if session.RunnerID != "runner-a" {
		t.Fatalf("registered runner = %q, want runner-a", session.RunnerID)
	}

	request := controlRequest("runner-a", "control", "request-a", "hash-a", RunnerDesiredStateDraining)
	request.Now = time.Unix(100, 0).UTC()
	first, err := directory.SetRunnerControl(ctx, request)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if first.Generation != 1 || first.DesiredState != RunnerDesiredStateDraining {
		t.Fatalf("first snapshot = %+v, want draining generation 1", first)
	}

	receiptID := runnerControlReceiptID(request)
	receipt, ok := directory.controls[request.RunnerID].receipts[receiptID]
	if !ok {
		t.Fatal("stored receipt is missing")
	}
	if !receipt.StoredAt.Equal(request.Now) || !receipt.ExpiresAt.Equal(request.Now.Add(time.Second)) || receipt.Status != http.StatusOK {
		t.Fatalf("receipt metadata = %+v, want stored_at=%v expires_at=%v status=%d", receipt, request.Now, request.Now.Add(time.Second), http.StatusOK)
	}

	beforeExpiry := request
	beforeExpiry.RequestHash = "hash-b"
	beforeExpiry.Now = time.Unix(100, 500_000_000).UTC()
	if _, err := directory.SetRunnerControl(ctx, beforeExpiry); !errors.Is(err, ErrRunnerControlRequestConflict) {
		t.Fatalf("unexpired receipt reuse error = %v, want ErrRunnerControlRequestConflict", err)
	}

	afterExpiry := beforeExpiry
	afterExpiry.DesiredState = RunnerDesiredStateActive
	afterExpiry.Reason = "resume reason"
	afterExpiry.Now = time.Unix(102, 0).UTC()
	reused, err := directory.SetRunnerControl(ctx, afterExpiry)
	if err != nil {
		t.Fatalf("expired receipt reuse: %v", err)
	}
	if reused.DesiredState != RunnerDesiredStateActive || reused.Generation != 2 {
		t.Fatalf("reused snapshot = %+v, want active generation 2", reused)
	}

	receipt, ok = directory.controls[request.RunnerID].receipts[receiptID]
	if !ok || receipt.RequestHash != afterExpiry.RequestHash || !receipt.StoredAt.Equal(afterExpiry.Now) {
		t.Fatalf("reused receipt = %+v, want replacement for expired request", receipt)
	}
}

func TestMemoryRunnerControlRecoveryOnlySuppressesNewClaimsAfterResume(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	mustEnqueueAssignment(t, ctx, directory, testAssignment("exec-1/queued/activation-1"))
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining)); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "resume", "request-b", "hash-b", RunnerDesiredStateActive)); err != nil {
		t.Fatal(err)
	}
	request := testClaimRequest(session, 1)
	request.RecoveryOnly = true
	if claim, ok, err := directory.ClaimForRunner(ctx, request); err != nil || ok {
		t.Fatalf("recovery-only after resume = %#v, %v, %v; want no new claim", claim, ok, err)
	}
}

func TestRunnerControlReceiptIDUsesUnambiguousComponentEncoding(t *testing.T) {
	// These inputs produced exactly the same NUL-delimited payload before the
	// receipt key switched to length-prefixed components. The public management
	// route uses fixed action strings, but the directory contract is broader and
	// must remain collision-free for every server-issued principal value.
	first := RunnerControlRequest{
		Actor: "operator\x00drain", Action: "resume", RunnerID: "runner-a", RequestID: "request-a",
	}
	second := RunnerControlRequest{
		Actor: "operator", Action: "drain\x00resume", RunnerID: "runner-a", RequestID: "request-a",
	}
	if runnerControlReceiptID(first) == runnerControlReceiptID(second) {
		t.Fatal("receipt IDs collided for distinct component tuples")
	}
}

func TestMemoryRunnerControlDrainCompletionRequiresLocalObservation(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	drain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	requireRunnerDrainProjection(t, drain, true, false, RunnerDrainPhaseQuiescing)

	if err := directory.Heartbeat(ctx, HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1, InFlight: 0, Now: time.Unix(101, 0),
		DrainObservation: &protocol.RunnerDrainObservation{
			Generation: drain.Generation, RecoveryOnly: true, ActiveActivations: 0,
		},
	}); err != nil {
		t.Fatalf("quiet draining heartbeat: %v", err)
	}
	current, found, err := directory.RunnerControl(ctx, session.RunnerID)
	if err != nil || !found {
		t.Fatalf("current control = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, true, RunnerDrainPhaseComplete)

	// A later draining heartbeat that omits the receipt is uncertainty, not
	// permission to retain a stale quiet sample indefinitely.
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

func TestMemoryRunnerControlDrainObservationRequiresQuietRecovery(t *testing.T) {
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
			directory := newHistoricalMemoryRunnerControlDirectory()
			session := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
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

func TestMemoryRunnerControlDrainObservationFencesGenerationAndSession(t *testing.T) {
	ctx := context.Background()
	directory := newHistoricalMemoryRunnerControlDirectory()
	firstSession := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	firstDrain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if err := directory.Heartbeat(ctx, quietDrainHeartbeat(firstSession, firstDrain.Generation, time.Unix(101, 0))); err != nil {
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
	if err := directory.Heartbeat(ctx, quietDrainHeartbeat(firstSession, firstDrain.Generation, time.Unix(102, 0))); err != nil {
		t.Fatalf("old-generation heartbeat: %v", err)
	}
	current, found, err = directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("control after old generation = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, false, RunnerDrainPhaseQuiescing)

	secondSession := mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	if secondSession.SessionID == firstSession.SessionID {
		t.Fatal("re-registration did not replace the session")
	}
	if err := directory.Heartbeat(ctx, quietDrainHeartbeat(firstSession, secondDrain.Generation, time.Unix(103, 0))); !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("old-session heartbeat error = %v, want ErrRunnerSessionStale", err)
	}
	current, found, err = directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("control after re-registration = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, false, RunnerDrainPhaseQuiescing)

	if err := directory.Heartbeat(ctx, quietDrainHeartbeat(secondSession, secondDrain.Generation, time.Unix(104, 0))); err != nil {
		t.Fatalf("replacement-session quiet heartbeat: %v", err)
	}
	current, found, err = directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found {
		t.Fatalf("final control = %+v, found=%v, err=%v", current, found, err)
	}
	requireRunnerDrainProjection(t, current, true, true, RunnerDrainPhaseComplete)
}

func quietDrainHeartbeat(session RunnerSession, generation uint64, now time.Time) HeartbeatRequest {
	return HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1, InFlight: 0, Now: now,
		DrainObservation: &protocol.RunnerDrainObservation{
			Generation: generation, RecoveryOnly: true, ActiveActivations: 0,
		},
	}
}

func requireRunnerDrainProjection(t *testing.T, snapshot RunnerControlSnapshot, wantServer, wantRunner bool, wantPhase RunnerDrainPhase) {
	t.Helper()
	if snapshot.Drain == nil {
		t.Fatalf("drain projection is nil in %+v", snapshot)
	}
	if snapshot.Drain.ServerQuiescent != wantServer || snapshot.Drain.RunnerQuiescent != wantRunner || snapshot.Drain.Phase != wantPhase {
		t.Fatalf("drain projection = %+v, want server_quiescent=%t runner_quiescent=%t phase=%q", snapshot.Drain, wantServer, wantRunner, wantPhase)
	}
}
