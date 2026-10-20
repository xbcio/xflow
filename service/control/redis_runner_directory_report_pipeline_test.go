package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

// The report and renewal paths run once per finished task, so their read cost
// is a per-task cost that grows with throughput. These tests pin the pipelined
// shapes and the two properties pipelining must not trade away: a lookup for
// another runner's assignment still reads no payload, and a per-command failure
// inside a pipeline is never masked by the pipeline's first error.

// reportLeaseFixture is one finalized, report-visible lease: a registered
// runner, the assignment it claimed, and the lease FinalizeClaim persisted. All
// of it is produced through the directory's own transitions so the indexes and
// records match what a real finalize writes.
type reportLeaseFixture struct {
	directory  *RedisRunnerDirectory
	rdb        *redis.Client
	session    RunnerSession
	assignment Assignment
	lease      *engine.TaskLease
}

func seedReportLease(t *testing.T, ctx context.Context) reportLeaseFixture {
	t.Helper()

	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-report", 1)
	assignment := redisDirectoryTestAssignment("exec-report/node-report/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{
		LeaseID:    "lease-report",
		LeaseToken: "token-report",
		Attempt:    2,
		Task:       assignment.Task,
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Unix(11, 0).UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	return reportLeaseFixture{
		directory:  directory,
		rdb:        rdb,
		session:    session,
		assignment: assignment,
		lease:      lease,
	}
}

// reportLookupKey is the identity a report result carries: the assignment it
// names and the lease identity it echoes.
func reportLookupKey(assignment Assignment, lease *engine.TaskLease) LeaseLookupKey {
	return LeaseLookupKey{
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		NodeName:     assignment.Task.NodeName,
		NodeIdx:      assignment.Task.NodeIdx,
	}
}

// keyCommandErrorHook fails one chosen command/key pair after letting the batch
// run, so the command keeps the reply a live read would have produced and
// reports a connection-style failure instead. Setting the error after the batch
// creates exactly the shape a pipeline must not mask: an earlier missing field
// (redis.Nil is the pipeline's first error) followed by a real failure.
type keyCommandErrorHook struct {
	command string
	key     string
	err     error
}

func (h keyCommandErrorHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h keyCommandErrorHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.observe(cmd)
		return err
	}
}

func (h keyCommandErrorHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			h.observe(cmd)
		}
		return err
	}
}

func (h keyCommandErrorHook) observe(cmd redis.Cmder) {
	if !strings.EqualFold(cmd.Name(), h.command) {
		return
	}
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	if key, ok := args[1].(string); !ok || key != h.key {
		return
	}
	cmd.SetErr(h.err)
}

// TestRedisRunnerDirectoryResolveLeaseAssignmentIDReadsInOneRoundTrip pins the
// report path's first phase: the explicit-identity, by-token and by-id lookups
// no longer cost one round trip each.
func TestRedisRunnerDirectoryResolveLeaseAssignmentIDReadsInOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)

	hook := &replayRoundTripHook{}
	f.rdb.AddHook(hook)
	hook.reset()

	resolved, ok, err := f.directory.resolveLeaseAssignmentID(ctx, reportLookupKey(f.assignment, f.lease))
	if err != nil || !ok || resolved != string(f.assignment.AssignmentID) {
		t.Fatalf("resolveLeaseAssignmentID() = %q, %v, %v; want %q, true, nil",
			resolved, ok, err, f.assignment.AssignmentID)
	}
	if got := hook.count(); got != 1 {
		t.Fatalf("resolveLeaseAssignmentID() round trips = %d, want 1 (the three resolution reads share one pipeline; the old shape cost up to 3)", got)
	}
}

// TestRedisRunnerDirectoryLookupLeaseAtReadsInTwoRoundTrips pins the second
// phase: one round trip fences the identity fields, a second reads the payload
// and lease metadata. The old shape issued five serial reads.
func TestRedisRunnerDirectoryLookupLeaseAtReadsInTwoRoundTrips(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)

	hook := &replayRoundTripHook{}
	f.rdb.AddHook(hook)
	hook.reset()

	lease, ok, err := f.directory.lookupLeaseAt(ctx, f.session.RunnerID, f.session.SessionID, string(f.assignment.AssignmentID))
	if err != nil || !ok || lease.LeaseToken != f.lease.LeaseToken {
		t.Fatalf("lookupLeaseAt() = %+v, %v, %v; want the finalized lease", lease, ok, err)
	}
	if got := hook.count(); got != 2 {
		t.Fatalf("lookupLeaseAt() round trips = %d, want 2 (identity fence, then payload and metadata together; the old shape cost 5)", got)
	}
}

// TestRedisRunnerDirectoryReportLookupReadsInThreeRoundTrips pins the whole
// directory lookup a report performs. A report names its assignment, so the
// resolve phase hits the explicit identity and the lookup succeeds; the old
// shape cost 6 round trips on that shape and 7-8 when an index resolved it.
func TestRedisRunnerDirectoryReportLookupReadsInThreeRoundTrips(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)

	hook := &replayRoundTripHook{}
	f.rdb.AddHook(hook)
	hook.reset()

	lease, ok, err := f.directory.LookupLease(ctx, f.session.RunnerID, f.session.SessionID, reportLookupKey(f.assignment, f.lease))
	if err != nil || !ok || lease.LeaseToken != f.lease.LeaseToken {
		t.Fatalf("LookupLease() = %+v, %v, %v; want the finalized lease", lease, ok, err)
	}
	if got := hook.count(); got != 3 {
		t.Fatalf("report LookupLease() round trips = %d, want 3 (resolve, identity fence, payload; the old shape cost 6-8)", got)
	}
}

// TestRedisRunnerDirectoryFinalizeClaimReadsInThreeRoundTrips pins the finalize
// read path: the reclaim script, the claim fields (now one pipeline instead of
// two serial reads) and the finalize script.
func TestRedisRunnerDirectoryFinalizeClaimReadsInThreeRoundTrips(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-finalize-rtt", 4)
	for _, assignment := range []Assignment{
		redisDirectoryTestAssignment("exec-finalize/node-1/activation-1"),
		redisDirectoryTestAssignment("exec-finalize/node-2/activation-1"),
	} {
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	}
	warm := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	measured := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	// Warm the Lua cache so the measured call pays EVALSHA, not a NOSCRIPT
	// fallback; it must not change the shape under test.
	if err := directory.FinalizeClaim(ctx, warm.ClaimID, &engine.TaskLease{
		LeaseID: "lease-warm", LeaseToken: "token-warm", Task: warm.Assignment.Task,
	}); err != nil {
		t.Fatalf("warm-up FinalizeClaim() error = %v", err)
	}

	hook := &replayRoundTripHook{}
	rdb.AddHook(hook)
	hook.reset()
	if err := directory.FinalizeClaim(ctx, measured.ClaimID, &engine.TaskLease{
		LeaseID: "lease-measured", LeaseToken: "token-measured", Task: measured.Assignment.Task,
	}); err != nil {
		t.Fatalf("measured FinalizeClaim() error = %v", err)
	}
	if got := hook.count(); got != 3 {
		t.Fatalf("FinalizeClaim() round trips = %d, want 3 (reclaim, claim fields, finalize; the claim fields used to be two serial reads)", got)
	}
}

// TestRedisRunnerDirectoryWalkLeasedAssignmentsReadsInTwoRoundTrips pins the
// renewal walk's read shape: the candidate index and one pipeline carrying the
// candidate fields plus the runner's live-lease count. The count used to be a
// third, serial read.
func TestRedisRunnerDirectoryWalkLeasedAssignmentsReadsInTwoRoundTrips(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)

	hook := &replayRoundTripHook{}
	f.rdb.AddHook(hook)
	hook.reset()

	lease, found, err := f.directory.walkLeasedAssignments(ctx, f.session.RunnerID,
		func(string, leasedAssignmentIdentity) (*engine.TaskLease, leasedAssignmentVerdict, error) {
			return nil, walkLeasedSkip, nil
		})
	if err != nil || found || lease != nil {
		t.Fatalf("walkLeasedAssignments() = %+v, %v, %v; want no match and no error", lease, found, err)
	}
	if got := hook.count(); got != 2 {
		t.Fatalf("walkLeasedAssignments() round trips = %d, want 2 (index read plus candidate/count pipeline; the count used to be a third read)", got)
	}
}

// TestRedisRunnerDirectoryRecoverableHandoffReadsCandidateInOneRoundTrip pins
// the poll path's handoff re-check: owner, session and state of one candidate
// share a pipeline instead of costing three serial reads. The candidate is
// owned by this runner and carries no state, so the scan continues without
// taking the resolver token.
func TestRedisRunnerDirectoryRecoverableHandoffReadsCandidateInOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	const (
		runnerID  = "runner-handoff-rtt"
		sessionID = "session-handoff-rtt"
		claimID   = "claim-handoff-rtt"
	)
	if err := rdb.HSet(ctx, directory.keys.handoffRunner, claimID, runnerID).Err(); err != nil {
		t.Fatalf("seed handoff owner: %v", err)
	}
	if err := rdb.HSet(ctx, directory.keys.handoffSession, claimID, sessionID).Err(); err != nil {
		t.Fatalf("seed handoff session: %v", err)
	}

	hook := &replayRoundTripHook{}
	rdb.AddHook(hook)
	hook.reset()

	if claim, ok, err := directory.recoverableHandoff(ctx, runnerID, sessionID, []string{claimID}); err != nil || ok {
		t.Fatalf("recoverableHandoff() = %+v, %v, %v; want no claim and no error", claim, ok, err)
	}
	if got := hook.count(); got != 1 {
		t.Fatalf("recoverableHandoff() candidate reads = %d round trips, want 1 (owner, session and state share one pipeline; the old shape cost 3)", got)
	}
}

// TestRedisRunnerDirectoryBuildHandoffClaimReadsInTwoRoundTrips pins the
// resolver's payload read: the transition record, then the assignment payload
// and generation together. The old shape cost three serial reads (four when a
// lease was recorded).
func TestRedisRunnerDirectoryBuildHandoffClaimReadsInTwoRoundTrips(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-handoff-build", 1)
	assignment := redisDirectoryTestAssignment("exec-handoff/build/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist() error = %v", err)
	}

	hook := &replayRoundTripHook{}
	rdb.AddHook(hook)
	hook.reset()

	built, ok, err := directory.buildHandoffClaim(ctx, string(claim.ClaimID), HandoffDebtLeaseMayExist)
	if err != nil || !ok || built.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("buildHandoffClaim() = %+v, %v, %v; want the claimed assignment", built, ok, err)
	}
	if got := hook.count(); got != 2 {
		t.Fatalf("buildHandoffClaim() round trips = %d, want 2 (transition record, then payload and generation together; the old shape cost 3)", got)
	}
}

// TestRedisRunnerDirectoryLookupLeaseReadsNoPayloadOnIdentityMismatch is the
// data-boundary guard: a lookup that resolves to an assignment this runner (or
// session) does not own must stop at the identity phase and never read that
// assignment's input or lease metadata.
func TestRedisRunnerDirectoryLookupLeaseReadsNoPayloadOnIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)

	payloadReads := newKeyCommandHook("hget", f.directory.keys.assignmentData)
	metaReads := newKeyCommandHook("get", f.directory.keys.assignmentLeaseMetaKey(string(f.assignment.AssignmentID)))
	identityReads := newKeyCommandHook("hget",
		f.directory.keys.assignmentState,
		f.directory.keys.assignmentRunner,
		f.directory.keys.assignmentSession,
	)
	f.rdb.AddHook(payloadReads)
	f.rdb.AddHook(metaReads)
	f.rdb.AddHook(identityReads)

	cases := []struct {
		name      string
		runnerID  string
		sessionID string
	}{
		{name: "wrong-session", runnerID: f.session.RunnerID, sessionID: "session-not-mine"},
		{name: "wrong-runner", runnerID: "runner-not-mine", sessionID: f.session.SessionID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payloadReads.reset()
			metaReads.reset()
			identityReads.reset()

			lease, ok, err := f.directory.LookupLease(ctx, tc.runnerID, tc.sessionID, reportLookupKey(f.assignment, f.lease))
			if err != nil {
				t.Fatalf("LookupLease() error = %v, want nil (identity mismatch is not an error)", err)
			}
			if ok || lease != nil {
				t.Fatalf("LookupLease() = %+v, %v; want no match for the mismatched identity", lease, ok)
			}
			if n := payloadReads.count(); n != 0 {
				t.Fatalf("assignment payload reads = %d on an identity mismatch, want 0", n)
			}
			if n := metaReads.count(); n != 0 {
				t.Fatalf("lease metadata reads = %d on an identity mismatch, want 0", n)
			}
			if n := identityReads.count(); n == 0 {
				t.Fatal("identity fields were not read; the zero-payload assertion would be vacuous")
			}
		})
	}
}

// TestRedisRunnerDirectoryLookupLeaseReportsStateReadFailure injects a
// connection-style failure on the state read and proves the lookup returns it
// rather than treating the batch as a clean miss.
func TestRedisRunnerDirectoryLookupLeaseReportsStateReadFailure(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)
	injected := errors.New("injected state read failure")
	f.rdb.AddHook(keyCommandErrorHook{command: "hget", key: f.directory.keys.assignmentState, err: injected})

	_, ok, err := f.directory.LookupLease(ctx, f.session.RunnerID, f.session.SessionID, reportLookupKey(f.assignment, f.lease))
	if ok {
		t.Fatal("LookupLease() ok=true despite a failed state read")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("LookupLease() error = %v, want the injected state failure", err)
	}
	if !strings.Contains(err.Error(), "lookup lease state") {
		t.Fatalf("LookupLease() error = %v, want it to name the state read", err)
	}
}

// TestRedisRunnerDirectoryResolveLeaseAssignmentIDDoesNotMaskTokenReadFailure
// builds the shape an Exec-only pipeline check would swallow: the explicit
// identity read misses, so its redis.Nil becomes the pipeline's first error,
// and the by-token read fails for real. The token failure must be returned
// rather than falling through to the raw assignment ID.
func TestRedisRunnerDirectoryResolveLeaseAssignmentIDDoesNotMaskTokenReadFailure(t *testing.T) {
	ctx := context.Background()
	f := seedReportLease(t, ctx)
	injected := errors.New("injected by-token read failure")
	f.rdb.AddHook(keyCommandErrorHook{command: "hget", key: f.directory.keys.leaseByToken, err: injected})

	_, ok, err := f.directory.resolveLeaseAssignmentID(ctx, LeaseLookupKey{
		AssignmentID: AssignmentID("exec-report/unknown/activation-1"),
		LeaseToken:   f.lease.LeaseToken,
	})
	if ok {
		t.Fatal("resolveLeaseAssignmentID() ok=true despite a failed by-token read")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("resolveLeaseAssignmentID() error = %v, want the injected by-token failure", err)
	}
	if !strings.Contains(err.Error(), "resolve lease by token") {
		t.Fatalf("resolveLeaseAssignmentID() error = %v, want it to name the by-token read", err)
	}
}

// TestRedisRunnerDirectoryFinalizeClaimKeepsMissingAssignmentVerdict injects a
// failure on the runner read while the claim's assignment is missing. The
// assignment verdict must still be decided first, exactly as when the two reads
// were serial: "not active", not the runner read's error.
func TestRedisRunnerDirectoryFinalizeClaimKeepsMissingAssignmentVerdict(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	injected := errors.New("injected runner read failure")
	rdb.AddHook(keyCommandErrorHook{command: "hget", key: directory.keys.claimsRunner, err: injected})

	err := directory.FinalizeClaim(ctx, ClaimID("claim-missing"), &engine.TaskLease{LeaseID: "lease-x", LeaseToken: "token-x"})
	if !errors.Is(err, errClaimNotActive) {
		t.Fatalf("FinalizeClaim(missing claim) error = %v, want errClaimNotActive; the assignment verdict decides before the runner read", err)
	}
}

// TestRedisRunnerDirectoryFinalizeClaimReportsRunnerReadFailure covers the
// other half: when the assignment is present, a failed runner read is a real
// error and keeps its original wording.
func TestRedisRunnerDirectoryFinalizeClaimReportsRunnerReadFailure(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-finalize-error", 1)
	assignment := redisDirectoryTestAssignment("exec-finalize/error/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	injected := errors.New("injected runner read failure")
	rdb.AddHook(keyCommandErrorHook{command: "hget", key: directory.keys.claimsRunner, err: injected})

	err := directory.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{
		LeaseID: "lease-error", LeaseToken: "token-error", Task: assignment.Task,
	})
	if !errors.Is(err, injected) {
		t.Fatalf("FinalizeClaim() error = %v, want the injected runner failure", err)
	}
	if !strings.Contains(err.Error(), "resolve claim runner") {
		t.Fatalf("FinalizeClaim() error = %v, want it to name the runner read", err)
	}
}
