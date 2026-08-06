package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// replayLease reads one assignment's record with a sequence of separate HGETs
// after listing states with HGETALL. Nothing holds those reads together, so a
// concurrent release — the sweeper reclaiming a dead runner's lease, or a
// report committing — can delete the record between the list and the reads.
//
// The assignment is then simply not this runner's to replay: it is released.
// Reporting that as an error is fatal out of proportion, because Runner's
// pollLoop RETURNS on a poll error (service/runner/runner.go) — the runner
// stops claiming work entirely. One lost race idles a healthy runner while its
// queue has work waiting, which is what left five re-queued batches unclaimed
// with both runners reporting zero leases.
func TestReplayLeaseSkipsAnAssignmentReleasedMidRead(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-torn", 2)

	// One assignment reaches the leased state, then loses its record the way a
	// concurrent release leaves it: the state entry lingers while the payload
	// and lease metadata are gone.
	torn := redisDirectoryTestAssignment("exec-torn/node-a/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, torn)
	tornClaim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	if err := directory.FinalizeClaim(ctx, tornClaim.ClaimID, &engine.TaskLease{
		LeaseID:    "lease-torn",
		LeaseToken: "token-torn",
		Attempt:    1,
		Task:       torn.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   torn.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	server.HDel(directory.keys.assignmentLeaseMeta, string(torn.AssignmentID))
	server.HDel(directory.keys.assignmentData, string(torn.AssignmentID))

	// Real work is waiting behind it.
	pending := redisDirectoryTestAssignment("exec-torn/node-b/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, pending)

	claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 2))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v; a record released mid-read is not a "+
			"failure, and returning one stops the runner's poll loop outright", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false; the queued assignment behind the torn " +
			"record must still be claimable")
	}
	if claim.Assignment.AssignmentID != pending.AssignmentID {
		t.Fatalf("claimed %q, want the queued assignment %q",
			claim.Assignment.AssignmentID, pending.AssignmentID)
	}
}

// A record that is present but unparseable is a different thing: the bytes are
// there and they are wrong. That must stay an error rather than being silently
// skipped as "released", or genuine corruption would look like ordinary churn.
func TestReplayLeaseStillFailsOnACorruptRecord(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-corrupt", 2)
	assignment := redisDirectoryTestAssignment("exec-corrupt/node-a/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{
		LeaseID:    "lease-corrupt",
		LeaseToken: "token-corrupt",
		Attempt:    1,
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	server.HSet(directory.keys.assignmentData, string(assignment.AssignmentID), "{not json")

	if _, _, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 2)); err == nil {
		t.Fatal("ClaimForRunner() returned no error for an unparseable assignment " +
			"record; corruption must not be indistinguishable from a concurrent release")
	}
}
