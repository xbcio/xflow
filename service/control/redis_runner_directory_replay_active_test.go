package control

import (
	"context"
	"testing"
	"time"

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
