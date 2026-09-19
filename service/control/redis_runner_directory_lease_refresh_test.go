package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// TestRedisRunnerDirectoryRefreshLeaseMetaExtendsExpiry pins the fix for the
// stranding path: FinalizeClaim arms the metadata expiry for the lease window
// plus a recovery margin, and a renewal must re-arm it. Before this, the expiry
// stayed at its finalized value while the engine kept extending the lease, so a
// node that outlived the original window lost the metadata its own next renewal
// is resolved through.
func TestRedisRunnerDirectoryRefreshLeaseMetaExtendsExpiry(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 1)
	assignment := redisDirectoryTestAssignment("exec-lease-refresh/extends/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-refresh", 3*time.Second)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	key := directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID))
	assertRedisRunnerDirectoryLeaseMetaTTL(t, server, key, lease.TTL+claimTTL)

	// Burn most of the original window, then renew with a 20s extension.
	server.FastForward(4 * time.Second)
	if got := server.TTL(key); got != time.Second {
		t.Fatalf("metadata TTL before refresh = %s, want 1s", got)
	}

	if err := directory.RefreshLeaseMeta(ctx, session.RunnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	}, 20*time.Second); err != nil {
		t.Fatalf("RefreshLeaseMeta() error = %v", err)
	}
	assertRedisRunnerDirectoryLeaseMetaTTL(t, server, key, 20*time.Second+claimTTL)

	// The refreshed window is the one that matters: the metadata has to outlive
	// the deadline it was originally armed with.
	server.FastForward(2 * time.Second)
	if !server.Exists(key) {
		t.Fatal("metadata expired at the finalized deadline; the refresh did not re-arm it")
	}
}

// TestRedisRunnerDirectoryRefreshLeaseMetaRefusesStaleCallers keeps a late
// renewal from a superseded runner or session from resurrecting an expiry the
// directory has already dropped or rebound.
func TestRedisRunnerDirectoryRefreshLeaseMetaRefusesStaleCallers(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 1)
	assignment := redisDirectoryTestAssignment("exec-lease-refresh/stale/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-refresh-stale", 3*time.Second)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	key := directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID))
	before := server.TTL(key)

	for _, tc := range []struct {
		name      string
		runnerID  string
		sessionID string
	}{
		{name: "other runner", runnerID: "runner-someone-else", sessionID: session.SessionID},
		{name: "superseded session", runnerID: session.RunnerID, sessionID: "session-old"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := directory.RefreshLeaseMeta(ctx, tc.runnerID, tc.sessionID, LeaseLookupKey{
				LeaseID:    lease.LeaseID,
				LeaseToken: lease.LeaseToken,
			}, 20*time.Second); err != nil {
				t.Fatalf("RefreshLeaseMeta() error = %v", err)
			}
			if got := server.TTL(key); got != before {
				t.Fatalf("metadata TTL after %s refresh = %s, want unchanged %s", tc.name, got, before)
			}
		})
	}
}

// TestRedisRunnerDirectoryRefreshLeaseMetaOnExpiredMetadataIsNotAnError: by the
// time the metadata is gone the lease is unrecoverable, and the renewal that
// led here has already succeeded, so the refresh reports success rather than
// turning a completed renewal into a failure.
func TestRedisRunnerDirectoryRefreshLeaseMetaOnExpiredMetadataIsNotAnError(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 1)
	assignment := redisDirectoryTestAssignment("exec-lease-refresh/expired/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-refresh-expired", 3*time.Second)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	key := directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID))
	server.FastForward(lease.TTL + claimTTL)
	if server.Exists(key) {
		t.Fatalf("metadata key %q survived its TTL", key)
	}

	if err := directory.RefreshLeaseMeta(ctx, session.RunnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	}, 20*time.Second); err != nil {
		t.Fatalf("RefreshLeaseMeta() on expired metadata error = %v, want nil", err)
	}
	if server.Exists(key) {
		t.Fatal("RefreshLeaseMeta() recreated expired metadata; it must only extend a live expiry")
	}
}

// refreshRecordingDirectory records the lease-metadata refreshes the renewal
// path issues. It embeds the concrete directory rather than the RunnerDirectory
// interface so that the optional capabilities renewLease asserts for
// (LeaseLookup, LeaseMetaRefresher) stay in the method set.
type refreshRecordingDirectory struct {
	*MemoryRunnerDirectory
	calls []refreshCall
	err   error
}

type refreshCall struct {
	runnerID  string
	sessionID string
	key       LeaseLookupKey
	live      time.Duration
}

func (d *refreshRecordingDirectory) RefreshLeaseMeta(_ context.Context, runnerID, sessionID string, key LeaseLookupKey, live time.Duration) error {
	d.calls = append(d.calls, refreshCall{runnerID: runnerID, sessionID: sessionID, key: key, live: live})
	return d.err
}

// TestRenewLeaseRefreshesDirectoryMetadata: a successful engine renewal must
// also push the directory's expiry forward, or the two deadlines drift apart
// until the next renewal is refused with "lease not found".
func TestRenewLeaseRefreshesDirectoryMetadata(t *testing.T) {
	ctx := context.Background()
	memory := NewMemoryRunnerDirectory()
	session, err := memory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-refresh",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := stableTestAssignment("node-refresh")
	mustEnqueueAssignment(t, ctx, memory, assignment)
	claim := mustClaimAssignment(t, ctx, memory, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-node-refresh",
		LeaseToken: "token-node-refresh",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.function",
	}
	if err := memory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	dir := &refreshRecordingDirectory{MemoryRunnerDirectory: memory}
	core := &Core{engine: &groupFakeEngine{nodeRenewResult: true}, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatalf("renewLease() renewed=false, want true (err=%q)", resp.Error)
	}
	if len(dir.calls) != 1 {
		t.Fatalf("directory refreshes = %d, want 1 -- without it the directory expiry "+
			"stops tracking the lease the engine just extended", len(dir.calls))
	}
	got := dir.calls[0]
	if got.runnerID != session.RunnerID || got.sessionID != session.SessionID {
		t.Errorf("refreshed for %q/%q, want %q/%q", got.runnerID, got.sessionID, session.RunnerID, session.SessionID)
	}
	if got.key.LeaseToken != lease.LeaseToken {
		t.Errorf("refreshed lease token %q, want %q", got.key.LeaseToken, lease.LeaseToken)
	}
	if got.live != 30*time.Second {
		t.Errorf("refreshed live window = %v, want 30s (the window the engine granted)", got.live)
	}
}

// TestRenewLeaseSkipsRefreshWhenEngineRefuses: a renewal the engine refused
// extends nothing, so it must not push the directory expiry either.
func TestRenewLeaseSkipsRefreshWhenEngineRefuses(t *testing.T) {
	ctx := context.Background()
	memory := NewMemoryRunnerDirectory()
	session, err := memory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-refresh-refused",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := stableTestAssignment("node-refresh-refused")
	mustEnqueueAssignment(t, ctx, memory, assignment)
	claim := mustClaimAssignment(t, ctx, memory, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-node-refresh-refused",
		LeaseToken: "token-node-refresh-refused",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.function",
	}
	if err := memory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	dir := &refreshRecordingDirectory{MemoryRunnerDirectory: memory}
	core := &Core{engine: &groupFakeEngine{nodeRenewResult: false}, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if resp.Renewed {
		t.Fatal("renewLease() renewed=true, want false")
	}
	if len(dir.calls) != 0 {
		t.Fatalf("directory refreshes = %d, want 0 for a refused renewal", len(dir.calls))
	}
}
