package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/distributed"
	backendlocal "github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestRedisRunnerDirectoryPersistsQueuedClaimedAndLeasedState(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	first := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, first, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-a/activation-1")

	if enqueued, err := first.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("first EnqueueAssignment() error = %v", err)
	} else if !enqueued {
		t.Fatal("first EnqueueAssignment() enqueued=false, want true")
	}

	recreatedForQueue := NewRedisRunnerDirectory(rdb)
	if enqueued, err := recreatedForQueue.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("duplicate EnqueueAssignment() error = %v", err)
	} else if enqueued {
		t.Fatal("duplicate EnqueueAssignment() after recreation enqueued=true, want false")
	}
	claimed := claimRedisDirectoryAssignment(t, ctx, recreatedForQueue, session, 1)
	if claimed.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("claimed AssignmentID = %q, want %q", claimed.Assignment.AssignmentID, assignment.AssignmentID)
	}
	if claimed.Assignment.Task.AutoDepth != assignment.Task.AutoDepth || claimed.Assignment.Task.ActivationID != assignment.Task.ActivationID {
		t.Fatalf("claimed task runtime metadata = auto_depth=%d activation_id=%d, want %d/%d", claimed.Assignment.Task.AutoDepth, claimed.Assignment.Task.ActivationID, assignment.Task.AutoDepth, assignment.Task.ActivationID)
	}

	recreatedForClaim := NewRedisRunnerDirectory(rdb)
	if err := recreatedForClaim.ReleaseClaim(ctx, claimed.ClaimID, ReleaseClaimRequeue); err != nil {
		t.Fatalf("ReleaseClaim() after recreation error = %v", err)
	}
	reclaimed := claimRedisDirectoryAssignment(t, ctx, recreatedForClaim, session, 1)
	if reclaimed.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("reclaimed AssignmentID = %q, want %q", reclaimed.Assignment.AssignmentID, assignment.AssignmentID)
	}
	lease := &engine.TaskLease{LeaseID: "lease-1", LeaseToken: "token-1"}
	if err := recreatedForClaim.FinalizeClaim(ctx, reclaimed.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	recreatedForLease := NewRedisRunnerDirectory(rdb)
	if err := recreatedForLease.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("ReleaseLeased() after recreation error = %v", err)
	}
	if enqueued, err := recreatedForLease.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("EnqueueAssignment() after leased release error = %v", err)
	} else if !enqueued {
		t.Fatal("EnqueueAssignment() after RemoveSeen enqueued=false, want true")
	}
}

func TestRedisRunnerDirectoryRejectsStaleSessionAfterReregistration(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	firstDirectory := NewRedisRunnerDirectory(rdb)
	first := registerRedisDirectoryRunner(t, ctx, firstDirectory, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-reregister/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, firstDirectory, assignment)
	claimed := claimRedisDirectoryAssignment(t, ctx, firstDirectory, first, 1)

	recreatedDirectory := NewRedisRunnerDirectory(rdb)
	second := registerRedisDirectoryRunner(t, ctx, recreatedDirectory, "runner-1", 1)
	if first.SessionID == second.SessionID {
		t.Fatalf("SessionID = %q for both registrations, want a new fenced session", first.SessionID)
	}
	reclaimed := claimRedisDirectoryAssignment(t, ctx, recreatedDirectory, second, 1)
	if reclaimed.ClaimID == claimed.ClaimID {
		t.Fatalf("reclaimed ClaimID = %q, want a fresh claim after re-registration", reclaimed.ClaimID)
	}
	if reclaimed.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("reclaimed AssignmentID = %q, want %q", reclaimed.Assignment.AssignmentID, assignment.AssignmentID)
	}
	if err := recreatedDirectory.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  first.RunnerID,
		SessionID: first.SessionID,
		Capacity:  1,
		InFlight:  0,
	}); !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("stale Heartbeat() error = %v, want ErrRunnerSessionStale", err)
	}
	if err := recreatedDirectory.ValidateSession(ctx, first.RunnerID, first.SessionID); !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("stale ValidateSession() error = %v, want ErrRunnerSessionStale", err)
	}
}

func TestRedisRunnerDirectoryReclaimsExpiredClaim(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(10*time.Second))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-expired/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	first := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(first.ClaimID),
	}).Err(); err != nil {
		t.Fatalf("expire claim index entry: %v", err)
	}
	if err := directory.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatalf("ReclaimExpiredClaims() error = %v", err)
	}

	second := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if second.ClaimID == first.ClaimID {
		t.Fatalf("reclaimed ClaimID = %q, want a fresh claim", second.ClaimID)
	}
	if second.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("reclaimed AssignmentID = %q, want %q", second.Assignment.AssignmentID, assignment.AssignmentID)
	}
}

func TestRedisRunnerDirectoryFencesLeasedReleaseByToken(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-token/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{LeaseID: "lease-current", LeaseToken: "token-current"}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	recreated := NewRedisRunnerDirectory(rdb)
	if err := recreated.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      "lease-stale",
		LeaseToken:   "token-stale",
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("stale ReleaseLeased() error = %v", err)
	}
	if enqueued, err := recreated.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("EnqueueAssignment() after stale release error = %v", err)
	} else if enqueued {
		t.Fatal("EnqueueAssignment() after stale release enqueued=true, want durable seen marker retained")
	}
	replay, ok, err := recreated.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() after stale release error = %v", err)
	}
	if !ok || replay.Lease == nil {
		t.Fatal("ClaimForRunner() after stale release did not replay the current leased assignment")
	}
	if replay.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("replayed lease token = %q, want %q", replay.Lease.LeaseToken, lease.LeaseToken)
	}

	if err := recreated.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("current ReleaseLeased() error = %v", err)
	}
	if enqueued, err := recreated.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("EnqueueAssignment() after current release error = %v", err)
	} else if !enqueued {
		t.Fatal("EnqueueAssignment() after current token release enqueued=false, want true")
	}
}

func TestNewControlPlanePrefersConfiguredRunnerDirectory(t *testing.T) {
	configured := NewMemoryRunnerDirectory()
	controlPlane, err := NewControlPlane(Config{
		Backend:         backendlocal.New(),
		RunnerDirectory: configured,
	})
	if err != nil {
		t.Fatalf("NewControlPlane() error = %v", err)
	}
	if controlPlane.runners != configured {
		t.Fatalf("ControlPlane runners = %T, want configured directory %T", controlPlane.runners, configured)
	}
}

func TestNewControlPlaneUsesRedisRunnerDirectoryWhenBackendExposesClient(t *testing.T) {
	redisServer, _ := newRedisRunnerDirectoryTestClient(t)
	backend, err := distributed.New(redisServer.Addr(), nil)
	if err != nil {
		t.Fatalf("distributed.New() error = %v", err)
	}
	controlPlane, err := NewControlPlane(Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane() error = %v", err)
	}
	if _, ok := controlPlane.runners.(*RedisRunnerDirectory); !ok {
		t.Fatalf("ControlPlane runners = %T, want *RedisRunnerDirectory", controlPlane.runners)
	}
}

func newRedisRunnerDirectoryTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(redisServer.Close)
	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return redisServer, rdb
}

func registerRedisDirectoryRunner(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, runnerID string, capacity int, tenants ...tenant.TenantID) RunnerSession {
	t.Helper()

	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     capacity,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Tenants:      tenants,
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

func mustEnqueueRedisDirectoryAssignment(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, assignment Assignment) {
	t.Helper()

	enqueued, err := directory.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatalf("EnqueueAssignment() enqueued=false for %q", assignment.AssignmentID)
	}
}

func claimRedisDirectoryAssignment(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession, capacity int) Claim {
	t.Helper()

	claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, capacity))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false, want claim")
	}
	return claim
}

func redisDirectoryClaimRequest(session RunnerSession, capacity int) ClaimRequest {
	return ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     capacity,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Now:          time.Unix(11, 0),
	}
}

func redisDirectoryTestAssignment(id AssignmentID, tenants ...tenant.TenantID) Assignment {
	tenantID := tenant.DefaultTenant
	if len(tenants) > 0 {
		tenantID = tenants[0]
	}
	return Assignment{
		AssignmentID: id,
		Task: engine.Task{
			ExecutionID:  "exec-1",
			NodeName:     string(id),
			NodeIdx:      4,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 7,
			AutoDepth:    3,
		},
		Routing:  engine.TaskRouting{NodeType: "xflow.function"},
		TenantID: tenantID,
	}
}

func TestRedisRunnerDirectoryReplaysFullLeaseAfterRestartAndReregistration(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	firstSession := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-replay/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, firstSession, 1)
	lease := &engine.TaskLease{
		LeaseID:     "lease-replay",
		LeaseToken:  "token-replay",
		Attempt:     2,
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

	recreated := NewRedisRunnerDirectory(rdb)
	replay, ok, err := recreated.ClaimForRunner(ctx, redisDirectoryClaimRequest(firstSession, 1))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() after recreation ok=%v err=%v, want replay", ok, err)
	}
	if replay.ClaimID != "" || replay.Lease == nil {
		t.Fatalf("replay = %+v, want durable leased replay", replay)
	}
	if replay.Lease.LeaseID != lease.LeaseID || replay.Lease.LeaseToken != lease.LeaseToken || replay.Lease.Attempt != lease.Attempt {
		t.Fatalf("replayed lease identity = %+v, want %+v", replay.Lease, lease)
	}
	if got := replay.Lease.Input.Data["request_id"]; got != "req-1" {
		t.Fatalf("replayed lease input request_id = %v, want req-1", got)
	}
	if replay.Lease.Task.AutoDepth != assignment.Task.AutoDepth || replay.Lease.Task.ActivationID != assignment.Task.ActivationID {
		t.Fatalf("replayed lease task metadata = %+v, want auto depth %d and activation %d", replay.Lease.Task, assignment.Task.AutoDepth, assignment.Task.ActivationID)
	}

	secondSession := registerRedisDirectoryRunner(t, ctx, recreated, "runner-1", 1)
	if secondSession.SessionID == firstSession.SessionID {
		t.Fatal("re-registration did not fence the prior session")
	}
	replay, ok, err = recreated.ClaimForRunner(ctx, redisDirectoryClaimRequest(secondSession, 1))
	if err != nil || !ok || replay.Lease == nil {
		t.Fatalf("ClaimForRunner() after re-registration replay=%+v ok=%v err=%v, want lease replay", replay, ok, err)
	}
	if replay.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("re-registered replay token = %q, want %q", replay.Lease.LeaseToken, lease.LeaseToken)
	}
}

func TestRedisRunnerDirectoryFinalizeCleansClaimIndexes(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-cleanup/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	if err := directory.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{
		LeaseID: "lease-cleanup", LeaseToken: "token-cleanup", Task: assignment.Task,
	}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	for _, key := range []string{directory.keys.claimsAssignment, directory.keys.claimsRunner, directory.keys.claimsSession} {
		if _, err := rdb.HGet(ctx, key, string(claim.ClaimID)).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("claim index %q still contains %q: err=%v", key, claim.ClaimID, err)
		}
	}
	if _, err := rdb.ZScore(ctx, directory.keys.claimsExpiry, string(claim.ClaimID)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("claim expiry index still contains %q: err=%v", claim.ClaimID, err)
	}
}

func TestRedisRunnerDirectoryTenantIsolation(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	sessionA := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", 1, "tenant-a")
	bAssignment := redisDirectoryTestAssignment("exec-b/node-b/activation-1", "tenant-b")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, bAssignment)

	_, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-a",
		SessionID:    sessionA.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if ok {
		t.Fatal("runner-a claimed tenant-b assignment, want cross-tenant isolation")
	}

	sessionB := registerRedisDirectoryRunner(t, ctx, directory, "runner-b", 1, "tenant-b")
	claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-b",
		SessionID:    sessionB.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("ClaimForRunner(runner-b) error = %v", err)
	}
	if !ok {
		t.Fatal("runner-b did not claim tenant-b assignment")
	}
	if claim.Assignment.TenantID != "tenant-b" {
		t.Fatalf("claimed assignment tenant = %q, want tenant-b", claim.Assignment.TenantID)
	}
}

func TestRedisRunnerDirectoryDefaultTenantBackCompat(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	// No explicit tenants -> single-tenant default.
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-a/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if claim.Assignment.TenantID != tenant.DefaultTenant {
		t.Fatalf("tenant = %q, want default", claim.Assignment.TenantID)
	}
}

func TestRedisRunnerDirectoryTenantRoundTripThroughCodec(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1, "tenant-a", "tenant-b")

	assignment := redisDirectoryTestAssignment("exec-1/node-a/activation-1", "tenant-b")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	// Recreate directory to force reading the persisted assignment record.
	recreated := NewRedisRunnerDirectory(rdb)
	claim := claimRedisDirectoryAssignment(t, ctx, recreated, session, 1)
	if claim.Assignment.TenantID != "tenant-b" {
		t.Fatalf("tenant = %q, want tenant-b", claim.Assignment.TenantID)
	}
}

func TestRedisRunnerDirectoryClaimForRunnerFallsBackToDefaultTenantWhenTenantsFieldMissing(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	// Simulate a runner session written by an older version that did not persist
	// the runnerTenants hash field. ClaimForRunner must treat the missing field
	// as the default tenant rather than returning an error.
	if err := rdb.HDel(ctx, directory.keys.runnerTenants, session.RunnerID).Err(); err != nil {
		t.Fatalf("HDel runnerTenants: %v", err)
	}

	assignment := redisDirectoryTestAssignment("exec-1/node-a/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if claim.Assignment.TenantID != tenant.DefaultTenant {
		t.Fatalf("tenant = %q, want default", claim.Assignment.TenantID)
	}
}

// TestRedisRunnerDirectoryLookupLeaseResolvesAuthoritativeLease proves the
// Redis directory's LookupLease returns the server-authoritative finalized
// lease (with the real TenantID and immutable identity) for the
// (runner, session, lease-token) triple — the authority source for the report
// path's tenant injection. Regression for the 2026-07-21 trust-boundary probe.
func TestRedisRunnerDirectoryLookupLeaseResolvesAuthoritativeLease(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1, tenant.TenantID("tenant-a"))
	assignment := redisDirectoryTestAssignment("exec-1/node-lookup/activation-1", tenant.TenantID("tenant-a"))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{
		LeaseID:    "lease-lookup",
		LeaseToken: "token-lookup",
		Attempt:    3,
		Task:       assignment.Task,
		NodeType:   assignment.Routing.NodeType,
		TenantID:   tenant.TenantID("tenant-a"),
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	resolved, ok, err := directory.LookupLease(ctx, session.RunnerID, session.SessionID, LeaseLookupKey{
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
	})
	if err != nil || !ok {
		t.Fatalf("LookupLease() ok=%v err=%v, want authoritative lease", ok, err)
	}
	if resolved.LeaseToken != lease.LeaseToken || resolved.LeaseID != lease.LeaseID || resolved.Attempt != lease.Attempt {
		t.Fatalf("resolved lease identity = %+v, want %+v", resolved, lease)
	}
	if resolved.TenantID != tenant.TenantID("tenant-a") {
		t.Fatalf("resolved tenant = %q, want tenant-a (authoritative)", resolved.TenantID)
	}
}

// TestRedisRunnerDirectoryLookupLeaseSurvivesRestart proves LookupLease resolves
// the authoritative lease after the control plane restarts (re-reading durable
// directory state), so a runner that re-registers under a new session can still
// have its report resolved to the correct tenant — the durable-lease recovery
// contract required by the report tenant authority fix.
func TestRedisRunnerDirectoryLookupLeaseSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	firstSession := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1, tenant.TenantID("tenant-a"))
	assignment := redisDirectoryTestAssignment("exec-1/node-restart/activation-1", tenant.TenantID("tenant-a"))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, firstSession, 1)
	lease := &engine.TaskLease{
		LeaseID:    "lease-restart",
		LeaseToken: "token-restart",
		Task:       assignment.Task,
		NodeType:   assignment.Routing.NodeType,
		TenantID:   tenant.TenantID("tenant-a"),
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	// Control plane restarts: a fresh directory over the same Redis state.
	recreated := NewRedisRunnerDirectory(rdb)
	// The lease was finalized under firstSession; LookupLease must still
	// resolve it for firstSession (durable), and must NOT resolve it under a
	// different arbitrary session.
	resolved, ok, err := recreated.LookupLease(ctx, firstSession.RunnerID, firstSession.SessionID, LeaseLookupKey{
		LeaseToken: lease.LeaseToken,
	})
	if err != nil || !ok {
		t.Fatalf("LookupLease after restart ok=%v err=%v, want durable lease", ok, err)
	}
	if resolved.TenantID != tenant.TenantID("tenant-a") {
		t.Fatalf("resolved tenant = %q, want tenant-a after restart", resolved.TenantID)
	}

	// A stale/wrong session must not resolve the lease (session fence).
	_, staleOk, err := recreated.LookupLease(ctx, firstSession.RunnerID, "wrong-session", LeaseLookupKey{
		LeaseToken: lease.LeaseToken,
	})
	if err != nil {
		t.Fatalf("LookupLease wrong-session err = %v", err)
	}
	if staleOk {
		t.Fatal("LookupLease wrong-session ok=true, want false (session fence)")
	}

	// After ReleaseLeased, LookupLease must report the lease as gone.
	if err := recreated.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID: firstSession.RunnerID, SessionID: firstSession.SessionID,
		AssignmentID: assignment.AssignmentID, LeaseToken: lease.LeaseToken, RemoveSeen: true,
	}); err != nil {
		t.Fatalf("ReleaseLeased: %v", err)
	}
	_, releasedOk, err := recreated.LookupLease(ctx, firstSession.RunnerID, firstSession.SessionID, LeaseLookupKey{
		LeaseToken: lease.LeaseToken,
	})
	if err != nil {
		t.Fatalf("LookupLease after release err = %v", err)
	}
	if releasedOk {
		t.Fatal("LookupLease after release ok=true, want false (lease released)")
	}
}
