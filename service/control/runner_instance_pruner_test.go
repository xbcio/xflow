package control

import (
	"context"
	"errors"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
)

type prunerTestLeader bool

func (l prunerTestLeader) IsLeader() bool { return bool(l) }

func createPrunerTestPool(t *testing.T, pools RunnerPoolStore, poolID string, now time.Time) {
	t.Helper()
	if err := pools.CreatePool(context.Background(), RunnerPool{
		ID: poolID, Name: poolID, OwnerKind: store.PoolOwnerTenant,
		OwnerNamespace: "team-a", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
}

func enrollPrunerTestInstance(t *testing.T, pools RunnerPoolStore, poolID, systemID, runnerID string, at time.Time) {
	t.Helper()
	if _, err := pools.EnrollInstance(context.Background(), store.EnrollInstanceRequest{
		PoolID: poolID, SystemID: systemID, InstanceUID: "uid-" + runnerID,
		CandidateRunnerID: runnerID, Now: at,
	}); err != nil {
		t.Fatalf("EnrollInstance(%s): %v", runnerID, err)
	}
}

func registerPrunerTestRunner(t *testing.T, directory *MemoryRunnerDirectory, runnerID string, at time.Time, capacity int) RunnerSession {
	t.Helper()
	session, err := directory.Register(context.Background(), RegisterRunnerRequest{
		RunnerID: runnerID, Capacity: capacity, InstanceUID: "uid-" + runnerID,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function", NodeVersion: 1}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{"*"}},
		Namespaces:   []namespace.Namespace{namespace.Default}, Now: at,
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", runnerID, err)
	}
	return session
}

func TestRunnerInstancePrunerIdleDecision(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 3, 0, 0, 0, time.UTC)
	pools := NewMemoryRunnerPoolStore()
	identities := NewMemoryIssuedIdentityStore()
	directory := NewMemoryRunnerDirectory()
	createPrunerTestPool(t, pools, "pool", now.Add(-48*time.Hour))
	for _, runnerID := range []string{"runner-fresh", "runner-stale", "runner-missing"} {
		enrollPrunerTestInstance(t, pools, "pool", runnerID, runnerID, now.Add(-48*time.Hour))
	}
	registerPrunerTestRunner(t, directory, "runner-fresh", now.Add(-time.Hour), 1)
	registerPrunerTestRunner(t, directory, "runner-stale", now.Add(-25*time.Hour), 1)

	pruner := NewRunnerInstancePruner(RunnerInstancePrunerConfig{
		Pools: pools, IssuedIdentities: identities, Directory: directory,
		Leader: prunerTestLeader(true), IdleTTL: 24 * time.Hour,
		Clock: func() time.Time { return now },
	})
	if err := pruner.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	active, err := pools.ListInstancesByState(ctx, store.InstanceActive)
	if err != nil || len(active) != 1 || active[0].RunnerID != "runner-fresh" {
		t.Fatalf("active = %+v, err=%v; want only runner-fresh", active, err)
	}
	pruned, err := pools.ListInstancesByState(ctx, store.InstancePruned)
	if err != nil || len(pruned) != 2 {
		t.Fatalf("pruned = %+v, err=%v; want stale and missing", pruned, err)
	}
	if _, ok := directory.Runner(ctx, "runner-stale"); ok {
		t.Fatal("stale runner directory entry survived pruning")
	}
}

func TestRunnerInstancePrunerRetriesOutstandingWorkThenRecyclesInstance(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 4, 0, 0, 0, time.UTC)
	pools := NewMemoryRunnerPoolStore()
	identities := NewMemoryIssuedIdentityStore()
	directory := NewMemoryRunnerDirectory()
	createPrunerTestPool(t, pools, "pool", now.Add(-48*time.Hour))
	enrollPrunerTestInstance(t, pools, "pool", "system-a", "runner-old", now.Add(-48*time.Hour))
	session := registerPrunerTestRunner(t, directory, "runner-old", now.Add(-25*time.Hour), 1)

	token := "runner-secret"
	if err := identities.Issue(ctx, IssuedIdentity{
		RunnerID: "runner-old", TokenHash: HashSecret(token), PoolID: "pool",
		OwnerNamespace: "team-a", IssuedAt: now.Add(-48 * time.Hour),
		Scope: RunnerPolicy{AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{"*"}},
	}); err != nil {
		t.Fatalf("Issue identity: %v", err)
	}
	assignment := Assignment{
		AssignmentID: "exec-pruner/node/activation-1",
		Task:         engine.Task{ExecutionID: "exec-pruner", NodeName: "node", Type: engine.TaskTypeNodeExec},
		Routing:      engine.TaskRouting{NodeType: "xflow.function"}, Namespace: namespace.Default,
	}
	if ok, err := directory.EnqueueAssignment(ctx, assignment); err != nil || !ok {
		t.Fatalf("EnqueueAssignment = %v, %v", ok, err)
	}
	claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{RunnerID: session.RunnerID, SessionID: session.SessionID})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner = %+v, %v, %v", claim, ok, err)
	}

	pruner := NewRunnerInstancePruner(RunnerInstancePrunerConfig{
		Pools: pools, IssuedIdentities: identities, Directory: directory,
		Leader: prunerTestLeader(true), IdleTTL: 24 * time.Hour,
		Clock: func() time.Time { return now },
	})
	if err := pruner.Sweep(ctx); err != nil {
		t.Fatalf("first Sweep: %v", err)
	}
	draining, err := pools.ListInstancesByState(ctx, store.InstanceDraining)
	if err != nil || len(draining) != 1 || draining[0].RunnerID != "runner-old" {
		t.Fatalf("draining = %+v, err=%v; want runner-old", draining, err)
	}
	if _, ok := directory.Runner(ctx, "runner-old"); !ok {
		t.Fatal("outstanding work did not keep directory entry")
	}
	auth := NewIssuedIdentityAuthenticator(identities)
	if _, err := auth.AuthenticateOngoing("runner-old", token, TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("authentication after revoke = %v, want ErrAuthUnknownToken", err)
	}

	if err := directory.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop); err != nil {
		t.Fatalf("ReleaseClaim: %v", err)
	}
	if err := pruner.Sweep(ctx); err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	pruned, err := pools.ListInstancesByState(ctx, store.InstancePruned)
	if err != nil || len(pruned) != 1 || pruned[0].RunnerID != "runner-old" {
		t.Fatalf("pruned = %+v, err=%v; want runner-old", pruned, err)
	}
	if _, ok := directory.Runner(ctx, "runner-old"); ok {
		t.Fatal("runner directory entry remains after work drained")
	}
	directory.mu.RLock()
	_, runnerStateRemains := directory.runners["runner-old"]
	_, controlRemains := directory.controls["runner-old"]
	_, inventoryRemains := directory.activationInventory["runner-old"]
	directory.mu.RUnlock()
	if runnerStateRemains || controlRemains || inventoryRemains {
		t.Fatalf("memory per-runner state remains: runner=%v control=%v inventory=%v", runnerStateRemains, controlRemains, inventoryRemains)
	}

	reenrolled, err := pools.EnrollInstance(ctx, store.EnrollInstanceRequest{
		PoolID: "pool", SystemID: "system-a", InstanceUID: "uid-new",
		CandidateRunnerID: "runner-new", Now: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("reenroll pruned instance: %v", err)
	}
	if !reenrolled.Created || reenrolled.Instance.RunnerID != "runner-new" || reenrolled.Instance.RunnerID == "runner-old" {
		t.Fatalf("reenrolled = %+v Created=%v", reenrolled.Instance, reenrolled.Created)
	}
}

func TestRunnerInstancePrunerNonLeaderDoesNothing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 5, 0, 0, 0, time.UTC)
	pools := NewMemoryRunnerPoolStore()
	createPrunerTestPool(t, pools, "pool", now.Add(-48*time.Hour))
	enrollPrunerTestInstance(t, pools, "pool", "system", "runner", now.Add(-48*time.Hour))
	pruner := NewRunnerInstancePruner(RunnerInstancePrunerConfig{
		Pools: pools, IssuedIdentities: NewMemoryIssuedIdentityStore(), Directory: NewMemoryRunnerDirectory(),
		Leader: prunerTestLeader(false), IdleTTL: 24 * time.Hour,
		Clock: func() time.Time { return now },
	})
	if err := pruner.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	active, err := pools.ListInstancesByState(ctx, store.InstanceActive)
	if err != nil || len(active) != 1 || active[0].RunnerID != "runner" {
		t.Fatalf("active after non-leader sweep = %+v, err=%v", active, err)
	}
}

func TestRunnerInstancePrunerWithoutLeaderDoesNothing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 5, 30, 0, 0, time.UTC)
	pools := NewMemoryRunnerPoolStore()
	createPrunerTestPool(t, pools, "pool", now.Add(-48*time.Hour))
	enrollPrunerTestInstance(t, pools, "pool", "system", "runner", now.Add(-48*time.Hour))
	pruner := NewRunnerInstancePruner(RunnerInstancePrunerConfig{
		Pools: pools, IssuedIdentities: NewMemoryIssuedIdentityStore(), Directory: NewMemoryRunnerDirectory(),
		IdleTTL: 24 * time.Hour, Clock: func() time.Time { return now },
	})
	if err := pruner.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	active, err := pools.ListInstancesByState(ctx, store.InstanceActive)
	if err != nil || len(active) != 1 || active[0].RunnerID != "runner" {
		t.Fatalf("active without leader gate = %+v, err=%v", active, err)
	}
}

func TestControlPlaneRunnerInstancePrunerWiring(t *testing.T) {
	withoutPools, err := NewControlPlane(Config{
		Backend: backendlocal.New(), IssuedIdentities: NewMemoryIssuedIdentityStore(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane(without pools): %v", err)
	}
	if withoutPools.instancePruner != nil {
		t.Fatal("pruner started without RunnerPools")
	}

	pools := NewMemoryRunnerPoolStore()
	withPools, err := NewControlPlane(Config{
		Backend: backendlocal.New(), RunnerPools: pools,
		IssuedIdentities:            NewMemoryIssuedIdentityStore(),
		RunnerInstanceIdleTTL:       2 * time.Hour,
		RunnerInstancePruneInterval: 3 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewControlPlane(with pools): %v", err)
	}
	if withPools.instancePruner == nil {
		t.Fatal("pruner not assembled with pool and identity stores")
	}
	if withPools.instancePruner.idleTTL != 2*time.Hour || withPools.instancePruneInterval != 3*time.Minute {
		t.Fatalf("pruner config = ttl %v interval %v", withPools.instancePruner.idleTTL, withPools.instancePruneInterval)
	}
}
