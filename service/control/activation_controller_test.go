package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// mockRunnerLister implements ActivationRunnerLister for tests.
type mockRunnerLister struct {
	runners []RunnerSnapshot
}

func (m *mockRunnerLister) ListLiveRunners(_ context.Context) []RunnerSnapshot {
	return m.runners
}

// mockRunnerDirectory is a minimal RunnerDirectory stub that satisfies the
// interface for the activation controller tests. Only Runner() is exercised.
type mockRunnerDirectory struct {
	snapshots map[string]RunnerSnapshot
}

func newMockRunnerDirectory() *mockRunnerDirectory {
	return &mockRunnerDirectory{snapshots: make(map[string]RunnerSnapshot)}
}

func (d *mockRunnerDirectory) Runner(_ context.Context, runnerID string) (RunnerSnapshot, bool) {
	snap, ok := d.snapshots[runnerID]
	return snap, ok
}

// Stub methods to satisfy RunnerDirectory interface.
func (d *mockRunnerDirectory) Register(context.Context, RegisterRunnerRequest) (RunnerSession, error) {
	return RunnerSession{}, nil
}
func (d *mockRunnerDirectory) ValidateSession(context.Context, string, string) error { return nil }
func (d *mockRunnerDirectory) Heartbeat(context.Context, HeartbeatRequest) error     { return nil }
func (d *mockRunnerDirectory) EnqueueAssignment(context.Context, Assignment) (bool, error) {
	return false, nil
}
func (d *mockRunnerDirectory) ClaimForRunner(context.Context, ClaimRequest) (Claim, bool, error) {
	return Claim{}, false, nil
}
func (d *mockRunnerDirectory) FinalizeClaim(context.Context, ClaimID, *engine.TaskLease) error {
	return nil
}
func (d *mockRunnerDirectory) ReleaseClaim(context.Context, ClaimID, ReleaseClaimReason) error {
	return nil
}
func (d *mockRunnerDirectory) ReleaseLeased(context.Context, ReleaseLeasedRequest) error {
	return nil
}
func (d *mockRunnerDirectory) ClearAssignment(context.Context, AssignmentID) error { return nil }
func (d *mockRunnerDirectory) ReleaseExpiredLease(context.Context, ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return ExpiredDirectoryLeaseReleased, nil
}

// --- Helper to build a controller for tests ---

func newTestController(lister ActivationRunnerLister, directory *mockRunnerDirectory, isLeader func() bool) (*ActivationController, *MemoryActivationStore) {
	store := NewMemoryActivationStore()
	sel := DefaultRunnerSelector()
	if isLeader == nil {
		isLeader = func() bool { return true }
	}
	cfg := ActivationControllerConfig{
		Store:           store,
		Directory:       directory,
		Lister:          lister,
		Selector:        &sel,
		IsLeader:        isLeader,
		ReconcilePeriod: 10 * time.Millisecond,
		LeaseTTL:        60 * time.Second,
		RenewThreshold:  20 * time.Second,
	}
	return NewActivationController(cfg), store
}

// --- Tests ---

func TestActivationController_ReconcileAssignsUnassigned(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	runner := RunnerSnapshot{
		RunnerID:      "runner-1",
		Capacity:      10,
		InFlight:      0,
		LastHeartbeat: now,
	}
	dir.snapshots["runner-1"] = runner

	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	if err := ac.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got == nil {
		t.Fatal("expected activation record")
	}
	if got.RunnerID != "runner-1" {
		t.Errorf("RunnerID = %q, want %q", got.RunnerID, "runner-1")
	}

	directives := ac.DirectivesForRunner("runner-1")
	if directives == nil {
		t.Fatal("expected directives for runner-1")
	}
	if len(directives.Activate) != 1 {
		t.Fatalf("Activate len = %d, want 1", len(directives.Activate))
	}
	if directives.Activate[0].WorkflowID != "wf-1" {
		t.Errorf("Activate[0].WorkflowID = %q, want %q", directives.Activate[0].WorkflowID, "wf-1")
	}
}

func TestActivationController_ReconcileRevokesDeadRunner(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	// Runner NOT in directory (dead).

	lister := &mockRunnerLister{runners: nil}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	// Manually assign
	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	gen, err := store.AssignRunner(ctx, key, "runner-dead", "", now.Add(60*time.Second))
	if err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}
	_ = gen

	if err := ac.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got.RunnerID != "" {
		t.Errorf("RunnerID = %q, want empty (revoked)", got.RunnerID)
	}

	directives := ac.DirectivesForRunner("runner-dead")
	if directives == nil {
		t.Fatal("expected deactivate directive for runner-dead")
	}
	if len(directives.Deactivate) != 1 {
		t.Fatalf("Deactivate len = %d, want 1", len(directives.Deactivate))
	}
}

func TestActivationController_ReconcileRenewsNearExpiry(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	runner := RunnerSnapshot{
		RunnerID:      "runner-1",
		Capacity:      10,
		InFlight:      0,
		LastHeartbeat: now,
	}
	dir.snapshots["runner-1"] = runner

	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	// Assign with a near-expiry deadline (10s remaining < 20s threshold).
	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	gen, err := store.AssignRunner(ctx, key, "runner-1", "", now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}

	if err := ac.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got.Generation != gen {
		t.Fatalf("Generation changed unexpectedly: got %d, want %d", got.Generation, gen)
	}
	// Lease should have been renewed: new deadline should be > now + 10s.
	if got.LeaseDeadline.Before(now.Add(50 * time.Second)) {
		t.Errorf("LeaseDeadline = %v, expected renewal to ~now+60s", got.LeaseDeadline)
	}
}

func TestActivationController_ReconcileRevokesExpiredLease(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	runner := RunnerSnapshot{
		RunnerID:      "runner-1",
		Capacity:      10,
		InFlight:      0,
		LastHeartbeat: now,
	}
	dir.snapshots["runner-1"] = runner

	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	// Assign with a deadline in the past.
	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	gen, err := store.AssignRunner(ctx, key, "runner-1", "", now.Add(-5*time.Second))
	if err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}
	_ = gen

	if err := ac.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got.RunnerID != "" {
		t.Errorf("RunnerID = %q, want empty (lease expired, revoked)", got.RunnerID)
	}

	directives := ac.DirectivesForRunner("runner-1")
	if directives == nil {
		t.Fatal("expected deactivate directive for expired lease")
	}
	if len(directives.Deactivate) != 1 {
		t.Fatalf("Deactivate len = %d, want 1", len(directives.Deactivate))
	}
}

func TestActivationController_DirectivesForRunner_ClearsAfterRead(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	runner := RunnerSnapshot{
		RunnerID:      "runner-1",
		Capacity:      10,
		InFlight:      0,
		LastHeartbeat: now,
	}
	dir.snapshots["runner-1"] = runner

	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	if err := ac.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	// First read returns directives.
	first := ac.DirectivesForRunner("runner-1")
	if first == nil || len(first.Activate) == 0 {
		t.Fatal("expected non-empty directives on first read")
	}

	// Second read returns nil (cleared).
	second := ac.DirectivesForRunner("runner-1")
	if second != nil {
		t.Errorf("expected nil on second read, got %+v", second)
	}
}

func TestActivationController_ClearDesired_EnqueuesDeactivate(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	lister := &mockRunnerLister{runners: nil}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	if _, err := store.AssignRunner(ctx, key, "runner-1", "", now.Add(60*time.Second)); err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}

	if err := ac.ClearDesired(ctx, key); err != nil {
		t.Fatalf("ClearDesired: %v", err)
	}

	directives := ac.DirectivesForRunner("runner-1")
	if directives == nil {
		t.Fatal("expected deactivate directive after ClearDesired")
	}
	if len(directives.Deactivate) != 1 {
		t.Fatalf("Deactivate len = %d, want 1", len(directives.Deactivate))
	}
	if directives.Deactivate[0].WorkflowID != "wf-1" {
		t.Errorf("Deactivate WorkflowID = %q, want %q", directives.Deactivate[0].WorkflowID, "wf-1")
	}
}

func TestActivationController_HandleAck_Active_RenewsLease(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	lister := &mockRunnerLister{runners: nil}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	gen, err := store.AssignRunner(ctx, key, "runner-1", "", now.Add(30*time.Second))
	if err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}

	ack := protocol.ActivationAck{
		RunnerID:   "runner-1",
		WorkflowID: "wf-1",
		GroupID:    "g1",
		Generation: gen,
		Status:     protocol.ActivationStatusActive,
	}
	if err := ac.HandleAck(ctx, ack); err != nil {
		t.Fatalf("HandleAck: %v", err)
	}

	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	// Lease should be extended beyond the original 30s deadline.
	if got.LeaseDeadline.Before(now.Add(50 * time.Second)) {
		t.Errorf("LeaseDeadline = %v, expected renewal to ~now+60s", got.LeaseDeadline)
	}
}

func TestActivationController_HandleAck_Failed_Revokes(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	lister := &mockRunnerLister{runners: nil}
	ac, store := newTestController(lister, dir, nil)

	ctx := context.Background()
	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	gen, err := store.AssignRunner(ctx, key, "runner-1", "", now.Add(60*time.Second))
	if err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}

	ack := protocol.ActivationAck{
		RunnerID:   "runner-1",
		WorkflowID: "wf-1",
		GroupID:    "g1",
		Generation: gen,
		Status:     protocol.ActivationStatusFailed,
	}
	if err := ac.HandleAck(ctx, ack); err != nil {
		t.Fatalf("HandleAck: %v", err)
	}

	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got.RunnerID != "" {
		t.Errorf("RunnerID = %q, want empty (revoked after failed ack)", got.RunnerID)
	}
}

func TestActivationController_LeaderGated_SkipsWhenNotLeader(t *testing.T) {
	now := time.Now()
	dir := newMockRunnerDirectory()
	runner := RunnerSnapshot{
		RunnerID:      "runner-1",
		Capacity:      10,
		InFlight:      0,
		LastHeartbeat: now,
	}
	dir.snapshots["runner-1"] = runner

	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	// IsLeader returns false.
	ac, store := newTestController(lister, dir, func() bool { return false })

	ctx, cancel := context.WithCancel(context.Background())

	act := engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "g1",
		PackageHash:     "abc",
		Desired:         true,
	}
	if err := store.SetDesired(ctx, act); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	// Run the controller loop briefly, then cancel.
	go ac.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	key := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-1", GroupID: "g1"}
	got, err := store.GetActivation(context.Background(), key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got.RunnerID != "" {
		t.Errorf("RunnerID = %q, want empty (not leader, should not assign)", got.RunnerID)
	}

	directives := ac.DirectivesForRunner("runner-1")
	if directives != nil {
		t.Errorf("expected no directives when not leader, got %+v", directives)
	}
}
