package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

func testKey() engine.ActivationKey {
	return engine.ActivationKey{
		Namespace:  namespace.Default,
		WorkflowID: "wf-test",
		GroupID:    "group-1",
	}
}

func testActivation() engine.TriggerActivation {
	return engine.TriggerActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-test",
		WorkflowVersion: "v1",
		GroupID:         "group-1",
		PackageHash:     "hash-abc",
		Desired:         true,
	}
}

func TestActivationStore_SetDesired_NewRecord(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	err := store.SetDesired(ctx, testActivation())
	if err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	got, err := store.GetActivation(ctx, testKey())
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got == nil {
		t.Fatal("expected activation record, got nil")
	}
	if got.Generation != 1 {
		t.Errorf("Generation = %d, want 1", got.Generation)
	}
	if !got.Desired {
		t.Error("Desired = false, want true")
	}
}

func TestActivationStore_SetDesired_BumpsGeneration_OnPackageHashChange(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())

	act := testActivation()
	act.PackageHash = "hash-xyz"
	_ = store.SetDesired(ctx, act)

	got, _ := store.GetActivation(ctx, testKey())
	if got.Generation != 2 {
		t.Errorf("Generation = %d, want 2", got.Generation)
	}
}

func TestActivationStore_SetDesired_BumpsGeneration_OnVersionChange(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())

	act := testActivation()
	act.WorkflowVersion = "v2"
	_ = store.SetDesired(ctx, act)

	got, _ := store.GetActivation(ctx, testKey())
	if got.Generation != 2 {
		t.Errorf("Generation = %d, want 2", got.Generation)
	}
}

func TestActivationStore_SetDesired_NoBump_SameValues(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())
	_ = store.SetDesired(ctx, testActivation())

	got, _ := store.GetActivation(ctx, testKey())
	if got.Generation != 1 {
		t.Errorf("Generation = %d, want 1 (no bump expected)", got.Generation)
	}
}

func TestActivationStore_ClearDesired(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())
	err := store.ClearDesired(ctx, testKey())
	if err != nil {
		t.Fatalf("ClearDesired: %v", err)
	}

	got, _ := store.GetActivation(ctx, testKey())
	if got.Desired {
		t.Error("Desired = true after ClearDesired, want false")
	}
	if got.Generation != 2 {
		t.Errorf("Generation = %d, want 2 after ClearDesired", got.Generation)
	}

	// Second ClearDesired is a no-op.
	_ = store.ClearDesired(ctx, testKey())
	got, _ = store.GetActivation(ctx, testKey())
	if got.Generation != 2 {
		t.Errorf("Generation = %d after second ClearDesired, want 2 (no-op)", got.Generation)
	}
}

func TestActivationStore_ListActive(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	act1 := testActivation()
	act2 := testActivation()
	act2.GroupID = "group-2"
	_ = store.SetDesired(ctx, act1)
	_ = store.SetDesired(ctx, act2)

	active, err := store.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("ListActive returned %d records, want 2", len(active))
	}

	// Clear one, should only return 1.
	_ = store.ClearDesired(ctx, testKey())
	active, _ = store.ListActive(ctx)
	if len(active) != 1 {
		t.Fatalf("ListActive after ClearDesired returned %d records, want 1", len(active))
	}
	if active[0].GroupID != "group-2" {
		t.Errorf("remaining active record GroupID = %q, want %q", active[0].GroupID, "group-2")
	}
}

func TestActivationStore_AssignRunner(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())

	deadline := time.Now().Add(30 * time.Second)
	gen, err := store.AssignRunner(ctx, testKey(), "runner-A", "session-1", deadline)
	if err != nil {
		t.Fatalf("AssignRunner: %v", err)
	}
	if gen != 2 {
		t.Errorf("AssignRunner returned generation %d, want 2", gen)
	}

	got, _ := store.GetActivation(ctx, testKey())
	if got.RunnerID != "runner-A" {
		t.Errorf("RunnerID = %q, want %q", got.RunnerID, "runner-A")
	}
	if got.SessionID != "session-1" {
		t.Errorf("SessionID = %q, want %q", got.SessionID, "session-1")
	}
	if got.LeaseDeadline.IsZero() {
		t.Error("LeaseDeadline is zero after AssignRunner")
	}
	if got.Generation != 2 {
		t.Errorf("Generation = %d, want 2", got.Generation)
	}
}

func TestActivationStore_AssignRunner_FailsIfNotDesired(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())
	_ = store.ClearDesired(ctx, testKey())

	deadline := time.Now().Add(30 * time.Second)
	_, err := store.AssignRunner(ctx, testKey(), "runner-A", "session-1", deadline)
	if err == nil {
		t.Fatal("AssignRunner should fail when activation is not desired")
	}
}

func TestActivationStore_RenewActivationLease(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())
	deadline := time.Now().Add(30 * time.Second)
	gen, _ := store.AssignRunner(ctx, testKey(), "runner-A", "session-1", deadline)

	// Correct generation → renewed.
	newDeadline := time.Now().Add(60 * time.Second)
	renewed, err := store.RenewActivationLease(ctx, testKey(), gen, newDeadline)
	if err != nil {
		t.Fatalf("RenewActivationLease: %v", err)
	}
	if !renewed {
		t.Error("RenewActivationLease returned false with correct generation")
	}

	got, _ := store.GetActivation(ctx, testKey())
	if got.LeaseDeadline.Before(deadline) {
		t.Error("LeaseDeadline was not updated")
	}

	// Wrong generation → not renewed.
	renewed, err = store.RenewActivationLease(ctx, testKey(), gen+999, newDeadline.Add(time.Hour))
	if err != nil {
		t.Fatalf("RenewActivationLease wrong gen: %v", err)
	}
	if renewed {
		t.Error("RenewActivationLease should return false with wrong generation")
	}
	got, _ = store.GetActivation(ctx, testKey())
	if got.LeaseDeadline.After(newDeadline.Add(time.Second)) {
		t.Error("LeaseDeadline should not be updated with wrong generation")
	}
}

func TestActivationStore_RevokeAssignment(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	_ = store.SetDesired(ctx, testActivation())
	deadline := time.Now().Add(30 * time.Second)
	gen, _ := store.AssignRunner(ctx, testKey(), "runner-A", "session-1", deadline)

	// Wrong generation → not revoked.
	revoked, err := store.RevokeAssignment(ctx, testKey(), gen+999)
	if err != nil {
		t.Fatalf("RevokeAssignment wrong gen: %v", err)
	}
	if revoked {
		t.Error("RevokeAssignment should return false with wrong generation")
	}

	// Correct generation → revoked.
	revoked, err = store.RevokeAssignment(ctx, testKey(), gen)
	if err != nil {
		t.Fatalf("RevokeAssignment: %v", err)
	}
	if !revoked {
		t.Error("RevokeAssignment returned false with correct generation")
	}

	got, _ := store.GetActivation(ctx, testKey())
	if got.RunnerID != "" {
		t.Errorf("RunnerID = %q after revoke, want empty", got.RunnerID)
	}
	if got.SessionID != "" {
		t.Errorf("SessionID = %q after revoke, want empty", got.SessionID)
	}
	if !got.LeaseDeadline.IsZero() {
		t.Error("LeaseDeadline should be zero after revoke")
	}
}

func TestActivationStore_ListByRunner(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	act1 := testActivation()
	act2 := testActivation()
	act2.GroupID = "group-2"
	_ = store.SetDesired(ctx, act1)
	_ = store.SetDesired(ctx, act2)

	deadline := time.Now().Add(30 * time.Second)
	_, _ = store.AssignRunner(ctx, testKey(), "runner-A", "session-1", deadline)
	key2 := engine.ActivationKey{Namespace: namespace.Default, WorkflowID: "wf-test", GroupID: "group-2"}
	_, _ = store.AssignRunner(ctx, key2, "runner-B", "session-2", deadline)

	// ListByRunner for runner-A.
	list, err := store.ListByRunner(ctx, "runner-A")
	if err != nil {
		t.Fatalf("ListByRunner: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListByRunner(runner-A) returned %d, want 1", len(list))
	}
	if list[0].GroupID != "group-1" {
		t.Errorf("ListByRunner(runner-A)[0].GroupID = %q, want %q", list[0].GroupID, "group-1")
	}

	// ListByRunner for runner-B.
	list, _ = store.ListByRunner(ctx, "runner-B")
	if len(list) != 1 {
		t.Fatalf("ListByRunner(runner-B) returned %d, want 1", len(list))
	}
	if list[0].GroupID != "group-2" {
		t.Errorf("ListByRunner(runner-B)[0].GroupID = %q, want %q", list[0].GroupID, "group-2")
	}
}

func TestActivationStore_GetActivation_NotFound(t *testing.T) {
	store := NewMemoryActivationStore()
	ctx := context.Background()

	key := engine.ActivationKey{
		Namespace:  namespace.Default,
		WorkflowID: "nonexistent",
		GroupID:    "no-group",
	}
	got, err := store.GetActivation(ctx, key)
	if err != nil {
		t.Fatalf("GetActivation: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for non-existent key, got %+v", got)
	}
}
