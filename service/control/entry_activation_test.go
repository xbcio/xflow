package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func testEntryActivation() engine.EntryActivation {
	return engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "tg",
		PackageHash:     "pkg-abc",
		Selector:        &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "a"}},
		Desired:         true,
	}
}

func keyOfActivation(a engine.EntryActivation) engine.EntryActivationKey {
	return engine.EntryActivationKey{
		Namespace:       a.Namespace,
		WorkflowID:      a.WorkflowID,
		WorkflowVersion: a.WorkflowVersion,
		EntryUnitID:     a.EntryUnitID,
		ReplicaIndex:    a.ReplicaIndex,
	}
}

func TestEntryActivationStore_Memory(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryEntryActivationStore()

	act := testEntryActivation()
	key := keyOfActivation(act)
	deadline := time.Now().Add(time.Minute)

	// Upsert the desired activation.
	if err := s.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Get returns the stored record.
	got, ok, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get: expected activation to exist")
	}
	if !got.Desired || got.PackageHash != "pkg-abc" {
		t.Fatalf("Get returned unexpected record: %+v", got)
	}

	// List returns the activation for the namespace.
	list, err := s.List(ctx, namespace.Default)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List len = %d, want 1", len(list))
	}

	// First writer wins on generation.
	ok, err = s.Assign(ctx, key, "runner-1", "sess-1", 1, deadline)
	if err != nil {
		t.Fatalf("Assign gen1: %v", err)
	}
	if !ok {
		t.Fatal("Assign gen1 must succeed (first writer)")
	}

	// Re-assign with the same generation must fail (already owned).
	ok, err = s.Assign(ctx, key, "runner-2", "sess-2", 1, deadline)
	if err != nil {
		t.Fatalf("Assign gen1 again: %v", err)
	}
	if ok {
		t.Fatal("Assign with stale generation must fail")
	}

	// Verify the owner is still runner-1.
	got, _, err = s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after re-assign: %v", err)
	}
	if got.RunnerID != "runner-1" || got.SessionID != "sess-1" {
		t.Fatalf("owner overwritten by stale assign: %+v", got)
	}
	if got.Generation != 1 {
		t.Fatalf("generation = %d, want 1", got.Generation)
	}

	// Fence generation 1, then assign generation 2 succeeds.
	if err := s.Fence(ctx, key, 1); err != nil {
		t.Fatalf("Fence gen1: %v", err)
	}
	ok, err = s.Assign(ctx, key, "runner-3", "sess-3", 2, deadline)
	if err != nil {
		t.Fatalf("Assign gen2: %v", err)
	}
	if !ok {
		t.Fatal("Assign gen2 after fence must succeed")
	}
	got, _, err = s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after gen2: %v", err)
	}
	if got.RunnerID != "runner-3" || got.Generation != 2 {
		t.Fatalf("gen2 assign not applied: %+v", got)
	}

	// Monotonicity: a lower generation cannot overwrite a higher one.
	ok, err = s.Assign(ctx, key, "runner-4", "sess-4", 2, deadline)
	if err != nil {
		t.Fatalf("Assign stale gen2: %v", err)
	}
	if ok {
		t.Fatal("Assign with non-monotonic generation must fail")
	}
}
