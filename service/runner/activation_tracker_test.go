package runner

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

// mockActivationHandler records Activate/Deactivate calls for test assertions.
type mockActivationHandler struct {
	mu            sync.Mutex
	activations   []protocol.ActivateDirective
	deactivations []protocol.DeactivateDirective
	activateErr   error
}

func (m *mockActivationHandler) Activate(_ context.Context, d protocol.ActivateDirective) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activations = append(m.activations, d)
	return m.activateErr
}

func (m *mockActivationHandler) Deactivate(d protocol.DeactivateDirective) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deactivations = append(m.deactivations, d)
	return nil
}

func (m *mockActivationHandler) activateCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.activations)
}

func (m *mockActivationHandler) deactivateCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.deactivations)
}

func TestActivationTracker_Activate_StartsSubscription(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()
	directives := &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{
				WorkflowID:  "wf-1",
				EntryUnitID: "grp-a",
				Generation:  1,
			},
		},
	}

	if err := tracker.ProcessDirectives(ctx, directives); err != nil {
		t.Fatalf("ProcessDirectives failed: %v", err)
	}

	if got := handler.activateCount(); got != 1 {
		t.Fatalf("expected 1 activation call, got %d", got)
	}

	handler.mu.Lock()
	got := handler.activations[0]
	handler.mu.Unlock()

	if got.WorkflowID != "wf-1" || got.EntryUnitID != "grp-a" || got.Generation != 1 {
		t.Fatalf("unexpected directive passed to handler: %+v", got)
	}
}

func TestActivationTracker_Activate_Idempotent_SameGeneration(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()
	directive := protocol.ActivateDirective{
		WorkflowID:  "wf-1",
		EntryUnitID: "grp-a",
		Generation:  1,
	}

	// Activate once.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{directive},
	})
	if err != nil {
		t.Fatalf("first ProcessDirectives failed: %v", err)
	}

	// Activate again with same generation — should be idempotent.
	err = tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{directive},
	})
	if err != nil {
		t.Fatalf("second ProcessDirectives failed: %v", err)
	}

	if got := handler.activateCount(); got != 1 {
		t.Fatalf("expected 1 activation call (idempotent), got %d", got)
	}
}

func TestActivationTracker_Activate_UpgradesGeneration(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()

	// Activate with generation 1.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("first ProcessDirectives failed: %v", err)
	}

	// Activate with generation 2 — should cancel old and start new.
	err = tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 2},
		},
	})
	if err != nil {
		t.Fatalf("second ProcessDirectives failed: %v", err)
	}

	if got := handler.activateCount(); got != 2 {
		t.Fatalf("expected 2 activation calls (upgrade), got %d", got)
	}

	// Inventory should reflect generation 2.
	inv := tracker.Inventory()
	if len(inv) != 1 {
		t.Fatalf("expected 1 inventory item, got %d", len(inv))
	}
	if inv[0].Generation != 2 {
		t.Fatalf("expected generation 2 in inventory, got %d", inv[0].Generation)
	}
}

func TestActivationTracker_Deactivate_StopsSubscription(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()

	// Activate first.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("activate failed: %v", err)
	}

	// Deactivate with matching generation.
	err = tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Deactivate: []protocol.DeactivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}

	if got := handler.deactivateCount(); got != 1 {
		t.Fatalf("expected 1 deactivation call, got %d", got)
	}

	// Inventory should be empty.
	inv := tracker.Inventory()
	if len(inv) != 0 {
		t.Fatalf("expected empty inventory after deactivation, got %d items", len(inv))
	}
}

func TestActivationTracker_Inventory_ReportsActive(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()

	// Activate two different workflow/group combos.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 3},
			{WorkflowID: "wf-2", EntryUnitID: "grp-b", Generation: 7},
		},
	})
	if err != nil {
		t.Fatalf("ProcessDirectives failed: %v", err)
	}

	inv := tracker.Inventory()
	if len(inv) != 2 {
		t.Fatalf("expected 2 inventory items, got %d", len(inv))
	}

	// Build a lookup for assertions (order is map-iteration-dependent).
	lookup := make(map[string]protocol.ActivationInventoryItem)
	for _, item := range inv {
		lookup[item.WorkflowID+"/"+item.EntryUnitID] = item
	}

	item1, ok := lookup["wf-1/grp-a"]
	if !ok {
		t.Fatal("missing wf-1/grp-a in inventory")
	}
	if item1.Generation != 3 {
		t.Fatalf("expected generation 3 for wf-1/grp-a, got %d", item1.Generation)
	}

	item2, ok := lookup["wf-2/grp-b"]
	if !ok {
		t.Fatal("missing wf-2/grp-b in inventory")
	}
	if item2.Generation != 7 {
		t.Fatalf("expected generation 7 for wf-2/grp-b, got %d", item2.Generation)
	}
}
