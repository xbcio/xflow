package runner

import (
	"context"
	"errors"
	"io"
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
	// lastCtx is the context of the most recent successfully established
	// subscription. It is only updated on a nil return, mirroring the real
	// ActivationHandler contract (Activate returns once the subscription is
	// established) so a failed upgrade attempt does not clobber it.
	lastCtx context.Context
}

func (m *mockActivationHandler) Activate(ctx context.Context, d protocol.ActivateDirective) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activations = append(m.activations, d)
	if m.activateErr == nil {
		m.lastCtx = ctx
	}
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

// 升级失败必须保留旧订阅：先 cancel 再 Activate 会在失败时留下一个指向已死
// context 的条目，使 runner 既不处理消息，又在 Inventory() 里声称自己在托管。
func TestActivateUpgradeFailureKeepsOldSubscriptionAlive(t *testing.T) {
	h := &mockActivationHandler{}
	tr := NewActivationTracker(h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	d1 := protocol.ActivateDirective{WorkflowID: "w", EntryUnitID: "e", Generation: 1}
	if err := tr.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d1},
	}); err != nil {
		t.Fatalf("first activate: %v", err)
	}

	// 第二次（升级到 gen 2）失败。
	h.activateErr = errors.New("supply not ready: rules")
	d2 := protocol.ActivateDirective{WorkflowID: "w", EntryUnitID: "e", Generation: 2}
	if err := tr.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d2},
	}); err != nil {
		t.Fatalf("ProcessDirectives must not propagate per-directive errors: %v", err)
	}

	inv := tr.Inventory()
	if len(inv) != 1 {
		t.Fatalf("inventory = %#v, want exactly the surviving gen-1 entry", inv)
	}
	if inv[0].Generation != 1 {
		t.Fatalf("generation = %d, want 1 (the old subscription must survive)", inv[0].Generation)
	}
	// 旧订阅的 context 必须仍然存活。
	h.mu.Lock()
	lastCtx := h.lastCtx
	h.mu.Unlock()
	if lastCtx == nil {
		t.Fatal("handler never received a context")
	}
	if err := lastCtx.Err(); err != nil {
		t.Fatalf("old subscription context was cancelled (%v); a failed upgrade must not kill it", err)
	}
}
