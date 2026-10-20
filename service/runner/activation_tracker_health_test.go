package runner

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

// healthMockActivationHandler is mockActivationHandler plus the optional
// subscriptionDoneProvider surface. Each activation installs a fresh done
// channel; a same-generation reactivation overwrites the generation's entry,
// exactly like a real handler replacing its stored subscription.
type healthMockActivationHandler struct {
	mu    sync.Mutex
	dones map[uint64]chan struct{}
	calls int
}

func newHealthMockActivationHandler() *healthMockActivationHandler {
	return &healthMockActivationHandler{dones: make(map[uint64]chan struct{})}
}

func (m *healthMockActivationHandler) Activate(_ context.Context, d protocol.ActivateDirective) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.dones[d.Generation] = make(chan struct{})
	return nil
}

func (m *healthMockActivationHandler) Deactivate(protocol.DeactivateDirective) error { return nil }

func (m *healthMockActivationHandler) SubscriptionDone(d protocol.ActivateDirective) (<-chan struct{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.dones[d.Generation]
	return ch, ok
}

func (m *healthMockActivationHandler) done(gen uint64) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dones[gen]
}

func (m *healthMockActivationHandler) activateCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// TestActivationTracker_InventoryOmitsExitedSubscription pins the core repair:
// once the subscription's own health signal fires, the runner stops claiming
// the activation in Inventory and ActiveCount. The heartbeat then reports it
// missing, which is what triggers the reconciler's redelivery within the same
// session.
func TestActivationTracker_InventoryOmitsExitedSubscription(t *testing.T) {
	handler := newHealthMockActivationHandler()
	tracker := NewActivationTracker(handler, slog.Default())

	d := protocol.ActivateDirective{WorkflowID: "wf-health", EntryUnitID: "grp-a", Generation: 1}
	if err := tracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got := len(tracker.Inventory()); got != 1 {
		t.Fatalf("inventory before exit = %d, want 1", got)
	}

	close(handler.done(1))

	waitUntil(t, "the exited subscription to leave the inventory", func() bool {
		return len(tracker.Inventory()) == 0
	})
	if got := tracker.ActiveCount(); got != 0 {
		t.Fatalf("ActiveCount after exit = %d, want 0", got)
	}
}

// TestActivationTracker_SameGenerationReactivatesAfterExit pins that a
// redelivered Activate is not skipped merely because the generation matches:
// the previous subscription has reported that it is gone, so a fresh install
// is the only correct response.
func TestActivationTracker_SameGenerationReactivatesAfterExit(t *testing.T) {
	handler := newHealthMockActivationHandler()
	tracker := NewActivationTracker(handler, slog.Default())

	d := protocol.ActivateDirective{WorkflowID: "wf-health", EntryUnitID: "grp-a", Generation: 1}
	if err := tracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d},
	}); err != nil {
		t.Fatalf("first activate: %v", err)
	}
	close(handler.done(1))
	waitUntil(t, "the first subscription to exit", func() bool {
		return len(tracker.Inventory()) == 0
	})

	if err := tracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d},
	}); err != nil {
		t.Fatalf("second activate: %v", err)
	}

	if got := handler.activateCount(); got != 2 {
		t.Fatalf("activate calls = %d, want 2 (a dead same-generation entry must not be skipped)", got)
	}
	if got := len(tracker.Inventory()); got != 1 {
		t.Fatalf("inventory after reactivation = %d, want 1", got)
	}
}

// TestActivationTracker_StaleExitWatcherDoesNotKillNewInstall pins the install
// identity guard. A reactivation can reuse the generation number, so a watcher
// belonging to the previous install must not mark the new entry exited when
// its (already closed) channel is observed later.
func TestActivationTracker_StaleExitWatcherDoesNotKillNewInstall(t *testing.T) {
	handler := newHealthMockActivationHandler()
	tracker := NewActivationTracker(handler, slog.Default())

	d := protocol.ActivateDirective{WorkflowID: "wf-health", EntryUnitID: "grp-a", Generation: 1}
	if err := tracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d},
	}); err != nil {
		t.Fatalf("first activate: %v", err)
	}
	firstDone := handler.done(1)
	close(firstDone)
	waitUntil(t, "the first subscription to exit", func() bool {
		return len(tracker.Inventory()) == 0
	})

	if err := tracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d},
	}); err != nil {
		t.Fatalf("second activate: %v", err)
	}

	// Install 1 is stale now; install 2 is the live one. Run the stale
	// watcher's body with the old identity and make sure it is a no-op.
	id := activationIDFromActivate(d)
	tracker.watchSubscriptionExit(id, 1, firstDone)

	if got := len(tracker.Inventory()); got != 1 {
		t.Fatalf("inventory after a stale exit watcher ran = %d, want 1", got)
	}
	if got := tracker.ActiveCount(); got != 1 {
		t.Fatalf("ActiveCount after a stale exit watcher ran = %d, want 1", got)
	}
}
