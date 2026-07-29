package runner

import (
	"context"
	"log/slog"
	"sync"

	"github.com/xbcio/xflow/service/protocol"
)

// ActivationTracker manages trigger-group activation subscriptions on the runner.
// It processes directives from the activation controller (via heartbeat responses)
// and maintains the lifecycle of trigger subscriptions.
type ActivationTracker struct {
	mu      sync.Mutex
	active  map[activationID]*activeSubscription
	handler ActivationHandler
	logger  *slog.Logger
}

type activationID struct {
	WorkflowID  string
	EntryUnitID string
}

type activeSubscription struct {
	Directive  protocol.ActivateDirective
	Generation uint64
	// cancel is called to stop the subscription goroutine.
	cancel context.CancelFunc
}

// ActivationHandler is the callback interface for actually starting/stopping
// trigger-group subscriptions. The runner's trigger system implements this.
type ActivationHandler interface {
	// Activate starts a trigger-group subscription. Returns when the subscription
	// is established (not when it finishes). The subscription runs until ctx is canceled.
	Activate(ctx context.Context, directive protocol.ActivateDirective) error
	// Deactivate is called after cancel for any cleanup.
	Deactivate(directive protocol.DeactivateDirective) error
}

// NewActivationTracker creates a new ActivationTracker with the given handler and logger.
func NewActivationTracker(handler ActivationHandler, logger *slog.Logger) *ActivationTracker {
	return &ActivationTracker{
		active:  make(map[activationID]*activeSubscription),
		handler: handler,
		logger:  logger,
	}
}

// ProcessDirectives handles activate/deactivate directives from a heartbeat response.
// Safe for concurrent use.
func (t *ActivationTracker) ProcessDirectives(ctx context.Context, directives *protocol.HeartbeatActivations) error {
	if directives == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Process deactivate directives first so we free resources before activating new ones.
	for _, d := range directives.Deactivate {
		t.deactivateLocked(d)
	}

	for _, d := range directives.Activate {
		if err := t.activateLocked(ctx, d); err != nil {
			t.logger.Error("activation failed",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"error", err,
			)
			// Continue processing remaining directives; don't fail the whole batch.
		}
	}

	return nil
}

func (t *ActivationTracker) activateLocked(ctx context.Context, d protocol.ActivateDirective) error {
	id := activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}
	existing, ok := t.active[id]

	if ok {
		if existing.Generation == d.Generation {
			// Same generation — idempotent, skip.
			return nil
		}
		// Different (older) generation — cancel old subscription before starting new.
		t.logger.Info("upgrading activation generation",
			"workflow_id", d.WorkflowID,
			"group_id", d.EntryUnitID,
			"old_generation", existing.Generation,
			"new_generation", d.Generation,
		)
		existing.cancel()
	}

	subCtx, cancel := context.WithCancel(ctx)
	if err := t.handler.Activate(subCtx, d); err != nil {
		cancel()
		return err
	}

	t.active[id] = &activeSubscription{
		Directive:  d,
		Generation: d.Generation,
		cancel:     cancel,
	}
	return nil
}

func (t *ActivationTracker) deactivateLocked(d protocol.DeactivateDirective) {
	id := activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}
	existing, ok := t.active[id]
	if !ok {
		// Not active — skip.
		return
	}
	if existing.Generation > d.Generation {
		// Runner has a newer generation than the deactivate directive — skip.
		return
	}

	existing.cancel()
	delete(t.active, id)

	// Best-effort cleanup via handler.
	if err := t.handler.Deactivate(d); err != nil {
		t.logger.Warn("deactivation cleanup error",
			"workflow_id", d.WorkflowID,
			"group_id", d.EntryUnitID,
			"generation", d.Generation,
			"error", err,
		)
	}
}

// Inventory returns the current activation inventory for reconnect reporting.
func (t *ActivationTracker) Inventory() []protocol.ActivationInventoryItem {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.active) == 0 {
		return nil
	}

	items := make([]protocol.ActivationInventoryItem, 0, len(t.active))
	for id, sub := range t.active {
		items = append(items, protocol.ActivationInventoryItem{
			WorkflowID:      id.WorkflowID,
			WorkflowVersion: sub.Directive.WorkflowVersion,
			EntryUnitID:     id.EntryUnitID,
			Generation:      sub.Generation,
		})
	}
	return items
}

// Shutdown stops all active subscriptions.
func (t *ActivationTracker) Shutdown(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, sub := range t.active {
		sub.cancel()
		if err := t.handler.Deactivate(protocol.DeactivateDirective{
			WorkflowID:  id.WorkflowID,
			EntryUnitID: id.EntryUnitID,
			Generation:  sub.Generation,
		}); err != nil {
			t.logger.Warn("shutdown deactivation error",
				"workflow_id", id.WorkflowID,
				"group_id", id.EntryUnitID,
				"error", err,
			)
		}
	}
	t.active = make(map[activationID]*activeSubscription)
}
