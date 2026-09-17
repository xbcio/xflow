package runner

import (
	"context"
	"fmt"
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

	// onActivateFailed, when set, is called for each directive whose activation
	// failed. It is how a failure escapes this process: without it the failure
	// stops at a local log line and the server never learns the activation was
	// not taken. Called WITHOUT t.mu held — the callback does network I/O.
	onActivateFailed func(protocol.ActivateDirective, error)
	// onDeactivated reports a successful, local trigger cleanup. It is invoked
	// only after ActivationHandler.Deactivate returns nil (or the activation was
	// already absent), so a durable server-side drain obligation is never settled
	// merely because the tracker canceled a context. Called WITHOUT t.mu held.
	onDeactivated func(protocol.DeactivateDirective)
}

type activationID struct {
	Namespace       string
	WorkflowID      string
	WorkflowVersion string
	EntryUnitID     string
	ReplicaIndex    uint32
}

// supplyConsumerOwner renders id as the owner identity passed to
// node.RegisterWasmSupplyConsumerByDigest / UnregisterWasmSupplyConsumerByDigest
// for the activation-time legacy binding (spec Z.4 / Z.8, see
// trigger_activation_handler.go's registerSupplyConsumers /
// unregisterSupplyConsumers). ReplicaIndex is included deliberately: two
// replicas of the SAME activation are independent owners of the digest-keyed
// registration, so replica 0's Deactivate must not release replica 1's hold.
// The "activation:" prefix keeps this owner namespace from colliding with the
// "node:" owners the warm-up consumer and the execution-time guard use (see
// wasmSupplyConsumerOwner in wasm_supply_declaration.go) -- a workflow name
// that happened to render into this same shape must not be treated as the
// same owner as an unrelated activation.
func (id activationID) supplyConsumerOwner() string {
	return fmt.Sprintf("activation:%s/%s/%s/%s/%d",
		id.Namespace, id.WorkflowID, id.WorkflowVersion, id.EntryUnitID, id.ReplicaIndex)
}

func activationIDFromActivate(d protocol.ActivateDirective) activationID {
	return activationID{
		Namespace:       d.Namespace,
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		EntryUnitID:     d.EntryUnitID,
		ReplicaIndex:    d.ReplicaIndex,
	}
}

func activationIDFromDeactivate(d protocol.DeactivateDirective) activationID {
	return activationID{
		Namespace:       d.Namespace,
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		EntryUnitID:     d.EntryUnitID,
		ReplicaIndex:    d.ReplicaIndex,
	}
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

// SetOnActivateFailed installs the failure callback. Not safe to call
// concurrently with ProcessDirectives; call it once during wiring.
func (t *ActivationTracker) SetOnActivateFailed(fn func(protocol.ActivateDirective, error)) {
	t.onActivateFailed = fn
}

// SetOnDeactivated installs the successful cleanup callback. Like
// SetOnActivateFailed, call it during runner wiring before ProcessDirectives
// can run; callbacks themselves execute after the tracker mutex is released.
func (t *ActivationTracker) SetOnDeactivated(fn func(protocol.DeactivateDirective)) {
	t.onDeactivated = fn
}

// ProcessDirectives handles activate/deactivate directives from a heartbeat response.
// Safe for concurrent use.
func (t *ActivationTracker) ProcessDirectives(ctx context.Context, directives *protocol.HeartbeatActivations) error {
	if directives == nil {
		return nil
	}

	// Callbacks are collected under the lock and reported after releasing it:
	// both can perform network I/O, and holding t.mu across either would block
	// every other directive batch for the duration of an HTTP round-trip.
	type failedActivation struct {
		directive protocol.ActivateDirective
		err       error
	}
	var failed []failedActivation
	var deactivated []protocol.DeactivateDirective

	t.mu.Lock()

	// Process deactivate directives first so we free resources before activating
	// new ones. A deactivation is acknowledged only after handler cleanup has
	// succeeded; a failure keeps the activation in the tracker so the durable
	// directive retry can attempt the cleanup again.
	for _, d := range directives.Deactivate {
		if t.deactivateLocked(d) {
			deactivated = append(deactivated, d)
		}
	}

	for _, d := range directives.Activate {
		if err := t.activateLocked(ctx, d); err != nil {
			t.logger.Error("activation failed",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"error", err,
			)
			failed = append(failed, failedActivation{directive: d, err: err})
			// Continue processing remaining directives; don't fail the whole batch.
		}
	}
	t.mu.Unlock()

	if t.onActivateFailed != nil {
		for _, f := range failed {
			t.invokeOnActivateFailed(f.directive, f.err)
		}
	}
	if t.onDeactivated != nil {
		for _, d := range deactivated {
			t.invokeOnDeactivated(d)
		}
	}

	return nil
}

// invokeOnActivateFailed calls the failure callback with a panic guard. The
// callback does network I/O (an HTTP ack, wired in by the runner), and a
// single malformed response must not crash the goroutine driving
// ProcessDirectives — that would turn one directive's failure into the loss
// of every other in-flight activation on the runner, and in the heartbeat
// goroutine an unrecovered panic takes the whole process down with it.
func (t *ActivationTracker) invokeOnActivateFailed(d protocol.ActivateDirective, activateErr error) {
	defer func() {
		if r := recover(); r != nil {
			t.logger.Error("onActivateFailed callback panicked",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"panic", r,
			)
		}
	}()
	t.onActivateFailed(d, activateErr)
}

// invokeOnDeactivated isolates the success callback just like the activate
// failure callback: an acknowledgement client must never be able to panic the
// heartbeat goroutine that drives directive processing.
func (t *ActivationTracker) invokeOnDeactivated(d protocol.DeactivateDirective) {
	defer func() {
		if r := recover(); r != nil {
			t.logger.Error("onDeactivated callback panicked",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"panic", r,
			)
		}
	}()
	t.onDeactivated(d)
}

func (t *ActivationTracker) activateLocked(ctx context.Context, d protocol.ActivateDirective) error {
	id := activationIDFromActivate(d)
	existing, ok := t.active[id]

	if ok && existing.Generation == d.Generation {
		// Same generation — idempotent, skip.
		return nil
	}

	// Start the new subscription BEFORE cancelling the old one. Cancelling first
	// means a failed Activate leaves t.active[id] holding an entry whose context
	// is already dead: the runner then neither processes messages nor stops
	// claiming the activation in Inventory(), and only a restart clears it. The
	// two subscriptions briefly coexist when the new one succeeds, which is safe
	// for the Kafka trigger handler (verified in trigger_activation_handler.go
	// and node/trigger/kafka/kafka.go: duplicate subscriptions within the same
	// consumer group are arbitrated by the group protocol).
	subCtx, cancel := context.WithCancel(ctx)
	if err := t.handler.Activate(subCtx, d); err != nil {
		cancel()
		return err // old subscription, if any, is left untouched and alive
	}

	if ok {
		t.logger.Info("upgrading activation generation",
			"workflow_id", d.WorkflowID,
			"group_id", d.EntryUnitID,
			"old_generation", existing.Generation,
			"new_generation", d.Generation,
		)
		existing.cancel()
	}

	t.active[id] = &activeSubscription{
		Directive:  d,
		Generation: d.Generation,
		cancel:     cancel,
	}
	return nil
}

// deactivateLocked returns true only when the directive's cleanup is known to
// be locally complete and can therefore receive a deactivated receipt. Keeping
// an entry after cleanup failure is intentional: its context has already been
// canceled, but retrying handler.Deactivate is the only way to prove external
// resources (consumer, connection, supply bindings) were released.
func (t *ActivationTracker) deactivateLocked(d protocol.DeactivateDirective) bool {
	id := activationIDFromDeactivate(d)
	existing, ok := t.active[id]
	if !ok {
		// A restarted runner may no longer have a subscription that an earlier
		// session hosted. It is already locally absent, so acknowledging this
		// idempotent cleanup is correct and lets the server clear the durable
		// obligation after its session fence has been checked.
		return true
	}
	if existing.Generation > d.Generation {
		// A delayed old-generation stop must not claim cleanup of an activation
		// that this tracker now hosts at a newer generation.
		return false
	}

	existing.cancel()
	if err := t.handler.Deactivate(d); err != nil {
		t.logger.Warn("deactivation cleanup error",
			"workflow_id", d.WorkflowID,
			"group_id", d.EntryUnitID,
			"generation", d.Generation,
			"error", err,
		)
		return false
	}
	delete(t.active, id)
	return true
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
			WorkflowVersion: id.WorkflowVersion,
			EntryUnitID:     id.EntryUnitID,
			ReplicaIndex:    id.ReplicaIndex,
			Generation:      sub.Generation,
		})
	}
	return items
}

// ActiveCount returns the locally managed activation/subscription count for a
// drain heartbeat. Unlike Inventory it performs no per-heartbeat allocation.
// A nil tracker means this runner owns no tracker-managed activations.
func (t *ActivationTracker) ActiveCount() uint32 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return uint32(len(t.active))
}

// Shutdown stops all active subscriptions.
func (t *ActivationTracker) Shutdown(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, sub := range t.active {
		sub.cancel()
		if err := t.handler.Deactivate(protocol.DeactivateDirective{
			Namespace:       id.Namespace,
			WorkflowID:      id.WorkflowID,
			WorkflowVersion: id.WorkflowVersion,
			EntryUnitID:     id.EntryUnitID,
			ReplicaIndex:    id.ReplicaIndex,
			Generation:      sub.Generation,
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
