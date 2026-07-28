package control

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

const (
	DefaultActivationReconcilePeriod = 10 * time.Second
	DefaultActivationLease           = 60 * time.Second
	DefaultRenewThreshold            = 20 * time.Second
)

// ActivationRunnerLister provides runner enumeration for the activation
// controller. Implementations should return only runners currently considered
// live (heartbeated within TTL).
type ActivationRunnerLister interface {
	ListLiveRunners(ctx context.Context) []RunnerSnapshot
}

// ActivationControllerConfig configures an ActivationController.
type ActivationControllerConfig struct {
	Store           engine.TriggerActivationStore
	Directory       RunnerDirectory
	Lister          ActivationRunnerLister
	Selector        *RunnerSelector
	IsLeader        func() bool
	Logger          engine.Logger
	ReconcilePeriod time.Duration // default 10s
	LeaseTTL        time.Duration // default 60s
	RenewThreshold  time.Duration // default 20s (renew when remaining < this)
}

// runnerDirectives holds pending activate/deactivate messages for a runner.
type runnerDirectives struct {
	activate   []protocol.ActivateDirective
	deactivate []protocol.DeactivateDirective
}

// ActivationController is the reconciliation loop that manages trigger-group
// desired state. It is leader-gated: only the elected leader performs
// assignment, revocation, and lease renewal.
type ActivationController struct {
	cfg ActivationControllerConfig

	mu         sync.Mutex
	directives map[string]*runnerDirectives // runnerID -> pending directives

	stopOnce sync.Once
	stopped  chan struct{}
}

// NewActivationController creates an ActivationController. Caller must invoke
// Run to start the reconciliation loop.
func NewActivationController(cfg ActivationControllerConfig) *ActivationController {
	if cfg.ReconcilePeriod <= 0 {
		cfg.ReconcilePeriod = DefaultActivationReconcilePeriod
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultActivationLease
	}
	if cfg.RenewThreshold <= 0 {
		cfg.RenewThreshold = DefaultRenewThreshold
	}
	return &ActivationController{
		cfg:        cfg,
		directives: make(map[string]*runnerDirectives),
		stopped:    make(chan struct{}),
	}
}

// Run starts the reconciliation loop. It blocks until ctx is canceled.
func (ac *ActivationController) Run(ctx context.Context) {
	ticker := time.NewTicker(ac.cfg.ReconcilePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			ac.stopOnce.Do(func() { close(ac.stopped) })
			return
		case <-ticker.C:
			if !ac.cfg.IsLeader() {
				continue
			}
			if err := ac.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				if ac.cfg.Logger != nil {
					ac.cfg.Logger.Error("activation reconcile failed", "err", err)
				}
			}
		}
	}
}

// ReconcileOnce performs a single reconciliation pass. Exported for testing.
func (ac *ActivationController) ReconcileOnce(ctx context.Context) error {
	activations, err := ac.cfg.Store.ListActive(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range activations {
		act := &activations[i]
		if err := ac.reconcileOne(ctx, act, now); err != nil {
			if ac.cfg.Logger != nil {
				ac.cfg.Logger.Warn("reconcile activation failed",
					"workflow_id", act.WorkflowID,
					"group_id", act.GroupID,
					"err", err,
				)
			}
			// Continue reconciling remaining activations.
		}
	}
	return nil
}

func (ac *ActivationController) reconcileOne(ctx context.Context, act *engine.TriggerActivation, now time.Time) error {
	key := engine.ActivationKey{
		Namespace:  act.Namespace,
		WorkflowID: act.WorkflowID,
		GroupID:    act.GroupID,
	}

	// Unassigned: try to find a runner and assign.
	if act.RunnerID == "" {
		return ac.tryAssign(ctx, key, act, now)
	}

	// Assigned: check runner liveness.
	snap, found := ac.cfg.Directory.Runner(ctx, act.RunnerID)
	if !found || !ac.cfg.Selector.IsLive(snap, now) {
		// Runner gone or dead — revoke assignment so it can be reassigned.
		revoked, err := ac.cfg.Store.RevokeAssignment(ctx, key, act.Generation)
		if err != nil {
			return err
		}
		if revoked {
			ac.enqueueDeactivate(act.RunnerID, protocol.DeactivateDirective{
				Namespace:  string(act.Namespace),
				WorkflowID: string(act.WorkflowID),
				GroupID:    act.GroupID,
				Generation: act.Generation,
			})
		}
		return nil
	}

	// Lease expired — revoke and let next tick reassign.
	if !act.LeaseDeadline.IsZero() && now.After(act.LeaseDeadline) {
		revoked, err := ac.cfg.Store.RevokeAssignment(ctx, key, act.Generation)
		if err != nil {
			return err
		}
		if revoked {
			ac.enqueueDeactivate(act.RunnerID, protocol.DeactivateDirective{
				Namespace:  string(act.Namespace),
				WorkflowID: string(act.WorkflowID),
				GroupID:    act.GroupID,
				Generation: act.Generation,
			})
		}
		return nil
	}

	// Lease about to expire — renew proactively.
	if !act.LeaseDeadline.IsZero() {
		remaining := act.LeaseDeadline.Sub(now)
		if remaining < ac.cfg.RenewThreshold {
			newDeadline := now.Add(ac.cfg.LeaseTTL)
			_, err := ac.cfg.Store.RenewActivationLease(ctx, key, act.Generation, newDeadline)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ac *ActivationController) tryAssign(ctx context.Context, key engine.ActivationKey, act *engine.TriggerActivation, now time.Time) error {
	if ac.cfg.Lister == nil {
		return nil
	}
	runners := ac.cfg.Lister.ListLiveRunners(ctx)
	if len(runners) == 0 {
		return nil
	}

	// Simple round-robin: pick the first live runner with capacity.
	// A more sophisticated scheduler can be plugged in later.
	var chosen *RunnerSnapshot
	for i := range runners {
		snap := &runners[i]
		if ac.cfg.Selector.IsLive(*snap, now) && snap.InFlight < snap.Capacity {
			chosen = snap
			break
		}
	}
	if chosen == nil {
		return nil
	}

	deadline := now.Add(ac.cfg.LeaseTTL)
	gen, err := ac.cfg.Store.AssignRunner(ctx, key, chosen.RunnerID, "", deadline)
	if err != nil {
		return err
	}

	ac.enqueueActivate(chosen.RunnerID, protocol.ActivateDirective{
		Namespace:       string(act.Namespace),
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		GroupID:         act.GroupID,
		Generation:      gen,
		PackageHash:     act.PackageHash,
	})
	return nil
}

// DirectivesForRunner returns and clears pending directives for a runner.
// Called by the heartbeat handler to piggyback activation directives on
// heartbeat responses.
func (ac *ActivationController) DirectivesForRunner(runnerID string) *protocol.HeartbeatActivations {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	rd, ok := ac.directives[runnerID]
	if !ok || (len(rd.activate) == 0 && len(rd.deactivate) == 0) {
		return nil
	}
	result := &protocol.HeartbeatActivations{
		Activate:   rd.activate,
		Deactivate: rd.deactivate,
	}
	delete(ac.directives, runnerID)
	return result
}

// SetDesired is called when a workflow with trigger-groups is registered or
// updated. It delegates to the store to set desired state.
func (ac *ActivationController) SetDesired(ctx context.Context, act engine.TriggerActivation) error {
	return ac.cfg.Store.SetDesired(ctx, act)
}

// ClearDesired is called when a workflow is removed or a trigger-group is
// deactivated.
func (ac *ActivationController) ClearDesired(ctx context.Context, key engine.ActivationKey) error {
	// Look up current assignment to send a deactivate directive.
	existing, err := ac.cfg.Store.GetActivation(ctx, key)
	if err != nil {
		return err
	}
	if err := ac.cfg.Store.ClearDesired(ctx, key); err != nil {
		return err
	}
	// If it was assigned, tell the runner to stop.
	if existing != nil && existing.RunnerID != "" {
		ac.enqueueDeactivate(existing.RunnerID, protocol.DeactivateDirective{
			Namespace:  string(key.Namespace),
			WorkflowID: string(key.WorkflowID),
			GroupID:    key.GroupID,
			Generation: existing.Generation,
		})
	}
	return nil
}

// HandleAck processes an activation acknowledgment from a runner.
func (ac *ActivationController) HandleAck(ctx context.Context, ack protocol.ActivationAck) error {
	// Construct a partial key; GetActivation resolves the full record including
	// namespace from (WorkflowID, GroupID).
	key := engine.ActivationKey{
		WorkflowID: types.WorkflowID(ack.WorkflowID),
		GroupID:    ack.GroupID,
	}
	existing, err := ac.cfg.Store.GetActivation(ctx, key)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}
	key.Namespace = existing.Namespace

	switch ack.Status {
	case protocol.ActivationStatusFailed:
		_, err := ac.cfg.Store.RevokeAssignment(ctx, key, ack.Generation)
		return err
	case protocol.ActivationStatusDeactivated:
		_, err := ac.cfg.Store.RevokeAssignment(ctx, key, ack.Generation)
		return err
	case protocol.ActivationStatusActive:
		newDeadline := time.Now().Add(ac.cfg.LeaseTTL)
		_, err := ac.cfg.Store.RenewActivationLease(ctx, key, ack.Generation, newDeadline)
		return err
	}
	return nil
}

func (ac *ActivationController) enqueueActivate(runnerID string, d protocol.ActivateDirective) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	rd := ac.getOrCreateDirectives(runnerID)
	rd.activate = append(rd.activate, d)
}

func (ac *ActivationController) enqueueDeactivate(runnerID string, d protocol.DeactivateDirective) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	rd := ac.getOrCreateDirectives(runnerID)
	rd.deactivate = append(rd.deactivate, d)
}

func (ac *ActivationController) getOrCreateDirectives(runnerID string) *runnerDirectives {
	rd, ok := ac.directives[runnerID]
	if !ok {
		rd = &runnerDirectives{}
		ac.directives[runnerID] = rd
	}
	return rd
}


