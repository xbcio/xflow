package control

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// Default reconciler timings. These mirror the retired group-centric
// ActivationController so behavior is preserved across the migration.
const (
	// DefaultEntryActivationReconcilePeriod is how often the reconcile loop runs
	// when driven by Run (not used by single-pass Reconcile).
	DefaultEntryActivationReconcilePeriod = 10 * time.Second
	// DefaultEntryActivationLeaseTTL is the assignment lease duration used when the
	// reconciler config leaves LeaseTTL unset.
	DefaultEntryActivationLeaseTTL = 60 * time.Second
	// DefaultEntryActivationRenewThreshold is the remaining-lease window under
	// which a live owner's lease is proactively renewed.
	DefaultEntryActivationRenewThreshold = 20 * time.Second
)

// EntryActivationReconcilerConfig configures an EntryActivationReconciler.
type EntryActivationReconcilerConfig struct {
	// Store is the durable desired-state store of entry activations.
	Store engine.EntryActivationStore
	// Lister enumerates currently-live runner sessions.
	Lister ActivationRunnerLister
	// Selector supplies liveness policy (LiveTTL). Optional — a default is used
	// when nil.
	Selector *RunnerSelector
	// Namespaces lists the namespaces whose activations this reconciler owns.
	// Empty defaults to {namespace.Default}.
	Namespaces []namespace.Namespace
	// ReconcilePeriod is how often the reconcile loop runs. Defaults to
	// DefaultEntryActivationReconcilePeriod. (Consumed by the loop driver, not by
	// single-pass Reconcile.)
	ReconcilePeriod time.Duration
	// LeaseTTL is how long an assignment is valid before it must be renewed or
	// is treated as expired. Defaults to DefaultEntryActivationLeaseTTL.
	LeaseTTL time.Duration
	// RenewThreshold is the remaining-lease window under which a live owner's
	// lease is proactively renewed. Defaults to
	// DefaultEntryActivationRenewThreshold.
	RenewThreshold time.Duration
	// Logger is optional.
	Logger engine.Logger
}

// entryRunnerDirectives holds pending activate/deactivate messages for a runner.
type entryRunnerDirectives struct {
	activate   []protocol.ActivateDirective
	deactivate []protocol.DeactivateDirective
}

// EntryActivationReconciler drives desired EntryActivation records toward a
// live runner assignment. For each desired-but-unassigned activation it assigns
// a capable, selector-matching live runner a fresh generation; activations
// whose lease has expired (or whose owner is no longer live) are fenced and
// reassigned. It is fail-closed: a runner that does not satisfy a required
// selector is never assigned.
//
// It also produces node-generic activate/deactivate directives per runner as a
// side effect of reconciliation: an Activate is enqueued after a successful
// Assign, and a Deactivate after a Fence/expiry-revoke or when the activation is
// no longer desired. DirectivesForRunner drains these (drain-once) so the
// heartbeat handler can piggyback them on the heartbeat response.
type EntryActivationReconciler struct {
	cfg      EntryActivationReconcilerConfig
	selector RunnerSelector

	mu         sync.Mutex
	directives map[string]*entryRunnerDirectives // runnerID -> pending directives
}

// NewEntryActivationReconciler constructs a reconciler with defaults applied.
func NewEntryActivationReconciler(cfg EntryActivationReconcilerConfig) *EntryActivationReconciler {
	if cfg.ReconcilePeriod <= 0 {
		cfg.ReconcilePeriod = DefaultEntryActivationReconcilePeriod
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultEntryActivationLeaseTTL
	}
	if cfg.RenewThreshold <= 0 {
		cfg.RenewThreshold = DefaultEntryActivationRenewThreshold
	}
	if len(cfg.Namespaces) == 0 {
		cfg.Namespaces = []namespace.Namespace{namespace.Default}
	}
	sel := DefaultRunnerSelector()
	if cfg.Selector != nil {
		sel = *cfg.Selector
	}
	return &EntryActivationReconciler{
		cfg:        cfg,
		selector:   sel,
		directives: make(map[string]*entryRunnerDirectives),
	}
}

// Reconcile performs a single reconciliation pass over every desired activation
// in the reconciler's namespaces, using now as the clock.
func (r *EntryActivationReconciler) Reconcile(ctx context.Context, now time.Time) error {
	live := r.liveRunners(ctx, now)
	for _, ns := range r.cfg.Namespaces {
		acts, err := r.cfg.Store.List(ctx, ns)
		if err != nil {
			return err
		}
		for i := range acts {
			if err := r.reconcileOne(ctx, &acts[i], live, now); err != nil {
				if r.cfg.Logger != nil {
					r.cfg.Logger.Warn("entry activation reconcile failed",
						"workflow_id", acts[i].WorkflowID,
						"entry_unit_id", acts[i].EntryUnitID,
						"err", err)
				}
				// Continue with the remaining activations.
			}
		}
	}
	return nil
}

func (r *EntryActivationReconciler) reconcileOne(ctx context.Context, act *engine.EntryActivation, live []RunnerSnapshot, now time.Time) error {
	key := engine.EntryActivationKey{
		Namespace:       act.Namespace,
		WorkflowID:      act.WorkflowID,
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
	}

	// A cleared / non-desired activation should not hold an assignment. Fence any
	// stale owner, tell it to deactivate, and leave it unassigned.
	if !act.Desired {
		if act.RunnerID != "" {
			prevRunner := act.RunnerID
			prevGen := act.Generation
			if err := r.cfg.Store.Fence(ctx, key, act.Generation); err != nil {
				return err
			}
			r.enqueueDeactivate(prevRunner, deactivateDirectiveFor(act, prevGen))
		}
		return nil
	}

	// Already assigned: keep it unless the lease expired, the owner is no longer
	// live, or the owner no longer satisfies the (possibly updated) desired
	// selector/capability. Otherwise fence the old generation and deactivate the
	// old owner before reassigning, so the stale runner can never keep driving the
	// entry unit and a material change (selector/capability) moves it to a runner
	// that matches the new desired state.
	if act.RunnerID != "" {
		expired := !act.LeaseDeadline.IsZero() && act.LeaseDeadline.Before(now)
		ownerLive := r.runnerIsLive(act.RunnerID, live, now)
		ownerMatches := ownerLive && r.ownerSatisfiesDesired(act, live, now)
		if !expired && ownerMatches {
			// Owner still valid: proactively renew the lease when it is within the
			// renew threshold of expiry, keeping the generation stable so the
			// hosting runner is not disrupted.
			if !act.LeaseDeadline.IsZero() && act.LeaseDeadline.Sub(now) < r.cfg.RenewThreshold {
				newDeadline := now.Add(r.cfg.LeaseTTL)
				if _, err := r.cfg.Store.Renew(ctx, key, act.Generation, newDeadline); err != nil {
					return err
				}
			}
			return nil
		}
		prevRunner := act.RunnerID
		prevGen := act.Generation
		if err := r.cfg.Store.Fence(ctx, key, act.Generation); err != nil {
			return err
		}
		// Tell the stale/dead/mismatched owner to stop (best-effort; a dead runner
		// simply never receives it).
		r.enqueueDeactivate(prevRunner, deactivateDirectiveFor(act, prevGen))
	}

	// Unassigned (either fresh or just fenced): choose a matching live runner.
	chosen, ok := r.chooseRunner(act, live, now)
	if !ok {
		// Fail-closed: no capable, selector-matching live runner. Leave it
		// unassigned for a later pass.
		// TODO(spec §11.7): default-selector grace fallback — when the selector
		// mode is "default" and no matching runner exists past the fallback
		// grace window, fall back to any capable live runner. Deferred by design
		// (tracked in the spec); only "required" semantics are implemented here.
		return nil
	}

	nextGen := act.Generation + 1
	deadline := now.Add(r.cfg.LeaseTTL)
	assigned, err := r.cfg.Store.Assign(ctx, key, chosen.RunnerID, "", nextGen, deadline)
	if err != nil {
		return err
	}
	if assigned {
		r.enqueueActivate(chosen.RunnerID, activateDirectiveFor(act, nextGen))
	}
	return nil
}

// chooseRunner returns the first live runner that satisfies the activation's
// selector. It is fail-closed on a required selector: a runner whose labels do
// not match is never chosen.
func (r *EntryActivationReconciler) chooseRunner(act *engine.EntryActivation, live []RunnerSnapshot, now time.Time) (RunnerSnapshot, bool) {
	for _, snap := range live {
		if !r.selector.IsLive(snap, now) {
			continue
		}
		if snap.Capacity > 0 && snap.InFlight >= snap.Capacity {
			continue
		}
		if !selectorMatches(act.Selector, snap.Labels) {
			continue
		}
		// Fail-closed capability match: skip runners that cannot host the entry
		// unit's node type(s). Older records / selector-only activations carry no
		// Requirements and are placed on selector match alone.
		if !capabilitiesSatisfy(act, snap) {
			continue
		}
		return snap, true
	}
	return RunnerSnapshot{}, false
}

// ownerSatisfiesDesired reports whether the activation's CURRENT owner still
// satisfies the (possibly updated) desired state. It detects a material change
// along three dimensions: selector, capability requirements, and content
// (PackageHash — which for a single trigger node fingerprints NodeType+Version+
// Params, and for a group is the projected package hash). When a workflow update
// narrows the selector or requirements so the current owner no longer qualifies,
// OR changes the trigger content within the same version (params/package), the
// reconciler must fence + deactivate the old owner and reassign so the new
// desired state (new params) reaches a runner at a new generation. Capacity is
// not re-checked here — the owner already holds the assignment. Returns false
// when the owner is no longer among the live runners (that case is already
// handled by the liveness check, but treating it as a non-match is safe).
func (r *EntryActivationReconciler) ownerSatisfiesDesired(act *engine.EntryActivation, live []RunnerSnapshot, now time.Time) bool {
	// Content drift: the desired PackageHash differs from the hash snapshotted
	// when the owner was assigned. AssignedPackageHash is absent (zero) on records
	// written before the field existed; in that case skip the content check
	// (backward-compatible — a legacy owner is not force-migrated) and fall
	// through to the selector/capability checks.
	if act.AssignedPackageHash != "" && act.AssignedPackageHash != act.PackageHash {
		return false
	}
	for _, snap := range live {
		if snap.RunnerID != act.RunnerID {
			continue
		}
		if !selectorMatches(act.Selector, snap.Labels) {
			return false
		}
		if !capabilitiesSatisfy(act, snap) {
			return false
		}
		return true
	}
	return false
}

// capabilitiesSatisfy reports whether the runner snapshot can host the entry
// unit's node type(s). Activations with no Requirements (older / selector-only
// records) are placed on selector match alone (returns true).
func capabilitiesSatisfy(act *engine.EntryActivation, snap RunnerSnapshot) bool {
	if len(act.Requirements) == 0 {
		return true
	}
	routing := engine.TaskRouting{
		Requirements: act.Requirements,
		NodeType:     act.Requirements[0].NodeType,
		NodeVersion:  act.Requirements[0].NodeVersion,
	}
	return MatchCapabilities(snap.Capabilities, routing)
}

func (r *EntryActivationReconciler) runnerIsLive(runnerID string, live []RunnerSnapshot, now time.Time) bool {
	for _, snap := range live {
		if snap.RunnerID == runnerID {
			return r.selector.IsLive(snap, now)
		}
	}
	return false
}

func (r *EntryActivationReconciler) liveRunners(ctx context.Context, now time.Time) []RunnerSnapshot {
	if r.cfg.Lister == nil {
		return nil
	}
	all := r.cfg.Lister.ListLiveRunners(ctx)
	out := all[:0:0]
	for _, snap := range all {
		if r.selector.IsLive(snap, now) {
			out = append(out, snap)
		}
	}
	return out
}

// selectorMatches applies runner-selector precedence. A nil selector is a
// default placement (any runner matches). A "required" selector demands the
// runner's labels satisfy MatchLabels. A "default" selector also currently
// demands the match; its post-grace fallback to any capable runner is deferred
// (see the TODO in reconcileOne, spec §11.7).
func selectorMatches(sel *types.RunnerSelector, runnerLabels map[string]string) bool {
	if sel == nil {
		return true
	}
	return MatchLabels(runnerLabels, sel.MatchLabels)
}

// activateDirectiveFor builds the node-generic activate directive for an
// activation at the given (post-assign) generation. Params carry the same
// trigger parameters the WorkflowDef already holds — no new secret surface.
func activateDirectiveFor(act *engine.EntryActivation, gen uint64) protocol.ActivateDirective {
	return protocol.ActivateDirective{
		Namespace:       string(act.Namespace),
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		NodeType:        act.NodeType,
		Params:          act.Params,
		Generation:      gen,
		PackageHash:     act.PackageHash,
	}
}

// deactivateDirectiveFor builds the node-generic deactivate directive for an
// activation at the given (pre-fence) generation.
func deactivateDirectiveFor(act *engine.EntryActivation, gen uint64) protocol.DeactivateDirective {
	return protocol.DeactivateDirective{
		Namespace:   string(act.Namespace),
		WorkflowID:  string(act.WorkflowID),
		EntryUnitID: act.EntryUnitID,
		Generation:  gen,
	}
}

// DirectivesForRunner returns and clears pending directives for a runner. Called
// by the heartbeat handler to piggyback activation directives on heartbeat
// responses. It is drain-once: the returned directives are removed from the
// queue, so a subsequent call returns nil until new directives are enqueued.
// Safe under concurrent heartbeats + reconcile.
func (r *EntryActivationReconciler) DirectivesForRunner(runnerID string) *protocol.HeartbeatActivations {
	r.mu.Lock()
	defer r.mu.Unlock()
	rd, ok := r.directives[runnerID]
	if !ok || (len(rd.activate) == 0 && len(rd.deactivate) == 0) {
		return nil
	}
	result := &protocol.HeartbeatActivations{
		Activate:   rd.activate,
		Deactivate: rd.deactivate,
	}
	delete(r.directives, runnerID)
	return result
}

func (r *EntryActivationReconciler) enqueueActivate(runnerID string, d protocol.ActivateDirective) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rd := r.getOrCreateDirectives(runnerID)
	rd.activate = append(rd.activate, d)
}

func (r *EntryActivationReconciler) enqueueDeactivate(runnerID string, d protocol.DeactivateDirective) {
	if runnerID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rd := r.getOrCreateDirectives(runnerID)
	rd.deactivate = append(rd.deactivate, d)
}

func (r *EntryActivationReconciler) getOrCreateDirectives(runnerID string) *entryRunnerDirectives {
	rd, ok := r.directives[runnerID]
	if !ok {
		rd = &entryRunnerDirectives{}
		r.directives[runnerID] = rd
	}
	return rd
}
