package control

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// Default reconciler timings. These preserve the timing behavior of the
// previous group-centric activation controller (since retired) across the
// migration to the node-generic path.
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
	// DefaultActivationRetryBackoffMin is the first retry delay after a runner
	// declines an activation. It matches the reconcile period: retrying sooner
	// cannot help, since a retry only takes effect on a reconcile pass.
	DefaultActivationRetryBackoffMin = 10 * time.Second
	// DefaultActivationRetryBackoffMax caps the retry delay. A supply that is
	// down for hours therefore costs one redispatch per 5 minutes, and recovery
	// is noticed within that window.
	DefaultActivationRetryBackoffMax = 5 * time.Minute
)

// ActivationRunnerLister provides runner enumeration for the activation
// reconciler. Implementations should return only runners currently considered
// live (heartbeated within TTL).
type ActivationRunnerLister interface {
	ListLiveRunners(ctx context.Context) []RunnerSnapshot
}

// EntryActivationReconcilerConfig configures an EntryActivationReconciler.
type EntryActivationReconcilerConfig struct {
	// Store is the durable desired-state store of entry activations.
	Store engine.EntryActivationStore
	// Lister enumerates currently-live runner sessions.
	Lister ActivationRunnerLister
	// WorkflowRegistry resolves a workflow's compiled graph so a GROUP entry
	// unit's ActivateDirective can carry a freshly re-projected
	// SubgraphPackage (spec 2026-08-07 §3.3). nil means group Activate
	// directives never carry a package — the runner-side group dispatch then
	// fails closed (see service/runner.TriggerActivationHandler), same as
	// before this feature existed. Standalone (non-group) trigger entry units
	// are unaffected either way.
	WorkflowRegistry backend.WorkflowRegistry
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
	// RetryBackoffMin is the first retry delay applied after a runner declines
	// an activation. Defaults to DefaultActivationRetryBackoffMin.
	RetryBackoffMin time.Duration
	// RetryBackoffMax caps the retry delay for an activation that keeps
	// failing. Defaults to DefaultActivationRetryBackoffMax.
	RetryBackoffMax time.Duration
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
// For "default" selector mode (spec §11.7): when no label-matching runner is
// available (or all matching runners are at capacity) for longer than
// FallbackGrace, the reconciler falls back to any capable live runner.
// Capability checks are never relaxed by fallback.
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

	// noMatchSince tracks, per activation key, the wall time at which the
	// reconciler first observed no selector-matching runner for a "default"-mode
	// activation. It is used to implement the fallback grace window. Entries are
	// cleared when a matching runner is found or the activation is assigned.
	// Intentionally not persisted — a restart resets the grace window at most one
	// period (acceptable: spec §11.7).
	noMatchSince map[engine.EntryActivationKey]time.Time

	// retryBackoff tracks, per activation key, when a redispatch may next be
	// attempted and how long the current backoff is. It exists because a runner
	// that declines an activation (e.g. required supply content unavailable)
	// would otherwise be redispatched every reconcile period forever.
	//
	// Jitter matters more than the backoff itself here: one supply shared by many
	// workflows means every activation referencing it fails at the same instant,
	// and without jitter they would retry in a synchronised pulse forever.
	//
	// Intentionally not persisted, for the same reason as noMatchSince: the
	// periodic reconcile is leader-gated, so this map is only ever read and
	// written on the leader. A leader change loses the backoff and costs at most
	// one extra immediate retry.
	retryBackoff map[engine.EntryActivationKey]activationRetryState
}

// activationRetryState is the per-key retry backoff bookkeeping held in
// EntryActivationReconciler.retryBackoff.
type activationRetryState struct {
	// recordedAt is the wall time of the failure that produced nextAttempt; it
	// is what retryDelayFor measures nextAttempt against.
	recordedAt time.Time
	// nextAttempt is the earliest wall time at which a redispatch may occur.
	// It is recordedAt plus the jittered delay.
	nextAttempt time.Time
	// delay is the un-jittered backoff computed for this failure; it is what
	// doubles on the next failure. Jitter is applied on top of it to produce
	// nextAttempt, so it is not itself the interval reported by
	// retryDelayFor.
	delay time.Duration
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
	if cfg.RetryBackoffMin <= 0 {
		cfg.RetryBackoffMin = DefaultActivationRetryBackoffMin
	}
	if cfg.RetryBackoffMax <= 0 {
		cfg.RetryBackoffMax = DefaultActivationRetryBackoffMax
	}
	if len(cfg.Namespaces) == 0 {
		cfg.Namespaces = []namespace.Namespace{namespace.Default}
	}
	sel := DefaultRunnerSelector()
	if cfg.Selector != nil {
		sel = *cfg.Selector
	}
	return &EntryActivationReconciler{
		cfg:          cfg,
		selector:     sel,
		directives:   make(map[string]*entryRunnerDirectives),
		noMatchSince: make(map[engine.EntryActivationKey]time.Time),
		retryBackoff: make(map[engine.EntryActivationKey]activationRetryState),
	}
}

// Reconcile performs a single reconciliation pass over every desired activation
// in the reconciler's namespaces, using now as the clock.
func (r *EntryActivationReconciler) Reconcile(ctx context.Context, now time.Time) error {
	live := r.liveRunners(ctx, now)
	seen := make(map[engine.EntryActivationKey]struct{})
	for _, ns := range r.cfg.Namespaces {
		acts, err := r.cfg.Store.List(ctx, ns)
		if err != nil {
			return err
		}
		for i := range acts {
			seen[keyOf(&acts[i])] = struct{}{}
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
	r.pruneNoMatch(seen)
	r.pruneRetryBackoff(seen)
	return nil
}

// pruneNoMatch drops grace-window tracking for activations that no longer exist
// in the store, so an unregistered workflow does not leak an entry forever. Only
// safe to call after a full pass over every configured namespace: a key absent
// from seen was not merely skipped, it is gone.
func (r *EntryActivationReconciler) pruneNoMatch(seen map[engine.EntryActivationKey]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.noMatchSince {
		if _, ok := seen[key]; !ok {
			delete(r.noMatchSince, key)
		}
	}
}

// pruneRetryBackoff drops retry-backoff state for activations that no longer
// exist in the store, mirroring pruneNoMatch: without this, an unregistered
// workflow's key would linger in retryBackoff forever. Only safe to call after
// a full pass over every configured namespace.
func (r *EntryActivationReconciler) pruneRetryBackoff(seen map[engine.EntryActivationKey]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.retryBackoff {
		if _, ok := seen[key]; !ok {
			delete(r.retryBackoff, key)
		}
	}
}

func keyOf(act *engine.EntryActivation) engine.EntryActivationKey {
	return engine.EntryActivationKey{
		Namespace:       act.Namespace,
		WorkflowID:      act.WorkflowID,
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
	}
}

func (r *EntryActivationReconciler) reconcileOne(ctx context.Context, act *engine.EntryActivation, live []RunnerSnapshot, now time.Time) error {
	key := keyOf(act)

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
			// Owner still valid: the activation is being hosted successfully, so any
			// backoff from a prior failure on this key no longer applies. (Task 5
			// wires this to the runner's ActivationAck; until then this is the best
			// available success signal — the owner still matches desired state.)
			r.clearRetryBackoff(key)
			// Proactively renew the lease when it is within the renew threshold of
			// expiry, keeping the generation stable so the hosting runner is not
			// disrupted.
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

	// Unassigned (either fresh or just fenced): if a prior failure on this key
	// put it in backoff, withhold redispatch until the backoff elapses. (Task 5
	// wires runner-reported failures into noteActivationFailure; until then this
	// is a dormant no-op since nothing populates retryBackoff yet.)
	if r.retryBlocked(key, now) {
		return nil
	}

	// choose a matching live runner.
	chosen, ok := r.chooseRunner(act, live, now)
	if !ok {
		// No selector-matching + capable runner found. Behavior depends on the
		// selector mode (spec §11.7).
		if r.selectorIsDefault(act.Selector) {
			chosen, ok = r.fallbackChooseRunner(act, live, now, key)
			if !ok {
				return nil
			}
			// Fallback succeeded — observable log.
			if r.cfg.Logger != nil {
				r.cfg.Logger.Info("default-selector fallback: assigning to non-matching runner",
					"workflow_id", act.WorkflowID,
					"entry_unit_id", act.EntryUnitID,
					"runner_id", chosen.RunnerID)
			}
		} else {
			// required mode (or nil selector which already matches anything in
			// chooseRunner): fail-closed — leave unassigned for a later pass.
			return nil
		}
	} else {
		// Matching runner found: clear any grace-window tracking for this key.
		r.clearNoMatch(key)
	}

	nextGen := act.Generation + 1
	deadline := now.Add(r.cfg.LeaseTTL)
	assigned, err := r.cfg.Store.Assign(ctx, key, chosen.RunnerID, "", nextGen, deadline)
	if err != nil {
		return err
	}
	if assigned {
		r.enqueueActivate(chosen.RunnerID, r.activateDirectiveFor(ctx, act, nextGen))
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

// ReconcileRunnerInventory reconciles a runner's freshly-registered activation
// inventory against the durable desired-state store. It is called from the
// register handler when a runner reconnects (a new session) and reports the
// activations it is currently hosting.
//
// For every desired activation currently ASSIGNED to runnerID:
//   - if the runner re-reports it (present in reported at the SAME generation),
//     the lease is RENEWED (deadline extended) with the generation UNCHANGED —
//     bumping the generation would fence the runner's own in-flight seeds, so a
//     reconnect that still hosts the activation must not disrupt it.
//   - if the runner does NOT report it (a new session that lost the
//     subscription on reconnect), the assignment is REVOKED (fenced +
//     unassigned + Deactivate enqueued) so a later reconcile pass reassigns it
//     to a live runner rather than orphaning it.
//
// A reported item at a DIFFERENT generation than the stored one is treated as
// unreported (revoke): the runner is hosting a superseded generation and must be
// reassigned at the current desired state. Reported items for activations NOT
// currently owned by this runner are ignored — the reconciler assigns owners; a
// runner cannot claim an activation by reporting it.
func (r *EntryActivationReconciler) ReconcileRunnerInventory(ctx context.Context, runnerID string, reported []protocol.ActivationInventoryItem, now time.Time) error {
	if runnerID == "" {
		return nil
	}
	// Index the reported inventory by (workflowID, workflowVersion, entryUnitID) → generation
	// so the per-activation lookup is O(1). When WorkflowVersion is empty (old
	// runner that does not report version), the item is stored under key with
	// empty version AND we mark it in a separate set so the matching step below
	// can fall back to a version-agnostic match — this prevents a behavioral
	// regression where an old runner's reconnect causes all versioned activations
	// to be incorrectly revoked.
	type invKey struct{ workflowID, workflowVersion, entryUnitID string }
	reportedGen := make(map[invKey]uint64, len(reported))
	// versionless tracks (workflowID, entryUnitID) → generation for items whose
	// WorkflowVersion is empty (old runner backward-compat fallback).
	type twoKey struct{ workflowID, entryUnitID string }
	versionless := make(map[twoKey]uint64)
	for _, item := range reported {
		reportedGen[invKey{item.WorkflowID, item.WorkflowVersion, item.EntryUnitID}] = item.Generation
		if item.WorkflowVersion == "" {
			versionless[twoKey{item.WorkflowID, item.EntryUnitID}] = item.Generation
		}
	}

	for _, ns := range r.cfg.Namespaces {
		acts, err := r.cfg.Store.List(ctx, ns)
		if err != nil {
			return err
		}
		for i := range acts {
			act := &acts[i]
			if !act.Desired || act.RunnerID != runnerID {
				continue
			}
			key := engine.EntryActivationKey{
				Namespace:       act.Namespace,
				WorkflowID:      act.WorkflowID,
				WorkflowVersion: act.WorkflowVersion,
				EntryUnitID:     act.EntryUnitID,
			}
			gen, ok := reportedGen[invKey{string(act.WorkflowID), act.WorkflowVersion, act.EntryUnitID}]
			if !ok {
				// Backward-compat: if the runner reported the item with empty
				// WorkflowVersion (old runner not aware of version), fall back to
				// a version-agnostic lookup. This avoids revoking activations
				// that a pre-version runner still legitimately hosts.
				gen, ok = versionless[twoKey{string(act.WorkflowID), act.EntryUnitID}]
			}
			if ok && gen == act.Generation {
				// Runner still hosts this exact generation → renew the lease,
				// generation UNCHANGED (do NOT fence the runner's in-flight seeds).
				newDeadline := now.Add(r.cfg.LeaseTTL)
				if _, err := r.cfg.Store.Renew(ctx, key, act.Generation, newDeadline); err != nil {
					if r.cfg.Logger != nil {
						r.cfg.Logger.Warn("entry activation inventory renew failed",
							"workflow_id", act.WorkflowID,
							"entry_unit_id", act.EntryUnitID,
							"err", err)
					}
				}
				continue
			}
			// Assigned to this runner but NOT reported (or reported at a stale
			// generation) → the reconnected session dropped it. Fence + deactivate
			// so a later reconcile reassigns it to a live runner.
			prevGen := act.Generation
			if err := r.cfg.Store.Fence(ctx, key, act.Generation); err != nil {
				if r.cfg.Logger != nil {
					r.cfg.Logger.Warn("entry activation inventory revoke failed",
						"workflow_id", act.WorkflowID,
						"entry_unit_id", act.EntryUnitID,
						"err", err)
				}
				continue
			}
			r.enqueueDeactivate(runnerID, deactivateDirectiveFor(act, prevGen))
		}
	}
	return nil
}

// selectorMatches applies runner-selector label matching. A nil selector is a
// default placement (any runner matches). For non-nil selectors, the runner's
// labels must satisfy all MatchLabels entries. This function does NOT consider
// the Mode field — mode-dependent fallback is handled by reconcileOne.
func selectorMatches(sel *types.RunnerSelector, runnerLabels map[string]string) bool {
	if sel == nil {
		return true
	}
	return MatchLabels(runnerLabels, sel.MatchLabels)
}

// selectorIsDefault reports whether the activation's selector uses "default"
// mode semantics. Per engine/graph/compile.go:resolveRunnerSelector, an empty
// Mode ("") is equivalent to RunnerSelectorModeDefault — the compiler defaults
// to "default" when no explicit mode is set. A nil selector means "any capable
// runner" (no labels to match), so fallback is irrelevant.
func (r *EntryActivationReconciler) selectorIsDefault(sel *types.RunnerSelector) bool {
	if sel == nil {
		return false
	}
	return sel.Mode == types.RunnerSelectorModeDefault || sel.Mode == ""
}

// fallbackChooseRunner implements the default-selector grace fallback (spec
// §11.7). It is called when no label-matching runner was found for a "default"
// mode activation. If the grace window has elapsed, it returns any live runner
// that satisfies capability checks (labels are relaxed, but capabilities are
// NOT — sending work to a runner that cannot execute it is always wrong).
// Returns false if the grace window has not elapsed or no capable runner exists.
func (r *EntryActivationReconciler) fallbackChooseRunner(act *engine.EntryActivation, live []RunnerSnapshot, now time.Time, key engine.EntryActivationKey) (RunnerSnapshot, bool) {
	r.mu.Lock()
	firstSeen, tracked := r.noMatchSince[key]
	if !tracked {
		r.noMatchSince[key] = now
		r.mu.Unlock()
		return RunnerSnapshot{}, false
	}
	r.mu.Unlock()

	grace := r.selector.FallbackGrace
	if grace <= 0 {
		grace = DefaultSelectorFallback
	}
	if now.Sub(firstSeen) < grace {
		// Grace window not yet elapsed — wait for a matching runner to appear.
		return RunnerSnapshot{}, false
	}

	// Grace elapsed: find any live + capable runner (labels relaxed).
	for _, snap := range live {
		if !r.selector.IsLive(snap, now) {
			continue
		}
		if snap.Capacity > 0 && snap.InFlight >= snap.Capacity {
			continue
		}
		// Capability check is never bypassed by fallback.
		if !capabilitiesSatisfy(act, snap) {
			continue
		}
		// Clear tracking on successful fallback assignment.
		r.clearNoMatch(key)
		return snap, true
	}
	return RunnerSnapshot{}, false
}

// clearNoMatch removes the grace-window tracking for key. Called when a
// matching runner is found or assignment succeeds.
func (r *EntryActivationReconciler) clearNoMatch(key engine.EntryActivationKey) {
	r.mu.Lock()
	delete(r.noMatchSince, key)
	r.mu.Unlock()
}

// noteActivationFailure records that a redispatch attempt for key failed (the
// runner declined the activation, e.g. required supply content unavailable)
// and computes the next backoff. The first failure sets the delay to
// RetryBackoffMin; each subsequent failure doubles the previous un-jittered
// delay, capped at RetryBackoffMax. The actual next-attempt time additionally
// applies +/-20% jitter to the delay, so that many keys failing at the same
// instant (a shared supply going down) do not all retry in lockstep.
func (r *EntryActivationReconciler) noteActivationFailure(key engine.EntryActivationKey, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	min := r.cfg.RetryBackoffMin
	if min <= 0 {
		min = DefaultActivationRetryBackoffMin
	}
	max := r.cfg.RetryBackoffMax
	if max <= 0 {
		max = DefaultActivationRetryBackoffMax
	}

	state, ok := r.retryBackoff[key]
	delay := min
	if ok {
		delay = state.delay * 2
		if delay > max {
			delay = max
		}
	}
	r.retryBackoff[key] = activationRetryState{
		recordedAt:  now,
		nextAttempt: now.Add(jitter(delay)),
		delay:       delay,
	}
}

// jitter applies +/-20% jitter to d using the package-level math/rand source
// (this is retry timing, not a security-sensitive use, so crypto/rand is not
// needed; Go 1.20+ auto-seeds the global source).
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := d / 5 // 20%
	if spread <= 0 {
		return d
	}
	// rand.Int63n panics on n<=0; spread is > 0 here. Offset is in
	// [-spread, +spread].
	offset := rand.Int63n(int64(spread)*2+1) - int64(spread)
	return d + time.Duration(offset)
}

// retryDelayFor returns the jittered delay currently recorded for key (0 if
// none), i.e. nextAttempt minus the wall time of the failure that produced it.
// Exposed for tests to assert growth/capping/jitter without depending on
// absolute nextAttempt values.
func (r *EntryActivationReconciler) retryDelayFor(key engine.EntryActivationKey) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.retryBackoff[key]
	if !ok {
		return 0
	}
	return state.nextAttempt.Sub(state.recordedAt)
}

// retryBlocked reports whether a redispatch for key must be withheld at now:
// false when there is no recorded failure, otherwise whether now is still
// before the recorded nextAttempt.
func (r *EntryActivationReconciler) retryBlocked(key engine.EntryActivationKey, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.retryBackoff[key]
	if !ok {
		return false
	}
	return now.Before(state.nextAttempt)
}

// clearRetryBackoff removes the retry-backoff state for key. Called once an
// activation is successfully accepted by a runner, so a supply's recovery is
// not masked by a lingering long backoff.
func (r *EntryActivationReconciler) clearRetryBackoff(key engine.EntryActivationKey) {
	r.mu.Lock()
	delete(r.retryBackoff, key)
	r.mu.Unlock()
}

// MarkActivationFailed records that a runner could not take an activation and
// makes it redispatchable. It deliberately does NOT assign a new owner: Fence
// clears runner_id, so the next reconcile pass sees an unassigned activation and
// runs the normal assignment path. Assigning here instead would race the
// leader's reconcile.
//
// Safe on any replica, leader or not: Fence is a single-key Redis CAS and this
// only ever fences the exact generation the ack names. A stale ack (naming a
// generation the store has already moved past) is ignored rather than allowed
// to tear down a healthy newer assignment.
//
// NOTE: because this method is intentionally leader-agnostic (to minimise time-
// to-fence after a decline), the backoff registered via noteActivationFailure is
// local to this replica. If the processing replica is NOT the current leader,
// the leader's retryBackoff map will not see this failure. This is accepted:
// the worst case is the leader attempts one immediate redispatch, which will
// fail again and register the backoff on the leader itself. One extra attempt is
// preferable to adding leader-only gating (which would delay fencing until the
// ack is forwarded or until the leader's next reconcile discovers the problem).
func (r *EntryActivationReconciler) MarkActivationFailed(ctx context.Context, runnerID string, ack protocol.ActivationAck) error {
	if ack.Status != protocol.ActivationStatusFailed {
		return nil
	}

	ns := namespace.FromContext(ctx)

	// WorkflowVersion is required to construct the store key. An empty value is
	// treated as a malformed request. This is NOT a backward-compatibility
	// concern: the ack send path (activation_acker.go) and the WorkflowVersion
	// field were introduced in the same feature branch — any runner capable of
	// sending an ActivationAck necessarily has WorkflowVersion available from
	// the ActivateDirective that triggered the failure. There is no deployed
	// runner that sends acks without this field.
	if ack.WorkflowVersion == "" {
		return ErrMissingWorkflowVersion
	}

	key := engine.EntryActivationKey{
		Namespace:       ns,
		WorkflowID:      types.WorkflowID(ack.WorkflowID),
		WorkflowVersion: ack.WorkflowVersion,
		EntryUnitID:     ack.GroupID,
	}

	act, ok, err := r.cfg.Store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		// The activation no longer exists (workflow unregistered or moved to
		// another namespace). Not an error.
		return nil
	}

	// Generation and owner must both match: a stale ack must never fence a
	// healthy newer assignment.
	if act.Generation != ack.Generation || act.RunnerID != runnerID {
		if r.cfg.Logger != nil {
			r.cfg.Logger.Info("ignoring stale activation ack",
				"workflow_id", act.WorkflowID,
				"entry_unit_id", act.EntryUnitID,
				"ack_generation", ack.Generation,
				"store_generation", act.Generation,
				"ack_runner", runnerID,
				"store_runner", act.RunnerID)
		}
		return nil
	}

	if err := r.cfg.Store.Fence(ctx, key, act.Generation); err != nil {
		return err
	}
	r.noteActivationFailure(key, time.Now())

	if r.cfg.Logger != nil {
		r.cfg.Logger.Info("activation fenced after runner decline",
			"workflow_id", act.WorkflowID,
			"entry_unit_id", act.EntryUnitID,
			"runner_id", runnerID,
			"generation", ack.Generation,
			"error", ack.Error)
	}
	return nil
}

// activateDirectiveFor builds the node-generic activate directive for an
// activation at the given (post-assign) generation. Params carry the same
// trigger parameters the WorkflowDef already holds — no new secret surface.
func (r *EntryActivationReconciler) activateDirectiveFor(ctx context.Context, act *engine.EntryActivation, gen uint64) protocol.ActivateDirective {
	d := protocol.ActivateDirective{
		Namespace:       string(act.Namespace),
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		NodeType:        act.NodeType,
		Params:          act.Params,
		Generation:      gen,
		PackageHash:     act.PackageHash,
		Kind:            activationKindFor(act),
		Supplies:        act.Supplies,
	}
	if act.NodeType == engine.GroupNodeType {
		d.Package = r.projectPackageForGroup(ctx, act)
	}
	return d
}

// projectPackageForGroup re-projects the SubgraphPackage for a GROUP entry
// unit at directive-build time (spec 2026-08-07 §3.3: the package is sent
// fresh on every Activate, never cached against a runner-reported hash).
//
// It returns nil — never an error — when the package cannot be resolved: no
// WorkflowRegistry configured, the workflow/version unknown, or the entry
// unit not found in the graph. A single activation's projection failure must
// not abort reconciling every other activation in the same pass (mirrors how
// resolveEntrySeedTopology fails one seed request, not the whole reconciler).
// The resulting directive still carries the group's PackageHash from desired
// state; a runner receiving a nil Package for a group NodeType fails that one
// activation closed (Task 6), which is the correct degraded behavior.
//
// The freshly projected hash is compared against the stored one. The runner's
// PackageCache.validatePackage (execution/subgraph/cache.go) recomputes the
// hash and hard-fails with "hash mismatch: got X, want Y" on any divergence.
// Those are two independently-produced values: act.PackageHash was computed by
// assignPackageHashes at COMPILE time and persisted, while pkg is projected
// here and now. They agree today, but nothing enforces that they always will —
// any future change to ProjectSubgraphPackage's output would make every stored
// workflow's group activation fail closed on the runner with an error pointing
// at hashes rather than at the real cause. Catching the drift here keeps the
// diagnosis on the server, where the two inputs are both visible.
func (r *EntryActivationReconciler) projectPackageForGroup(ctx context.Context, act *engine.EntryActivation) *graph.SubgraphPackage {
	if r.cfg.WorkflowRegistry == nil {
		return nil
	}
	rec, err := r.cfg.WorkflowRegistry.GetWorkflow(ctx, act.WorkflowID)
	if err != nil {
		if r.cfg.Logger != nil {
			r.cfg.Logger.Warn("entry activation: resolve workflow for group package projection failed",
				"workflow_id", act.WorkflowID, "entry_unit_id", act.EntryUnitID, "err", err)
		}
		return nil
	}
	// Same version-mismatch fail-closed rule as resolveEntrySeedTopology: an
	// empty declared version, or a declared version that disagrees with the
	// registered record, must not be admitted.
	if act.WorkflowVersion == "" || (rec.Version != "" && act.WorkflowVersion != rec.Version) {
		return nil
	}
	if rec.Graph == nil {
		return nil
	}
	unitIdx, ok := entryUnitIndex(rec.Graph, act.EntryUnitID)
	if !ok {
		return nil
	}
	pkg, hash, err := graph.ProjectSubgraphPackage(rec.Graph, unitIdx)
	if err != nil {
		if r.cfg.Logger != nil {
			r.cfg.Logger.Warn("entry activation: project group package failed",
				"workflow_id", act.WorkflowID, "entry_unit_id", act.EntryUnitID, "err", err)
		}
		return nil
	}
	if act.PackageHash != "" && hash != act.PackageHash {
		// Shipping this package would make the runner reject it with a
		// "hash mismatch" that names neither side's provenance. Fail the one
		// activation here instead, with both values recorded.
		if r.cfg.Logger != nil {
			r.cfg.Logger.Error("entry activation: group package hash drift; refusing to ship package",
				"workflow_id", act.WorkflowID, "entry_unit_id", act.EntryUnitID,
				"stored_hash", act.PackageHash, "projected_hash", hash)
		}
		return nil
	}
	return pkg
}

// activationKindFor classifies an activation for runner-side dispatch. Today
// every activation record is a trigger entry unit; the supply collection face
// (pull mode, P3d) will produce ActivationKindSupply records.
func activationKindFor(act *engine.EntryActivation) string {
	if strings.HasPrefix(act.NodeType, "xflow.supply.") {
		return protocol.ActivationKindSupply
	}
	return protocol.ActivationKindTrigger
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
