package control

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// DefaultEntryActivationLeaseTTL is the assignment lease duration used when the
// reconciler config leaves LeaseTTL unset.
const DefaultEntryActivationLeaseTTL = 60 * time.Second

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
	// LeaseTTL is how long an assignment is valid before it must be renewed or
	// is treated as expired. Defaults to DefaultEntryActivationLeaseTTL.
	LeaseTTL time.Duration
	// Logger is optional.
	Logger engine.Logger
}

// EntryActivationReconciler drives desired EntryActivation records toward a
// live runner assignment. For each desired-but-unassigned activation it assigns
// a capable, selector-matching live runner a fresh generation; activations
// whose lease has expired (or whose owner is no longer live) are fenced and
// reassigned. It is fail-closed: a runner that does not satisfy a required
// selector is never assigned.
type EntryActivationReconciler struct {
	cfg      EntryActivationReconcilerConfig
	selector RunnerSelector
}

// NewEntryActivationReconciler constructs a reconciler with defaults applied.
func NewEntryActivationReconciler(cfg EntryActivationReconcilerConfig) *EntryActivationReconciler {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultEntryActivationLeaseTTL
	}
	if len(cfg.Namespaces) == 0 {
		cfg.Namespaces = []namespace.Namespace{namespace.Default}
	}
	sel := DefaultRunnerSelector()
	if cfg.Selector != nil {
		sel = *cfg.Selector
	}
	return &EntryActivationReconciler{cfg: cfg, selector: sel}
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
	// stale owner and leave it unassigned.
	if !act.Desired {
		if act.RunnerID != "" {
			return r.cfg.Store.Fence(ctx, key, act.Generation)
		}
		return nil
	}

	// Already assigned: keep it unless the lease expired or the owner is no
	// longer live. Otherwise fence the old generation before reassigning so the
	// stale runner can never keep driving the entry unit.
	if act.RunnerID != "" {
		expired := !act.LeaseDeadline.IsZero() && act.LeaseDeadline.Before(now)
		ownerLive := r.runnerIsLive(act.RunnerID, live, now)
		if !expired && ownerLive {
			return nil
		}
		if err := r.cfg.Store.Fence(ctx, key, act.Generation); err != nil {
			return err
		}
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
	if _, err := r.cfg.Store.Assign(ctx, key, chosen.RunnerID, "", nextGen, deadline); err != nil {
		return err
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
		return snap, true
	}
	return RunnerSnapshot{}, false
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
