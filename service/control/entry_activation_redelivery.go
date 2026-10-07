package control

import (
	"context"

	"github.com/xbcio/xflow/engine"
)

// This file reconciles the assignment ledger against what runners report they
// are actually hosting. It closes the one gap the lease cannot see: a runner
// whose heartbeat is healthy and whose selector still matches is renewed
// indefinitely (reconcileExisting's healthy branch), even if the Activate
// directive that assignment depends on never reached it. Nothing about the
// runner's liveness distinguishes "hosting the assignment" from "hosting
// nothing", and the only party that knows the difference is the runner — hence
// the hosted-activation report.
//
// Both directions are deliberately narrow. They only run when the directory
// offers the ActivationDeliveryDirectory capability AND the runner reports a
// fresh, session-matching report; a runner that does not report (an old
// binary) is skipped entirely, which keeps this a pure addition on top of the
// existing lease-based reconciliation.

// hostedActivationsView is one reconcile pass's cached read of every live
// runner's most recent hosted-activation report. The pass reads each runner
// once, before any per-activation work, so the cost of the report never scales
// with the activation count.
type hostedActivationsView struct {
	// reports holds the runners whose report was readable and fresh, keyed by
	// runner ID.
	reports map[string]hostedRunnerActivations
}

// hostedRunnerActivations is one runner's report, indexed for lookup by
// activation identity.
type hostedRunnerActivations struct {
	// session is the session that sent the report. Every consumer compares it
	// against the session it expects before trusting the index.
	session string
	// index maps namespace-free activation identity to the highest generation
	// the runner reported hosting for it.
	index map[deactivationInventoryKey]uint64
}

func (v hostedActivationsView) reportFor(runnerID string) (hostedRunnerActivations, bool) {
	report, ok := v.reports[runnerID]
	return report, ok
}

// loadHostedActivations reads every live runner's report once. A read error is
// logged and treated as "no report" for that runner: the report is an input to
// recovery, never an authority, so a storage fault must degrade to the
// pre-existing lease-only behavior rather than fail the pass (which would
// stop assignments and renewals too).
func (r *EntryActivationReconciler) loadHostedActivations(ctx context.Context, live []RunnerSnapshot) hostedActivationsView {
	view := hostedActivationsView{reports: make(map[string]hostedRunnerActivations)}
	if r.cfg.Delivery == nil {
		return view
	}
	for _, snap := range live {
		report, ok, err := r.cfg.Delivery.HostedActivations(ctx, snap.RunnerID)
		if err != nil {
			if r.cfg.Logger != nil {
				r.cfg.Logger.Warn("entry activation: read hosted activations failed",
					"runner_id", snap.RunnerID,
					"err", err)
			}
			continue
		}
		if !ok || report.SessionID == "" {
			continue
		}
		index := make(map[deactivationInventoryKey]uint64, len(report.Items))
		for _, item := range report.Items {
			key := deactivationInventoryKeyFromItem(item)
			// One identity can appear twice only in a malformed report; keep
			// the highest generation so a duplicate can never make the runner
			// look like it hosts less than it says it does.
			if existing, seen := index[key]; !seen || item.Generation > existing {
				index[key] = item.Generation
			}
		}
		view.reports[snap.RunnerID] = hostedRunnerActivations{session: report.SessionID, index: index}
	}
	return view
}

// deactivationInventoryKeyFromActivation renders a ledger record as the
// namespace-free identity a runner report can carry. Namespace is deliberately
// absent on the wire (see deactivationInventoryKey), so the lookup is by the
// four components both sides share.
func deactivationInventoryKeyFromActivation(act *engine.EntryActivation) deactivationInventoryKey {
	return deactivationInventoryKey{
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		ReplicaIndex:    act.ReplicaIndex,
	}
}

// redeliverMissingActivation re-sends the Activate for an activation the
// ledger assigns to a live owner whose own report does not show it hosted at
// the current generation. It is called from the healthy branch of
// reconcileExisting — the branch that renews the owner, which is exactly the
// state a lost directive produces.
//
// What it must NOT do, and does not: fence, advance the generation, clear the
// owner, or mark the activation revivable. A redelivery is a repair of the
// delivery path, not a change of ownership — the assignment is already
// correct, only the runner has not heard about it. Resending at the store's
// CURRENT generation is also load-bearing in the other direction: the runner's
// activateLocked treats a same-generation Activate as an idempotent no-op and
// a higher one as an upgrade, but has no fence against a LOWER generation — it
// would tear down the newer subscription and install the older one. That is
// why the generation comes from a fresh store read, never from arithmetic on
// the pass's snapshot.
func (r *EntryActivationReconciler) redeliverMissingActivation(ctx context.Context, act *engine.EntryActivation, hosted hostedActivationsView) {
	if r.cfg.Delivery == nil || act.SessionID == "" {
		return
	}
	report, ok := hosted.reportFor(act.RunnerID)
	// A report from a different session cannot speak for this assignment: it
	// was written before the reconnect that produced the recorded session, so
	// its "hosts nothing" may simply be the state the reconnect wiped.
	if !ok || report.session != act.SessionID {
		return
	}
	identity := deactivationInventoryKeyFromActivation(act)
	if generation, reported := report.index[identity]; reported && generation >= act.Generation {
		// Hosted at the current (or a newer) generation — nothing to repair.
		return
	}

	// Re-read the store before sending anything: the pass's snapshot may be
	// stale relative to a concurrent fence/reassign, and a directive derived
	// from a stale owner could either bounce off the new owner's session key
	// or, worse, carry an old generation. The fresh record is also what the
	// directive is built from, so params/package reflect current desired state.
	key := keyOf(act)
	current, found, err := r.cfg.Store.Get(ctx, key)
	if err != nil {
		if r.cfg.Logger != nil {
			r.cfg.Logger.Warn("entry activation: redelivery re-read failed",
				"workflow_id", act.WorkflowID,
				"entry_unit_id", act.EntryUnitID,
				"replica_index", act.ReplicaIndex,
				"runner_id", act.RunnerID,
				"err", err)
		}
		return
	}
	if !found || !current.Desired || current.RunnerID != act.RunnerID || current.SessionID != act.SessionID {
		return
	}
	// Re-check against the freshly read generation: between the pass snapshot
	// and now the assignment may have advanced, in which case the report may
	// already cover it.
	if generation, reported := report.index[identity]; reported && generation >= current.Generation {
		return
	}

	r.deliverActivationDirective(ctx, current.RunnerID, current.SessionID, r.activateDirectiveFor(ctx, &current, current.Generation))
	r.recordGroupActivationRedelivered(&current, "activate")
	if r.cfg.Logger != nil {
		reported, _ := report.index[identity]
		r.cfg.Logger.Warn("entry activation: redelivered activate directive the runner did not report hosting",
			"workflow_id", current.WorkflowID,
			"workflow_version", current.WorkflowVersion,
			"namespace", current.Namespace,
			"entry_unit_id", current.EntryUnitID,
			"replica_index", current.ReplicaIndex,
			"runner_id", current.RunnerID,
			"generation", current.Generation,
			"reported_generation", reported)
	}
}

// cleanupForeignHostedActivations is the conservative reverse direction: a
// runner reports hosting an activation the ledger does not assign to it (its
// owner is empty after a fence, or a different runner altogether), so it is
// told to stop. Without this a fenced subscription could keep consuming from a
// runner that no longer owns it, invisibly to a ledger that has already moved
// on.
//
// Deliberately narrow, per the "prefer less over wrong" rule for this
// direction:
//   - the deactivate carries the LEDGER's current generation, reusing
//     deactivateDirectiveFor exactly as fenceAndDeactivate does — there is no
//     generation arithmetic here. If the report shows a NEWER generation than
//     the ledger, the runner's own deactivateLocked will refuse the stop (its
//     delayed-stop fence), and that refusal is the correct outcome: a newer
//     generation is not this ledger record's to clean up.
//   - an activation missing from the ledger is skipped by construction (it is
//     never visited), and a report whose identity is absent from the ledger is
//     likewise left alone. There is no store write here at all: the reconciler
//     never fences, reassigns, or revives on behalf of this check.
func (r *EntryActivationReconciler) cleanupForeignHostedActivations(ctx context.Context, act *engine.EntryActivation, live []RunnerSnapshot, hosted hostedActivationsView) {
	if r.cfg.Delivery == nil {
		return
	}
	identity := deactivationInventoryKeyFromActivation(act)
	for _, snap := range live {
		report, ok := hosted.reportFor(snap.RunnerID)
		if !ok || snap.SessionID == "" || report.session != snap.SessionID {
			// Only the runner's CURRENT session can receive a directive for
			// it; an older session's report is stale evidence and its key is
			// no longer drained.
			continue
		}
		if _, reported := report.index[identity]; !reported {
			continue
		}
		if snap.RunnerID == act.RunnerID && (act.SessionID == "" || act.SessionID == report.session) {
			// The ledger owner is the reporter: normal state, nothing to
			// repair. The session comparison tolerates a ledger record that
			// predates session tracking (empty) so a legacy row cannot cause a
			// stop for a runner that is in fact its owner.
			continue
		}
		r.deliverDeactivationDirective(ctx, snap.RunnerID, snap.SessionID, deactivateDirectiveFor(act, act.Generation))
		r.recordGroupActivationRedelivered(act, "deactivate")
		if r.cfg.Logger != nil {
			reported, _ := report.index[identity]
			r.cfg.Logger.Warn("entry activation: runner reports hosting an activation the ledger does not assign to it; sent deactivate",
				"workflow_id", act.WorkflowID,
				"workflow_version", act.WorkflowVersion,
				"namespace", act.Namespace,
				"entry_unit_id", act.EntryUnitID,
				"replica_index", act.ReplicaIndex,
				"runner_id", snap.RunnerID,
				"ledger_owner", act.RunnerID,
				"generation", act.Generation,
				"reported_generation", reported)
		}
	}
}

// recordGroupActivationRedelivered reports a redelivery to cfg.Metrics, but
// ONLY for GROUP entry units, mirroring recordGroupActivation.
func (r *EntryActivationReconciler) recordGroupActivationRedelivered(act *engine.EntryActivation, action string) {
	if r.cfg.Metrics == nil || act.NodeType != engine.GroupNodeType {
		return
	}
	r.cfg.Metrics.OnGroupActivationRedelivered(action)
}
