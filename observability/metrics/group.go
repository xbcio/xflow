package metrics

import (
	"time"

	"github.com/xbcio/xflow/namespace"
)

// GroupMetrics exposes group execution observability counters.
type GroupMetrics struct {
	m *Metrics
}

// NewGroupMetrics creates a GroupMetrics bound to the shared Metrics registry.
func NewGroupMetrics(m *Metrics) *GroupMetrics {
	return &GroupMetrics{m: m}
}

// The lease, commit, admission and renewal families below take the namespace as
// a parameter and always emit the namespace label, so every emitter of one
// family shares one label set (the registry drops a registration whose label set
// conflicts with an earlier one). GroupObserverAdapter supplies it from the
// hook's ctx; summing over it recovers the deployment-wide total.

// OnGroupLeaseAcquired increments the group lease acquired counter.
func (g *GroupMetrics) OnGroupLeaseAcquired(ns namespace.Namespace) {
	g.m.Inc("xflow_group_lease_acquired_total", map[string]string{"namespace": string(ns)})
}

// OnGroupCommit increments the group commit counter with the outcome label.
func (g *GroupMetrics) OnGroupCommit(ns namespace.Namespace, outcome string) {
	g.m.Inc("xflow_group_commit_total", map[string]string{"namespace": string(ns), "outcome": outcome})
}

// OnGroupLeaseExpired increments the group lease expiry counter.
func (g *GroupMetrics) OnGroupLeaseExpired(ns namespace.Namespace) {
	g.m.Inc("xflow_group_lease_expired_total", map[string]string{"namespace": string(ns)})
}

// OnGroupSelectorFallback increments the selector fallback counter. The
// caller (service/control/entry_activation_reconciler.go's assignUnowned)
// calls this only once fallbackChooseRunner has returned ok == true — a
// "default"-mode activation's grace window elapsed and it was assigned to a
// runner whose labels do not match — never on the grace-window-pending calls
// that precede a success, which return ok == false and must not be counted.
//
// ns is the activation's own namespace, passed explicitly rather than read from
// a context: the reconciler walks several namespaces in one pass, so no single
// context namespace describes the activation being reported.
//
// No "mode" label: the only production call site is gated by
// selectorIsDefault(act.Selector), and the "required" branch returns before
// ever reaching fallbackChooseRunner — types.RunnerSelectorMode has only
// "default" and "required", so fallback is unreachable in required mode. A
// mode label on this counter would therefore carry exactly one distinct
// value ("default", or "" for a legacy record that selectorIsDefault also
// treats as default) — not a label, just overhead.
func (g *GroupMetrics) OnGroupSelectorFallback(ns namespace.Namespace) {
	g.m.Inc("xflow_group_selector_fallback_total", map[string]string{"namespace": string(ns)})
}

// OnGroupPackageCache increments the package cache counter.
func (g *GroupMetrics) OnGroupPackageCache(result string) {
	g.m.Inc("xflow_group_package_cache_total", map[string]string{"result": result})
}

// OnGroupExecDuration records the group execution duration.
func (g *GroupMetrics) OnGroupExecDuration(ns namespace.Namespace, d time.Duration) {
	g.m.Observe("xflow_group_exec_duration_seconds", map[string]string{"namespace": string(ns)}, d)
}

// --- Trigger admission metrics ---

// OnGroupAdmission increments the group admission counter with the outcome label.
// Outcome values: accepted, duplicate, conflict, error.
func (g *GroupMetrics) OnGroupAdmission(ns namespace.Namespace, outcome string) {
	g.m.Inc("xflow_group_admission_total", map[string]string{"namespace": string(ns), "outcome": outcome})
}

// OnGroupAdmissionDuration records the duration of a trigger admission attempt.
func (g *GroupMetrics) OnGroupAdmissionDuration(ns namespace.Namespace, d time.Duration) {
	g.m.Observe("xflow_group_admission_duration_seconds", map[string]string{"namespace": string(ns)}, d)
}

// --- Activation controller metrics ---

// OnGroupActivation increments the group activation counter with the action label.
// Action values: activate, deactivate.
//
// Never "reconcile": Reconcile() is the ticker-driven entry point for a whole
// sweep, not a per-activation action, so nothing produces it as a label value.
//
// Never "revoke": protocol.DeactivateDirective (service/protocol/activation.go)
// carries only Namespace/WorkflowID/EntryUnitID/Generation — there is no reason
// field — and enqueueDeactivate (service/control/entry_activation_reconciler.go)
// does not add one either. Every stop the reconciler produces, whatever caused
// it (no-longer-desired, expired lease, stale owner, dead runner, inventory
// mismatch), collapses to the same directive, so there is no signal left to
// distinguish a "revoke" from a plain "deactivate".
//
// ns is the activation's own namespace (see OnGroupSelectorFallback for why it
// is a parameter): summing over it recovers the deployment-wide total.
func (g *GroupMetrics) OnGroupActivation(ns namespace.Namespace, action string) {
	g.m.Inc("xflow_group_activation_total", map[string]string{"namespace": string(ns), "action": action})
}

// OnGroupActivationFenced increments the generation-fenced activation counter
// for the activation's namespace.
func (g *GroupMetrics) OnGroupActivationFenced(ns namespace.Namespace) {
	g.m.Inc("xflow_group_activation_generation_fenced_total", map[string]string{"namespace": string(ns)})
}

// OnGroupActivationRedelivered increments the redelivered directive counter
// with the action label. Action values: activate, deactivate.
//
// It counts directives RE-SENT because the runner's hosted-activation report
// disagreed with the assignment ledger — the observable form of a directive
// that was lost and has now been recovered. The caller
// (entry_activation_reconciler.go) reports only GROUP entry units, mirroring
// the other xflow_group_* series.
func (g *GroupMetrics) OnGroupActivationRedelivered(ns namespace.Namespace, action string) {
	g.m.Inc("xflow_group_activation_redelivered_total", map[string]string{"namespace": string(ns), "action": action})
}

// SetGroupActivationActive sets the gauge of currently active group activations
// in one namespace. The deployment-wide figure is the PromQL sum over the
// namespace label. After a leader failover the previous leader's last value for
// a namespace lingers until its series goes stale, so aggregate with
// max by (namespace) before summing across namespaces if both leaders are
// scraped.
func (g *GroupMetrics) SetGroupActivationActive(ns namespace.Namespace, value float64) {
	g.m.Set("xflow_group_activation_active", map[string]string{"namespace": string(ns)}, value)
}

// --- Lease renewal metrics ---

// OnGroupLeaseRenew increments the lease renewal counter with the result label.
// Result values: ok, not_renewed, error.
//
// Never "fenced": the GroupStateStore contract returns a bare (bool, error), so
// a real fence loss, an already-terminal unit, and a vanished lease all arrive
// as the same (false, nil). The producer (engine.RenewGroupLease) names that
// ambiguity "not_renewed" rather than inventing a distinction the backend never
// gave it.
func (g *GroupMetrics) OnGroupLeaseRenew(ns namespace.Namespace, result string) {
	g.m.Inc("xflow_group_lease_renew_total", map[string]string{"namespace": string(ns), "result": result})
}

// OnGroupLeaseRenewDuration records the duration of a lease renewal attempt.
func (g *GroupMetrics) OnGroupLeaseRenewDuration(ns namespace.Namespace, d time.Duration) {
	g.m.Observe("xflow_group_lease_renew_duration_seconds", map[string]string{"namespace": string(ns)}, d)
}
