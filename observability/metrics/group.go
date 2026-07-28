package metrics

import "time"

// GroupMetrics exposes group execution observability counters.
type GroupMetrics struct {
	m *Metrics
}

// NewGroupMetrics creates a GroupMetrics bound to the shared Metrics registry.
func NewGroupMetrics(m *Metrics) *GroupMetrics {
	return &GroupMetrics{m: m}
}

// OnGroupLeaseAcquired increments the group lease acquired counter.
func (g *GroupMetrics) OnGroupLeaseAcquired() {
	g.m.Inc("xflow_group_lease_acquired_total", nil)
}

// OnGroupCommit increments the group commit counter with the outcome label.
func (g *GroupMetrics) OnGroupCommit(outcome string) {
	g.m.Inc("xflow_group_commit_total", map[string]string{"outcome": outcome})
}

// OnGroupLeaseExpired increments the group lease expiry counter.
func (g *GroupMetrics) OnGroupLeaseExpired() {
	g.m.Inc("xflow_group_lease_expired_total", nil)
}

// OnGroupSelectorFallback increments the selector fallback counter.
func (g *GroupMetrics) OnGroupSelectorFallback(mode string) {
	g.m.Inc("xflow_group_selector_fallback_total", map[string]string{"mode": mode})
}

// OnGroupPackageCache increments the package cache counter.
func (g *GroupMetrics) OnGroupPackageCache(result string) {
	g.m.Inc("xflow_group_package_cache_total", map[string]string{"result": result})
}

// OnGroupExecDuration records the group execution duration.
func (g *GroupMetrics) OnGroupExecDuration(d time.Duration) {
	g.m.Observe("xflow_group_exec_duration_seconds", nil, d)
}

// --- Trigger admission metrics ---

// OnGroupAdmission increments the group admission counter with the outcome label.
// Outcome values: accepted, duplicate, conflict, error.
func (g *GroupMetrics) OnGroupAdmission(outcome string) {
	g.m.Inc("xflow_group_admission_total", map[string]string{"outcome": outcome})
}

// OnGroupAdmissionDuration records the duration of a trigger admission attempt.
func (g *GroupMetrics) OnGroupAdmissionDuration(d time.Duration) {
	g.m.Observe("xflow_group_admission_duration_seconds", nil, d)
}

// --- Activation controller metrics ---

// OnGroupActivation increments the group activation counter with the action label.
// Action values: activate, deactivate, revoke, reconcile.
func (g *GroupMetrics) OnGroupActivation(action string) {
	g.m.Inc("xflow_group_activation_total", map[string]string{"action": action})
}

// OnGroupActivationFenced increments the generation-fenced activation counter.
func (g *GroupMetrics) OnGroupActivationFenced() {
	g.m.Inc("xflow_group_activation_generation_fenced_total", nil)
}

// SetGroupActivationActive sets the gauge of currently active group activations.
func (g *GroupMetrics) SetGroupActivationActive(value float64) {
	g.m.Set("xflow_group_activation_active", nil, value)
}

// --- Lease renewal metrics ---

// OnGroupLeaseRenew increments the lease renewal counter with the result label.
// Result values: ok, fenced, error.
func (g *GroupMetrics) OnGroupLeaseRenew(result string) {
	g.m.Inc("xflow_group_lease_renew_total", map[string]string{"result": result})
}

// OnGroupLeaseRenewDuration records the duration of a lease renewal attempt.
func (g *GroupMetrics) OnGroupLeaseRenewDuration(d time.Duration) {
	g.m.Observe("xflow_group_lease_renew_duration_seconds", nil, d)
}

// --- Emit metrics (runner side) ---

// OnGroupEmit increments the group emit counter with the result label.
// Result values: accepted, conflict, error, timeout.
func (g *GroupMetrics) OnGroupEmit(result string) {
	g.m.Inc("xflow_group_emit_total", map[string]string{"result": result})
}

// OnGroupEmitDuration records the duration of an emit operation.
func (g *GroupMetrics) OnGroupEmitDuration(d time.Duration) {
	g.m.Observe("xflow_group_emit_duration_seconds", nil, d)
}

// OnGroupEmitBatchSize records the batch size of an emit operation.
func (g *GroupMetrics) OnGroupEmitBatchSize(size int) {
	g.m.Observe("xflow_group_emit_batch_size", nil, time.Duration(size)*time.Second)
}

// SetGroupEmitInflight sets the gauge of currently in-flight emit operations.
func (g *GroupMetrics) SetGroupEmitInflight(value float64) {
	g.m.Set("xflow_group_emit_inflight", nil, value)
}

// --- Suspend/resume metrics ---

// OnGroupSuspend increments the suspend/resume counter with the action label.
// Action values: suspended, resumed, canceled, timeout.
func (g *GroupMetrics) OnGroupSuspend(action string) {
	g.m.Inc("xflow_group_suspend_total", map[string]string{"action": action})
}

// --- Backpressure metrics ---

// OnGroupBackpressurePaused increments the backpressure paused counter.
func (g *GroupMetrics) OnGroupBackpressurePaused() {
	g.m.Inc("xflow_group_backpressure_paused_total", nil)
}
