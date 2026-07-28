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
