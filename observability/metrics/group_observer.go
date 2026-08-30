package metrics

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
)

// GroupObserverAdapter adapts a *Metrics registry to engine.GroupObserver.
//
// It exists because every engine observer's first parameter is ctx
// (engine/observers.go's established shape), while GroupMetrics's methods
// (group.go) take none — group.go predates this observer and was written to
// be called directly by a future wiring point, not through the ctx-taking
// observer convention. The two shapes cannot be satisfied structurally, so
// this adapter drops ctx and forwards to the existing GroupMetrics methods.
//
// It does not reimplement the counters/histograms itself: GroupMetrics
// already owns the four Prometheus calls this needs, and duplicating them
// here would create two code paths that could observe the same event
// differently.
type GroupObserverAdapter struct {
	metrics *GroupMetrics
}

// NewGroupObserver builds a GroupObserverAdapter bound to the shared Metrics
// registry.
func NewGroupObserver(m *Metrics) *GroupObserverAdapter {
	return &GroupObserverAdapter{metrics: NewGroupMetrics(m)}
}

// OnGroupLeaseAcquired records one group lease acquisition.
func (a *GroupObserverAdapter) OnGroupLeaseAcquired(_ context.Context) {
	a.metrics.OnGroupLeaseAcquired()
}

// OnGroupLeaseExpired records one group lease expiry (reclaimed or expired).
func (a *GroupObserverAdapter) OnGroupLeaseExpired(_ context.Context) {
	a.metrics.OnGroupLeaseExpired()
}

// OnGroupLeaseRenew fans one renewal attempt out into two series: the
// counter partitioned by result, and the duration histogram. One call in ->
// one call to each out, so the two can never disagree about how many
// attempts happened (see the caller, engine.Engine.RenewGroupLease, for why
// splitting this into two observer methods would reopen that gap).
func (a *GroupObserverAdapter) OnGroupLeaseRenew(_ context.Context, result string, d time.Duration) {
	a.metrics.OnGroupLeaseRenew(result)
	a.metrics.OnGroupLeaseRenewDuration(d)
}

// OnGroupCommit fans one group unit's terminal commit out into two series:
// the counter partitioned by outcome, and the exec-duration histogram.
func (a *GroupObserverAdapter) OnGroupCommit(_ context.Context, outcome string, d time.Duration) {
	a.metrics.OnGroupCommit(outcome)
	a.metrics.OnGroupExecDuration(d)
}

// OnGroupAdmission fans one GROUP entry-unit admission attempt out into two
// series: the counter partitioned by outcome, and the admission-duration
// histogram. Same one-call-in, two-calls-out shape as OnGroupCommit and for
// the same reason: splitting this into two observer methods would reopen the
// gap where the count and the duration histogram could disagree about how
// many admission attempts happened.
func (a *GroupObserverAdapter) OnGroupAdmission(_ context.Context, outcome string, d time.Duration) {
	a.metrics.OnGroupAdmission(outcome)
	a.metrics.OnGroupAdmissionDuration(d)
}

var _ engine.GroupObserver = (*GroupObserverAdapter)(nil)
