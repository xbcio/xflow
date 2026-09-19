package metrics

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
)

// Outbox dispatch-loop metrics. They exist because the two numbers an operator
// needs to tell "delivery is slow" from "discovery cannot see the backlog" —
// how much ready work is waiting, and what one drain costs — were previously
// not exported at all: the only way to read them was to count Redis keys by
// hand, which is exactly how a dispatch ceiling below the creation rate stayed
// invisible for two releases.
const (
	// metricOutboxReady is the due-now share of the durable ready indexes.
	metricOutboxReady = "xflow_outbox_ready"
	// metricOutboxDrainDiscovered is how many executions one drain discovered.
	metricOutboxDrainDiscovered = "xflow_outbox_drain_discovered"
	// metricOutboxDrainDuration is how long one whole drain took.
	metricOutboxDrainDuration = "xflow_outbox_drain_duration_seconds"
)

// OutboxMetrics implements engine.OutboxDispatchObserver in addition to
// engine.OutboxObserver, so an installed NewOutboxMetrics observer also
// receives the dispatcher loop's own observations.
var _ engine.OutboxDispatchObserver = OutboxMetrics{}

// OnOutboxDrain records one dispatcher pass: how many executions it discovered
// with ready work, and how long the pass took end to end.
//
// Read the pair together. A duration that keeps exceeding the dispatcher's
// interval means a drain cannot finish inside its tick, so the loop runs back
// to back and dispatch is bounded by its own work. A discovered count that
// stays flat — or drops to zero — while xflow_outbox_ready is non-zero means
// the pass is not finding the backlog it is supposed to drain; on a
// keyspace-scanned store that is the page size, which the host can raise (see
// engine.WithOutboxDiscoveryPage).
//
// discovered is a GAUGE, not a counter: it describes the most recent pass, so a
// drop to zero after a backlog clears is the healthy reading, not a lost
// measurement.
func (o OutboxMetrics) OnOutboxDrain(ctx context.Context, discovered int, duration time.Duration) {
	o.Metrics.Set(metricOutboxDrainDiscovered, withNamespace(ctx, nil), float64(discovered))
	o.Metrics.Observe(metricOutboxDrainDuration, withNamespace(ctx, nil), duration)
}

// OnOutboxBacklog records the dispatcher's throttled backlog scan, which is the
// only place the due-now share of the ready indexes is known.
//
// It reports xflow_outbox_ready alone and leaves pending / dead-lettered /
// oldest-age to OutboxPending, which the engine still delivers to every
// observer for compatibility. Splitting them this way is what let the existing
// OnOutboxObserver contract stay untouched: it has no field for Ready, and
// widening it would have broken every implementation of it.
func (o OutboxMetrics) OnOutboxBacklog(ctx context.Context, snapshot engine.OutboxMetricsSnapshot) {
	o.Metrics.Set(metricOutboxReady, withNamespace(ctx, nil), float64(snapshot.Ready))
}
