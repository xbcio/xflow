package metrics

import (
	"context"
	"sync"
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
	// metricOutboxDecodeFailures counts pending outbox entries whose body the
	// backlog scan could not decode. It is registered on the first failure, so a
	// healthy deployment exports no series.
	metricOutboxDecodeFailures = "xflow_outbox_metrics_decode_failures_total"
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
	o.observeDecodeFailures(ctx, snapshot.DecodeFailures)
}

// outboxDecodeSeen remembers, per Metrics and namespace label, the largest
// DecodeFailures the last backlog scan reported. It lives here rather than on
// OutboxMetrics because that type is a value copied freely, so state kept on it
// would not be shared between the copies. The entries are tiny and exist once
// per Metrics a process builds, which is one in production.
var outboxDecodeSeen = struct {
	sync.Mutex
	last map[outboxDecodeKey]int
}{last: map[outboxDecodeKey]int{}}

type outboxDecodeKey struct {
	metrics   *Metrics
	namespace string
}

// observeDecodeFailures counts pending outbox entries the backlog scan could not
// decode, as growth of the scan's own reading rather than as a sum of readings.
//
// DecodeFailures describes the scan, not an event: it is how many bad bodies the
// scan met, and a bad body that stays pending is met again by every later scan.
// Adding each reading to the counter would turn one corrupt entry into a rate
// proportional to the scan frequency, which says nothing about how much is
// wrong. So only the increase over the previous reading is added: a bad entry is
// counted when first seen, a steady reading adds nothing, and a reading that
// falls (entries removed) just lowers the baseline so a later rise counts again.
// The counter therefore answers "did new undecodable entries appear", which is
// what an alert needs; the persistent condition is visible through
// xflow_outbox_pending and xflow_outbox_oldest_pending_age_seconds, which keep
// reporting it. A scan that reports zero adds nothing and registers nothing.
//
// Two edges follow from keeping the baseline in memory. A restart forgets it, so
// entries that are still undecodable are counted once more after the restart.
// And only net growth is seen: if one bad entry leaves and another arrives
// between two scans, the reading is unchanged and nothing is added.
//
// The reading itself is bounded. The scan decodes only the first
// outboxMetricsHeadCap (16) entries at the head of each execution's ready set,
// so an undecodable entry further back is invisible here, and when the head
// reorders (a lease or a retry backoff moves entries) the same bad entry can
// leave and re-enter the window and be counted again.
func (o OutboxMetrics) observeDecodeFailures(ctx context.Context, failures int) {
	if o.Metrics == nil {
		return
	}
	labels := withNamespace(ctx, nil)
	key := outboxDecodeKey{metrics: o.Metrics, namespace: labels["namespace"]}

	outboxDecodeSeen.Lock()
	grew := failures - outboxDecodeSeen.last[key]
	if failures > 0 {
		outboxDecodeSeen.last[key] = failures
	} else {
		delete(outboxDecodeSeen.last, key)
	}
	outboxDecodeSeen.Unlock()

	if grew > 0 {
		o.Metrics.Add(metricOutboxDecodeFailures, labels, float64(grew))
	}
}
