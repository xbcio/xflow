package metrics

import (
	"context"
	"strconv"
	"time"

	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
)

// Trigger metric names.
const (
	metricTriggerMessagesDiscarded   = "xflow_trigger_messages_discarded_total"
	metricTriggerMessagesDeadLetterd = "xflow_trigger_messages_dead_lettered_total"
	metricTriggerBatchFlushed        = "xflow_trigger_batches_flushed_total"
	metricTriggerBatchFlushOutcome   = "xflow_trigger_batch_flush_outcomes_total"
	metricTriggerBatchSize           = "xflow_trigger_batch_size"
	metricTriggerBatchAdmission      = "xflow_trigger_batch_admissions_total"
	metricTriggerConsumerLag         = "xflow_trigger_consumer_lag"
	metricTriggerLastFetchTimestamp  = "xflow_trigger_last_fetch_timestamp_seconds"
)

// TriggerMetrics observes trigger message-handling outcomes.
//
// Labels are topic plus a fixed reason/result enum, and — on the consumer-lag
// gauges only — partition. Topic is bounded by the number of configured
// triggers, partition by the assignment. Message keys, offsets, and content are
// deliberately absent: the first two are unbounded and the third is not an
// operator's to read from a metric.
type TriggerMetrics struct {
	Metrics *Metrics
}

func NewTriggerMetrics(m *Metrics) TriggerMetrics { return TriggerMetrics{Metrics: m} }

// OnMessageDiscarded counts a message that was consumed but never emitted.
//
// This is the counter that makes an invisible failure visible: a producer that
// starts emitting records missing a required field previously looked exactly
// like an idle topic, because the offsets were still being committed and so
// consumer-group lag stayed at zero. Alert on any nonzero rate here.
func (t TriggerMetrics) OnMessageDiscarded(ctx context.Context, topic, reason string) {
	t.Metrics.Inc(metricTriggerMessagesDiscarded, withNamespace(ctx, map[string]string{
		"topic": topic, "reason": reason,
	}))
}

// OnMessageDeadLettered counts a dead-letter publish attempt. result is "ok" or
// "error". An "error" rate means invalid messages are being redelivered rather
// than parked, so the source partition is stalled — that is the more urgent of
// the two.
func (t TriggerMetrics) OnMessageDeadLettered(ctx context.Context, topic, result string) {
	t.Metrics.Inc(metricTriggerMessagesDeadLetterd, withNamespace(ctx, map[string]string{
		"topic": topic, "result": result,
	}))
}

// OnBatchFlushed counts batches by what caused the flush, and observes the
// batch size distribution. A flush mix dominated by "timeout" means the size
// threshold is never reached — the batch is configured larger than the traffic.
//
// This counts ATTEMPTS, so dividing the "error" outcomes below by it gives the
// retry rate. Before that split the histogram only saw successes, which made a
// partition wedged at its buffer cap indistinguishable from a quiet one.
//
// Size goes through ObserveCount, not ObserveBytes: the unit is records, and
// ObserveBytes' buckets start at 1 KiB, so every batch bounded by max_size=100
// would land in one bucket and the distribution would be unreadable.
func (t TriggerMetrics) OnBatchFlushed(ctx context.Context, topic, trigger string, size int) {
	t.Metrics.Inc(metricTriggerBatchFlushed, withNamespace(ctx, map[string]string{
		"topic": topic, "trigger": trigger,
	}))
	t.Metrics.ObserveCount(metricTriggerBatchSize, withNamespace(ctx, map[string]string{
		"topic": topic,
	}), size)
}

// OnBatchFlushOutcome counts how those attempts ended. A sustained "error" rate
// means the partition is re-flushing the same buffer without committing, which
// consumes downstream capacity while consumed-vs-produced stays flat — the
// failure mode that reads as "the pipeline is merely slow".
func (t TriggerMetrics) OnBatchFlushOutcome(ctx context.Context, topic, trigger, result string) {
	t.Metrics.Inc(metricTriggerBatchFlushOutcome, withNamespace(ctx, map[string]string{
		"topic": topic, "trigger": trigger, "result": result,
	}))
}

// OnBatchAdmission counts control-plane admission responses. Delivery is
// at-least-once by design, so a nonzero conflict/duplicate rate is expected
// rather than alarming — but its MAGNITUDE is the only evidence available for
// deciding whether the duplicate window is worth closing.
//
// reason narrows state="error", which by itself conflated four failures that
// call for four different responses: the group never ran (execute_group), it
// ran and failed (group_outcome), it ran and succeeded but the admission round
// trip failed and threw that work away (seed_transport), or this runner and
// the control plane disagree about the protocol (unknown_state). Only the last
// three redeliver, and only one of them wastes completed downstream work per
// occurrence.
//
// Both labels are closed enums built inside the kafka trigger package. Neither
// is derived from a message, a hash, or a runtime's free-text error — the
// cause text belongs in the log, where unbounded values are safe.
func (t TriggerMetrics) OnBatchAdmission(ctx context.Context, topic, state, reason string) {
	t.Metrics.Inc(metricTriggerBatchAdmission, withNamespace(ctx, map[string]string{
		"topic": topic, "state": state, "reason": reason,
	}))
}

// OnConsumerLag records how far a partition's fetch position sits behind the
// broker's high-water mark, plus when that reading was taken.
//
// Two series, not one. Lag alone is a trap: it only advances when a message is
// fetched, so a consumer that has STOPPED fetching holds its last value
// forever, and a stalled consumer reads as a healthy one — the exact failure
// this metric exists to catch. xflow_trigger_last_fetch_timestamp_seconds makes
// the freeze visible: alert on time() - last_fetch_timestamp, and treat a lag
// figure as meaningless whenever that is large.
//
// partition is a label here and nowhere else in this file. Lag is a
// per-partition quantity, so setting one gauge from every partition would
// report whichever partition wrote last, and a single caught-up partition would
// mask a stalled one. Cardinality stays bounded by the assignment — tens of
// series per topic, and nothing a producer can inflate.
func (t TriggerMetrics) OnConsumerLag(ctx context.Context, topic string, partition int, lag int64, fetchedAt time.Time) {
	labels := withNamespace(ctx, map[string]string{
		"topic": topic, "partition": strconv.Itoa(partition),
	})
	t.Metrics.Set(metricTriggerConsumerLag, labels, float64(lag))
	// Seconds plus a fractional part, not UnixNano()/1e9: a nanosecond count of
	// the current era is past 2^53 and no longer exactly representable as a
	// float64, so that form quantizes the reading to a couple of hundred
	// nanoseconds' error at an arbitrary offset. Splitting the sum keeps the
	// whole seconds exact, which is the part an alert on time()-this compares.
	t.Metrics.Set(metricTriggerLastFetchTimestamp, labels,
		float64(fetchedAt.Unix())+float64(fetchedAt.Nanosecond())/1e9)
}

var _ kafkatrigger.Observer = TriggerMetrics{}
