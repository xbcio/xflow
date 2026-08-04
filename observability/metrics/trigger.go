package metrics

import (
	"context"

	xnode "github.com/xbcio/xflow/node"
)

// Trigger metric names.
const (
	metricTriggerMessagesDiscarded   = "xflow_trigger_messages_discarded_total"
	metricTriggerMessagesDeadLetterd = "xflow_trigger_messages_dead_lettered_total"
	metricTriggerBatchFlushed        = "xflow_trigger_batches_flushed_total"
	metricTriggerBatchSize           = "xflow_trigger_batch_size"
	metricTriggerBatchAdmission      = "xflow_trigger_batch_admissions_total"
)

// TriggerMetrics observes trigger message-handling outcomes.
//
// Labels are topic plus a fixed reason/result enum. Topic is bounded by the
// number of configured triggers. Message keys, offsets, and content are
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

// OnBatchAdmission counts control-plane admission responses. Delivery is
// at-least-once by design, so a nonzero conflict/duplicate rate is expected
// rather than alarming — but its MAGNITUDE is the only evidence available for
// deciding whether the duplicate window is worth closing.
func (t TriggerMetrics) OnBatchAdmission(ctx context.Context, topic, state string) {
	t.Metrics.Inc(metricTriggerBatchAdmission, withNamespace(ctx, map[string]string{
		"topic": topic, "state": state,
	}))
}

var _ xnode.TriggerObserver = TriggerMetrics{}
