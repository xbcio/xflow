package metrics

import (
	"context"

	xnode "github.com/xbcio/xflow/node"
)

// Trigger metric names.
const (
	metricTriggerMessagesDiscarded   = "xflow_trigger_messages_discarded_total"
	metricTriggerMessagesDeadLetterd = "xflow_trigger_messages_dead_lettered_total"
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

var _ xnode.TriggerObserver = TriggerMetrics{}
