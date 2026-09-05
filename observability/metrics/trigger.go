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
	metricTriggerConsumptionBlocked  = "xflow_trigger_consumption_blocked"
	metricTriggerOffsetCommit        = "xflow_trigger_offset_commit_duration_seconds"
	metricTriggerOffsetCommitSize    = "xflow_trigger_offset_commit_size"
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
//
// Split the alert by reason, because two of the three are not the same event.
// schema/schema_fail describe a message that could not be used. buffer_overflow
// describes a usable message the aggregator threw away under load, with the
// commit frontier advancing past its offset — nothing redelivers it, so that
// series is a running total of permanently lost records.
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

// OnConsumptionBlocked records whether a partition has stopped consuming to
// avoid dropping records, under on_overflow=block.
//
// A gauge rather than a counter because the question an operator asks is "is it
// blocked right now", and because the transitions are already in the log. It is
// Set on transitions only, so between two edges the series holds — which is
// what a gauge is for.
//
// partition is a label, and this is the second place in this file that admits
// one. It is not optional here: the state IS per-partition, so one gauge Set
// from every partition would report whichever wrote last, and one blocked
// partition among seventeen healthy ones — the case worth paging on — would be
// erased by the next healthy partition's report. That failure is not
// hypothetical; xflow_wasm_instance_total shipped without an identity label and
// reported 8 while the truth was 16.
//
// Alert on this together with xflow_trigger_messages_discarded_total, not
// instead of it: the two policies fail in opposite directions, and a topic
// shows exactly one of them.
func (t TriggerMetrics) OnConsumptionBlocked(ctx context.Context, topic string, partition int, blocked bool) {
	v := 0.0
	if blocked {
		v = 1.0
	}
	t.Metrics.Set(metricTriggerConsumptionBlocked, withNamespace(ctx, map[string]string{
		"topic": topic, "partition": strconv.Itoa(partition),
	}), v)
}

// OnOffsetCommit records the call that advances the committed offset: how long
// it blocked, and how many offsets it carried.
//
// This is the only metric in this file that measures WAITING rather than work.
// Everything else here counts or sizes something the pipeline did; a profile
// built from those alone accounted for ~111 ms of real work per batch while the
// pipeline released ~1.03 batches per second per partition, and the missing
// 9-35x was this hop. It was previously invisible in a way worth naming:
// xflow_commit_outcomes_total sounds like it covers this and does not — that
// one is the execution layer's commit, carries no topic or partition, and its
// value tracked execution_completed_total{status="success"} exactly.
//
// The duration is NOT a broker round trip, despite the name. Under
// CommitInterval: 0 all partitions of a Reader queue their commits onto one
// channel drained by one goroutine, so this is queueing plus the round trip —
// see OnOffsetCommit's comment in the kafka trigger package for the mechanism
// and the first measurement. An operator reading this as network latency will
// go looking at the brokers, which is the wrong place.
//
// Two series, because duration alone cannot tell which knob matters. The first
// run showed commits completing at 7.12/s process-wide — the serial goroutine's
// ceiling, independent of partition count — while mean size was 165 against a
// max_size of 100, because a partition stuck in the queue keeps buffering.
// Throughput is the product of those two, and only the second is reachable from
// config. Size goes through ObserveCount for the same reason
// xflow_trigger_batch_size does: ObserveBytes' buckets start at 1 KiB and a
// count in the hundreds would land entirely in the first one.
//
// result is on the duration series only. An errored commit's duration is the
// number an operator wants separated — a timeout at aggregateCommitTimeout
// would otherwise drag the success distribution's tail with it — whereas the
// size of a failed commit is the size of the batch that will simply be retried.
func (t TriggerMetrics) OnOffsetCommit(ctx context.Context, topic, result string, messages int, d time.Duration) {
	t.Metrics.Observe(metricTriggerOffsetCommit, withNamespace(ctx, map[string]string{
		"topic": topic, "result": result,
	}), d)
	t.Metrics.ObserveCount(metricTriggerOffsetCommitSize, withNamespace(ctx, map[string]string{
		"topic": topic,
	}), messages)
}

var _ kafkatrigger.Observer = TriggerMetrics{}
