package metrics

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xbcio/xflow/engine"
	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/types"
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
	metricTriggerFireDropped         = "xflow_trigger_fire_dropped_total"
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
// series is a running total of permanently lost records. It is the series the
// DEFAULT policy produces: a topic configured with on_overflow=block reports
// into xflow_trigger_consumption_blocked instead, and one configured with
// on_overflow=dead_letter reports into xflow_trigger_messages_dead_lettered_total,
// so this counter keeps meaning "gone" on every topic.
func (t TriggerMetrics) OnMessageDiscarded(ctx context.Context, topic, reason string) {
	t.Metrics.Inc(metricTriggerMessagesDiscarded, withNamespace(ctx, map[string]string{
		"topic": topic, "reason": reason,
	}))
}

// OnMessageDeadLettered counts a dead-letter publish attempt. result is "ok" or
// "error". An "error" rate means records are being redelivered rather than
// parked, so the source partition is stalled — that is the more urgent of the
// two.
//
// Both axes report here. Which one it was is not a label: the axis is already
// carried by the published record's xflow-dlq-reason header, and adding it to
// the label set would double every series to say something the DLQ itself
// records. On the overflow axis an "error" is a partition that has stopped
// consuming and is retrying a dead-letter write, which is the same operator
// response as the schema axis: go look at the dead-letter broker.
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
// state="refused" is not an error at all: the workflow's own concurrency policy
// declined the batch, nothing was persisted, and the offsets are held back and
// retried. It is its own state rather than another error reason because it is
// the only one of these that a correctly-configured deployment is SUPPOSED to
// produce, so its rate must be readable without looking like an outage. Its
// reason narrows which wait is holding the batch back — concurrency_limit (a
// slot) versus queue_full (the queue-mode backlog at max_queued).
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
// avoid dropping records. Five trigger conditions set it: on_overflow=block
// sitting at its retained bound; on_overflow=dead_letter while a record's
// republish is failing; a per-record entry seed whose workflow's concurrency
// policy refused it; a legacy emit that failed and is being retried
// (emit_retry, including its bounded phase's dead-letter publish); and a
// schema-invalid record under message_schema's on_invalid=fail, or while its
// on_invalid=dead_letter republish keeps failing. The last three retry the
// SAME record with backoff — deliverInvalid and deliverLegacy hold the
// per-message worker — so no later offset can commit past it.
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
// The duration is NOT a plain broker round trip, despite the name. A commit
// goes through the trigger's commit coalescer, so this covers the wait for the
// merged call currently in flight plus the one this caller joins — about 1.5x
// the trip on the cluster this was measured against (179 ms against 118 ms).
// Before that coalescer existed it was far worse: kafka-go serialized every
// partition of a Reader onto one commit goroutine, so the figure was dominated
// by queueing and its mean was 2.24 s. See OnOffsetCommit's comment in the
// kafka trigger package for the mechanism and the measurements. Either way, an
// operator reading this as network latency will go looking at the brokers,
// which is the wrong place: a p50 in the seconds means callers are backing up,
// not that the broker is sick.
//
// Two series, because duration alone cannot tell which knob matters. Size is
// the other half: a mean well above the configured max_size means partitions
// are buffering while they wait here, which is the shape the pre-coalescer
// pipeline showed (mean 165 against max_size 100 while commits completed at
// 7.12/s process-wide). Size goes through ObserveCount for the same reason
// xflow_trigger_batch_size does: ObserveBytes' buckets start at 1 KiB and a
// count in the hundreds would land entirely in the first one.
//
// One caveat specific to coalescing, because it inflates the error series: a
// merged call carries several partitions, and kafka-go returns a single error
// if ANY of them is rejected, discarding which. So one bad partition reports as
// an error for every caller it was batched with. Read a nonzero error rate as
// "at least one partition is failing", not as a count of failing partitions.
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

var _ types.TriggerFireObserver = TriggerMetrics{}

// OnTriggerFireDropped counts a firing a time-based trigger decided not to
// admit: a cron firing or a timer tick that produced no execution, partitioned
// by reason (refused/overlap/transport/emit_error, the
// types.TriggerFireDropped* vocabulary).
//
// The series exists because a dropped firing is invisible everywhere else. The
// trigger's watermark records that the instant passed, not that anything was
// running, and a workflow that never ran leaves no execution to look at — so
// without this counter a schedule that is throttled by its own workflow reads
// exactly like a schedule that never fired, which is the same blind spot
// xflow_trigger_messages_discarded_total closes for messages.
//
// err is classified, never labeled: the triggers report the failure they got, and
// this is the layer that can name the engine's sentinels — node/ cannot import
// engine at all (engine's own tests import node, so the import back would close a
// cycle), which is why the narrowing lives here rather than where the drop
// happened. The label set stays closed: the error's text is unbounded and
// belongs in the trigger's log line.
//
// Read the reasons apart. refused is the workflow's OWN limit declining the
// firing, on either hosting path (the control plane's admission response, or the
// engine's concurrency limit as narrowed below): the schedule is fine and the
// workflow is busy, so the fix is capacity or a queue mode, and a rate that
// tracks a workload's peak is expected. overlap is the cron trigger's own policy
// holding a firing back while the run it started last is still going, which is
// the parameter doing its job rather than a fault. transport and emit_error are
// the trigger failing to get its work in at all (unreachable control plane, an
// engine that would not take the submission): those are the two that page, and
// any sustained rate of either is lost work.
//
// The label set is bounded on purpose: the workflow and the node name are
// definition data, they are not labels here, and the trigger's own Warn line
// carries them.
func (t TriggerMetrics) OnTriggerFireDropped(ctx context.Context, reason string, err error) {
	t.Metrics.Inc(metricTriggerFireDropped, withNamespace(ctx, map[string]string{
		"reason": fireDropReason(reason, err),
	}))
}

// fireDropReason narrows the one reason a trigger cannot narrow itself.
//
// A trigger reports the concurrency limit as emit_error, because the sentinel
// that identifies it lives in package engine and no production code under node/
// may import that package. The two cases it merges are not the same event for an
// operator — the concurrency limit is the workflow throttling its own schedule
// and is expected under load, while anything else the engine rejects is a broken
// submission — so they are split here, at the layer that owns the engine import.
//
// The comparison is on the sentinel, never on the message: the text is for the
// log line, and it changes.
func fireDropReason(reason string, err error) string {
	if reason == types.TriggerFireDroppedEmitError && errors.Is(err, engine.ErrConcurrencyLimit) {
		return types.TriggerFireDroppedRefused
	}
	return reason
}
