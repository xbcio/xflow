package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/node/internal/utils/conv"
)

// MessageSchema defines a simple required-fields schema for Kafka message
// values. A message is valid when its JSON-decoded value is an object that
// contains all RequiredFields as top-level keys with non-nil values.
type MessageSchema struct {
	RequiredFields []string
	// OnInvalid selects what happens to a message that fails validation.
	// Empty means onInvalidDiscard.
	OnInvalid string
	// DeadLetterTopic is the topic invalid messages are republished to when
	// OnInvalid is onInvalidDeadLetter. Required in that mode.
	DeadLetterTopic string
}

// Invalid-message policies. The default is discard because that is the
// pre-existing behaviour, and silently changing a running deployment's data
// path on upgrade would be worse than the gap being closed. What changes for
// existing configs is that a discard is now counted and logged instead of
// invisible.
const (
	// onInvalidDiscard commits the offset without emitting. The message is
	// gone, but the drop is counted (xflow_trigger_messages_discarded_total)
	// and logged at a throttled rate.
	onInvalidDiscard = "discard"
	// onInvalidFail withholds the record from commit and holds its partition:
	// the worker stops on it (deliverInvalid) until the activation is canceled,
	// so nothing behind it is emitted or committed and a later offset can never
	// sweep past it. Use only when invalid messages are expected to be transient
	// (e.g. a producer being rolled back): a permanently malformed message
	// blocks its partition forever, which is the correct choice only if silent
	// data loss is worse than a stall.
	onInvalidFail = "fail"
	// onInvalidDeadLetter republishes the message to DeadLetterTopic and
	// commits only if the republish succeeded. A failed republish holds the
	// partition and retries the same record with the emit-retry cadence rather
	// than dropping it — the whole point of a DLQ is that nothing vanishes.
	onInvalidDeadLetter = "dead_letter"
)

// ---------------------------------------------------------------------------
// Message schema validation
// ---------------------------------------------------------------------------

// validateMessageSchema checks whether a Kafka message's value conforms to
// the declared schema. The value must be valid JSON and contain all required
// fields as top-level keys with non-nil values.
func validateMessageSchema(msg Message, schema *MessageSchema) bool {
	if schema == nil || len(schema.RequiredFields) == 0 {
		return true
	}
	if len(msg.Value) == 0 {
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(msg.Value, &obj); err != nil {
		return false
	}
	for _, field := range schema.RequiredFields {
		if _, ok := obj[field]; !ok {
			return false
		}
	}
	return true
}

// handleInvalidMessage applies the schema's OnInvalid policy to a message
// that failed validation, and reports whether the caller may commit the offset.
//
// Every path here counts the outcome. The pre-existing behaviour committed
// silently, so a producer that started emitting malformed records looked
// identical to an idle topic: no error, no log, no metric, and consumer-group
// lag at zero because the offsets were being committed. That is the failure mode
// this function exists to make visible.
//
// Returning false means "do not commit", which leaves the message for
// redelivery — but only if the caller HOLDS the partition on that answer. A
// caller that instead moves on to the next record lets a later commit sweep
// past this offset, and Kafka replays only from the committed offset, so the
// record is never redelivered in this process. deliverInvalid (per-message) and
// executeBatch's retry (aggregate) are the callers that hold; that is the
// correct fallback for a failed dead-letter publish, because redelivering a
// message forever is recoverable and dropping it is not.
func handleInvalidMessage(ctx context.Context, rt invalidMessageHandler, msg Message) (commit bool) {
	schema := rt.schema()
	policy := schema.OnInvalid
	if policy == "" {
		policy = onInvalidDiscard
	}
	switch policy {
	case onInvalidFail:
		obs().OnMessageDiscarded(ctx, msg.Topic, "schema_fail")
		logInvalidMessage(msg, "schema_fail", "message withheld from commit for redelivery")
		return false
	case onInvalidDeadLetter:
		publisher := rt.deadLetters()
		if publisher == nil {
			// Activation validated the config, so a nil publisher here means the
			// construction seam returned nil without an error. Withhold the
			// commit rather than fall through to a drop.
			obs().OnMessageDeadLettered(ctx, msg.Topic, "error")
			logInvalidMessage(msg, "dead_letter", "dead-letter publisher unavailable; withholding commit")
			return false
		}
		if err := publisher.Publish(ctx, schema.DeadLetterTopic, msg, deadLetterReasonSchema); err != nil {
			obs().OnMessageDeadLettered(ctx, msg.Topic, "error")
			logInvalidMessage(msg, "dead_letter", "dead-letter publish failed: "+err.Error())
			return false
		}
		obs().OnMessageDeadLettered(ctx, msg.Topic, "ok")
		return true
	default:
		obs().OnMessageDiscarded(ctx, msg.Topic, "schema")
		logInvalidMessage(msg, "schema", "message discarded")
		return true
	}
}

// invalidMessageHandler is the narrow view of a runtime that
// handleInvalidMessage needs, so the per-message and aggregate runtimes
// share one policy implementation rather than each growing its own copy.
type invalidMessageHandler interface {
	schema() *MessageSchema
	deadLetters() DeadLetterPublisher
}

// deliverInvalid is the per-message path's disposition for a record that failed
// messageSchema validation, and the counterpart to deliverLegacy: it does not
// return while the record must remain uncommitted.
//
// The dispatch this replaced applied the policy once and then moved on. Under
// on_invalid=fail, and under on_invalid=dead_letter when the republish failed,
// that left this offset uncommitted but returned to the worker loop, so the
// next record's commit advanced the partition past it. Kafka replays only from
// the committed offset, so "not committed" then meant "lost in this process"
// rather than "redelivered" — the opposite of what the policy comments in this
// file promised. Holding the partition is what makes the no-commit decision
// mean redelivery, exactly as deliverLegacy does for the emit axis.
//
// The two policies hold differently, and the difference is the policy:
//
//   - fail: the record must never be committed. The policy is applied once —
//     handleInvalidMessage counts and logs it, and the count must not grow
//     while the worker waits — and then this worker blocks until cancellation.
//     Nothing behind the record is delivered or committed for the life of the
//     activation.
//   - dead_letter: the record is only safe to commit once it is durably on the
//     dead-letter topic. A failed republish (or an absent publisher, which
//     handleInvalidMessage fails closed) is retried for the SAME record with
//     the emit-retry cadence; only a successful publish commits, after which
//     the worker continues with the following offsets.
//
// Both hold paths exit on cancellation without committing anything, and the
// blocked gauge is cleared on every exit: records already queued are left for
// the next generation, exactly as deliverLegacy leaves them.
func (w *partitionWorker) deliverInvalid(ctx context.Context, msg Message) {
	if ctx.Err() != nil {
		return
	}
	rt := w.rt
	policy := rt.messageSchema.OnInvalid
	if policy == "" {
		policy = onInvalidDiscard
	}
	// The first, and for the fail policy the only, application of the policy.
	// A discard or a successful dead-letter publish returns true here, which is
	// the pre-existing commit semantics.
	if handleInvalidMessage(ctx, rt, msg) {
		_ = commitMessages(rt.baseCtx, rt.consumer, msg)
		return
	}
	blocked := false
	reportBlocked := func() {
		if !blocked {
			blocked = true
			obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, true)
		}
	}
	defer func() {
		if blocked {
			// Cleared on EVERY exit path, cancellation included: a gauge left at
			// 1 by a shutdown would report the partition as blocked forever.
			obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, false)
		}
	}()
	reportBlocked()
	if policy != onInvalidDeadLetter {
		// fail. Hold the partition: the offset is never committed, so the next
		// generation redelivers the record, and committing anything behind it
		// here would drop it permanently.
		<-ctx.Done()
		return
	}
	// The dead-letter publish did not succeed. Retry the SAME record;
	// handleInvalidMessage re-applies the policy on each attempt, so a later
	// success takes the ordinary publish-then-commit path.
	for attempt := 1; ; attempt++ {
		if !waitEmitRetry(ctx, attempt) {
			return
		}
		if handleInvalidMessage(ctx, rt, msg) {
			blocked = false
			obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, false)
			_ = commitMessages(rt.baseCtx, rt.consumer, msg)
			return
		}
	}
}

// logInvalidMessage emits a throttled log line. It never logs the message
// value: a malformed record is still production traffic and may carry
// credentials or PII. Topic/partition/offset are enough to fetch the record
// deliberately with a separate tool.
//
// The throttle key includes partition. Callers run one goroutine per
// partition (perMessageRuntime.worker and partitionAggregator both key by
// partitionKey{topic, partition}), so several partitions of one topic can be
// discarding invalid messages concurrently; a topic-only key would let one of
// them win the gate for 30s while the printed partition/offset — the whole
// point of this line — belonged to whichever call happened to get there
// first, same defect as logBatchAdmission.
func logInvalidMessage(msg Message, reason, action string) {
	emit, count := discardLog.allow(time.Now(),
		msg.Topic+"\x00"+reason+"\x00"+strconv.Itoa(msg.Partition))
	if !emit {
		return
	}
	slog.Warn("kafka message failed schema validation",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"reason", reason,
		"action", action,
		"occurrences", count,
	)
}

// messageSchemaFromParams parses the optional message_schema param into a
// MessageSchema. Returns nil when no schema is declared (the common case).
// The param format is:
//
//	{"required_fields": ["field1"], "on_invalid": "discard|fail|dead_letter",
//	 "dead_letter_topic": "events-dlq"}
//
// An unrecognized on_invalid, or dead_letter without a topic, is an error
// rather than a silent fallback to discard: a config that asked not to lose
// messages must never be quietly downgraded to the policy that loses them.
func messageSchemaFromParams(params map[string]any) (*MessageSchema, error) {
	raw, ok := params["message_schema"]
	if !ok || raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, nil
	}
	fields := conv.NonEmptyStringSlice(m["required_fields"])
	if len(fields) == 0 {
		return nil, nil
	}
	schema := &MessageSchema{
		RequiredFields:  fields,
		OnInvalid:       strings.ToLower(strings.TrimSpace(cast.ToString(m["on_invalid"]))),
		DeadLetterTopic: strings.TrimSpace(cast.ToString(m["dead_letter_topic"])),
	}
	if schema.OnInvalid == "" {
		schema.OnInvalid = onInvalidDiscard
	}
	switch schema.OnInvalid {
	case onInvalidDiscard, onInvalidFail:
	case onInvalidDeadLetter:
		if schema.DeadLetterTopic == "" {
			return nil, fmt.Errorf("kafka message_schema on_invalid %q requires dead_letter_topic", schema.OnInvalid)
		}
	default:
		return nil, fmt.Errorf("kafka message_schema on_invalid %q is not supported (supported: %s, %s, %s)",
			schema.OnInvalid, onInvalidDiscard, onInvalidFail, onInvalidDeadLetter)
	}
	return schema, nil
}
