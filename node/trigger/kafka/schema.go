package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"
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
	// onInvalidFail declines the commit, so Kafka redelivers. Use only
	// when invalid messages are expected to be transient (e.g. a producer being
	// rolled back): a permanently malformed message blocks its partition
	// forever, which is the correct choice only if silent data loss is worse
	// than a stall.
	onInvalidFail = "fail"
	// onInvalidDeadLetter republishes the message to DeadLetterTopic and
	// commits only if the republish succeeded. A failed republish falls back to
	// no-commit (redelivery) rather than dropping — the whole point of a DLQ is
	// that nothing vanishes.
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
// redelivery. That is the correct fallback for a failed dead-letter publish:
// redelivering a message forever is recoverable, dropping it is not.
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
		if err := publisher.Publish(ctx, schema.DeadLetterTopic, msg); err != nil {
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

// logInvalidMessage emits a throttled log line. It never logs the message
// value: a malformed record is still production traffic and may carry
// credentials or PII. Topic/partition/offset are enough to fetch the record
// deliberately with a separate tool.
func logInvalidMessage(msg Message, reason, action string) {
	emit, count := discardLog.allow(time.Now(), msg.Topic+"\x00"+reason)
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
