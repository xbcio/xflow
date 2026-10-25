// Package kafka implements the xflow.trigger.kafka trigger: it consumes a Kafka
// topic through a consumer group and emits either one TriggerEvent per message
// or one per aggregated batch, committing offsets only after the downstream
// side effect succeeded.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/node/internal/utils/conv"

	"github.com/xbcio/xflow/types"
)

// defaultTriggerMaxInflight bounds this trigger's concurrent in-flight work.
// The redis trigger declares its own constant of the same value; the two are
// independent per-trigger backpressure windows that happen to coincide, NOT a
// contract that must stay in sync. Change one without changing the other.
const defaultTriggerMaxInflight = 64

type Consumer interface {
	Messages() <-chan Message
	Close() error
}

// consumerSubscription is the subscription handle both Kafka activation shapes
// return: the Close surface every trigger provides, plus the optional health
// surface declared by types.SubscriptionHealth. done closes when the reader
// goroutine exits for any reason (Close, parent cancellation, or the consumer's
// message stream closing), which is what lets the runner's activation tracker
// notice a subscription that died on its own instead of continuing to report it
// as hosted.
type consumerSubscription struct {
	closeFn types.CloseFunc
	done    <-chan struct{}
}

func (s *consumerSubscription) Close(ctx context.Context) error { return s.closeFn(ctx) }

func (s *consumerSubscription) Done() <-chan struct{} { return s.done }

var _ types.SubscriptionHealth = (*consumerSubscription)(nil)

type messageCommitter interface {
	CommitMessages(context.Context, ...Message) error
}

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
	Headers   map[string]string
}

type ConsumerConfig struct {
	Brokers     []string
	Topic       string
	Group       string
	StartOffset string
	MaxInflight int
	Aggregate   AggregateConfig
	// SASL authentication. All three must be set for SASL to activate.
	SASLMechanism string // "plain", "scram-sha-256", "scram-sha-512"
	SASLUsername  string
	SASLPassword  string
	// MessageSchema optionally validates each message's JSON value before emit.
	// When non-nil, messages that fail validation are handled per
	// MessageSchema.OnInvalid.
	MessageSchema *MessageSchema
	// EmitRetry configures what the legacy per-message emit path does when Emit
	// fails. The zero value retries the same record forever; see
	// EmitRetryConfig and deliverLegacy.
	EmitRetry EmitRetryConfig
	// Tuning holds the consumer knobs (fetch sizing, dial timeout, group
	// liveness windows). The zero value selects defaultTuning.
	Tuning TuningConfig
	// ValueJSON splices a message payload that is itself JSON into the item
	// verbatim, instead of escaping it into a string. See messageData; it takes
	// effect only on the in-process group path.
	ValueJSON bool
}

var newConsumer = newKafkaGoConsumer

type Node struct {
	nodeinternal.BaseTrigger
	BrokersValue       []string
	TopicValue         string
	GroupValue         string
	StartOffsetValue   string
	MaxInflightValue   int
	AggregateValue     AggregateConfig
	MessageSchemaValue *MessageSchema
	EmitRetryValue     EmitRetryConfig
	TuningValue        TuningConfig
	ValueJSONValue     bool
}

func New() *Node {
	return &Node{StartOffsetValue: "latest", MaxInflightValue: defaultTriggerMaxInflight}
}

func (n *Node) Brokers(brokers ...string) *Node {
	n.BrokersValue = brokers
	return n
}

func (n *Node) Topic(topic string) *Node {
	n.TopicValue = topic
	return n
}

func (n *Node) Group(group string) *Node {
	n.GroupValue = group
	return n
}

func (n *Node) StartOffset(offset string) *Node {
	n.StartOffsetValue = offset
	return n
}

func (n *Node) MaxInflight(max int) *Node {
	n.MaxInflightValue = max
	return n
}

// Tuning overrides the consumer knobs. Unset fields keep their defaults, so
// setting only SessionTimeout leaves everything else exactly as before.
func (n *Node) Tuning(cfg TuningConfig) *Node {
	n.TuningValue = cfg
	return n
}

// ValueAsJSON carries a message payload that is itself JSON into `$item.value`
// as JSON, rather than as a string holding an escaped copy of it.
//
// Use it when the topic carries JSON and a downstream node parses it. Both the
// escaping here and the un-escaping there disappear -- on an 8.8 KB apisix
// access log the un-escaping alone was 34.5% of the receiving wasm guest's
// eval. What it costs is a json.Valid scan per message, and a payload that
// fails it keeps the string form.
//
// It changes the TYPE a downstream expression sees: `$item.value` becomes an
// object, so anything doing a string comparison or a regex on it must be
// updated. It also takes effect ONLY when the trigger is a member of a node
// group, because only then does the batch stay inside one process; activation
// logs and ignores it otherwise rather than silently shipping an object into
// Redis where a string is expected.
func (n *Node) ValueAsJSON() *Node {
	n.ValueJSONValue = true
	return n
}

func (n *Node) AggregateByPartition(maxSize int, flushInterval time.Duration) *Node {
	n.AggregateValue = AggregateConfig{
		Enabled:       true,
		By:            aggregateByPartition,
		MaxSize:       maxSize,
		FlushInterval: flushInterval,
		Dedup:         aggregateDedupMessage,
	}
	return n
}

func (n *Node) Aggregate(cfg AggregateConfig) *Node {
	n.AggregateValue = normalizeAggregateConfig(cfg)
	return n
}

// BlockOnOverflow switches the at-cap policy from dropping the arriving record
// to halting consumption on that partition until the backlog drains. Requires
// aggregation to be enabled.
//
// It is the on_invalid: fail of overflow, and it costs the same kind of thing.
// A stalled partition stops fetching, and the group Reader that serves the
// whole assignment stops with it, so a downstream that never recovers stalls
// every partition this runner owns rather than one. What it buys is that no
// record is dropped, which is the only shape in which the aggregator obeys the
// principle the rest of this package is built on: redelivering a message
// forever is recoverable, dropping it is not.
//
// Choose it for a topic where a lost record is a lost record — an audit or
// billing stream — and leave the default for one where staying current matters
// more than completeness.
func (n *Node) BlockOnOverflow() *Node {
	n.AggregateValue.OnOverflow = onOverflowBlock
	return n
}

// DeadLetterOnOverflow switches the at-cap policy from dropping the arriving
// record to republishing it to topic (a dead-letter/overflow topic) and only
// then letting its offset advance. Requires aggregation to be enabled, and
// topic must be non-empty: a node that selects this policy without one fails
// activation rather than silently falling back to the drop it was leaving.
//
// It is the only one of the three policies that neither loses the record nor
// deliberately stalls the assignment, and it is not free. The republish runs on
// that partition's coordinator, so a slow dead-letter topic stalls the shared
// reader exactly as BlockOnOverflow does — bounded by the publisher's write
// timeout rather than by the downstream recovering. A FAILED republish is
// treated as backpressure: the record is held, the partition stops consuming,
// and the write is retried, because committing past a record that is not yet
// durably parked is the loss this option exists to avoid.
//
// Choose it for a topic where a stalled assignment is unacceptable AND a lost
// record is unacceptable, and give the overflow topic a consumer — the records
// are durable but nothing replays them on its own. The published record keeps
// its original key and payload byte-for-byte, with the source topic, partition,
// offset and reason in xflow-dlq-* headers.
func (n *Node) DeadLetterOnOverflow(topic string) *Node {
	n.AggregateValue.OnOverflow = onOverflowDeadLetter
	n.AggregateValue.DeadLetterTopic = topic
	return n
}

// MessageSchema requires each message value to be a JSON object carrying all of
// fields as top-level keys. Invalid messages are discarded (offset committed,
// message dropped) but counted and logged — see DiscardInvalid/DeadLetterInvalid
// to choose a different policy.
func (n *Node) MessageSchema(fields ...string) *Node {
	n.MessageSchemaValue = &MessageSchema{RequiredFields: fields, OnInvalid: onInvalidDiscard}
	return n
}

// FailOnInvalid switches the invalid-message policy to withholding the offset
// commit, so Kafka redelivers. Zero data loss, at the cost of a permanently
// malformed message blocking its partition forever. Requires MessageSchema.
func (n *Node) FailOnInvalid() *Node {
	if n.MessageSchemaValue != nil {
		n.MessageSchemaValue.OnInvalid = onInvalidFail
	}
	return n
}

// DeadLetterInvalid republishes invalid messages to topic and commits only after
// a successful republish. This is the policy that neither loses messages nor
// stalls the partition. Requires MessageSchema.
func (n *Node) DeadLetterInvalid(topic string) *Node {
	if n.MessageSchemaValue != nil {
		n.MessageSchemaValue.OnInvalid = onInvalidDeadLetter
		n.MessageSchemaValue.DeadLetterTopic = topic
	}
	return n
}

// DeadLetterOnEmitFailure switches the legacy per-message emit path (the one
// used when aggregation is off and the trigger is not entry-seed) from
// "retry the record forever" to "retry it for a bounded time, then park it on
// topic and commit only after the park succeeded".
//
// It exists because failing to deliver a record must not skip it. Emit then
// commit is the at-least-once rule: the offset advances only after the side
// effect. When the side effect keeps failing, the partition has to stop, and
// the only question is how it eventually resumes. Retrying forever (the
// default) never loses anything but needs the downstream to recover; this
// option bounds the wait and preserves the record durably instead, with the
// source topic/partition/offset and the last emit error in xflow-dlq-* headers.
//
// The cost is the same shape as DeadLetterOnOverflow: the republish runs on the
// partition's own worker, so a slow dead-letter topic stalls that partition and,
// through the shared reader, backpressure reaches the others once its channel
// fills. A FAILED republish is treated as backpressure — the record is held and
// the republish retried — because committing past a record that is not durably
// parked is the loss this option exists to avoid.
//
// topic must be non-empty: a node that selects this policy without a
// destination fails activation rather than silently falling back to the
// unbounded retry it was leaving. Bound the wait with EmitRetryPolicy.
func (n *Node) DeadLetterOnEmitFailure(topic string) *Node {
	n.EmitRetryValue.OnFailure = onEmitFailureDeadLetter
	n.EmitRetryValue.DeadLetterTopic = topic
	return n
}

// EmitRetryPolicy bounds how long the legacy emit path holds a partition for one
// undeliverable record before DeadLetterOnEmitFailure parks it. maxAttempts is
// the number of failed emit attempts; maxBlock bounds the wall time spent
// holding the record. Whichever is reached first wins, and a zero field keeps
// its default (10 attempts, 30s).
//
// It has NO effect unless DeadLetterOnEmitFailure is also set (or emit_retry
// arrives with on_failure=dead_letter): with no destination there is no safe
// bounded outcome, so the retry remains unbounded by design and these bounds are
// ignored. Setting them without a dead-letter topic is therefore not an error —
// it is simply not a policy this path can honour without losing the record.
func (n *Node) EmitRetryPolicy(maxAttempts int, maxBlock time.Duration) *Node {
	n.EmitRetryValue.MaxAttempts = maxAttempts
	n.EmitRetryValue.MaxBlock = maxBlock
	return n
}

func (n *Node) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.kafka",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Kafka Trigger",
		Description: "消费 Kafka 主题消息并触发工作流，支持分区批量聚合。",
		Category:    "trigger",
		Examples: []types.DescriptorExample{{
			Name: "消费订单主题",
			Params: map[string]any{
				"brokers": []string{"127.0.0.1:9092"},
				"topic":   "orders",
				"group":   "xflow-orders",
			},
		}},
		CardSummary: []string{"brokers", "topic+group"},
		Params: []types.ParamSpec{
			{Name: "brokers", DisplayName: "Brokers", Type: types.ParamArray, Required: true,
				Group: "connection", Item: nodeinternal.StringItem()},
			{Name: "topic", DisplayName: "Topic", Type: types.ParamString, Required: true, Group: "connection"},
			{Name: "group", DisplayName: "Group", Type: types.ParamString, Required: true, Group: "connection"},
			// startOffsetFor matches case-insensitively and accepts aliases.
			{Name: "start_offset", DisplayName: "Start Offset", Type: types.ParamString, Default: "latest",
				Group: "connection",
				Enum: []types.EnumOption{
					{Value: "latest", DisplayName: "Latest", Description: "Only records produced after the group first joins"},
					{Value: "earliest", DisplayName: "Earliest", Description: "From the oldest retained record"},
					{Value: "last", DisplayName: "Last", Description: "Alias of latest"},
					{Value: "newest", DisplayName: "Newest", Description: "Alias of latest"},
					{Value: "first", DisplayName: "First", Description: "Alias of earliest"},
					{Value: "oldest", DisplayName: "Oldest", Description: "Alias of earliest"},
					{Value: "beginning", DisplayName: "Beginning", Description: "Alias of earliest"},
				}},
			{Name: "max_inflight", DisplayName: "Max Inflight", Type: types.ParamNumber, Default: float64(defaultTriggerMaxInflight), Group: "processing"},
			{Name: "aggregate", DisplayName: "Aggregate", Type: types.ParamObject, Description: "Optional partition batch aggregation: enabled, by, max_size, flush_interval, dedup, on_overflow, dead_letter_topic. Under entry-seed hosting the batch is admitted to the control plane instead of emitted locally, with an admission key covering the batch's actual offset range; delivery is at-least-once (a batch may be reprocessed once if its offsets fail to commit), so consumers must be idempotent on (topic, partition, offset). flush_interval defaults to 1s in entry-seed mode and 100ms on the legacy emit path. on_overflow decides what happens when a partition reaches its retained bound: \"discard\" (the default) drops the arriving record and a later commit sweeps past its offset, so it is never redelivered — the loss is counted in xflow_trigger_messages_discarded_total{reason=\"buffer_overflow\"} and logged; \"block\" loses nothing but halts fetching for the WHOLE assignment until that partition drains, and neither lag gauge can report it, so watch xflow_trigger_consumption_blocked instead; \"dead_letter\" republishes the record to dead_letter_topic (required with this value) before letting its offset advance, so nothing is lost and nothing stalls while the overflow topic is healthy — a failed republish is retried as backpressure (the partition stops consuming) rather than dropping, counted in xflow_trigger_messages_dead_lettered_total. Set on_overflow explicitly on any topic that cannot afford the default loss.",
				Group: "processing",
				Fields: []types.ParamSpec{
					{Name: "enabled", DisplayName: "Enabled", Type: types.ParamBool, Description: "Aggregation is off unless this is true; every other field is ignored while it is off"},
					{Name: "by", DisplayName: "By", Type: types.ParamString, Description: "Batch key; only \"partition\" is supported (the default)",
						Enum: nodeinternal.Options(aggregateByPartition)},
					{Name: "max_size", DisplayName: "Max Size", Type: types.ParamNumber, Description: "Records per batch; unset or non-positive means 100"},
					{Name: "flush_interval", DisplayName: "Flush Interval", Type: types.ParamString, Description: "Positive duration; defaults to 1s in entry-seed mode and 100ms otherwise",
						Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
					{Name: "dedup", DisplayName: "Dedup", Type: types.ParamString, Description: "Deduplication unit; only \"message\" is supported (the default)",
						Enum: nodeinternal.Options(aggregateDedupMessage)},
					{Name: "on_overflow", DisplayName: "On Overflow", Type: types.ParamString, Description: "Policy when a partition reaches its retained bound; unset means discard",
						Enum: []types.EnumOption{
							{Value: onOverflowDiscard, DisplayName: "Discard", Description: "Drop the arriving record (counted and logged)"},
							{Value: onOverflowBlock, DisplayName: "Block", Description: "Halt fetching for the whole assignment until the partition drains"},
							{Value: onOverflowDeadLetter, DisplayName: "Dead Letter", Description: "Republish to dead_letter_topic before advancing the offset"},
						}},
					{Name: "dead_letter_topic", DisplayName: "Dead Letter Topic", Type: types.ParamString, Description: "Overflow topic; read only when on_overflow is dead_letter",
						VisibleWhen:  nodeinternal.CondEq("on_overflow", onOverflowDeadLetter),
						RequiredWhen: nodeinternal.CondEq("on_overflow", onOverflowDeadLetter)},
				}},
			{Name: "message_schema", DisplayName: "Message Schema", Type: types.ParamObject, Description: "Optional message validation: {required_fields: [\"f\"], on_invalid: \"discard|fail|dead_letter\", dead_letter_topic: \"t-dlq\"}. on_invalid defaults to discard (offset committed, message dropped, drop counted and logged).",
				Group: "processing",
				Fields: []types.ParamSpec{
					{Name: "required_fields", DisplayName: "Required Fields", Type: types.ParamArray, Description: "Top-level keys every message value must carry; an empty list disables validation",
						Item: nodeinternal.StringItem()},
					{Name: "on_invalid", DisplayName: "On Invalid", Type: types.ParamString, Description: "Policy for an invalid message; unset means discard",
						Enum: []types.EnumOption{
							{Value: onInvalidDiscard, DisplayName: "Discard", Description: "Commit the offset and drop the message (counted and logged)"},
							{Value: onInvalidFail, DisplayName: "Fail", Description: "Withhold the commit so Kafka redelivers; blocks the partition"},
							{Value: onInvalidDeadLetter, DisplayName: "Dead Letter", Description: "Republish to dead_letter_topic, then commit"},
						}},
					{Name: "dead_letter_topic", DisplayName: "Dead Letter Topic", Type: types.ParamString, Description: "Read only when on_invalid is dead_letter",
						VisibleWhen:  nodeinternal.CondEq("on_invalid", onInvalidDeadLetter),
						RequiredWhen: nodeinternal.CondEq("on_invalid", onInvalidDeadLetter)},
				}},
			{Name: "emit_retry", DisplayName: "Emit Retry", Type: types.ParamObject, Description: "Optional failure policy for the legacy per-message emit path (aggregation off and not entry-seed). A failed emit holds the partition and retries the SAME record with exponential backoff (100ms doubling, jittered, capped at 5s); nothing behind it is emitted, and no commit can pass its offset. With on_failure unset or \"retry\" the retry is UNBOUNDED and ends only when the trigger stops — the safe default, because a bounded retry with nowhere to put the record would have to lose it. \"dead_letter\" bounds the wait by max_attempts (default 10) and max_block (default 30s), whichever comes first, then republishes the record to dead_letter_topic (required) with the last emit error in xflow-dlq-reason and commits the offset only after the republish succeeds; a failing republish keeps the partition held and is retried. Retries are visible in xflow_trigger_consumption_blocked and the dead-letter outcome in xflow_trigger_messages_dead_lettered_total. Delivery stays at-least-once; consumers must be idempotent on (topic, partition, offset).",
				Group: "processing",
				Fields: []types.ParamSpec{
					{Name: "on_failure", DisplayName: "On Failure", Type: types.ParamString, Description: "Policy once emit keeps failing; unset means retry forever",
						Enum: []types.EnumOption{
							{Value: onEmitFailureRetry, DisplayName: "Retry", Description: "Retry the same record forever; only stopping the trigger ends it (never skips, so nothing is lost)"},
							{Value: onEmitFailureDeadLetter, DisplayName: "Dead Letter", Description: "Retry within the bounds, then republish to dead_letter_topic before committing the offset"},
						}},
					{Name: "max_attempts", DisplayName: "Max Attempts", Type: types.ParamNumber, Description: "Failed emit attempts before the record is parked; default 10. Read only when on_failure is dead_letter",
						VisibleWhen: nodeinternal.CondEq("on_failure", onEmitFailureDeadLetter),
						Constraints: &types.Constraints{Min: nodeinternal.Float(1)}},
					{Name: "max_block", DisplayName: "Max Block", Type: types.ParamString, Description: "Wall time this partition may hold one record before it is parked; default 30s. Read only when on_failure is dead_letter",
						VisibleWhen: nodeinternal.CondEq("on_failure", onEmitFailureDeadLetter),
						Widget:      nodeinternal.WidgetDuration,
						Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
					{Name: "dead_letter_topic", DisplayName: "Dead Letter Topic", Type: types.ParamString, Description: "Where an undeliverable record is parked; read only when on_failure is dead_letter",
						VisibleWhen:  nodeinternal.CondEq("on_failure", onEmitFailureDeadLetter),
						RequiredWhen: nodeinternal.CondEq("on_failure", onEmitFailureDeadLetter)},
				}},
			{Name: "tuning", DisplayName: "Tuning", Type: types.ParamObject, Description: "Optional consumer tuning: fetch_min_bytes (1), fetch_max_bytes (10000000), max_wait (10s), dial_timeout (10s), session_timeout (30s), heartbeat_interval (3s), rebalance_timeout (30s). Durations are strings (\"45s\"). heartbeat_interval must stay below session_timeout or the group rebalances continuously. Offsets always commit synchronously after the side effect; that is not tunable.",
				Group: "advanced",
				Fields: []types.ParamSpec{
					{Name: "fetch_min_bytes", DisplayName: "Fetch Min Bytes", Type: types.ParamNumber, Description: "Default 1; must not exceed fetch_max_bytes",
						Constraints: &types.Constraints{Min: nodeinternal.Float(1)}},
					{Name: "fetch_max_bytes", DisplayName: "Fetch Max Bytes", Type: types.ParamNumber, Description: "Default 10000000",
						Constraints: &types.Constraints{Min: nodeinternal.Float(1)}},
					{Name: "max_wait", DisplayName: "Max Wait", Type: types.ParamString, Description: "Default 10s",
						Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
					{Name: "dial_timeout", DisplayName: "Dial Timeout", Type: types.ParamString, Description: "Default 10s",
						Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
					{Name: "session_timeout", DisplayName: "Session Timeout", Type: types.ParamString, Description: "Default 30s",
						Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
					{Name: "heartbeat_interval", DisplayName: "Heartbeat Interval", Type: types.ParamString, Description: "Default 3s; must stay below session_timeout",
						Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
					{Name: "rebalance_timeout", DisplayName: "Rebalance Timeout", Type: types.ParamString, Description: "Default 30s",
						Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
				}},
		},
		Groups: []types.GroupSpec{
			{Key: "connection", DisplayName: "Connection"},
			{Key: "processing", DisplayName: "Processing"},
			{Key: "advanced", DisplayName: "Advanced", Collapsed: true},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *Node) NodeType() string { return "xflow.trigger.kafka" }
func (n *Node) RawParams() any {
	offset := n.StartOffsetValue
	if offset == "" {
		offset = "latest"
	}
	maxInflight := n.MaxInflightValue
	if maxInflight <= 0 {
		maxInflight = defaultTriggerMaxInflight
	}
	params := map[string]any{
		"brokers":      n.BrokersValue,
		"topic":        n.TopicValue,
		"group":        n.GroupValue,
		"start_offset": offset,
		"max_inflight": maxInflight,
	}
	if n.AggregateValue.Enabled {
		aggregate := normalizeAggregateConfig(n.AggregateValue)
		// NOTE: flush_interval is always serialized here, which means the Go DSL
		// path (AggregateByPartition / Aggregate) bakes the interval at construction
		// time — before we know whether the activation will be entry-seed. The
		// runtime mode-aware default (1s for entry-seed vs 100ms for legacy) only
		// takes effect on the YAML/JSON params path where flush_interval is absent
		// from the map. Go DSL users who want the entry-seed 1s default should pass
		// time.Second explicitly. Tracked as a known limitation rather than adding a
		// "was-explicitly-set" flag to AggregateConfig.
		params["aggregate"] = map[string]any{
			"enabled":        aggregate.Enabled,
			"by":             aggregate.By,
			"max_size":       aggregate.MaxSize,
			"flush_interval": aggregate.FlushInterval.String(),
			"dedup":          aggregate.Dedup,
		}
		// Emitted only when it is not the default, for the reason value_json and
		// tuning are: normalize fills OnOverflow in for everyone, so writing it
		// unconditionally would move the definition hash of every workflow that
		// already aggregates, over a field none of them set.
		//
		// It must be written at all, though. This map is rebuilt key by key, and
		// a key that is not listed here does not reach the runtime — the config
		// round-trips through it, so a DSL caller's OnOverflow would parse back
		// as "" on the other side and normalize to discard. The setting would
		// appear to take and then quietly not apply, which for this particular
		// setting means losing the records it was chosen to protect.
		if aggregate.OnOverflow != onOverflowDiscard {
			agg := params["aggregate"].(map[string]any)
			agg["on_overflow"] = aggregate.OnOverflow
			// Written only for the policy that reads it, and only when it is set.
			// A dead_letter node that reaches the runtime without its topic is
			// rejected at activation, which is the fail-closed outcome: emitting
			// the policy without the destination would otherwise look like a
			// working definition that quietly drops records.
			if aggregate.OnOverflow == onOverflowDeadLetter && aggregate.DeadLetterTopic != "" {
				agg["dead_letter_topic"] = aggregate.DeadLetterTopic
			}
		}
	}
	if n.MessageSchemaValue != nil && len(n.MessageSchemaValue.RequiredFields) > 0 {
		schema := map[string]any{
			"required_fields": n.MessageSchemaValue.RequiredFields,
			"on_invalid":      n.MessageSchemaValue.OnInvalid,
		}
		if n.MessageSchemaValue.DeadLetterTopic != "" {
			schema["dead_letter_topic"] = n.MessageSchemaValue.DeadLetterTopic
		}
		params["message_schema"] = schema
	}
	// Omitted entirely when unset, for the same hash-stability reason tuning and
	// value_json are. It must still be written whenever it is set: this map is
	// rebuilt key by key and is the only way a DSL caller's policy reaches the
	// runtime, so skipping it would make DeadLetterOnEmitFailure appear to take
	// and then quietly not apply — the failure mode that loses the records the
	// option exists to protect.
	if !n.EmitRetryValue.isZero() {
		params["emit_retry"] = n.EmitRetryValue.rawParams()
	}
	// Omitted entirely when unset, so definitions stored before tuning existed
	// keep the same shape.
	if !n.TuningValue.isZero() {
		params["tuning"] = n.TuningValue.rawParams()
	}
	// Same: absent rather than false, so an existing definition's hash does not
	// move because a field was added.
	if n.ValueJSONValue {
		params["value_json"] = true
	}
	return params
}
func (n *Node) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *Node) TriggerHandler() types.TriggerHandler { return n }

func (n *Node) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := configFromParams(in.Params, mergedSupplyContent(in.Supplies), isEntrySeedActivation(in))
	if err != nil {
		return nil, err
	}
	consumer, err := newConsumer(cfg)
	if err != nil {
		return nil, err
	}
	// The dead-letter publisher is built only when a policy needs it, and
	// eagerly rather than on first invalid message: a broker-unreachable DLQ
	// should fail activation (which self-heals via retry) instead of surfacing
	// as an unbounded redelivery loop the first time a malformed record arrives.
	//
	// Three axes can need it now — invalid messages (message_schema.on_invalid),
	// buffer overflow (aggregate.on_overflow) and undeliverable legacy emits
	// (emit_retry.on_failure) — and one publisher serves all: it is one Kafka
	// writer, and each Publish call carries its own topic and reason. Building
	// one per axis would open a second producer connection per trigger for a
	// deployment that has more than one, which is the cost
	// TestKafkaActivateSkipsPublisherForDiscardPolicy exists to refuse.
	var deadLetters DeadLetterPublisher
	if needsDeadLetterPublisher(cfg) {
		deadLetters, err = newDeadLetterPublisher(cfg)
		if err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("kafka trigger: dead-letter publisher: %w", err)
		}
	}
	if cfg.Aggregate.Enabled {
		return activateAggregate(ctx, in, cfg, consumer, deadLetters), nil
	}
	return activatePerMessage(ctx, in, cfg, consumer, deadLetters), nil
}

// needsDeadLetterPublisher reports whether any configured policy republishes
// records, and therefore whether activation must open a dead-letter writer.
//
// Every check is on the VALUE, not on the presence of the config block: a
// message_schema with on_invalid=discard, an aggregate with on_overflow=block
// and an emit_retry with on_failure=retry must open no writer at all. The
// aggregate and emit axes additionally require a non-empty topic, so a config
// that reached this function without one (only possible by constructing
// ConsumerConfig directly, since the params path rejects it) yields no
// publisher — and the runtime then fails closed by holding the record and
// retrying rather than by dropping it.
func needsDeadLetterPublisher(cfg ConsumerConfig) bool {
	if cfg.MessageSchema != nil && cfg.MessageSchema.OnInvalid == onInvalidDeadLetter {
		return true
	}
	if cfg.EmitRetry.deadLetterReady() {
		return true
	}
	return cfg.Aggregate.Enabled && cfg.Aggregate.OnOverflow == onOverflowDeadLetter &&
		cfg.Aggregate.DeadLetterTopic != ""
}

func activatePerMessage(ctx context.Context, in *types.TriggerActivateInput, cfg ConsumerConfig, consumer Consumer, deadLetters DeadLetterPublisher) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &perMessageRuntime{
		runCtx:              runCtx,
		baseCtx:             context.WithoutCancel(runCtx),
		in:                  in,
		consumer:            consumer,
		buffer:              cfg.MaxInflight,
		workers:             make(map[partitionKey]*partitionWorker),
		entrySeed:           isEntrySeedActivation(in),
		messageSchema:       cfg.MessageSchema,
		emitRetry:           cfg.EmitRetry,
		deadLetterPublisher: deadLetters,
	}
	rt.valueJSON = resolveValueJSON(cfg, in, rt.entrySeed)
	done := make(chan struct{})
	go func() {
		// Registered in this order so close(done) runs FIRST — see the same
		// ordering note in activateAggregate: Done must not wait behind
		// teardown I/O.
		defer rt.close(context.Background())
		defer close(done)
		for {
			select {
			case <-runCtx.Done():
				return
			case msg, ok := <-consumer.Messages():
				if !ok {
					return
				}
				if !rt.submit(runCtx, msg) {
					return
				}
			}
		}
	}()
	return &consumerSubscription{
		closeFn: types.CloseFunc(func(closeCtx context.Context) error {
			// Cancel first so any submit blocked on a full worker/aggregator channel
			// wakes immediately rather than waiting for consumer.Close to drain it.
			cancel()
			err := consumer.Close()
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			rt.close(closeCtx)
			return err
		}),
		done: done,
	}
}

// perMessageRuntime routes each message to a per-partition worker that
// processes messages serially: emit then commit, in offset order. This replaces
// the previous fan-out where every message ran in its own goroutine and
// committed its own offset independently — under that scheme a higher offset
// committing before a lower one caused the lower message to be skipped on
// rebalance. Per-partition serial commit preserves at-least-once ordering.
type perMessageRuntime struct {
	runCtx context.Context
	// baseCtx is runCtx with its cancellation removed, for the same reason
	// aggregateRuntime.baseCtx exists (see the comment there): the commit paths
	// below must not be cut short when runCtx is canceled, which is why they
	// reached for context.Background() — but Background also drops the namespace
	// that observability/metrics reads off the context for every label set, so
	// every commit made here was filed under the default namespace. WithoutCancel
	// keeps the values without restoring the Done channel.
	baseCtx   context.Context
	in        *types.TriggerActivateInput
	consumer  Consumer
	buffer    int
	mu        sync.Mutex
	closeOnce sync.Once
	workers   map[partitionKey]*partitionWorker
	// entrySeed selects the entry-unit seed admission path over the legacy
	// Emit path for each message. Set once at activation from the trigger
	// params (see isEntrySeedActivation).
	entrySeed bool
	// messageSchema, when non-nil, validates each message before emit. Messages
	// that fail validation are handled per messageSchema.OnInvalid.
	messageSchema *MessageSchema
	// emitRetry is the legacy emit path's failure policy (retry forever, or
	// retry within bounds and then park on the dead-letter topic). Set once at
	// activation from the trigger params; unused on the entry-seed path, which
	// has its own refusal loop.
	emitRetry EmitRetryConfig
	// deadLetterPublisher is non-nil when any configured policy republishes
	// records: messageSchema.OnInvalid=dead_letter, emitRetry.on_failure=
	// dead_letter, or (in the aggregate runtime) on_overflow=dead_letter. Owned
	// by this runtime: closed by close().
	deadLetterPublisher DeadLetterPublisher
	// valueJSON splices a JSON payload into the item verbatim instead of
	// escaping it into a string. Resolved once at activation by
	// resolveValueJSON, which is where it can still be refused out loud.
	valueJSON bool
}

// resolveValueJSON decides whether the spliced-value form is actually safe for
// this activation, and says so when it is not.
//
// The form is only safe where the item stays in this process, which is the
// trigger-group path: seedEntryBatchViaGroupExec runs the group's members on an
// in-process backend. Everywhere else the item is marshalled into Redis, SQL or
// the control-plane wire and read back with json.Unmarshal, which turns a
// spliced value into a map[string]any -- so a consumer that asked for a string
// gets an object, and the escape this was meant to avoid is replaced by a
// full parse and re-marshal at every hop.
//
// Refusing quietly would be worse than refusing: the flag's whole effect is a
// speed one, so a deployment that lost it would look exactly like a deployment
// that never had it. The log is the only way anyone finds out.
func resolveValueJSON(cfg ConsumerConfig, in *types.TriggerActivateInput, entrySeed bool) bool {
	if !cfg.ValueJSON {
		return false
	}
	if entrySeed && groupExecCapable(in) {
		return true
	}
	slog.Warn("kafka trigger: value_json ignored",
		"node", in.NodeName,
		"reason", "the spliced value form requires a trigger that is a member of a node group, "+
			"so the batch never crosses a JSON boundary; this activation is not one",
		"entry_seed", entrySeed)
	return false
}

// groupExecCapable reports whether the runtime can execute the trigger's group
// locally, which is the same assertion partitionAggregator.emitBatch makes
// before it takes that path.
func groupExecCapable(in *types.TriggerActivateInput) bool {
	if in == nil {
		return false
	}
	_, ok := in.Runtime.(interface {
		types.EntrySeedRuntime
		types.GroupExecRuntime
	})
	return ok
}

func (r *perMessageRuntime) schema() *MessageSchema { return r.messageSchema }

func (r *perMessageRuntime) deadLetters() DeadLetterPublisher {
	return r.deadLetterPublisher
}

// isEntrySeedActivation reports whether a Kafka trigger should route each
// message through the entry-unit seed admission path (seedKafkaEntryBatch)
// instead of the legacy Emit path. It requires BOTH that the runtime supports
// entry-seed admission (implements types.EntrySeedRuntime) AND that the trigger
// is configured as an entry unit — signalled by params `entry_seed=true` or the
// presence of a non-empty `entry_unit_id`. Requiring the runtime capability
// keeps existing triggers on the legacy path when the runtime cannot admit
// seeds, so a misconfigured param can never silently drop messages.
func isEntrySeedActivation(in *types.TriggerActivateInput) bool {
	if in == nil {
		return false
	}
	if _, ok := in.Runtime.(types.EntrySeedRuntime); !ok {
		return false
	}
	if cast.ToBool(in.Params["entry_seed"]) {
		return true
	}
	if id, _ := in.Params["entry_unit_id"].(string); id != "" {
		return true
	}
	return false
}

type partitionWorker struct {
	key         partitionKey
	rt          *perMessageRuntime
	ch          chan Message
	done        chan struct{}
	idleTimeout time.Duration
}

func (r *perMessageRuntime) submit(ctx context.Context, msg Message) bool {
	key := partitionKey{topic: msg.Topic, partition: msg.Partition}
	w := r.worker(key)
	select {
	case w.ch <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *perMessageRuntime) worker(key partitionKey) *partitionWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[key]; ok {
		return w
	}
	buf := r.buffer
	if buf <= 0 {
		buf = defaultTriggerMaxInflight
	}
	w := &partitionWorker{
		key:         key,
		rt:          r,
		ch:          make(chan Message, buf),
		done:        make(chan struct{}),
		idleTimeout: workerIdleTimeout,
	}
	r.workers[key] = w
	go w.run()
	return w
}

// workerIdleTimeout bounds how long a per-message worker idles before
// assuming its partition was revoked by rebalance and self-terminating. Without
// it, a revoked partition's worker goroutine and map entry would leak for the
// process lifetime (kafka-go exposes no revocation callback).
const workerIdleTimeout = 5 * time.Minute

func (r *perMessageRuntime) close(ctx context.Context) {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		workers := make([]*partitionWorker, 0, len(r.workers))
		for _, w := range r.workers {
			workers = append(workers, w)
		}
		r.mu.Unlock()
		for _, w := range workers {
			close(w.ch)
		}
		// Close the dead-letter publisher only after every worker has drained,
		// so an in-flight publish is never cut off mid-write. Deferred rather
		// than placed after the loop because the ctx.Done() path below returns
		// early.
		defer func() {
			if r.deadLetterPublisher != nil {
				_ = r.deadLetterPublisher.Close()
			}
		}()
		for _, w := range workers {
			select {
			case <-w.done:
			case <-ctx.Done():
				return
			}
		}
	})
}

// evictWorker removes an idle worker that self-terminated so a later message
// for that partition lazily spawns a fresh one.
func (r *perMessageRuntime) evictWorker(key partitionKey, w *partitionWorker) {
	r.mu.Lock()
	if existing, ok := r.workers[key]; ok && existing == w {
		delete(r.workers, key)
	}
	r.mu.Unlock()
}

func (w *partitionWorker) run() {
	defer close(w.done)
	defer w.rt.evictWorker(w.key, w)
	idleTimer := time.NewTimer(w.idleTimeout)
	defer idleTimer.Stop()
	for {
		select {
		case msg, ok := <-w.ch:
			if !ok {
				return
			}
			w.deliver(msg)
		case <-idleTimer.C:
			// The timer and a message can become ready at the same instant, and
			// select chooses between ready cases at random. Reaping on the timer
			// without looking at the channel would abandon records already
			// buffered for this partition: they were fetched but never emitted
			// and no commit covers them, so they would not come back until a
			// rebalance or restart. Give a queued record priority over the reap;
			// only a genuinely idle channel ends the worker.
			select {
			case msg, ok := <-w.ch:
				if !ok {
					return
				}
				w.deliver(msg)
			default:
				return
			}
		}
		// Partition still assigned — restart the idle reap window only now that
		// this record is done. Arming it BEFORE the work above (the previous
		// shape) put a five-minute timer on processing that can legitimately
		// outlast it: an entry-seed refusal loop or a legacy emit hold both
		// retry until the record resolves. An expired timer would then be ready
		// in the select above, and picking it would reap a worker whose channel
		// still holds records for the partition — those records were fetched but
		// never emitted, and no commit covers them, so they would not come back
		// until a rebalance or restart.
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(w.idleTimeout)
	}
}

// deliver runs one message through the activation's mode-specific path. It is
// called from both the message branch and the timer branch of run (the latter
// after a reap found a queued record), so the two paths cannot drift apart.
func (w *partitionWorker) deliver(msg Message) {
	// Serial per-partition processing: emit then commit in offset order so
	// a rebalance can never skip a lower offset whose higher peer committed
	// first. On the legacy emit path a failure does not move on to the next
	// record: deliverLegacy holds this partition and retries the same
	// record, because a later record's commit would sweep past the failed
	// offset and that record would never be redelivered. deliverInvalid does
	// the same for the schema axis: under on_invalid=fail, and while a
	// dead-letter republish is failing, it holds this partition instead of
	// stepping over the record.
	//
	// Schema validation runs BEFORE the mode split. An earlier revision
	// chained it as `else if` after the entry-seed branch, so entry-seed
	// activations silently ignored a declared schema — the one mode where
	// invalid content is most expensive, because a seeded execution is
	// durable.
	if w.rt.messageSchema != nil && !validateMessageSchema(msg, w.rt.messageSchema) {
		// Returns only once the record is commit-safe (discarded, or durably
		// republished) or the activation is canceled; on fail it holds this
		// partition so no later offset can commit past the record.
		w.deliverInvalid(w.rt.runCtx, msg)
	} else if w.rt.entrySeed {
		// Entry-seed mode: admission drives the seed, which commits the
		// offset internally on accept/duplicate/conflict. Do NOT
		// double-commit here.
		//
		// This call blocks while the workflow's concurrency policy refuses
		// the record: it retries the same record with backoff rather than
		// returning, so this worker — serial per partition — is what keeps a
		// later record from committing past a refused one. It still returns
		// promptly on errors and on cancellation.
		_ = seedEntryBatch(w.rt.runCtx, w.rt.in, w.rt.consumer, msg)
	} else {
		// Legacy emit path. Returns only once the record is committed (after
		// a successful emit or a successful dead-letter republish) or the
		// activation is canceled; see deliverLegacy.
		w.deliverLegacy(w.rt.runCtx, msg)
	}
}

type partitionKey struct {
	topic     string
	partition int
}

// emitMessage is one attempt at the legacy single-message emit, used only when
// the runtime does NOT implement types.EntrySeedRuntime (see
// isEntrySeedActivation). It returns the Emit error; the caller commits only on
// nil. Partition-order and retry semantics live in deliverLegacy, which is the
// only production caller: an error here means "this attempt failed", not "the
// record is skipped".
//
// P0-1: the previous implementation ran a pre-emit Dedup SETNX here. If the
// process crashed after the SETNX marked the message "seen" but before Emit, the
// message was lost forever (redelivery saw the dedup marker and skipped it). The
// SETNX has been removed: the ordered emit-then-commit is the at-least-once
// guarantee. The downstream is idempotent (host idempotency contract), so a
// possible duplicate on crash-after-emit-before-commit is safe — and so is a
// retry of the same record, which delivers the same event ID.
func emitMessage(ctx context.Context, in *types.TriggerActivateInput, msg Message) error {
	event := singleEvent(in.NodeName, msg)
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	_, err := in.Emit(ctx, event)
	if err == nil {
		return nil
	}
	// The offset stays uncommitted, and this line is the only record that the
	// message was refused: without it a workflow refusing its trigger traffic —
	// a REJECT-mode concurrency limit is the ordinary case — looks exactly like
	// a topic with no messages. A QUEUE-mode workflow does not reach here: its
	// over-limit firings are parked and admitted later instead of refused.
	//
	// Throttled per topic+partition because deliverLegacy now calls this once
	// per backoff tick for as long as the record is held; an unthrottled line
	// would be one per tick per partition for the length of an outage. The
	// partition state is on xflow_trigger_consumption_blocked and in the
	// transition log; this line carries the error and its occurrence count.
	if emit, count := discardLog.allow(time.Now(),
		msg.Topic+"\x00emit_retry\x00"+strconv.Itoa(msg.Partition)); emit {
		slog.Warn("kafka trigger emit did not succeed; the offset is not committed",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"node", in.NodeName,
			"occurrences", count,
			"err", err)
	}
	return err
}

// commitMessages advances the committed offset for messages, and is the single
// layer every commit path in this package passes through — the aggregate
// batch commit (aggregate.go), the entry-seed per-message commit
// (entryseed.go) and both legacy per-message paths below. That is why the
// observation lives here rather than at the call sites: a commit added later
// through a fourth path is measured without anyone remembering to measure it.
//
// The timing brackets ONLY the CommitMessages call. Note what that call is
// under kafka-go: with CommitInterval: 0 it enqueues onto a Reader-wide channel
// and waits for a single serial goroutine to run the RPC, so the sample is
// queueing behind sibling partitions plus the round trip, not the round trip.
// The two early returns above it are not commits and must not be reported as
// instant ones — an empty message set is a no-op, and a consumer without
// messageCommitter (every test fake that does not model offsets) would
// otherwise flood the histogram with zero-duration samples and make the real
// distribution unreadable.
func commitMessages(ctx context.Context, consumer Consumer, messages ...Message) error {
	if len(messages) == 0 {
		return nil
	}
	committer, ok := consumer.(messageCommitter)
	if !ok {
		return nil
	}
	start := time.Now()
	err := committer.CommitMessages(ctx, messages...)
	result := "ok"
	if err != nil {
		result = "error"
	}
	// messages[0].Topic, not a topic threaded down from the runtime: a commit set
	// is built from one partition's buffer, so every element carries the same
	// topic. Reading it from the payload keeps this function's signature free of
	// a parameter that exists only for a label.
	obs().OnOffsetCommit(ctx, messages[0].Topic, result, len(messages), time.Since(start))
	return err
}

func singleEvent(nodeName string, msg Message) *types.TriggerEvent {
	event := &types.TriggerEvent{
		ID:      messageID(msg),
		Kind:    "kafka",
		Source:  nodeName,
		Time:    msg.Time,
		Headers: msg.Headers,
		Data:    singleEventData(msg),
		Raw:     msg.Value,
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	return event
}

func batchEvent(nodeName string, messages []Message) *types.TriggerEvent {
	first := messages[0]
	last := messages[len(messages)-1]
	event := &types.TriggerEvent{
		ID:      fmt.Sprintf("%s/%d/%d-%d", first.Topic, first.Partition, first.Offset, last.Offset),
		Kind:    "kafka.batch",
		Source:  nodeName,
		Time:    first.Time,
		Headers: first.Headers,
		Data: map[string]any{
			"topic":        first.Topic,
			"partition":    first.Partition,
			"start_offset": first.Offset,
			"end_offset":   last.Offset,
			"count":        len(messages),
			"messages":     messageDataList(messages, false),
		},
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	return event
}

func singleEventData(msg Message) map[string]any {
	data := messageData(msg, false)
	data["count"] = 1
	data["messages"] = []map[string]any{messageData(msg, false)}
	return data
}

func messageDataList(messages []Message, valueJSON bool) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	for _, msg := range messages {
		out = append(out, messageData(msg, valueJSON))
	}
	return out
}

// messageData is one Kafka message as the item a downstream node sees.
//
// valueJSON decides how the payload is carried, and it is not a formatting
// preference. A message whose payload is itself JSON -- an access log, an event
// envelope -- goes into a Go string by default, so every marshal on the way
// downstream escapes all of it and every consumer un-escapes all of it. For an
// 8.8 KB apisix record that round trip measured 34.5% of the receiving wasm
// guest's whole eval: 0.169 of it finding where the string token ends, 0.168
// decoding escapes that were applied moments earlier by this process.
//
// With valueJSON the payload is spliced in as the JSON it already is and both
// halves of that disappear. The cost is one json.Valid scan here, and the
// consequence is that `$item.value` is an OBJECT rather than a string -- which
// is why it is opt-in, and why activate() refuses to turn it on outside the
// in-process group path (see resolveValueJSON). A payload that is not
// valid JSON falls back to the string form rather than failing: the flag says
// "splice this when you can", and a topic that carries a mixture must keep
// working.
func messageData(msg Message, valueJSON bool) map[string]any {
	var value any = string(msg.Value)
	if valueJSON && json.Valid(msg.Value) {
		// A RawMessage, not the bytes: json.Marshal writes a []byte as base64
		// and would silently ship an unreadable field.
		value = json.RawMessage(msg.Value)
	}
	return map[string]any{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
		"value":     value,
		"headers":   msg.Headers,
		"time":      msg.Time,
	}
}

func messageID(msg Message) string {
	return fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
}

func configFromParams(params map[string]any, supply map[string]any, entrySeed bool) (ConsumerConfig, error) {
	aggregate, err := aggregateConfigFromParamForMode(params["aggregate"], entrySeed)
	if err != nil {
		return ConsumerConfig{}, err
	}
	schema, err := messageSchemaFromParams(params)
	if err != nil {
		return ConsumerConfig{}, err
	}
	tuning, err := tuningFromParams(params["tuning"])
	if err != nil {
		return ConsumerConfig{}, err
	}
	emitRetry, err := emitRetryFromParams(params["emit_retry"])
	if err != nil {
		return ConsumerConfig{}, err
	}
	cfg := ConsumerConfig{
		Brokers:       conv.NonEmptyStringSlice(params["brokers"]),
		Topic:         cast.ToString(params["topic"]),
		Group:         cast.ToString(params["group"]),
		StartOffset:   cast.ToString(params["start_offset"]),
		MaxInflight:   conv.PositiveInt(params["max_inflight"], defaultTriggerMaxInflight),
		Aggregate:     aggregate,
		MessageSchema: schema,
		EmitRetry:     emitRetry,
		Tuning:        tuning,
		ValueJSON:     cast.ToBool(params["value_json"]),
	}
	if cfg.StartOffset == "" {
		cfg.StartOffset = "latest"
	}
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" {
		return ConsumerConfig{}, fmt.Errorf("kafka brokers, topic, and group are required")
	}

	// SASL credentials from supply content (takes precedence over params).
	if supply != nil {
		if m := cast.ToString(supply["sasl_mechanism"]); m != "" {
			cfg.SASLMechanism = m
		}
		if u := cast.ToString(supply["sasl_username"]); u != "" {
			cfg.SASLUsername = u
		}
		if p := cast.ToString(supply["sasl_password"]); p != "" {
			cfg.SASLPassword = p
		}
	}
	// Validate SASL consistency: if mechanism is set, username and password are required.
	if cfg.SASLMechanism != "" {
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return ConsumerConfig{}, fmt.Errorf("kafka: sasl_mechanism %q requires sasl_username and sasl_password", cfg.SASLMechanism)
		}
	}

	return cfg, nil
}

func init() { registry.RegisterTrigger(&Node{}) }

// mergedSupplyContent merges all supply entries (keyed by node name) into a
// single flat map. When a trigger depends on exactly one supply (the typical
// case), this is a type assertion. When multiple supplies are declared, later
// entries overwrite earlier ones on key collision — acceptable because multiple
// supplies for one trigger is uncommon and the caller controls naming.
func mergedSupplyContent(supplies map[string]any) map[string]any {
	if len(supplies) == 0 {
		return nil
	}
	merged := map[string]any{}
	for _, v := range supplies {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		for k, val := range m {
			merged[k] = val
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}
