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
	"sync"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"

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

func (n *Node) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.kafka",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Kafka Trigger",
		Params: []types.ParamSpec{
			{Name: "brokers", DisplayName: "Brokers", Type: types.ParamArray, Required: true},
			{Name: "topic", DisplayName: "Topic", Type: types.ParamString, Required: true},
			{Name: "group", DisplayName: "Group", Type: types.ParamString, Required: true},
			{Name: "start_offset", DisplayName: "Start Offset", Type: types.ParamString, Default: "latest"},
			{Name: "max_inflight", DisplayName: "Max Inflight", Type: types.ParamNumber, Default: float64(defaultTriggerMaxInflight)},
			{Name: "aggregate", DisplayName: "Aggregate", Type: types.ParamObject, Description: "Optional partition batch aggregation: enabled, by, max_size, flush_interval, dedup. Under entry-seed hosting the batch is admitted to the control plane instead of emitted locally, with an admission key covering the batch's actual offset range; delivery is at-least-once (a batch may be reprocessed once if its offsets fail to commit), so consumers must be idempotent on (topic, partition, offset). flush_interval defaults to 1s in entry-seed mode and 100ms on the legacy emit path."},
			{Name: "message_schema", DisplayName: "Message Schema", Type: types.ParamObject, Description: "Optional message validation: {required_fields: [\"f\"], on_invalid: \"discard|fail|dead_letter\", dead_letter_topic: \"t-dlq\"}. on_invalid defaults to discard (offset committed, message dropped, drop counted and logged)."},
			{Name: "tuning", DisplayName: "Tuning", Type: types.ParamObject, Description: "Optional consumer tuning: fetch_min_bytes (1), fetch_max_bytes (10000000), max_wait (10s), dial_timeout (10s), session_timeout (30s), heartbeat_interval (3s), rebalance_timeout (30s). Durations are strings (\"45s\"). heartbeat_interval must stay below session_timeout or the group rebalances continuously. Offsets always commit synchronously after the side effect; that is not tunable."},
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
			params["aggregate"].(map[string]any)["on_overflow"] = aggregate.OnOverflow
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
	// The dead-letter publisher is built only when the policy needs it, and
	// eagerly rather than on first invalid message: a broker-unreachable DLQ
	// should fail activation (which self-heals via retry) instead of surfacing
	// as an unbounded redelivery loop the first time a malformed record arrives.
	var deadLetters DeadLetterPublisher
	if cfg.MessageSchema != nil && cfg.MessageSchema.OnInvalid == onInvalidDeadLetter {
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
		deadLetterPublisher: deadLetters,
	}
	rt.valueJSON = resolveValueJSON(cfg, in, rt.entrySeed)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer rt.close(context.Background())
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
	return types.CloseFunc(func(closeCtx context.Context) error {
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
	})
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
	// deadLetterPublisher is non-nil only when messageSchema.OnInvalid is
	// dead_letter. Owned by this runtime: closed by close().
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
			// Partition still assigned — reset the idle reap window.
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(w.idleTimeout)
			// Serial per-partition processing: emit then commit in offset order so
			// a rebalance can never skip a lower offset whose higher peer committed
			// first. Emit failure skips commit, leaving the message redelivered.
			//
			// Schema validation runs BEFORE the mode split. An earlier revision
			// chained it as `else if` after the entry-seed branch, so entry-seed
			// activations silently ignored a declared schema — the one mode where
			// invalid content is most expensive, because a seeded execution is
			// durable.
			if w.rt.messageSchema != nil && !validateMessageSchema(msg, w.rt.messageSchema) {
				if handleInvalidMessage(w.rt.runCtx, w.rt, msg) {
					_ = commitMessages(w.rt.baseCtx, w.rt.consumer, msg)
				}
			} else if w.rt.entrySeed {
				// Entry-seed mode: admission drives the seed, which commits the
				// offset internally on accept/duplicate/conflict. Do NOT
				// double-commit here.
				_ = seedEntryBatch(w.rt.runCtx, w.rt.in, w.rt.consumer, msg)
			} else if emitMessage(w.rt.runCtx, w.rt.in, msg) {
				_ = commitMessages(w.rt.baseCtx, w.rt.consumer, msg)
			}
		case <-idleTimer.C:
			// No message for the idle window: assume the partition was revoked
			// and self-terminate to reclaim the goroutine and map entry.
			return
		}
	}
}

type partitionKey struct {
	topic     string
	partition int
}

// emitMessage is the legacy single-message emit path, used only when the
// runtime does NOT implement types.EntrySeedRuntime (see isEntrySeedActivation).
// It emits directly and lets the per-partition serial worker commit the offset
// only after Emit succeeds (partitionWorker.run) — offset durability
// follows the side effect, never precedes it.
//
// P0-1: the previous implementation ran a pre-emit Dedup SETNX here. If the
// process crashed after the SETNX marked the message "seen" but before Emit, the
// message was lost forever (redelivery saw the dedup marker and skipped it). The
// SETNX has been removed: the ordered emit-then-commit is the at-least-once
// guarantee. The downstream is idempotent (host idempotency contract), so a
// possible duplicate on crash-after-emit-before-commit is safe.
func emitMessage(ctx context.Context, in *types.TriggerActivateInput, msg Message) bool {
	event := singleEvent(in.NodeName, msg)
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	if _, err := in.Emit(ctx, event); err != nil {
		return false
	}
	return true
}

// commitMessages advances the committed offset for messages, and is the single
// layer every commit path in this package passes through — the aggregate
// batch commit (aggregate.go), the entry-seed per-message commit
// (entryseed.go) and both legacy per-message paths below. That is why the
// observation lives here rather than at the call sites: a commit added later
// through a fourth path is measured without anyone remembering to measure it.
//
// The timing brackets ONLY the broker round trip. The two early returns above
// it are not commits and must not be reported as instant ones — an empty
// message set is a no-op, and a consumer without messageCommitter (every test
// fake that does not model offsets) would otherwise flood the histogram with
// zero-duration samples and make the real distribution unreadable.
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
	cfg := ConsumerConfig{
		Brokers:       conv.NonEmptyStringSlice(params["brokers"]),
		Topic:         cast.ToString(params["topic"]),
		Group:         cast.ToString(params["group"]),
		StartOffset:   cast.ToString(params["start_offset"]),
		MaxInflight:   conv.PositiveInt(params["max_inflight"], defaultTriggerMaxInflight),
		Aggregate:     aggregate,
		MessageSchema: schema,
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
