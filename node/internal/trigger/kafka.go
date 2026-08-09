package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"
)

const defaultTriggerMaxInflight = 64
const defaultKafkaAggregateMaxSize = 100
const defaultKafkaAggregateFlushInterval = 100 * time.Millisecond

// defaultKafkaEntrySeedFlushInterval is the flush timeout for the entry-seed
// batch path. It is 10x the legacy Emit default because entry-seed flushes cross
// the network to the control plane, and batching exists precisely to cut those
// round trips — a 100ms window would make 10 of them per second per partition.
//
// The legacy default is deliberately left alone: changing it would silently
// alter the data-path timing of deployments already running.
const defaultKafkaEntrySeedFlushInterval = time.Second
const kafkaAggregateByPartition = "partition"
const kafkaAggregateDedupMessage = "message"

// defaultFlushIntervalFor returns the mode-aware flush interval default.
// This only takes effect on the YAML/JSON params path; the Go DSL path
// normalizes at construction time before the mode is known — see the NOTE in
// RawParams for the documented limitation.
func defaultFlushIntervalFor(entrySeed bool) time.Duration {
	if entrySeed {
		return defaultKafkaEntrySeedFlushInterval
	}
	return defaultKafkaAggregateFlushInterval
}

type KafkaConsumer interface {
	Messages() <-chan KafkaMessage
	Close() error
}

type kafkaMessageCommitter interface {
	CommitMessages(context.Context, ...KafkaMessage) error
}

type KafkaMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
	Headers   map[string]string
}

type KafkaConsumerConfig struct {
	Brokers     []string
	Topic       string
	Group       string
	StartOffset string
	MaxInflight int
	Aggregate   KafkaAggregateConfig
	// SASL authentication. All three must be set for SASL to activate.
	SASLMechanism string // "plain", "scram-sha-256", "scram-sha-512"
	SASLUsername  string
	SASLPassword  string
	// MessageSchema optionally validates each message's JSON value before emit.
	// When non-nil, messages that fail validation are handled per
	// MessageSchema.OnInvalid.
	MessageSchema *KafkaMessageSchema
}

// KafkaMessageSchema defines a simple required-fields schema for Kafka message
// values. A message is valid when its JSON-decoded value is an object that
// contains all RequiredFields as top-level keys with non-nil values.
type KafkaMessageSchema struct {
	RequiredFields []string
	// OnInvalid selects what happens to a message that fails validation.
	// Empty means kafkaOnInvalidDiscard.
	OnInvalid string
	// DeadLetterTopic is the topic invalid messages are republished to when
	// OnInvalid is kafkaOnInvalidDeadLetter. Required in that mode.
	DeadLetterTopic string
}

// Invalid-message policies. The default is discard because that is the
// pre-existing behaviour, and silently changing a running deployment's data
// path on upgrade would be worse than the gap being closed. What changes for
// existing configs is that a discard is now counted and logged instead of
// invisible.
const (
	// kafkaOnInvalidDiscard commits the offset without emitting. The message is
	// gone, but the drop is counted (xflow_trigger_messages_discarded_total)
	// and logged at a throttled rate.
	kafkaOnInvalidDiscard = "discard"
	// kafkaOnInvalidFail declines the commit, so Kafka redelivers. Use only
	// when invalid messages are expected to be transient (e.g. a producer being
	// rolled back): a permanently malformed message blocks its partition
	// forever, which is the correct choice only if silent data loss is worse
	// than a stall.
	kafkaOnInvalidFail = "fail"
	// kafkaOnInvalidDeadLetter republishes the message to DeadLetterTopic and
	// commits only if the republish succeeded. A failed republish falls back to
	// no-commit (redelivery) rather than dropping — the whole point of a DLQ is
	// that nothing vanishes.
	kafkaOnInvalidDeadLetter = "dead_letter"
)

type KafkaAggregateConfig struct {
	Enabled       bool
	By            string
	MaxSize       int
	FlushInterval time.Duration
	Dedup         string
}

var newKafkaConsumer = newKafkaGoConsumer

type KafkaTriggerNode struct {
	nodeinternal.BaseTrigger
	BrokersValue       []string
	TopicValue         string
	GroupValue         string
	StartOffsetValue   string
	MaxInflightValue   int
	AggregateValue     KafkaAggregateConfig
	MessageSchemaValue *KafkaMessageSchema
}

func KafkaTrigger() *KafkaTriggerNode {
	return &KafkaTriggerNode{StartOffsetValue: "latest", MaxInflightValue: defaultTriggerMaxInflight}
}

func (n *KafkaTriggerNode) Brokers(brokers ...string) *KafkaTriggerNode {
	n.BrokersValue = brokers
	return n
}

func (n *KafkaTriggerNode) Topic(topic string) *KafkaTriggerNode {
	n.TopicValue = topic
	return n
}

func (n *KafkaTriggerNode) Group(group string) *KafkaTriggerNode {
	n.GroupValue = group
	return n
}

func (n *KafkaTriggerNode) StartOffset(offset string) *KafkaTriggerNode {
	n.StartOffsetValue = offset
	return n
}

func (n *KafkaTriggerNode) MaxInflight(max int) *KafkaTriggerNode {
	n.MaxInflightValue = max
	return n
}

func (n *KafkaTriggerNode) AggregateByPartition(maxSize int, flushInterval time.Duration) *KafkaTriggerNode {
	n.AggregateValue = KafkaAggregateConfig{
		Enabled:       true,
		By:            kafkaAggregateByPartition,
		MaxSize:       maxSize,
		FlushInterval: flushInterval,
		Dedup:         kafkaAggregateDedupMessage,
	}
	return n
}

func (n *KafkaTriggerNode) Aggregate(cfg KafkaAggregateConfig) *KafkaTriggerNode {
	n.AggregateValue = normalizeKafkaAggregateConfig(cfg)
	return n
}

// MessageSchema requires each message value to be a JSON object carrying all of
// fields as top-level keys. Invalid messages are discarded (offset committed,
// message dropped) but counted and logged — see DiscardInvalid/DeadLetterInvalid
// to choose a different policy.
func (n *KafkaTriggerNode) MessageSchema(fields ...string) *KafkaTriggerNode {
	n.MessageSchemaValue = &KafkaMessageSchema{RequiredFields: fields, OnInvalid: kafkaOnInvalidDiscard}
	return n
}

// FailOnInvalid switches the invalid-message policy to withholding the offset
// commit, so Kafka redelivers. Zero data loss, at the cost of a permanently
// malformed message blocking its partition forever. Requires MessageSchema.
func (n *KafkaTriggerNode) FailOnInvalid() *KafkaTriggerNode {
	if n.MessageSchemaValue != nil {
		n.MessageSchemaValue.OnInvalid = kafkaOnInvalidFail
	}
	return n
}

// DeadLetterInvalid republishes invalid messages to topic and commits only after
// a successful republish. This is the policy that neither loses messages nor
// stalls the partition. Requires MessageSchema.
func (n *KafkaTriggerNode) DeadLetterInvalid(topic string) *KafkaTriggerNode {
	if n.MessageSchemaValue != nil {
		n.MessageSchemaValue.OnInvalid = kafkaOnInvalidDeadLetter
		n.MessageSchemaValue.DeadLetterTopic = topic
	}
	return n
}

func (n *KafkaTriggerNode) Descriptor() types.Descriptor {
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
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *KafkaTriggerNode) NodeType() string { return "xflow.trigger.kafka" }
func (n *KafkaTriggerNode) RawParams() any {
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
		aggregate := normalizeKafkaAggregateConfig(n.AggregateValue)
		// NOTE: flush_interval is always serialized here, which means the Go DSL
		// path (AggregateByPartition / Aggregate) bakes the interval at construction
		// time — before we know whether the activation will be entry-seed. The
		// runtime mode-aware default (1s for entry-seed vs 100ms for legacy) only
		// takes effect on the YAML/JSON params path where flush_interval is absent
		// from the map. Go DSL users who want the entry-seed 1s default should pass
		// time.Second explicitly. Tracked as a known limitation rather than adding a
		// "was-explicitly-set" flag to KafkaAggregateConfig.
		params["aggregate"] = map[string]any{
			"enabled":        aggregate.Enabled,
			"by":             aggregate.By,
			"max_size":       aggregate.MaxSize,
			"flush_interval": aggregate.FlushInterval.String(),
			"dedup":          aggregate.Dedup,
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
	return params
}
func (n *KafkaTriggerNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *KafkaTriggerNode) TriggerHandler() types.TriggerHandler { return n }

func (n *KafkaTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := kafkaConfigFromParams(in.Params, mergedSupplyContent(in.Supplies), isEntrySeedActivation(in))
	if err != nil {
		return nil, err
	}
	consumer, err := newKafkaConsumer(cfg)
	if err != nil {
		return nil, err
	}
	// The dead-letter publisher is built only when the policy needs it, and
	// eagerly rather than on first invalid message: a broker-unreachable DLQ
	// should fail activation (which self-heals via retry) instead of surfacing
	// as an unbounded redelivery loop the first time a malformed record arrives.
	var deadLetters KafkaDeadLetterPublisher
	if cfg.MessageSchema != nil && cfg.MessageSchema.OnInvalid == kafkaOnInvalidDeadLetter {
		deadLetters, err = newKafkaDeadLetterPublisher(cfg)
		if err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("kafka trigger: dead-letter publisher: %w", err)
		}
	}
	if cfg.Aggregate.Enabled {
		return activateKafkaAggregate(ctx, in, cfg, consumer, deadLetters), nil
	}
	return activateKafkaPerMessage(ctx, in, cfg, consumer, deadLetters), nil
}

func activateKafkaPerMessage(ctx context.Context, in *types.TriggerActivateInput, cfg KafkaConsumerConfig, consumer KafkaConsumer, deadLetters KafkaDeadLetterPublisher) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &kafkaPerMessageRuntime{
		runCtx:              runCtx,
		in:                  in,
		consumer:            consumer,
		buffer:              cfg.MaxInflight,
		workers:             make(map[kafkaPartitionKey]*kafkaPartitionWorker),
		entrySeed:           isEntrySeedActivation(in),
		messageSchema:       cfg.MessageSchema,
		deadLetterPublisher: deadLetters,
	}
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

// kafkaPerMessageRuntime routes each message to a per-partition worker that
// processes messages serially: emit then commit, in offset order. This replaces
// the previous fan-out where every message ran in its own goroutine and
// committed its own offset independently — under that scheme a higher offset
// committing before a lower one caused the lower message to be skipped on
// rebalance. Per-partition serial commit preserves at-least-once ordering.
type kafkaPerMessageRuntime struct {
	runCtx    context.Context
	in        *types.TriggerActivateInput
	consumer  KafkaConsumer
	buffer    int
	mu        sync.Mutex
	closeOnce sync.Once
	workers   map[kafkaPartitionKey]*kafkaPartitionWorker
	// entrySeed selects the entry-unit seed admission path over the legacy
	// Emit path for each message. Set once at activation from the trigger
	// params (see isEntrySeedActivation).
	entrySeed bool
	// messageSchema, when non-nil, validates each message before emit. Messages
	// that fail validation are handled per messageSchema.OnInvalid.
	messageSchema *KafkaMessageSchema
	// deadLetterPublisher is non-nil only when messageSchema.OnInvalid is
	// dead_letter. Owned by this runtime: closed by close().
	deadLetterPublisher KafkaDeadLetterPublisher
}

func (r *kafkaPerMessageRuntime) schema() *KafkaMessageSchema { return r.messageSchema }

func (r *kafkaPerMessageRuntime) deadLetters() KafkaDeadLetterPublisher {
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

type kafkaPartitionWorker struct {
	key         kafkaPartitionKey
	rt          *kafkaPerMessageRuntime
	ch          chan KafkaMessage
	done        chan struct{}
	idleTimeout time.Duration
}

func (r *kafkaPerMessageRuntime) submit(ctx context.Context, msg KafkaMessage) bool {
	key := kafkaPartitionKey{topic: msg.Topic, partition: msg.Partition}
	w := r.worker(key)
	select {
	case w.ch <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *kafkaPerMessageRuntime) worker(key kafkaPartitionKey) *kafkaPartitionWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[key]; ok {
		return w
	}
	buf := r.buffer
	if buf <= 0 {
		buf = defaultTriggerMaxInflight
	}
	w := &kafkaPartitionWorker{
		key:         key,
		rt:          r,
		ch:          make(chan KafkaMessage, buf),
		done:        make(chan struct{}),
		idleTimeout: kafkaWorkerIdleTimeout,
	}
	r.workers[key] = w
	go w.run()
	return w
}

// kafkaWorkerIdleTimeout bounds how long a per-message worker idles before
// assuming its partition was revoked by rebalance and self-terminating. Without
// it, a revoked partition's worker goroutine and map entry would leak for the
// process lifetime (kafka-go exposes no revocation callback).
const kafkaWorkerIdleTimeout = 5 * time.Minute

func (r *kafkaPerMessageRuntime) close(ctx context.Context) {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		workers := make([]*kafkaPartitionWorker, 0, len(r.workers))
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
func (r *kafkaPerMessageRuntime) evictWorker(key kafkaPartitionKey, w *kafkaPartitionWorker) {
	r.mu.Lock()
	if existing, ok := r.workers[key]; ok && existing == w {
		delete(r.workers, key)
	}
	r.mu.Unlock()
}

func (w *kafkaPartitionWorker) run() {
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
			if w.rt.messageSchema != nil && !validateKafkaMessageSchema(msg, w.rt.messageSchema) {
				if handleInvalidKafkaMessage(w.rt.runCtx, w.rt, msg) {
					_ = commitKafkaMessages(context.Background(), w.rt.consumer, msg)
				}
			} else if w.rt.entrySeed {
				// Entry-seed mode: admission drives the seed, which commits the
				// offset internally on accept/duplicate/conflict. Do NOT
				// double-commit here.
				_ = seedKafkaEntryBatch(w.rt.runCtx, w.rt.in, w.rt.consumer, msg)
			} else if emitKafkaMessage(w.rt.runCtx, w.rt.in, msg) {
				_ = commitKafkaMessages(context.Background(), w.rt.consumer, msg)
			}
		case <-idleTimer.C:
			// No message for the idle window: assume the partition was revoked
			// and self-terminate to reclaim the goroutine and map entry.
			return
		}
	}
}

type kafkaPartitionKey struct {
	topic     string
	partition int
}

type kafkaAggregateRuntime struct {
	in          *types.TriggerActivateInput
	cfg         KafkaAggregateConfig
	consumer    KafkaConsumer
	emitSem     chan struct{}
	mu          sync.Mutex
	closeOnce   sync.Once
	aggregators map[kafkaPartitionKey]*kafkaPartitionAggregator
	// messageSchema validates each message BEFORE it enters a batch. An earlier
	// revision omitted this field entirely, so an aggregate-mode trigger that
	// declared message_schema had it silently ignored — validation existed only
	// on the per-message path. Filtering pre-batch (rather than post-flush) is
	// what keeps one malformed record from invalidating a whole batch.
	messageSchema *KafkaMessageSchema
	// entrySeed selects the entry-unit seed admission path over the legacy Emit
	// path when a batch flushes. Set once at activation (see isEntrySeedActivation).
	entrySeed bool
	// deadLetterPublisher is non-nil only when messageSchema.OnInvalid is
	// dead_letter. Owned by this runtime: closed by close().
	deadLetterPublisher KafkaDeadLetterPublisher
}

func (r *kafkaAggregateRuntime) schema() *KafkaMessageSchema { return r.messageSchema }

func (r *kafkaAggregateRuntime) deadLetters() KafkaDeadLetterPublisher {
	return r.deadLetterPublisher
}

type kafkaPartitionAggregator struct {
	key         kafkaPartitionKey
	rt          *kafkaAggregateRuntime
	ch          chan KafkaMessage
	done        chan struct{}
	idleTimeout time.Duration
}

func activateKafkaAggregate(ctx context.Context, in *types.TriggerActivateInput, cfg KafkaConsumerConfig, consumer KafkaConsumer, deadLetters KafkaDeadLetterPublisher) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &kafkaAggregateRuntime{
		in:                  in,
		cfg:                 cfg.Aggregate,
		consumer:            consumer,
		emitSem:             make(chan struct{}, cfg.MaxInflight),
		aggregators:         make(map[kafkaPartitionKey]*kafkaPartitionAggregator),
		messageSchema:       cfg.MessageSchema,
		entrySeed:           isEntrySeedActivation(in),
		deadLetterPublisher: deadLetters,
	}
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

func (r *kafkaAggregateRuntime) submit(ctx context.Context, msg KafkaMessage) bool {
	key := kafkaPartitionKey{topic: msg.Topic, partition: msg.Partition}
	agg := r.aggregator(key)
	select {
	case agg.ch <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *kafkaAggregateRuntime) aggregator(key kafkaPartitionKey) *kafkaPartitionAggregator {
	r.mu.Lock()
	defer r.mu.Unlock()
	agg, ok := r.aggregators[key]
	if ok {
		return agg
	}
	agg = &kafkaPartitionAggregator{
		key:         key,
		rt:          r,
		ch:          make(chan KafkaMessage, r.cfg.MaxSize),
		done:        make(chan struct{}),
		idleTimeout: kafkaAggregatorIdleTimeout(r.cfg.FlushInterval),
	}
	r.aggregators[key] = agg
	go agg.run()
	return agg
}

// evictAggregator removes an idle aggregator that self-terminated. kafka-go does
// not expose partition-revocation callbacks, so an aggregator whose partition
// was revoked by a rebalance would otherwise block on an empty channel forever
// (leaking the goroutine and map entry for the process lifetime). The
// aggregator instead exits after an idle window; this call reclaims its entry
// so a later message for that partition lazily spawns a fresh aggregator.
func (r *kafkaAggregateRuntime) evictAggregator(key kafkaPartitionKey, agg *kafkaPartitionAggregator) {
	r.mu.Lock()
	if existing, ok := r.aggregators[key]; ok && existing == agg {
		delete(r.aggregators, key)
	}
	r.mu.Unlock()
}

// kafkaAggregatorIdleTimeout bounds how long an aggregator waits for a new
// message before assuming its partition was revoked and self-terminating. Set
// well above FlushInterval so a low-traffic but still-assigned partition is not
// prematurely reaped; the next message simply spawns a new aggregator.
func kafkaAggregatorIdleTimeout(flushInterval time.Duration) time.Duration {
	idle := flushInterval * 10
	if idle < 5*time.Second {
		idle = 5 * time.Second
	}
	return idle
}

func (r *kafkaAggregateRuntime) close(ctx context.Context) {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		aggregators := make([]*kafkaPartitionAggregator, 0, len(r.aggregators))
		for _, agg := range r.aggregators {
			aggregators = append(aggregators, agg)
		}
		r.mu.Unlock()
		for _, agg := range aggregators {
			close(agg.ch)
		}
		// Close the publisher only after every aggregator has drained, so a
		// final flush's dead-letter publishes are not cut off. Deferred because
		// the ctx.Done() path below returns early.
		defer func() {
			if r.deadLetterPublisher != nil {
				_ = r.deadLetterPublisher.Close()
			}
		}()
		for _, agg := range aggregators {
			select {
			case <-agg.done:
			case <-ctx.Done():
				return
			}
		}
	})
}

func (a *kafkaPartitionAggregator) run() {
	defer close(a.done)
	defer a.rt.evictAggregator(a.key, a)
	var buffer []KafkaMessage
	// discarded holds offsets of schema-invalid messages that were resolved
	// (discarded or dead-lettered) but whose offsets must NOT be committed
	// independently. Committing one immediately would move the group offset past
	// lower offsets still sitting in buffer, so a rebalance right then would skip
	// them — the same skip hazard the per-partition serial design exists to
	// prevent. They ride along with the next successful flush instead.
	var discarded []KafkaMessage
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	timerActive := false
	defer timer.Stop()
	// idleTimer reaps the aggregator after a quiet window so a partition
	// revoked by rebalance does not leak the goroutine and map entry.
	idleTimer := time.NewTimer(a.idleTimeout)
	defer idleTimer.Stop()
	for {
		select {
		case msg, ok := <-a.ch:
			if !ok {
				a.flush(context.Background(), buffer, discarded, "close")
				return
			}
			// A new message arrived: this partition is still assigned — reset
			// the idle reap window.
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(a.idleTimeout)
			// Schema validation happens here, before the message joins a batch,
			// so one malformed record cannot invalidate an otherwise good batch.
			if a.rt.messageSchema != nil && !validateKafkaMessageSchema(msg, a.rt.messageSchema) {
				if handleInvalidKafkaMessage(context.Background(), a.rt, msg) {
					discarded = append(discarded, msg)
					// Start the flush timer even for an all-invalid stream, or
					// these offsets would sit uncommitted until a valid message
					// happened to arrive.
					if len(buffer) == 0 && len(discarded) == 1 {
						resetKafkaAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
					}
				}
				continue
			}
			// No pre-emit dedup: a message must never be marked "seen" before its
			// side effect is durable, or a crash between the two loses it for good.
			// At-least-once here rests on flush's emit-then-commit ordering, and the
			// downstream absorbs duplicates via the host idempotency contract.
			buffer = append(buffer, msg)
			if len(buffer) == 1 {
				resetKafkaAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
			}
			if len(buffer) >= a.rt.cfg.MaxSize {
				if a.flush(context.Background(), buffer, discarded, "size") {
					buffer = nil
					discarded = nil
					stopKafkaAggregateTimer(timer, &timerActive)
				}
			}
		case <-timer.C:
			timerActive = false
			if a.flush(context.Background(), buffer, discarded, "timeout") {
				buffer = nil
				discarded = nil
			} else if len(buffer) > 0 || len(discarded) > 0 {
				resetKafkaAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
			}
		case <-idleTimer.C:
			// No message for the idle window: assume the partition was revoked
			// by a rebalance. Flush any pending buffer, then self-terminate so
			// the goroutine and map entry are reclaimed. A failed flush here
			// drops the in-memory buffer, which is safe: the offsets were never
			// committed, so Kafka redelivers to whoever owns the partition next.
			if len(buffer) > 0 || len(discarded) > 0 {
				a.flush(context.Background(), buffer, discarded, "idle")
			}
			return
		}
	}
}

// flush emits messages as one batch and commits their offsets, plus the offsets
// of any discarded messages that were withheld from independent commit.
//
// discarded offsets are committed only alongside a successful emit, or on their
// own when there is nothing to emit. They are never committed after a failed
// emit: the failed batch will be redelivered from the lowest uncommitted offset,
// and advancing past a discarded offset that sits below it would skip valid
// messages.
func (a *kafkaPartitionAggregator) flush(ctx context.Context, messages, discarded []KafkaMessage, trigger string) bool {
	if len(messages) == 0 {
		if len(discarded) == 0 {
			return true
		}
		// Nothing to emit — an all-invalid window. Commit the resolved offsets
		// so the group does not stall on messages that will never be emitted.
		_ = commitKafkaMessages(ctx, a.rt.consumer, discarded...)
		return true
	}
	select {
	case a.rt.emitSem <- struct{}{}:
		defer func() { <-a.rt.emitSem }()
	case <-ctx.Done():
		return false
	}
	// Entry-seed mode admits the batch to the control plane instead of emitting
	// locally. Both paths share the SAME commit rule below: the batch is only
	// durable-enough-to-commit after the side effect succeeded.
	if a.rt.entrySeed {
		// A trigger-group activation's Runtime additionally implements
		// types.GroupExecRuntime (see groupExecTriggerRuntime in
		// service/runner) — when present, run the batch through the group's
		// real member nodes and admit the real resulting exits instead of
		// the raw-message exits the plain EntrySeedRuntime path would
		// synthesize.
		if gr, ok := a.rt.in.Runtime.(interface {
			types.EntrySeedRuntime
			types.GroupExecRuntime
		}); ok {
			if !seedKafkaEntryBatchViaGroupExec(ctx, a.rt.in, gr, messages) {
				return false
			}
		} else if rt, ok := a.rt.in.Runtime.(types.EntrySeedRuntime); ok {
			if !seedKafkaEntryBatchMessages(ctx, a.rt.in, rt, messages) {
				return false
			}
		} else {
			// isEntrySeedActivation already required at least EntrySeedRuntime,
			// so reaching here means the runtime changed under us. Withhold the
			// commit rather than silently falling back to Emit with a different
			// key space.
			return false
		}
	} else {
		event := kafkaBatchEvent(a.rt.in.NodeName, messages)
		if _, err := a.rt.in.Emit(ctx, event); err != nil {
			return false
		}
	}
	obs().OnBatchFlushed(ctx, messages[0].Topic, trigger, len(messages))
	// Copy rather than append(messages, discarded...): appending would write
	// into buffer's spare capacity, aliasing a slice the caller still holds.
	commits := make([]KafkaMessage, 0, len(messages)+len(discarded))
	commits = append(commits, messages...)
	commits = append(commits, discarded...)
	_ = commitKafkaMessages(ctx, a.rt.consumer, commits...)
	return true
}

// emitKafkaMessage is the legacy single-message emit path, used only when the
// runtime does NOT implement types.EntrySeedRuntime (see isEntrySeedActivation).
// It emits directly and lets the per-partition serial worker commit the offset
// only after Emit succeeds (kafkaPartitionWorker.run) — offset durability
// follows the side effect, never precedes it.
//
// P0-1: the previous implementation ran a pre-emit Dedup SETNX here. If the
// process crashed after the SETNX marked the message "seen" but before Emit, the
// message was lost forever (redelivery saw the dedup marker and skipped it). The
// SETNX has been removed: the ordered emit-then-commit is the at-least-once
// guarantee. The downstream is idempotent (host idempotency contract), so a
// possible duplicate on crash-after-emit-before-commit is safe.
func emitKafkaMessage(ctx context.Context, in *types.TriggerActivateInput, msg KafkaMessage) bool {
	event := kafkaSingleEvent(in.NodeName, msg)
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	if _, err := in.Emit(ctx, event); err != nil {
		return false
	}
	return true
}

func commitKafkaMessages(ctx context.Context, consumer KafkaConsumer, messages ...KafkaMessage) error {
	if len(messages) == 0 {
		return nil
	}
	committer, ok := consumer.(kafkaMessageCommitter)
	if !ok {
		return nil
	}
	return committer.CommitMessages(ctx, messages...)
}

func kafkaSingleEvent(nodeName string, msg KafkaMessage) *types.TriggerEvent {
	event := &types.TriggerEvent{
		ID:      kafkaMessageID(msg),
		Kind:    "kafka",
		Source:  nodeName,
		Time:    msg.Time,
		Headers: msg.Headers,
		Data:    kafkaSingleEventData(msg),
		Raw:     msg.Value,
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	return event
}

func kafkaBatchEvent(nodeName string, messages []KafkaMessage) *types.TriggerEvent {
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
			"messages":     kafkaMessageDataList(messages),
		},
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	return event
}

func kafkaSingleEventData(msg KafkaMessage) map[string]any {
	data := kafkaMessageData(msg)
	data["count"] = 1
	data["messages"] = []map[string]any{kafkaMessageData(msg)}
	return data
}

func kafkaMessageDataList(messages []KafkaMessage) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	for _, msg := range messages {
		out = append(out, kafkaMessageData(msg))
	}
	return out
}

func kafkaMessageData(msg KafkaMessage) map[string]any {
	return map[string]any{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
		"value":     string(msg.Value),
		"headers":   msg.Headers,
		"time":      msg.Time,
	}
}

func kafkaMessageID(msg KafkaMessage) string {
	return fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
}

func resetKafkaAggregateTimer(timer *time.Timer, active *bool, d time.Duration) {
	if *active {
		timer.Stop()
	}
	timer.Reset(d)
	*active = true
}

func stopKafkaAggregateTimer(timer *time.Timer, active *bool) {
	if !*active {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	*active = false
}

func kafkaConfigFromParams(params map[string]any, supply map[string]any, entrySeed bool) (KafkaConsumerConfig, error) {
	aggregate, err := kafkaAggregateConfigFromParamForMode(params["aggregate"], entrySeed)
	if err != nil {
		return KafkaConsumerConfig{}, err
	}
	schema, err := kafkaMessageSchemaFromParams(params)
	if err != nil {
		return KafkaConsumerConfig{}, err
	}
	cfg := KafkaConsumerConfig{
		Brokers:       conv.NonEmptyStringSlice(params["brokers"]),
		Topic:         cast.ToString(params["topic"]),
		Group:         cast.ToString(params["group"]),
		StartOffset:   cast.ToString(params["start_offset"]),
		MaxInflight:   conv.PositiveInt(params["max_inflight"], defaultTriggerMaxInflight),
		Aggregate:     aggregate,
		MessageSchema: schema,
	}
	if cfg.StartOffset == "" {
		cfg.StartOffset = "latest"
	}
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" {
		return KafkaConsumerConfig{}, fmt.Errorf("kafka brokers, topic, and group are required")
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
			return KafkaConsumerConfig{}, fmt.Errorf("kafka: sasl_mechanism %q requires sasl_username and sasl_password", cfg.SASLMechanism)
		}
	}

	return cfg, nil
}

func kafkaAggregateConfigFromParamForMode(v any, entrySeed bool) (KafkaAggregateConfig, error) {
	if v == nil {
		return KafkaAggregateConfig{}, nil
	}
	raw, ok := v.(map[string]any)
	if !ok {
		rawAny, err := cast.ToStringMapE(v)
		if err != nil {
			return KafkaAggregateConfig{}, fmt.Errorf("kafka aggregate must be an object")
		}
		raw = rawAny
	}
	cfg := KafkaAggregateConfig{
		Enabled:       cast.ToBool(raw["enabled"]),
		By:            cast.ToString(raw["by"]),
		MaxSize:       conv.PositiveInt(raw["max_size"], defaultKafkaAggregateMaxSize),
		FlushInterval: defaultFlushIntervalFor(entrySeed),
		Dedup:         cast.ToString(raw["dedup"]),
	}
	if !cfg.Enabled {
		return KafkaAggregateConfig{}, nil
	}
	if raw["flush_interval"] != nil {
		flushInterval, err := conv.PositiveDuration(raw["flush_interval"])
		if err != nil {
			return KafkaAggregateConfig{}, fmt.Errorf("kafka aggregate flush_interval: %w", err)
		}
		cfg.FlushInterval = flushInterval
	}
	cfg = normalizeKafkaAggregateConfig(cfg)
	if cfg.By != kafkaAggregateByPartition {
		return KafkaAggregateConfig{}, fmt.Errorf("kafka aggregate by %q is not supported", cfg.By)
	}
	if cfg.Dedup != kafkaAggregateDedupMessage {
		return KafkaAggregateConfig{}, fmt.Errorf("kafka aggregate dedup %q is not supported", cfg.Dedup)
	}
	return cfg, nil
}

func normalizeKafkaAggregateConfig(cfg KafkaAggregateConfig) KafkaAggregateConfig {
	if !cfg.Enabled {
		return KafkaAggregateConfig{}
	}
	if cfg.By == "" {
		cfg.By = kafkaAggregateByPartition
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = defaultKafkaAggregateMaxSize
	}
	// This fallback only fires on the Go DSL path (Aggregate/AggregateByPartition
	// at construction time) where the mode is not yet known. The runtime params
	// path sets mode-aware defaults before calling normalize, so this branch is
	// unreachable there. Always falls back to the legacy 100ms — entry-seed mode
	// is handled upstream by kafkaAggregateConfigFromParamForMode.
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultKafkaAggregateFlushInterval
	}
	if cfg.Dedup == "" {
		cfg.Dedup = kafkaAggregateDedupMessage
	}
	return cfg
}

func init() { registry.RegisterTrigger(&KafkaTriggerNode{}) }

// ---------------------------------------------------------------------------
// Message schema validation
// ---------------------------------------------------------------------------

// validateKafkaMessageSchema checks whether a Kafka message's value conforms to
// the declared schema. The value must be valid JSON and contain all required
// fields as top-level keys with non-nil values.
func validateKafkaMessageSchema(msg KafkaMessage, schema *KafkaMessageSchema) bool {
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

// handleInvalidKafkaMessage applies the schema's OnInvalid policy to a message
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
func handleInvalidKafkaMessage(ctx context.Context, rt invalidMessageHandler, msg KafkaMessage) (commit bool) {
	schema := rt.schema()
	policy := schema.OnInvalid
	if policy == "" {
		policy = kafkaOnInvalidDiscard
	}
	switch policy {
	case kafkaOnInvalidFail:
		obs().OnMessageDiscarded(ctx, msg.Topic, "schema_fail")
		logInvalidKafkaMessage(msg, "schema_fail", "message withheld from commit for redelivery")
		return false
	case kafkaOnInvalidDeadLetter:
		publisher := rt.deadLetters()
		if publisher == nil {
			// Activation validated the config, so a nil publisher here means the
			// construction seam returned nil without an error. Withhold the
			// commit rather than fall through to a drop.
			obs().OnMessageDeadLettered(ctx, msg.Topic, "error")
			logInvalidKafkaMessage(msg, "dead_letter", "dead-letter publisher unavailable; withholding commit")
			return false
		}
		if err := publisher.Publish(ctx, schema.DeadLetterTopic, msg); err != nil {
			obs().OnMessageDeadLettered(ctx, msg.Topic, "error")
			logInvalidKafkaMessage(msg, "dead_letter", "dead-letter publish failed: "+err.Error())
			return false
		}
		obs().OnMessageDeadLettered(ctx, msg.Topic, "ok")
		return true
	default:
		obs().OnMessageDiscarded(ctx, msg.Topic, "schema")
		logInvalidKafkaMessage(msg, "schema", "message discarded")
		return true
	}
}

// invalidMessageHandler is the narrow view of a runtime that
// handleInvalidKafkaMessage needs, so the per-message and aggregate runtimes
// share one policy implementation rather than each growing its own copy.
type invalidMessageHandler interface {
	schema() *KafkaMessageSchema
	deadLetters() KafkaDeadLetterPublisher
}

// logInvalidKafkaMessage emits a throttled log line. It never logs the message
// value: a malformed record is still production traffic and may carry
// credentials or PII. Topic/partition/offset are enough to fetch the record
// deliberately with a separate tool.
func logInvalidKafkaMessage(msg KafkaMessage, reason, action string) {
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

// kafkaMessageSchemaFromParams parses the optional message_schema param into a
// KafkaMessageSchema. Returns nil when no schema is declared (the common case).
// The param format is:
//
//	{"required_fields": ["field1"], "on_invalid": "discard|fail|dead_letter",
//	 "dead_letter_topic": "events-dlq"}
//
// An unrecognized on_invalid, or dead_letter without a topic, is an error
// rather than a silent fallback to discard: a config that asked not to lose
// messages must never be quietly downgraded to the policy that loses them.
func kafkaMessageSchemaFromParams(params map[string]any) (*KafkaMessageSchema, error) {
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
	schema := &KafkaMessageSchema{
		RequiredFields:  fields,
		OnInvalid:       strings.ToLower(strings.TrimSpace(cast.ToString(m["on_invalid"]))),
		DeadLetterTopic: strings.TrimSpace(cast.ToString(m["dead_letter_topic"])),
	}
	if schema.OnInvalid == "" {
		schema.OnInvalid = kafkaOnInvalidDiscard
	}
	switch schema.OnInvalid {
	case kafkaOnInvalidDiscard, kafkaOnInvalidFail:
	case kafkaOnInvalidDeadLetter:
		if schema.DeadLetterTopic == "" {
			return nil, fmt.Errorf("kafka message_schema on_invalid %q requires dead_letter_topic", schema.OnInvalid)
		}
	default:
		return nil, fmt.Errorf("kafka message_schema on_invalid %q is not supported (supported: %s, %s, %s)",
			schema.OnInvalid, kafkaOnInvalidDiscard, kafkaOnInvalidFail, kafkaOnInvalidDeadLetter)
	}
	return schema, nil
}

// ---------------------------------------------------------------------------
// Entry-seed mode: admission-based emit (Milestone G)
// ---------------------------------------------------------------------------

// seedKafkaEntryBatch processes one message through the entry-unit (single node
// or group node) seed admission path. Instead of Emit+Dedup, it calls
// SeedExecutionFromEntry on the runtime. Only accepted/duplicate-accepted/conflict
// responses commit the Kafka offset. Transient errors return false (no commit →
// Kafka redelivery).
//
// This function is the entry-seed analogue of emitKafkaMessage for the
// per-partition serial worker. It is NOT used by the legacy Emit path.
func seedKafkaEntryBatch(ctx context.Context, in *types.TriggerActivateInput, consumer KafkaConsumer, msg KafkaMessage) bool {
	rt, ok := in.Runtime.(types.EntrySeedRuntime)
	if !ok {
		// Fallback: runtime does not support entry-seed. This should not happen
		// in a properly configured entry-seed activation.
		return false
	}

	entryUnitID, _ := in.Params["entry_unit_id"].(string)
	if entryUnitID == "" {
		// Single-node entry unit ID = node name (spec §11.5).
		entryUnitID = in.NodeName
	}
	workflowVersion, _ := in.Params["workflow_version"].(string)

	// Build the admission key from the message's stable source identity.
	admissionKey := fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d-%d",
		"", // namespace is set server-side
		in.WorkflowID, workflowVersion, entryUnitID,
		msg.Topic, msg.Partition, msg.Offset, msg.Offset)

	// Build exits — for a single-message entry unit, the output is the message data.
	exits := []types.BoundaryExit{{
		NodeName: in.NodeName,
		Port:     "main",
		Data: map[string]any{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"key":       string(msg.Key),
			"value":     string(msg.Value),
		},
	}}

	req := types.EntrySeedRequest{
		AdmissionKey:    admissionKey,
		WorkflowID:      in.WorkflowID,
		WorkflowVersion: workflowVersion,
		EntryUnitID:     entryUnitID,
		Outcome:         "success",
		Exits:           exits,
	}

	resp, err := rt.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		// Transient error (network timeout, etc.) — do NOT commit offset.
		// Kafka will redeliver the message.
		return false
	}

	// Accepted, duplicate-accepted, or conflict: the admission was handled.
	// Commit the Kafka offset regardless — for conflict, another runner already
	// admitted a result for this key, so the message is consumed.
	if resp.Accepted || resp.Duplicate || resp.Conflict {
		if commitErr := commitKafkaMessages(ctx, consumer, msg); commitErr != nil {
			// Commit failed — the message will be redelivered. On redelivery,
			// SeedExecutionFromEntry returns duplicate-accepted, which is safe.
			return false
		}
		return true
	}

	// Unknown state — defensive: don't commit.
	return false
}

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
