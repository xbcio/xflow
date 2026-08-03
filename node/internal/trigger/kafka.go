package trigger

import (
	"context"
	"fmt"
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
const kafkaAggregateByPartition = "partition"
const kafkaAggregateDedupMessage = "message"

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
}

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
	BrokersValue     []string
	TopicValue       string
	GroupValue       string
	StartOffsetValue string
	MaxInflightValue int
	AggregateValue   KafkaAggregateConfig
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
			{Name: "aggregate", DisplayName: "Aggregate", Type: types.ParamObject, Description: "Optional partition batch aggregation: enabled, by, max_size, flush_interval, dedup"},
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
		params["aggregate"] = map[string]any{
			"enabled":        aggregate.Enabled,
			"by":             aggregate.By,
			"max_size":       aggregate.MaxSize,
			"flush_interval": aggregate.FlushInterval.String(),
			"dedup":          aggregate.Dedup,
		}
	}
	return params
}
func (n *KafkaTriggerNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *KafkaTriggerNode) TriggerHandler() types.TriggerHandler { return n }

func (n *KafkaTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := kafkaConfigFromParams(in.Params, mergedSupplyContent(in.Supplies))
	if err != nil {
		return nil, err
	}
	consumer, err := newKafkaConsumer(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Aggregate.Enabled {
		// The aggregate path emits batches through the legacy Runtime.Emit and has
		// no entry-seed admission equivalent (a batch admission key would have to
		// express an offset range that the control-plane fence accepts as the same
		// key across retries). Fail closed instead of activating a consumer whose
		// every flush would error and replay forever.
		if isEntrySeedActivation(in) {
			_ = consumer.Close()
			return nil, fmt.Errorf("kafka trigger: aggregate mode is not supported for entry-seed hosting")
		}
		return activateKafkaAggregate(ctx, in, cfg, consumer), nil
	}
	return activateKafkaPerMessage(ctx, in, cfg, consumer), nil
}

func activateKafkaPerMessage(ctx context.Context, in *types.TriggerActivateInput, cfg KafkaConsumerConfig, consumer KafkaConsumer) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &kafkaPerMessageRuntime{
		runCtx:    runCtx,
		in:        in,
		consumer:  consumer,
		buffer:    cfg.MaxInflight,
		workers:   make(map[kafkaPartitionKey]*kafkaPartitionWorker),
		entrySeed: isEntrySeedActivation(in),
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
			if w.rt.entrySeed {
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
}

type kafkaPartitionAggregator struct {
	key         kafkaPartitionKey
	rt          *kafkaAggregateRuntime
	ch          chan KafkaMessage
	done        chan struct{}
	idleTimeout time.Duration
}

func activateKafkaAggregate(ctx context.Context, in *types.TriggerActivateInput, cfg KafkaConsumerConfig, consumer KafkaConsumer) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &kafkaAggregateRuntime{
		in:          in,
		cfg:         cfg.Aggregate,
		consumer:    consumer,
		emitSem:     make(chan struct{}, cfg.MaxInflight),
		aggregators: make(map[kafkaPartitionKey]*kafkaPartitionAggregator),
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
				a.flush(context.Background(), buffer)
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
			// No pre-emit dedup: a message must never be marked "seen" before its
			// side effect is durable, or a crash between the two loses it for good.
			// At-least-once here rests on flush's emit-then-commit ordering, and the
			// downstream absorbs duplicates via the host idempotency contract.
			buffer = append(buffer, msg)
			if len(buffer) == 1 {
				resetKafkaAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
			}
			if len(buffer) >= a.rt.cfg.MaxSize {
				if a.flush(context.Background(), buffer) {
					buffer = nil
					stopKafkaAggregateTimer(timer, &timerActive)
				}
			}
		case <-timer.C:
			timerActive = false
			if a.flush(context.Background(), buffer) {
				buffer = nil
			} else if len(buffer) > 0 {
				resetKafkaAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
			}
		case <-idleTimer.C:
			// No message for the idle window: assume the partition was revoked
			// by a rebalance. Flush any pending buffer, then self-terminate so
			// the goroutine and map entry are reclaimed. A failed flush here
			// drops the in-memory buffer, which is safe: the offsets were never
			// committed, so Kafka redelivers to whoever owns the partition next.
			if len(buffer) > 0 {
				a.flush(context.Background(), buffer)
			}
			return
		}
	}
}

func (a *kafkaPartitionAggregator) flush(ctx context.Context, messages []KafkaMessage) bool {
	if len(messages) == 0 {
		return true
	}
	select {
	case a.rt.emitSem <- struct{}{}:
		defer func() { <-a.rt.emitSem }()
	case <-ctx.Done():
		return false
	}
	event := kafkaBatchEvent(a.rt.in.NodeName, messages)
	if _, err := a.rt.in.Emit(ctx, event); err != nil {
		return false
	}
	_ = commitKafkaMessages(ctx, a.rt.consumer, messages...)
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

func kafkaConfigFromParams(params map[string]any, supply map[string]any) (KafkaConsumerConfig, error) {
	aggregate, err := kafkaAggregateConfigFromParam(params["aggregate"])
	if err != nil {
		return KafkaConsumerConfig{}, err
	}
	cfg := KafkaConsumerConfig{
		Brokers:     conv.NonEmptyStringSlice(params["brokers"]),
		Topic:       cast.ToString(params["topic"]),
		Group:       cast.ToString(params["group"]),
		StartOffset: cast.ToString(params["start_offset"]),
		MaxInflight: conv.PositiveInt(params["max_inflight"], defaultTriggerMaxInflight),
		Aggregate:   aggregate,
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

func kafkaAggregateConfigFromParam(v any) (KafkaAggregateConfig, error) {
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
		FlushInterval: defaultKafkaAggregateFlushInterval,
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
