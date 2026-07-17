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
	cfg, err := kafkaConfigFromParams(in.Params)
	if err != nil {
		return nil, err
	}
	consumer, err := newKafkaConsumer(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Aggregate.Enabled {
		return activateKafkaAggregate(ctx, in, cfg, consumer), nil
	}
	return activateKafkaPerMessage(ctx, in, cfg, consumer), nil
}

func activateKafkaPerMessage(ctx context.Context, in *types.TriggerActivateInput, cfg KafkaConsumerConfig, consumer KafkaConsumer) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &kafkaPerMessageRuntime{
		runCtx:   runCtx,
		in:       in,
		consumer: consumer,
		buffer:   cfg.MaxInflight,
		workers:  make(map[kafkaPartitionKey]*kafkaPartitionWorker),
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
			if emitKafkaMessage(w.rt.runCtx, w.rt.in, msg) {
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
			ok, err := dedupKafkaMessage(context.Background(), a.rt.in, msg)
			if err != nil {
				continue
			}
			if !ok {
				_ = commitKafkaMessages(context.Background(), a.rt.consumer, msg)
				continue
			}
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
			// the goroutine and map entry are reclaimed.
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

func emitKafkaMessage(ctx context.Context, in *types.TriggerActivateInput, msg KafkaMessage) bool {
	event := kafkaSingleEvent(in.NodeName, msg)
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	ok, err := dedupKafkaMessage(ctx, in, msg)
	if err != nil {
		return false
	}
	if ok {
		if _, err := in.Emit(ctx, event); err != nil {
			return false
		}
	}
	return true
}

func dedupKafkaMessage(ctx context.Context, in *types.TriggerActivateInput, msg KafkaMessage) (bool, error) {
	eventID := kafkaMessageID(msg)
	ok, err := in.Runtime.Dedup(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+eventID, 24*time.Hour)
	return ok, err
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

func kafkaConfigFromParams(params map[string]any) (KafkaConsumerConfig, error) {
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
