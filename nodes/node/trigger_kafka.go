package node

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	BaseNode
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

func (n *KafkaTriggerNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.trigger.kafka",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Kafka Trigger",
		Params: []ParamSpec{
			{Name: "brokers", DisplayName: "Brokers", Type: ParamArray, Required: true},
			{Name: "topic", DisplayName: "Topic", Type: ParamString, Required: true},
			{Name: "group", DisplayName: "Group", Type: ParamString, Required: true},
			{Name: "start_offset", DisplayName: "Start Offset", Type: ParamString, Default: "latest"},
			{Name: "max_inflight", DisplayName: "Max Inflight", Type: ParamNumber, Default: float64(defaultTriggerMaxInflight)},
			{Name: "aggregate", DisplayName: "Aggregate", Type: ParamObject, Description: "Optional partition batch aggregation: enabled, by, max_size, flush_interval, dedup"},
		},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
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
func (n *KafkaTriggerNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}
func (n *KafkaTriggerNode) TriggerHandler() TriggerHandler { return n }
func (n *KafkaTriggerNode) Execute(_ context.Context, input *Input) (*Output, error) {
	return executeTriggerEntry(input)
}

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
	sem := make(chan struct{}, cfg.MaxInflight)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-runCtx.Done():
				return
			case msg, ok := <-consumer.Messages():
				if !ok {
					return
				}
				select {
				case sem <- struct{}{}:
				case <-runCtx.Done():
					return
				}
				go func(msg KafkaMessage) {
					defer func() { <-sem }()
					if emitKafkaMessage(runCtx, in, msg) {
						_ = commitKafkaMessages(context.Background(), consumer, msg)
					}
				}(msg)
			}
		}
	}()
	return types.CloseFunc(func(context.Context) error {
		cancel()
		err := consumer.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		return err
	})
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
	key  kafkaPartitionKey
	rt   *kafkaAggregateRuntime
	ch   chan KafkaMessage
	done chan struct{}
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
		err := consumer.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
			}
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
		key:  key,
		rt:   r,
		ch:   make(chan KafkaMessage, r.cfg.MaxSize),
		done: make(chan struct{}),
	}
	r.aggregators[key] = agg
	go agg.run()
	return agg
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
	var buffer []KafkaMessage
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	timerActive := false
	defer timer.Stop()
	for {
		select {
		case msg, ok := <-a.ch:
			if !ok {
				a.flush(context.Background(), buffer)
				return
			}
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
		Brokers:     stringSliceParam(params["brokers"]),
		Topic:       cast.ToString(params["topic"]),
		Group:       cast.ToString(params["group"]),
		StartOffset: cast.ToString(params["start_offset"]),
		MaxInflight: positiveIntParam(params["max_inflight"], defaultTriggerMaxInflight),
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
		MaxSize:       positiveIntParam(raw["max_size"], defaultKafkaAggregateMaxSize),
		FlushInterval: defaultKafkaAggregateFlushInterval,
		Dedup:         cast.ToString(raw["dedup"]),
	}
	if !cfg.Enabled {
		return KafkaAggregateConfig{}, nil
	}
	if raw["flush_interval"] != nil {
		flushInterval, err := triggerDurationParam(raw["flush_interval"])
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

func init() { RegisterTrigger(&KafkaTriggerNode{}) }
