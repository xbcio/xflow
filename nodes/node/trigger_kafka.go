package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"
)

const defaultTriggerMaxInflight = 64

type KafkaConsumer interface {
	Messages() <-chan KafkaMessage
	Close() error
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
}

var newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) {
	return nil, errors.New("kafka consumer factory is not configured")
}

type KafkaTriggerNode struct {
	BaseNode
	BrokersValue     []string
	TopicValue       string
	GroupValue       string
	StartOffsetValue string
	MaxInflightValue int
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
	return map[string]any{
		"brokers":      n.BrokersValue,
		"topic":        n.TopicValue,
		"group":        n.GroupValue,
		"start_offset": offset,
		"max_inflight": maxInflight,
	}
}
func (n *KafkaTriggerNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}
func (n *KafkaTriggerNode) TriggerHandler() TriggerHandler { return n }

func (n *KafkaTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := kafkaConfigFromParams(in.Params)
	if err != nil {
		return nil, err
	}
	consumer, err := newKafkaConsumer(cfg)
	if err != nil {
		return nil, err
	}
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
					emitKafkaMessage(runCtx, in, msg)
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
	}), nil
}

func emitKafkaMessage(ctx context.Context, in *types.TriggerActivateInput, msg KafkaMessage) {
	eventID := fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
	event := &types.TriggerEvent{
		ID:      eventID,
		Kind:    "kafka",
		Source:  in.NodeName,
		Time:    msg.Time,
		Headers: msg.Headers,
		Data: map[string]any{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"key":       string(msg.Key),
			"value":     string(msg.Value),
		},
		Raw: msg.Value,
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	if ok, _ := in.Runtime.Dedup(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+eventID, 24*time.Hour); ok {
		_, _ = in.Emit(ctx, event)
	}
}

func kafkaConfigFromParams(params map[string]any) (KafkaConsumerConfig, error) {
	cfg := KafkaConsumerConfig{
		Brokers:     stringSliceParam(params["brokers"]),
		Topic:       stringParam(params["topic"]),
		Group:       stringParam(params["group"]),
		StartOffset: stringParam(params["start_offset"]),
		MaxInflight: intParam(params["max_inflight"], defaultTriggerMaxInflight),
	}
	if cfg.StartOffset == "" {
		cfg.StartOffset = "latest"
	}
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" {
		return KafkaConsumerConfig{}, fmt.Errorf("kafka brokers, topic, and group are required")
	}
	return cfg, nil
}

func init() { RegisterTrigger(&KafkaTriggerNode{}) }
