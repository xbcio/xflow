package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"
)

type RedisHubConsumer interface {
	Messages() <-chan RedisHubMessage
	Close() error
}

type RedisHubMessage struct {
	ID      string
	Stream  string
	Channel string
	Payload []byte
	Values  map[string]any
	Time    time.Time
}

type RedisHubConsumerConfig struct {
	Mode        string
	Stream      string
	Group       string
	Channel     string
	MaxInflight int
}

var newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) {
	return nil, errors.New("redis hub consumer factory is not configured")
}

type RedisHubTriggerNode struct {
	BaseNode
	ModeValue        string
	StreamValue      string
	GroupValue       string
	ChannelValue     string
	MaxInflightValue int
}

func RedisHubTrigger() *RedisHubTriggerNode {
	return &RedisHubTriggerNode{ModeValue: "stream", MaxInflightValue: defaultTriggerMaxInflight}
}

func (n *RedisHubTriggerNode) Mode(mode string) *RedisHubTriggerNode {
	n.ModeValue = mode
	return n
}

func (n *RedisHubTriggerNode) Stream(stream string) *RedisHubTriggerNode {
	n.StreamValue = stream
	return n
}

func (n *RedisHubTriggerNode) Group(group string) *RedisHubTriggerNode {
	n.GroupValue = group
	return n
}

func (n *RedisHubTriggerNode) Channel(channel string) *RedisHubTriggerNode {
	n.ChannelValue = channel
	return n
}

func (n *RedisHubTriggerNode) MaxInflight(max int) *RedisHubTriggerNode {
	n.MaxInflightValue = max
	return n
}

func (n *RedisHubTriggerNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.trigger.redis_hub",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Redis Hub Trigger",
		Params: []ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: true, Default: "stream"},
			{Name: "stream", DisplayName: "Stream", Type: ParamString},
			{Name: "group", DisplayName: "Group", Type: ParamString},
			{Name: "channel", DisplayName: "Channel", Type: ParamString},
			{Name: "max_inflight", DisplayName: "Max Inflight", Type: ParamNumber, Default: float64(defaultTriggerMaxInflight)},
		},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *RedisHubTriggerNode) NodeType() string { return "xflow.trigger.redis_hub" }
func (n *RedisHubTriggerNode) RawParams() any {
	mode := n.ModeValue
	if mode == "" {
		mode = "stream"
	}
	maxInflight := n.MaxInflightValue
	if maxInflight <= 0 {
		maxInflight = defaultTriggerMaxInflight
	}
	return map[string]any{
		"mode":         mode,
		"stream":       n.StreamValue,
		"group":        n.GroupValue,
		"channel":      n.ChannelValue,
		"max_inflight": maxInflight,
	}
}
func (n *RedisHubTriggerNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}
func (n *RedisHubTriggerNode) TriggerHandler() TriggerHandler { return n }

func (n *RedisHubTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := redisHubConfigFromParams(in.Params)
	if err != nil {
		return nil, err
	}
	var lock types.TriggerLock
	if cfg.Mode == "pubsub" {
		l, ok, err := in.Runtime.TryLock(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":pubsub", time.Minute)
		if err != nil || !ok {
			return nil, err
		}
		lock = l
	}
	consumer, err := newRedisHubConsumer(cfg)
	if err != nil {
		if lock != nil {
			_ = lock.Release(ctx)
		}
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
				go func(msg RedisHubMessage) {
					defer func() { <-sem }()
					emitRedisHubMessage(runCtx, in, cfg.Mode, msg)
				}(msg)
			}
		}
	}()
	return types.CloseFunc(func(context.Context) error {
		cancel()
		err := consumer.Close()
		if lock != nil {
			_ = lock.Release(context.Background())
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		return err
	}), nil
}

func emitRedisHubMessage(ctx context.Context, in *types.TriggerActivateInput, mode string, msg RedisHubMessage) {
	eventID := msg.ID
	if mode == "stream" {
		eventID = msg.Stream + "/" + msg.ID
	}
	if eventID == "" {
		eventID = msg.Channel + "/" + fmt.Sprint(time.Now().UnixNano())
	}
	event := &types.TriggerEvent{
		ID:     eventID,
		Kind:   "redis_hub",
		Source: in.NodeName,
		Time:   msg.Time,
		Data: map[string]any{
			"mode":    mode,
			"stream":  msg.Stream,
			"channel": msg.Channel,
			"values":  msg.Values,
			"payload": string(msg.Payload),
		},
		Raw: msg.Payload,
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	if ok, _ := in.Runtime.Dedup(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+eventID, 24*time.Hour); ok {
		_, _ = in.Emit(ctx, event)
	}
}

func redisHubConfigFromParams(params map[string]any) (RedisHubConsumerConfig, error) {
	cfg := RedisHubConsumerConfig{
		Mode:        stringParam(params["mode"]),
		Stream:      stringParam(params["stream"]),
		Group:       stringParam(params["group"]),
		Channel:     stringParam(params["channel"]),
		MaxInflight: intParam(params["max_inflight"], defaultTriggerMaxInflight),
	}
	if cfg.Mode == "" {
		cfg.Mode = "stream"
	}
	switch cfg.Mode {
	case "stream":
		if cfg.Stream == "" || cfg.Group == "" {
			return RedisHubConsumerConfig{}, fmt.Errorf("redis stream and group are required")
		}
	case "pubsub":
		if cfg.Channel == "" {
			return RedisHubConsumerConfig{}, fmt.Errorf("redis pubsub channel is required")
		}
	default:
		return RedisHubConsumerConfig{}, fmt.Errorf("unsupported redis hub mode %q", cfg.Mode)
	}
	return cfg, nil
}

func stringParam(v any) string {
	s, _ := v.(string)
	return s
}

func stringSliceParam(v any) []string {
	switch items := v.(type) {
	case []string:
		return items
	case []any:
		out := make([]string, 0, len(items))
		for _, item := range items {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func intParam(v any, fallback int) int {
	switch n := v.(type) {
	case int:
		if n > 0 {
			return n
		}
	case int64:
		if n > 0 {
			return int(n)
		}
	case float64:
		if n > 0 {
			return int(n)
		}
	}
	return fallback
}

func init() { RegisterTrigger(&RedisHubTriggerNode{}) }
