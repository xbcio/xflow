package trigger

import (
	"context"
	"errors"
	"fmt"
	. "github.com/xbcio/xflow/node/internal"
	"sync"
	"time"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"

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

var redisHubPubSubLockTTL = time.Minute

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

func (n *RedisHubTriggerNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.redis_hub",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Redis Hub Trigger",
		Params: []types.ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: true, Default: "stream"},
			{Name: "stream", DisplayName: "Stream", Type: types.ParamString},
			{Name: "group", DisplayName: "Group", Type: types.ParamString},
			{Name: "channel", DisplayName: "Channel", Type: types.ParamString},
			{Name: "max_inflight", DisplayName: "Max Inflight", Type: types.ParamNumber, Default: float64(defaultTriggerMaxInflight)},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
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
func (n *RedisHubTriggerNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *RedisHubTriggerNode) TriggerHandler() types.TriggerHandler { return n }
func (n *RedisHubTriggerNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return ExecuteTriggerEntry(input)
}

func (n *RedisHubTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := redisHubConfigFromParams(in.Params)
	if err != nil {
		return nil, err
	}
	var (
		lock      types.TriggerLock
		renewable types.RenewableTriggerLock
	)
	if cfg.Mode == "pubsub" {
		l, ok, err := in.Runtime.TryLock(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":pubsub", redisHubPubSubLockTTL)
		if err != nil || !ok {
			return nil, err
		}
		r, ok := l.(types.RenewableTriggerLock)
		if !ok {
			_ = l.Release(ctx)
			return nil, errors.New("redis hub pubsub trigger requires renewable lock")
		}
		lock = l
		renewable = r
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
	var (
		closeOnce sync.Once
		closeErr  error
	)
	stop := func(releaseCtx context.Context) error {
		closeOnce.Do(func() {
			cancel()
			if err := consumer.Close(); err != nil {
				closeErr = err
			}
			if lock != nil {
				if err := lock.Release(releaseCtx); err != nil && closeErr == nil {
					closeErr = err
				}
			}
		})
		return closeErr
	}
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
	if renewable != nil {
		renewEvery := redisHubPubSubLockTTL / 2
		if renewEvery <= 0 {
			renewEvery = time.Millisecond
		}
		ticker := time.NewTicker(renewEvery)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
					renewed, err := renewable.Renew(runCtx, redisHubPubSubLockTTL)
					if err != nil || !renewed {
						_ = stop(context.Background())
						return
					}
				}
			}
		}()
	}
	return types.CloseFunc(func(context.Context) error {
		err := stop(context.Background())
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
	if ok, err := in.Runtime.Dedup(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+eventID, 24*time.Hour); err == nil && ok {
		_, _ = in.Emit(ctx, event)
	}
}

func redisHubConfigFromParams(params map[string]any) (RedisHubConsumerConfig, error) {
	cfg := RedisHubConsumerConfig{
		Mode:        cast.ToString(params["mode"]),
		Stream:      cast.ToString(params["stream"]),
		Group:       cast.ToString(params["group"]),
		Channel:     cast.ToString(params["channel"]),
		MaxInflight: conv.PositiveInt(params["max_inflight"], defaultTriggerMaxInflight),
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

func init() { RegisterTrigger(&RedisHubTriggerNode{}) }
