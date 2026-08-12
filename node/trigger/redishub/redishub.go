// Package redishub implements the xflow.trigger.redis_hub trigger: it consumes
// a Redis Stream consumer group or a Pub/Sub channel and emits one TriggerEvent
// per message.
package redishub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"
)

// defaultTriggerMaxInflight bounds the concurrent emit goroutines this trigger
// runs. The kafka trigger declares its own constant of the same value; the two
// are independent per-trigger backpressure windows that happen to coincide, NOT
// a contract that must stay in sync. Change one without changing the other.
const defaultTriggerMaxInflight = 64

// Consumer delivers Redis Stream / Pub/Sub messages. The host process supplies
// the implementation; see newConsumer.
type Consumer interface {
	Messages() <-chan Message
	Close() error
}

// Message is one Redis Stream entry or Pub/Sub delivery.
type Message struct {
	ID      string
	Stream  string
	Channel string
	Payload []byte
	Values  map[string]any
	Time    time.Time
}

// ConsumerConfig is the resolved subscription shape handed to newConsumer.
type ConsumerConfig struct {
	Mode        string
	Stream      string
	Group       string
	Channel     string
	MaxInflight int
}

// newConsumer is the consumer factory seam. It stays UNEXPORTED on purpose:
// tests in this package swap it, but the production builder offers no injection
// point (see the spec's decision 3 — test reachability must not shape the API).
var newConsumer = func(ConsumerConfig) (Consumer, error) {
	return nil, errors.New("redis hub consumer factory is not configured")
}

var pubSubLockTTL = time.Minute

// Node is the xflow.trigger.redis_hub trigger node.
type Node struct {
	nodeinternal.BaseTrigger
	ModeValue        string
	StreamValue      string
	GroupValue       string
	ChannelValue     string
	MaxInflightValue int
}

// New returns a redis-hub trigger defaulting to stream mode.
func New() *Node {
	return &Node{ModeValue: "stream", MaxInflightValue: defaultTriggerMaxInflight}
}

func (n *Node) Mode(mode string) *Node {
	n.ModeValue = mode
	return n
}

func (n *Node) Stream(stream string) *Node {
	n.StreamValue = stream
	return n
}

func (n *Node) Group(group string) *Node {
	n.GroupValue = group
	return n
}

func (n *Node) Channel(channel string) *Node {
	n.ChannelValue = channel
	return n
}

func (n *Node) MaxInflight(max int) *Node {
	n.MaxInflightValue = max
	return n
}

func init() { registry.RegisterTrigger(&Node{}) }

func (n *Node) Descriptor() types.Descriptor {
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

func (n *Node) NodeType() string { return "xflow.trigger.redis_hub" }
func (n *Node) RawParams() any {
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
func (n *Node) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *Node) TriggerHandler() types.TriggerHandler { return n }

func (n *Node) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := configFromParams(in.Params)
	if err != nil {
		return nil, err
	}
	var (
		lock      types.TriggerLock
		renewable types.RenewableTriggerLock
	)
	if cfg.Mode == "pubsub" {
		l, ok, err := in.Runtime.TryLock(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":pubsub", pubSubLockTTL)
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
	consumer, err := newConsumer(cfg)
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
		emitWG    sync.WaitGroup
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
				emitWG.Add(1)
				go func(msg Message) {
					defer emitWG.Done()
					defer func() { <-sem }()
					emitMessage(runCtx, in, cfg.Mode, msg)
				}(msg)
			}
		}
	}()
	if renewable != nil {
		renewEvery := pubSubLockTTL / 2
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
					renewed, err := renewable.Renew(runCtx, pubSubLockTTL)
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
		// Wait for in-flight emit goroutines to finish so shutdown does not
		// drop events still being processed. done is closed by the main loop
		// once it returns from runCtx.Done() (cancelled by stop above), but
		// emitWG.Wait() is the real barrier for the worker goroutines.
		emitWG.Wait()
		<-done
		return err
	}), nil
}

func emitMessage(ctx context.Context, in *types.TriggerActivateInput, mode string, msg Message) {
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

func configFromParams(params map[string]any) (ConsumerConfig, error) {
	cfg := ConsumerConfig{
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
			return ConsumerConfig{}, fmt.Errorf("redis stream and group are required")
		}
	case "pubsub":
		if cfg.Channel == "" {
			return ConsumerConfig{}, fmt.Errorf("redis pubsub channel is required")
		}
	default:
		return ConsumerConfig{}, fmt.Errorf("unsupported redis hub mode %q", cfg.Mode)
	}
	return cfg, nil
}
