// Package redis implements the xflow.trigger.redis trigger: it consumes
// a Redis Stream consumer group or a Pub/Sub channel and emits one TriggerEvent
// per message.
package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/node/internal/utils/conv"

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
//
// Username and Password are filled from the node's supplies, never from Params.
// That is the same arrangement the kafka trigger uses for SASL credentials, and
// it is the only one available: types.ParamSpec has no "secret" flag, so a
// credential is kept out of the param surface by not being declared there at
// all.
type ConsumerConfig struct {
	Addr        string
	DB          int
	Username    string
	Password    string
	DialTimeout time.Duration

	Mode     string
	Stream   string
	Group    string
	Consumer string
	StartID  string
	Channel  string

	PayloadField string
	ClaimMinIdle time.Duration
	MaxInflight  int
}

// newConsumer is the consumer factory seam. It stays UNEXPORTED on purpose:
// tests in this package swap it, but the production builder offers no injection
// point (see the spec's decision 3 — test reachability must not shape the API).
var newConsumer = newRedisConsumer

var pubSubLockTTL = time.Minute

// Tuning holds the knobs that only stream mode has, kept out of the top-level
// param surface the way the kafka trigger keeps its own: an operator
// configuring a stream needs addr/stream/group and nothing else, and burying
// the rest here means adding one later does not widen what everybody sees.
type Tuning struct {
	DB           int
	Consumer     string
	StartID      string
	PayloadField string
	ClaimMinIdle time.Duration
	DialTimeout  time.Duration
}

func (t Tuning) isZero() bool { return t == Tuning{} }

func (t Tuning) rawParams() map[string]any {
	params := map[string]any{}
	if t.DB != 0 {
		params["db"] = t.DB
	}
	if t.Consumer != "" {
		params["consumer"] = t.Consumer
	}
	if t.StartID != "" {
		params["start_id"] = t.StartID
	}
	if t.PayloadField != "" {
		params["payload_field"] = t.PayloadField
	}
	if t.ClaimMinIdle > 0 {
		params["claim_min_idle"] = t.ClaimMinIdle.String()
	}
	if t.DialTimeout > 0 {
		params["dial_timeout"] = t.DialTimeout.String()
	}
	return params
}

// Node is the xflow.trigger.redis trigger node.
type Node struct {
	nodeinternal.BaseTrigger
	AddrValue        string
	ModeValue        string
	StreamValue      string
	GroupValue       string
	ChannelValue     string
	MaxInflightValue int
	TuningValue      Tuning
}

// New returns a redis trigger defaulting to stream mode.
func New() *Node {
	return &Node{ModeValue: "stream", MaxInflightValue: defaultTriggerMaxInflight}
}

// Addr sets the Redis server address ("host:port"). Credentials do not belong
// here: attach a supply to the node instead, and see mergedSupplyContent.
func (n *Node) Addr(addr string) *Node {
	n.AddrValue = addr
	return n
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

func (n *Node) Tuning(tuning Tuning) *Node {
	n.TuningValue = tuning
	return n
}

func init() { registry.RegisterTrigger(&Node{}) }

func (n *Node) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.redis",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Redis Trigger",
		Params: []types.ParamSpec{
			{Name: "addr", DisplayName: "Address", Type: types.ParamString, Required: true, Description: "Redis server address, host:port. Credentials are not params: attach a supply with username/password to the node."},
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: true, Default: "stream"},
			{Name: "stream", DisplayName: "Stream", Type: types.ParamString},
			{Name: "group", DisplayName: "Group", Type: types.ParamString},
			{Name: "channel", DisplayName: "Channel", Type: types.ParamString},
			{Name: "max_inflight", DisplayName: "Max Inflight", Type: types.ParamNumber, Default: float64(defaultTriggerMaxInflight)},
			{Name: "tuning", DisplayName: "Tuning", Type: types.ParamObject, Description: "Stream-mode knobs: db (0), consumer (defaults to the node name), start_id (\"$\", i.e. only entries added after the group is created), payload_field (unset means the whole entry is JSON-encoded into the event payload), claim_min_idle (\"1m\"), dial_timeout (\"10s\"). Durations are strings. claim_min_idle is how long an unacknowledged entry must sit before another consumer takes it over, so it must exceed the slowest workflow this stream drives or entries will be delivered twice."},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *Node) NodeType() string { return "xflow.trigger.redis" }
func (n *Node) RawParams() any {
	mode := n.ModeValue
	if mode == "" {
		mode = "stream"
	}
	maxInflight := n.MaxInflightValue
	if maxInflight <= 0 {
		maxInflight = defaultTriggerMaxInflight
	}
	params := map[string]any{
		"addr":         n.AddrValue,
		"mode":         mode,
		"stream":       n.StreamValue,
		"group":        n.GroupValue,
		"channel":      n.ChannelValue,
		"max_inflight": maxInflight,
	}
	// Omitted entirely when unset, matching the kafka trigger: a key that is
	// always written would move the definition hash of every workflow that
	// never asked for it.
	if !n.TuningValue.isZero() {
		params["tuning"] = n.TuningValue.rawParams()
	}
	return params
}
func (n *Node) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *Node) TriggerHandler() types.TriggerHandler { return n }

func (n *Node) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	cfg, err := configFromParams(in.Params, mergedSupplyContent(in.Supplies))
	if err != nil {
		return nil, err
	}
	if cfg.Consumer == "" {
		// The consumer name is this runner's identity inside the group, and it
		// decides which pending entries are its own. Defaulting to the node
		// name keeps it stable across restarts, so a runner that comes back
		// reclaims what it left behind instead of orphaning it under a name
		// nothing will ever use again.
		cfg.Consumer = in.NodeName
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
			return nil, errors.New("redis pubsub trigger requires renewable lock")
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
					// Acknowledge only what actually reached the engine. An
					// unacknowledged entry stays pending and is reclaimed
					// later, so a crash between emit and ack costs a duplicate
					// rather than a lost message. Acknowledging unconditionally
					// would turn every emit failure into silent data loss.
					if emitMessage(runCtx, in, cfg.Mode, msg) {
						ackMessage(runCtx, consumer, msg)
					}
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

// emitMessage reports whether the message is finished with, which is what the
// caller acknowledges on. A duplicate counts as finished — some earlier
// delivery already reached the engine — while a dedup lookup that errored does
// not, because the trigger cannot tell whether it emitted or not and leaving
// the entry pending costs a redelivery rather than a lost message.
func emitMessage(ctx context.Context, in *types.TriggerActivateInput, mode string, msg Message) bool {
	eventID := msg.ID
	if mode == "stream" {
		eventID = msg.Stream + "/" + msg.ID
	}
	if eventID == "" {
		eventID = msg.Channel + "/" + fmt.Sprint(time.Now().UnixNano())
	}
	event := &types.TriggerEvent{
		ID:     eventID,
		Kind:   "redis",
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
	fresh, err := in.Runtime.Dedup(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+eventID, 24*time.Hour)
	if err != nil {
		return false
	}
	if !fresh {
		return true
	}
	if _, err := in.Emit(ctx, event); err != nil {
		return false
	}
	return true
}

// ackMessage tells the consumer the entry is done with. The capability is
// discovered rather than required, exactly as the kafka trigger discovers
// messageCommitter: pub/sub has nothing to acknowledge, and the scripted
// consumers the tests inject implement only Messages and Close.
func ackMessage(ctx context.Context, consumer Consumer, msg Message) {
	acker, ok := consumer.(messageAcker)
	if !ok {
		return
	}
	// A failed ack is not worth stopping for: the entry stays pending and the
	// reclaim loop hands it back later, which is the same path a crash takes.
	_ = acker.Ack(ctx, msg)
}

// mergedSupplyContent flattens the node's supplies into one lookup map. It is a
// deliberate copy of the kafka trigger's function of the same name rather than
// a shared helper: that one is unexported, and the two triggers read different
// keys out of the result, so a shared version would have to be an exported
// utility that nothing else wants.
//
// Later supplies win on a key collision, which only matters when a node is
// given two supplies that both carry credentials — a configuration mistake this
// resolves quietly rather than failing activation over.
func mergedSupplyContent(supplies map[string]any) map[string]any {
	if len(supplies) == 0 {
		return nil
	}
	merged := map[string]any{}
	for _, value := range supplies {
		content, ok := value.(map[string]any)
		if !ok {
			continue
		}
		for key, item := range content {
			merged[key] = item
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func configFromParams(params map[string]any, supply map[string]any) (ConsumerConfig, error) {
	cfg := ConsumerConfig{
		Addr:        cast.ToString(params["addr"]),
		Mode:        cast.ToString(params["mode"]),
		Stream:      cast.ToString(params["stream"]),
		Group:       cast.ToString(params["group"]),
		Channel:     cast.ToString(params["channel"]),
		MaxInflight: conv.PositiveInt(params["max_inflight"], defaultTriggerMaxInflight),
	}
	if err := applyTuningParams(&cfg, params["tuning"]); err != nil {
		return ConsumerConfig{}, err
	}
	// Credentials come from the supply only. Reading them from params as well
	// would give an operator a way to put a password into the definition the
	// platform stores and hashes, which is the thing keeping them out of the
	// param surface is meant to prevent.
	cfg.Username = cast.ToString(supply["username"])
	cfg.Password = cast.ToString(supply["password"])

	if cfg.Mode == "" {
		cfg.Mode = "stream"
	}
	if cfg.Addr == "" {
		return ConsumerConfig{}, fmt.Errorf("redis addr is required")
	}
	if cfg.StartID == "" {
		// "$" means only entries added after the group is created. Starting at
		// "0" instead would replay the whole stream the first time a workflow
		// is activated, which for a long-lived stream is a flood nobody asked
		// for; an operator who wants that can set start_id explicitly.
		cfg.StartID = "$"
	}
	if cfg.ClaimMinIdle <= 0 {
		cfg.ClaimMinIdle = time.Minute
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
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
		return ConsumerConfig{}, fmt.Errorf("unsupported redis mode %q", cfg.Mode)
	}
	return cfg, nil
}

// applyTuningParams reads the optional tuning object. A tuning key that is
// present but not an object is an error rather than an ignored value: silently
// dropping it would leave a trigger running with defaults an operator believes
// they overrode.
func applyTuningParams(cfg *ConsumerConfig, raw any) error {
	if raw == nil {
		return nil
	}
	tuning, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("redis tuning must be an object, got %T", raw)
	}
	cfg.DB = conv.PositiveInt(tuning["db"], 0)
	cfg.Consumer = cast.ToString(tuning["consumer"])
	cfg.StartID = cast.ToString(tuning["start_id"])
	cfg.PayloadField = cast.ToString(tuning["payload_field"])
	// Every knob here is optional, so an absent key keeps the caller's default
	// rather than being rejected. conv.PositiveDuration treats "missing" as an
	// error, which is right for a required duration and wrong for these, hence
	// the presence check before each call.
	if raw, ok := tuning["claim_min_idle"]; ok {
		claimMinIdle, err := conv.PositiveDuration(raw)
		if err != nil {
			return fmt.Errorf("redis tuning claim_min_idle: %w", err)
		}
		cfg.ClaimMinIdle = claimMinIdle
	}
	if raw, ok := tuning["dial_timeout"]; ok {
		dialTimeout, err := conv.PositiveDuration(raw)
		if err != nil {
			return fmt.Errorf("redis tuning dial_timeout: %w", err)
		}
		cfg.DialTimeout = dialTimeout
	}
	return nil
}
