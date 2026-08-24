package kafka

import (
	"fmt"
	"time"

	"github.com/spf13/cast"

	"github.com/xbcio/xflow/node/internal/utils/conv"
)

// TuningConfig holds the consumer knobs an operator may need to move without a
// code change: fetch sizing, the dial timeout, and the consumer-group liveness
// windows. Every field previously lived as a literal inside newKafkaGoConsumer,
// so a deployment whose broker wanted a different session timeout had no way to
// say so.
//
// Deliberately NOT exposed: CommitInterval stays 0 so offsets commit
// synchronously after the side effect (see TestCommitStaysSynchronous);
// ReadLagInterval stays -1 because kafkaGoConsumer.run samples lag off the
// high-water mark of each fetched message and reports it through
// Observer.OnConsumerLag, which costs no extra broker round trip;
// GroupBalancers, ReadBackoff*, RetentionTime, and OffsetOutOfRangeError have
// no reported operational need, and each knob is a value someone can set wrong.
type TuningConfig struct {
	// FetchMinBytes is the smallest fetch the broker will answer. 1 means
	// "return as soon as anything is available", which is what a trigger
	// optimizing for latency wants; raise it to trade latency for throughput.
	FetchMinBytes int
	// FetchMaxBytes caps a single fetch response.
	FetchMaxBytes int
	// MaxWait bounds how long the broker holds a fetch open waiting for
	// FetchMinBytes to accumulate.
	MaxWait time.Duration
	// DialTimeout bounds connection establishment to a broker.
	DialTimeout time.Duration
	// SessionTimeout is how long the coordinator waits without a heartbeat
	// before evicting this member and rebalancing the group.
	SessionTimeout time.Duration
	// HeartbeatInterval is how often this member heartbeats. It must stay
	// comfortably below SessionTimeout.
	HeartbeatInterval time.Duration
	// RebalanceTimeout bounds how long the coordinator waits for members to
	// rejoin during a rebalance.
	RebalanceTimeout time.Duration
}

// defaultTuning reproduces the literals that were inlined in
// newKafkaGoConsumer before tuning became configurable. Not setting `tuning`
// must behave exactly as before.
func defaultTuning() TuningConfig {
	return TuningConfig{
		FetchMinBytes:     1,
		FetchMaxBytes:     10e6,
		MaxWait:           10 * time.Second,
		DialTimeout:       10 * time.Second,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		RebalanceTimeout:  30 * time.Second,
	}
}

// tuningFromParams parses the optional `tuning` param object, filling unset
// fields from defaultTuning and validating the result.
//
// Validation is not optional politeness: kafka-go's NewReader panics on a
// config its Validate rejects, so an operator typo in a YAML file would take
// down the runner process rather than failing this one trigger's activation.
func tuningFromParams(v any) (TuningConfig, error) {
	cfg := defaultTuning()
	if v == nil {
		return cfg, nil
	}
	raw, ok := v.(map[string]any)
	if !ok {
		converted, err := cast.ToStringMapE(v)
		if err != nil {
			return TuningConfig{}, fmt.Errorf("kafka tuning must be an object")
		}
		raw = converted
	}
	if err := applyIntTuning(raw, "fetch_min_bytes", &cfg.FetchMinBytes); err != nil {
		return TuningConfig{}, err
	}
	if err := applyIntTuning(raw, "fetch_max_bytes", &cfg.FetchMaxBytes); err != nil {
		return TuningConfig{}, err
	}
	durations := []struct {
		key   string
		field *time.Duration
	}{
		{"max_wait", &cfg.MaxWait},
		{"dial_timeout", &cfg.DialTimeout},
		{"session_timeout", &cfg.SessionTimeout},
		{"heartbeat_interval", &cfg.HeartbeatInterval},
		{"rebalance_timeout", &cfg.RebalanceTimeout},
	}
	for _, d := range durations {
		if raw[d.key] == nil {
			continue
		}
		parsed, err := conv.PositiveDuration(raw[d.key])
		if err != nil {
			return TuningConfig{}, fmt.Errorf("kafka tuning %s: %w", d.key, err)
		}
		*d.field = parsed
	}
	if err := cfg.validate(); err != nil {
		return TuningConfig{}, err
	}
	return cfg, nil
}

func applyIntTuning(raw map[string]any, key string, field *int) error {
	if raw[key] == nil {
		return nil
	}
	n, err := cast.ToIntE(raw[key])
	if err != nil {
		return fmt.Errorf("kafka tuning %s must be a number", key)
	}
	*field = n
	return nil
}

func (c TuningConfig) validate() error {
	if c.FetchMinBytes <= 0 {
		return fmt.Errorf("kafka tuning fetch_min_bytes must be positive, got %d", c.FetchMinBytes)
	}
	if c.FetchMaxBytes <= 0 {
		return fmt.Errorf("kafka tuning fetch_max_bytes must be positive, got %d", c.FetchMaxBytes)
	}
	if c.FetchMinBytes > c.FetchMaxBytes {
		return fmt.Errorf("kafka tuning fetch_min_bytes (%d) exceeds fetch_max_bytes (%d)", c.FetchMinBytes, c.FetchMaxBytes)
	}
	// A member that heartbeats no more often than the coordinator's eviction
	// window is evicted between heartbeats, so the group rebalances forever and
	// the trigger consumes nothing while looking healthy.
	if c.HeartbeatInterval >= c.SessionTimeout {
		return fmt.Errorf("kafka tuning heartbeat_interval (%s) must be below session_timeout (%s); "+
			"the group would rebalance continuously", c.HeartbeatInterval, c.SessionTimeout)
	}
	return nil
}

// isZero reports whether no knob was set, so RawParams can omit the object
// entirely and leave stored workflow definitions byte-identical.
func (c TuningConfig) isZero() bool { return c == TuningConfig{} }

// rawParams serializes the knobs that were explicitly set. Durations serialize
// as strings so the YAML path and the Go DSL path agree on the wire form, the
// same convention aggregate.flush_interval uses.
func (c TuningConfig) rawParams() map[string]any {
	out := map[string]any{}
	if c.FetchMinBytes > 0 {
		out["fetch_min_bytes"] = c.FetchMinBytes
	}
	if c.FetchMaxBytes > 0 {
		out["fetch_max_bytes"] = c.FetchMaxBytes
	}
	if c.MaxWait > 0 {
		out["max_wait"] = c.MaxWait.String()
	}
	if c.DialTimeout > 0 {
		out["dial_timeout"] = c.DialTimeout.String()
	}
	if c.SessionTimeout > 0 {
		out["session_timeout"] = c.SessionTimeout.String()
	}
	if c.HeartbeatInterval > 0 {
		out["heartbeat_interval"] = c.HeartbeatInterval.String()
	}
	if c.RebalanceTimeout > 0 {
		out["rebalance_timeout"] = c.RebalanceTimeout.String()
	}
	return out
}
