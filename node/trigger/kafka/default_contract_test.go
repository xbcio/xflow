package kafka

// Trigger-activate Default contract (descriptor-contract design §2.4): Activate
// must derive the same effective ConsumerConfig whether start_offset and
// max_inflight are absent, set to the descriptor's Default, or set to that
// Default after a JSON round trip. The config is observed where Activate hands
// it to newConsumer, i.e. after every fallback has been applied.

import (
	"context"
	"encoding/json"
	"maps"
	"reflect"
	"testing"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func kafkaDescriptorDefault(t *testing.T, name string) any {
	t.Helper()
	for _, p := range New().Descriptor().Params {
		if p.Name == name {
			if p.Default == nil {
				t.Fatalf("xflow.trigger.kafka/%s has no Default", name)
			}
			return p.Default
		}
	}
	t.Fatalf("xflow.trigger.kafka has no param %q", name)
	return nil
}

func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// activatedKafkaConfig runs Activate with params and returns the config it
// handed to newConsumer.
func activatedKafkaConfig(t *testing.T, params map[string]any) ConsumerConfig {
	t.Helper()
	orig := newConsumer
	t.Cleanup(func() { newConsumer = orig })
	var captured *ConsumerConfig
	newConsumer = func(cfg ConsumerConfig) (Consumer, error) {
		captured = &cfg
		return newScriptedConsumer(nil), nil
	}
	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     params,
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err != nil {
		t.Fatalf("Activate(%v): %v", params, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = sub.Close(ctx)
	if captured == nil {
		t.Fatal("Activate did not build a consumer")
	}
	return *captured
}

func kafkaBaseParams() map[string]any {
	return map[string]any{"brokers": []any{"localhost:9092"}, "topic": "orders", "group": "workers"}
}

func TestDefaultContractKafkaActivate(t *testing.T) {
	for _, name := range []string{"start_offset", "max_inflight"} {
		t.Run(name, func(t *testing.T) {
			def := kafkaDescriptorDefault(t, name)
			absent := activatedKafkaConfig(t, kafkaBaseParams())
			for variant, value := range map[string]any{"default": def, "default_json": jsonRoundTrip(t, def)} {
				params := kafkaBaseParams()
				params[name] = value
				if got := activatedKafkaConfig(t, params); !reflect.DeepEqual(got, absent) {
					t.Errorf("%s=%#v (%s): config %+v, want %+v (param absent)", name, value, variant, got, absent)
				}
			}
		})
	}

	// start_offset is resolved once more, to a kafka-go offset, when the real
	// consumer is built; the Default must land on the same one as absent.
	def := kafkaDescriptorDefault(t, "start_offset").(string)
	absentOffset, err := startOffsetFor(activatedKafkaConfig(t, kafkaBaseParams()).StartOffset)
	if err != nil {
		t.Fatal(err)
	}
	defaultOffset, err := startOffsetFor(def)
	if err != nil || defaultOffset != absentOffset {
		t.Fatalf("startOffsetFor(%q) = %d, %v; want %d (param absent)", def, defaultOffset, err, absentOffset)
	}
}

// TestFallbackContractKafka exercises every BuiltinFallbacks entry for the
// kafka trigger through Activate.
func TestFallbackContractKafka(t *testing.T) {
	// withNested returns base with block[key] absent or set.
	withNested := func(block string, blockBase map[string]any) func(key string, value any, set bool) map[string]any {
		return func(key string, value any, set bool) map[string]any {
			nested := maps.Clone(blockBase)
			delete(nested, key)
			if set {
				nested[key] = value
			}
			params := kafkaBaseParams()
			params[block] = nested
			return params
		}
	}
	aggregate := withNested("aggregate", map[string]any{"enabled": true})
	schema := withNested("message_schema", map[string]any{"required_fields": []any{"id"}})
	tuning := withNested("tuning", map[string]any{})

	constant := func(build func(key string, value any, set bool) map[string]any, key string) func(*testing.T, nodeinternal.Fallback) {
		return func(t *testing.T, f nodeinternal.Fallback) {
			if f.Value == nil {
				t.Fatalf("%s has no constant Value", f.Param)
			}
			absent := activatedKafkaConfig(t, build(key, nil, false))
			set := activatedKafkaConfig(t, build(key, f.Value, true))
			if !reflect.DeepEqual(absent, set) {
				t.Fatalf("%s=%#v: config %+v, want %+v (absent)", f.Param, f.Value, set, absent)
			}
		}
	}

	checks := map[string]func(*testing.T, nodeinternal.Fallback){
		"aggregate.by":              constant(aggregate, "by"),
		"aggregate.dedup":           constant(aggregate, "dedup"),
		"aggregate.max_size":        constant(aggregate, "max_size"),
		"aggregate.on_overflow":     constant(aggregate, "on_overflow"),
		"message_schema.on_invalid": constant(schema, "on_invalid"),
		"tuning.dial_timeout":       constant(tuning, "dial_timeout"),
		"tuning.fetch_max_bytes":    constant(tuning, "fetch_max_bytes"),
		"tuning.fetch_min_bytes":    constant(tuning, "fetch_min_bytes"),
		"tuning.heartbeat_interval": constant(tuning, "heartbeat_interval"),
		"tuning.max_wait":           constant(tuning, "max_wait"),
		"tuning.rebalance_timeout":  constant(tuning, "rebalance_timeout"),
		"tuning.session_timeout":    constant(tuning, "session_timeout"),
		// Derived: 100ms on the emit path, 1s on the entry-seed path.
		"aggregate.flush_interval": func(t *testing.T, _ nodeinternal.Fallback) {
			if got := activatedKafkaConfig(t, aggregate("flush_interval", nil, false)).Aggregate.FlushInterval; got != 100*time.Millisecond {
				t.Fatalf("emit-path flush_interval fallback = %v, want 100ms", got)
			}
			cfg, err := aggregateConfigFromParamForMode(map[string]any{"enabled": true}, true)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.FlushInterval != time.Second {
				t.Fatalf("entry-seed flush_interval fallback = %v, want 1s", cfg.FlushInterval)
			}
		},
	}
	runFallbackChecks(t, "xflow.trigger.kafka", 1, checks)
}

func runFallbackChecks(t *testing.T, typ string, version int, checks map[string]func(*testing.T, nodeinternal.Fallback)) {
	t.Helper()
	entries := nodeinternal.BuiltinFallbacksFor(typ, version)
	if len(entries) == 0 {
		t.Fatalf("BuiltinFallbacks has no entry for %s@%d", typ, version)
	}
	seen := map[string]bool{}
	for _, f := range entries {
		seen[f.Param] = true
		check, ok := checks[f.Param]
		if !ok {
			t.Errorf("BuiltinFallbacks entry %s@%d/%s has no handler check", typ, version, f.Param)
			continue
		}
		t.Run(f.Param, func(t *testing.T) { check(t, f) })
	}
	for param := range checks {
		if !seen[param] {
			t.Errorf("check for %s@%d/%s has no BuiltinFallbacks entry", typ, version, param)
		}
	}
}
