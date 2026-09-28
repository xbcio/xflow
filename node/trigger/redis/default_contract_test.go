package redis

// Trigger-activate Default contract (descriptor-contract design §2.4): Activate
// must derive the same effective ConsumerConfig whether mode and max_inflight
// are absent, set to the descriptor's Default, or set to that Default after a
// JSON round trip. The config is observed where Activate hands it to
// newConsumer, i.e. after every fallback -- including the consumer-name one
// Activate itself applies -- has run.

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func redisDescriptorDefault(t *testing.T, name string) any {
	t.Helper()
	for _, p := range New().Descriptor().Params {
		if p.Name == name {
			if p.Default == nil {
				t.Fatalf("xflow.trigger.redis/%s has no Default", name)
			}
			return p.Default
		}
	}
	t.Fatalf("xflow.trigger.redis has no param %q", name)
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

func activatedRedisConfig(t *testing.T, params map[string]any) ConsumerConfig {
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
		NodeName:   "redis",
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

// Stream mode needs stream and group; pubsub would need a renewable lock and a
// channel, and is not what either Default selects.
func redisBaseParams() map[string]any {
	return map[string]any{"addr": "127.0.0.1:6379", "stream": "orders", "group": "workers"}
}

func TestDefaultContractRedisActivate(t *testing.T) {
	for _, name := range []string{"mode", "max_inflight"} {
		t.Run(name, func(t *testing.T) {
			def := redisDescriptorDefault(t, name)
			absent := activatedRedisConfig(t, redisBaseParams())
			for variant, value := range map[string]any{"default": def, "default_json": jsonRoundTrip(t, def)} {
				params := redisBaseParams()
				params[name] = value
				if got := activatedRedisConfig(t, params); !reflect.DeepEqual(got, absent) {
					t.Errorf("%s=%#v (%s): config %+v, want %+v (param absent)", name, value, variant, got, absent)
				}
			}
		})
	}
}

// TestFallbackContractRedis exercises every BuiltinFallbacks entry for the
// redis trigger through Activate.
func TestFallbackContractRedis(t *testing.T) {
	tuning := func(key string, value any, set bool) map[string]any {
		params := redisBaseParams()
		nested := map[string]any{}
		if set {
			nested[key] = value
		}
		params["tuning"] = nested
		return params
	}
	constant := func(key string) func(*testing.T, nodeinternal.Fallback) {
		return func(t *testing.T, f nodeinternal.Fallback) {
			if f.Value == nil {
				t.Fatalf("%s has no constant Value", f.Param)
			}
			absent := activatedRedisConfig(t, tuning(key, nil, false))
			set := activatedRedisConfig(t, tuning(key, f.Value, true))
			if !reflect.DeepEqual(absent, set) {
				t.Fatalf("%s=%#v: config %+v, want %+v (absent)", f.Param, f.Value, set, absent)
			}
		}
	}
	checks := map[string]func(*testing.T, nodeinternal.Fallback){
		"tuning.claim_min_idle": constant("claim_min_idle"),
		"tuning.db":             constant("db"),
		"tuning.dial_timeout":   constant("dial_timeout"),
		"tuning.start_id":       constant("start_id"),
		// Derived: the node name ("redis" in activatedRedisConfig).
		"tuning.consumer": func(t *testing.T, _ nodeinternal.Fallback) {
			absent := activatedRedisConfig(t, tuning("consumer", nil, false))
			set := activatedRedisConfig(t, tuning("consumer", "redis", true))
			if absent.Consumer != "redis" || !reflect.DeepEqual(absent, set) {
				t.Fatalf("absent consumer config %+v, want %+v", absent, set)
			}
		},
	}
	runFallbackChecks(t, "xflow.trigger.redis", 1, checks)

	// Without a tuning block at all the same fallbacks apply.
	if got, want := activatedRedisConfig(t, redisBaseParams()), activatedRedisConfig(t, tuning("", nil, false)); !reflect.DeepEqual(got, want) {
		t.Fatalf("no tuning block = %+v, want %+v (empty tuning block)", got, want)
	}
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
