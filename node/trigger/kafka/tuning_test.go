package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// The defaults must reproduce the literals that were previously inlined in
// newKafkaGoConsumer, byte for byte. Exposing knobs is only safe if not
// configuring them changes nothing for existing deployments.
func TestTuningDefaultsMatchThePreviousLiterals(t *testing.T) {
	tuning, err := tuningFromParams(nil)
	if err != nil {
		t.Fatalf("tuningFromParams(nil): %v", err)
	}
	want := TuningConfig{
		FetchMinBytes:     1,
		FetchMaxBytes:     10e6,
		MaxWait:           10 * time.Second,
		DialTimeout:       10 * time.Second,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		RebalanceTimeout:  30 * time.Second,
	}
	if tuning != want {
		t.Fatalf("default tuning = %+v, want %+v", tuning, want)
	}
}

func TestTuningFromParamsReadsEveryKnob(t *testing.T) {
	tuning, err := tuningFromParams(map[string]any{
		"fetch_min_bytes":    1024,
		"fetch_max_bytes":    2048,
		"max_wait":           "250ms",
		"dial_timeout":       "3s",
		"session_timeout":    "45s",
		"heartbeat_interval": "5s",
		"rebalance_timeout":  "60s",
	})
	if err != nil {
		t.Fatalf("tuningFromParams: %v", err)
	}
	want := TuningConfig{
		FetchMinBytes:     1024,
		FetchMaxBytes:     2048,
		MaxWait:           250 * time.Millisecond,
		DialTimeout:       3 * time.Second,
		SessionTimeout:    45 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		RebalanceTimeout:  60 * time.Second,
	}
	if tuning != want {
		t.Fatalf("tuning = %+v, want %+v", tuning, want)
	}
}

// kafka-go's NewReader panics on a config its Validate rejects, and panics
// again when ReadBackoffMax < ReadBackoffMin. A trigger whose params come from
// a YAML file an operator edits must never reach that: one bad number would
// take down the runner process, not just this trigger. Every knob is validated
// before the reader is built.
func TestTuningRejectsValuesThatWouldPanicTheReader(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   string
	}{
		{"negative fetch_min_bytes", map[string]any{"fetch_min_bytes": -1}, "fetch_min_bytes"},
		{"zero fetch_max_bytes", map[string]any{"fetch_max_bytes": 0}, "fetch_max_bytes"},
		{"min above max", map[string]any{"fetch_min_bytes": 4096, "fetch_max_bytes": 1024}, "fetch_min_bytes"},
		{"zero max_wait", map[string]any{"max_wait": "0s"}, "max_wait"},
		{"negative dial_timeout", map[string]any{"dial_timeout": "-1s"}, "dial_timeout"},
		{"unparseable duration", map[string]any{"session_timeout": "soon"}, "session_timeout"},
		// The broker drops a member that misses its session window. A heartbeat
		// interval at or above the session timeout guarantees that miss, so the
		// group rebalances forever and the trigger consumes nothing.
		{"heartbeat at session timeout", map[string]any{
			"heartbeat_interval": "30s", "session_timeout": "30s",
		}, "heartbeat_interval"},
		{"heartbeat above session timeout", map[string]any{
			"heartbeat_interval": "45s", "session_timeout": "30s",
		}, "heartbeat_interval"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tuningFromParams(tc.params)
			if err == nil {
				t.Fatalf("tuningFromParams(%v) succeeded; kafka-go would panic or the group would rebalance forever", tc.params)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

func TestTuningRejectsANonObject(t *testing.T) {
	if _, err := tuningFromParams("30s"); err == nil {
		t.Fatal("a scalar tuning param was accepted")
	}
}

// The knobs are worthless if they do not reach the reader. This asserts on the
// built ReaderConfig rather than on TuningConfig, so a knob that is parsed and
// then dropped on the floor is caught.
func TestTuningReachesTheReaderConfig(t *testing.T) {
	readerCfg, err := readerConfigFor(ConsumerConfig{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "orders",
		Group:   "workers",
		Tuning: TuningConfig{
			FetchMinBytes:     1024,
			FetchMaxBytes:     2048,
			MaxWait:           250 * time.Millisecond,
			DialTimeout:       3 * time.Second,
			SessionTimeout:    45 * time.Second,
			HeartbeatInterval: 5 * time.Second,
			RebalanceTimeout:  60 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("readerConfigFor: %v", err)
	}
	if readerCfg.MinBytes != 1024 || readerCfg.MaxBytes != 2048 {
		t.Errorf("MinBytes/MaxBytes = %d/%d, want 1024/2048", readerCfg.MinBytes, readerCfg.MaxBytes)
	}
	if readerCfg.MaxWait != 250*time.Millisecond {
		t.Errorf("MaxWait = %v, want 250ms", readerCfg.MaxWait)
	}
	if readerCfg.SessionTimeout != 45*time.Second {
		t.Errorf("SessionTimeout = %v, want 45s", readerCfg.SessionTimeout)
	}
	if readerCfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 5s", readerCfg.HeartbeatInterval)
	}
	if readerCfg.RebalanceTimeout != 60*time.Second {
		t.Errorf("RebalanceTimeout = %v, want 60s", readerCfg.RebalanceTimeout)
	}
	// The dialer was previously built only when SASL was configured, so without
	// SASL there was nowhere for a dial timeout to go: kafka-go substituted its
	// own DefaultDialer and the configured value vanished silently.
	if readerCfg.Dialer == nil {
		t.Fatal("no dialer built without SASL; dial_timeout has nowhere to land")
	}
	if readerCfg.Dialer.Timeout != 3*time.Second {
		t.Errorf("Dialer.Timeout = %v, want 3s", readerCfg.Dialer.Timeout)
	}
}

// CommitInterval stays 0 (synchronous commit) and is deliberately NOT a knob.
// The whole at-least-once contract is emit-then-commit in offset order; a
// periodic async commit would let an offset land before its side effect, which
// is the data-loss mode the per-partition serial worker exists to prevent.
func TestCommitStaysSynchronous(t *testing.T) {
	readerCfg, err := readerConfigFor(ConsumerConfig{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "orders",
		Group:   "workers",
		Tuning:  defaultTuning(),
	})
	if err != nil {
		t.Fatalf("readerConfigFor: %v", err)
	}
	if readerCfg.CommitInterval != 0 {
		t.Fatalf("CommitInterval = %v, want 0; offsets would commit on a timer "+
			"rather than after the side effect succeeded", readerCfg.CommitInterval)
	}
}

// The knobs must survive the WHOLE chain: params -> configFromParams ->
// ConsumerConfig.Tuning -> readerConfigFor -> ReaderConfig. Asserting only on
// the two ends leaves the middle link untested -- deleting the Tuning field
// from configFromParams's struct literal makes the reader silently fall back to
// defaults with no diagnostic, and every other test here still passes.
//
// This drives the real Activate path rather than calling configFromParams
// directly, so a tuning that is parsed but never handed to the consumer factory
// is caught too.
func TestTuningSurvivesTheWholeActivationChain(t *testing.T) {
	orig := newConsumer
	var captured ConsumerConfig
	newConsumer = func(cfg ConsumerConfig) (Consumer, error) {
		captured = cfg
		return newScriptedKafkaConsumer(nil), nil
	}
	t.Cleanup(func() { newConsumer = orig })

	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params: map[string]any{
			"brokers": []string{"127.0.0.1:1"},
			"topic":   "orders",
			"group":   "workers",
			"tuning":  map[string]any{"session_timeout": "45s", "fetch_min_bytes": 1024},
		},
		Runtime: triggertest.NewFakeRuntime(),
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	if captured.Tuning.SessionTimeout != 45*time.Second {
		t.Fatalf("consumer received SessionTimeout = %v, want 45s; the tuning param "+
			"was parsed and then dropped, so the reader silently uses defaults",
			captured.Tuning.SessionTimeout)
	}
	readerCfg, err := readerConfigFor(captured)
	if err != nil {
		t.Fatalf("readerConfigFor: %v", err)
	}
	if readerCfg.SessionTimeout != 45*time.Second || readerCfg.MinBytes != 1024 {
		t.Fatalf("reader SessionTimeout/MinBytes = %v/%d, want 45s/1024",
			readerCfg.SessionTimeout, readerCfg.MinBytes)
	}
}

// A bad tuning value must fail activation, not panic. Without the pre-validation
// this test does not fail -- it takes the whole test binary down.
func TestBadTuningFailsActivationInsteadOfPanicking(t *testing.T) {
	_, err := configFromParams(map[string]any{
		"brokers": []string{"127.0.0.1:1"},
		"topic":   "orders",
		"group":   "workers",
		"tuning":  map[string]any{"fetch_min_bytes": 4096, "fetch_max_bytes": 1024},
	}, nil, false)
	if err == nil {
		t.Fatal("configFromParams accepted a tuning that kafka-go would panic on")
	}
}

func TestTuningRoundTripsThroughRawParams(t *testing.T) {
	params := New().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		Tuning(TuningConfig{SessionTimeout: 45 * time.Second, HeartbeatInterval: 5 * time.Second}).
		RawParams().(map[string]any)

	tuning, ok := params["tuning"].(map[string]any)
	if !ok {
		t.Fatalf("tuning params = %#v, want map[string]any", params["tuning"])
	}
	if got := tuning["session_timeout"]; got != "45s" {
		t.Errorf("session_timeout = %v, want 45s", got)
	}
	if got := tuning["heartbeat_interval"]; got != "5s" {
		t.Errorf("heartbeat_interval = %v, want 5s", got)
	}

	// Serialized params must survive the trip back, or the Go DSL and the YAML
	// path disagree about what the trigger was configured with.
	parsed, err := tuningFromParams(tuning)
	if err != nil {
		t.Fatalf("tuningFromParams(round trip): %v", err)
	}
	if parsed.SessionTimeout != 45*time.Second || parsed.HeartbeatInterval != 5*time.Second {
		t.Fatalf("round-tripped tuning = %+v", parsed)
	}
}

// Omitting tuning entirely must leave the params map byte-identical to what it
// was before this knob existed, so existing stored workflow definitions do not
// change shape.
func TestRawParamsOmitsTuningWhenUnset(t *testing.T) {
	params := New().Brokers("localhost:9092").Topic("orders").Group("workers").RawParams().(map[string]any)
	if _, ok := params["tuning"]; ok {
		t.Fatalf("tuning present in params without being configured: %#v", params["tuning"])
	}
}
