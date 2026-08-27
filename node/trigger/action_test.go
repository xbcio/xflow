package trigger

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestBuiltInTriggerExecuteForwardsTriggerEvent walks every built-in trigger
// through the shared Execute path. It doubles as the guard that all five
// implement types.ActionHandler: the slice literal below would not compile
// otherwise. This is why it lives here rather than in a subpackage — only this
// package sees all five.
func TestBuiltInTriggerExecuteForwardsTriggerEvent(t *testing.T) {
	event := &types.TriggerEvent{ID: "evt-1", Kind: "timer", Source: "timer"}
	triggers := []types.ActionHandler{
		Timer(), Cron(), Webhook(), Kafka(), Redis(),
	}
	for _, h := range triggers {
		out, err := h.Execute(context.Background(), &types.Input{
			Data: map[string]any{"trigger": event},
		})
		if err != nil {
			t.Fatalf("%s Execute error: %v", h.Descriptor().Type, err)
		}
		if out == nil || out.Port != "main" {
			t.Fatalf("%s output = %+v, want main output", h.Descriptor().Type, out)
		}
		if out.Data["trigger"] != event {
			t.Fatalf("%s trigger data = %#v, want original event", h.Descriptor().Type, out.Data["trigger"])
		}
	}
}

func TestCustomTriggerExecuteForwardsTriggerEvent(t *testing.T) {
	tr := Define("test.trigger.forward", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	event := &types.TriggerEvent{ID: "evt-1"}
	out, err := tr.Execute(context.Background(), &types.Input{Data: map[string]any{"trigger": event}})
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Port != "main" || out.Data["trigger"] != event {
		t.Fatalf("output = %+v, want trigger forwarded on main", out)
	}
}

// TestFactoriesRegisterAllFiveTriggerTypes asserts that importing this package
// links every subpackage and runs its init() registration. The five node type
// strings are frozen contract (persisted in workflow definitions, sent across
// server/runner, declared in runner capabilities) — this is the assertion that
// a rename would break loudly.
//
// Task 13's node/trigger_registry_test.go asserts the same thing one hop
// further out (importing only "node"), which is what catches a missing
// blank import in node/node.go. This one catches a missing subpackage import
// here.
func TestFactoriesRegisterAllFiveTriggerTypes(t *testing.T) {
	want := []string{
		"xflow.trigger.timer",
		"xflow.trigger.cron",
		"xflow.trigger.webhook",
		"xflow.trigger.kafka",
		"xflow.trigger.redis",
	}
	for _, nodeType := range want {
		if _, ok := registry.LookupTrigger(nodeType); !ok {
			t.Errorf("registry.LookupTrigger(%q) = not found, want registered", nodeType)
		}
	}
}

func TestFactoryNodeTypesMatch(t *testing.T) {
	cases := map[string]types.ActionHandler{
		"xflow.trigger.timer":   Timer(),
		"xflow.trigger.cron":    Cron(),
		"xflow.trigger.webhook": Webhook(),
		"xflow.trigger.kafka":   Kafka(),
		"xflow.trigger.redis":   Redis(),
	}
	for want, h := range cases {
		if got := h.Descriptor().Type; got != want {
			t.Errorf("Descriptor().Type = %q, want %q", got, want)
		}
	}
}
