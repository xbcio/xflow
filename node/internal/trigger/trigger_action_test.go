package trigger

import (
	"context"
	"testing"

	. "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
)

func TestBuiltInTriggerExecuteForwardsTriggerEvent(t *testing.T) {
	event := &types.TriggerEvent{ID: "evt-1", Kind: "timer", Source: "timer"}
	triggers := []types.ActionHandler{
		TimerTrigger(),
		CronTrigger(),
		WebhookTrigger(),
		KafkaTrigger(),
		RedisHubTrigger(),
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
	tr := DefineTrigger("test.trigger.forward", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	event := &types.TriggerEvent{ID: "evt-1"}

	out, err := tr.Execute(context.Background(), &types.Input{
		Data: map[string]any{"trigger": event},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Port != "main" || out.Data["trigger"] != event {
		t.Fatalf("output = %+v, want trigger forwarded on main", out)
	}
}
