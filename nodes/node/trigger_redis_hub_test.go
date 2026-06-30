package node

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestRedisHubTriggerDescriptor(t *testing.T) {
	n := RedisHubTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.redis_hub" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestRedisHubTriggerStreamRequiresStreamAndGroup(t *testing.T) {
	_, err := RedisHubTrigger().Mode("stream").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "stream"},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing stream/group error")
	}
}

func TestRedisHubTriggerPubSubRequiresChannel(t *testing.T) {
	_, err := RedisHubTrigger().Mode("pubsub").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "pubsub"},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing channel error")
	}
}
