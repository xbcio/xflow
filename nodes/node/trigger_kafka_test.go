package node

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestKafkaTriggerDescriptor(t *testing.T) {
	n := KafkaTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.kafka" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestKafkaTriggerRequiresBrokersTopicAndGroup(t *testing.T) {
	_, err := KafkaTrigger().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     map[string]any{},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing brokers/topic/group error")
	}
}

func TestKafkaTriggerDefaultsStartOffsetLatest(t *testing.T) {
	params := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers").RawParams().(map[string]any)
	if got := params["start_offset"]; got != "latest" {
		t.Fatalf("start_offset = %v, want latest", got)
	}
}
