package trigger

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestBuildKafkaBatchAdmissionKey_UsesActualRange(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 3, Offset: 100},
		{Topic: "t", Partition: 3, Offset: 101},
		{Topic: "t", Partition: 3, Offset: 147},
	}
	got := buildKafkaBatchAdmissionKey("wf-1", "v2", "unit-a", msgs)
	want := "/wf-1/v2/unit-a/t/3/100-147"
	if got != want {
		t.Fatalf("admission key = %q, want %q", got, want)
	}
}

func TestBuildKafkaBatchAdmissionKey_SingleMessage(t *testing.T) {
	msgs := []KafkaMessage{{Topic: "t", Partition: 0, Offset: 7}}
	got := buildKafkaBatchAdmissionKey("wf-1", "v1", "unit-a", msgs)
	want := "/wf-1/v1/unit-a/t/0/7-7"
	if got != want {
		t.Fatalf("admission key = %q, want %q", got, want)
	}
}

func TestBuildKafkaBatchExits_CarriesAllMessages(t *testing.T) {
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 1, Offset: 10, Key: []byte("k1"), Value: []byte(`{"a":1}`)},
		{Topic: "t", Partition: 1, Offset: 11, Key: []byte("k2"), Value: []byte(`{"a":2}`)},
	}
	exits := buildKafkaBatchExits("kafka-node", msgs)
	if len(exits) != 1 {
		t.Fatalf("exits len = %d, want 1", len(exits))
	}
	if exits[0].NodeName != "kafka-node" || exits[0].Port != "main" {
		t.Fatalf("exit identity = %q/%q", exits[0].NodeName, exits[0].Port)
	}
	if got := exits[0].Data["count"]; got != 2 {
		t.Fatalf("count = %v, want 2", got)
	}
	if got := exits[0].Data["start_offset"]; got != int64(10) {
		t.Fatalf("start_offset = %v, want 10", got)
	}
	if got := exits[0].Data["end_offset"]; got != int64(11) {
		t.Fatalf("end_offset = %v, want 11", got)
	}
	list, ok := exits[0].Data["messages"].([]map[string]any)
	if !ok || len(list) != 2 {
		t.Fatalf("messages = %#v, want 2 entries", exits[0].Data["messages"])
	}
	if list[1]["value"] != `{"a":2}` {
		t.Fatalf("second message value = %v", list[1]["value"])
	}
}

// 空批不该产生 key —— 调用方永远不该传空，但静默返回一个畸形 key 比 panic 更难查。
func TestBuildKafkaBatchAdmissionKey_EmptyReturnsEmpty(t *testing.T) {
	if got := buildKafkaBatchAdmissionKey("wf", "v1", "u", nil); got != "" {
		t.Fatalf("empty batch key = %q, want empty", got)
	}
}

var _ = types.BoundaryExit{}
