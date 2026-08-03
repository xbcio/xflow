package trigger

import (
	"context"
	"errors"
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

func TestSeedKafkaEntryBatchMessages_AcceptedAllowsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	in := &types.TriggerActivateInput{
		NodeName:   "kafka-node",
		WorkflowID: "wf-1",
		Params:     map[string]any{"entry_unit_id": "unit-a", "workflow_version": "v2"},
	}
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "t", Partition: 0, Offset: 9},
	}
	if !seedKafkaEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("accepted admission must allow commit")
	}
	calls := rt.getCalls()
	if len(calls) != 1 {
		t.Fatalf("seed calls = %d, want 1", len(calls))
	}
	if calls[0].AdmissionKey != "/wf-1/v2/unit-a/t/0/5-9" {
		t.Fatalf("admission key = %q", calls[0].AdmissionKey)
	}
	if calls[0].Outcome != "success" {
		t.Fatalf("outcome = %q, want success", calls[0].Outcome)
	}
}

func TestSeedKafkaEntryBatchMessages_DuplicateAllowsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Duplicate: true}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []KafkaMessage{{Topic: "t", Partition: 0, Offset: 1}}
	if !seedKafkaEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("duplicate admission must allow commit")
	}
}

func TestSeedKafkaEntryBatchMessages_ConflictAllowsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Conflict: true}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []KafkaMessage{{Topic: "t", Partition: 0, Offset: 1}}
	if !seedKafkaEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("conflict means another runner admitted it; commit is correct")
	}
}

// 这条是 offset 安全的关键：stale_generation 经由 error 返回，绝不能提交。
func TestSeedKafkaEntryBatchMessages_ErrorWithholdsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{err: errors.New("entry-seed: rejected by generation fence: stale_generation")}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []KafkaMessage{{Topic: "t", Partition: 0, Offset: 1}}
	if seedKafkaEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("a fence rejection must NOT commit the offset — Kafka must redeliver")
	}
}

// 全 false 的响应是未知状态，防御性地不提交。
func TestSeedKafkaEntryBatchMessages_UnknownStateWithholdsCommit(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	msgs := []KafkaMessage{{Topic: "t", Partition: 0, Offset: 1}}
	if seedKafkaEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("unknown admission state must not commit")
	}
}

func TestSeedKafkaEntryBatchMessages_EmptyBatchIsNoop(t *testing.T) {
	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}}
	in := &types.TriggerActivateInput{
		NodeName: "n", WorkflowID: "wf", Params: map[string]any{},
	}
	if !seedKafkaEntryBatchMessages(context.Background(), in, rt, nil) {
		t.Fatal("empty batch is trivially done")
	}
	if len(rt.getCalls()) != 0 {
		t.Fatal("empty batch must not call the control plane")
	}
}
