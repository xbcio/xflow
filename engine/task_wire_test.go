package engine

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestTaskJSONUsesStableWireFieldNames(t *testing.T) {
	task := Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "parse",
		NodeIdx:     2,
		Type:        TaskTypeNodeExec,
	}

	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"execution_id", "node_name", "node_idx", "type"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("expected wire field %q in %s", key, data)
		}
	}
	if _, ok := got["ExecutionID"]; ok {
		t.Fatalf("unexpected Go field name in wire payload: %s", data)
	}
}

func TestRunnerProtocolTypesUseStableWireFieldNames(t *testing.T) {
	lease := TaskLease{
		LeaseID:     LeaseID("lease-1"),
		LeaseToken:  LeaseToken("token-1"),
		Attempt:     2,
		Task:        Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "parse"},
		NodeType:    "xflow.function",
		NodeVersion: 1,
	}

	data, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"lease_id", "lease_token", "attempt", "task", "node_type", "node_version"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("expected wire field %q in %s", key, data)
		}
	}
	if _, ok := got["LeaseToken"]; ok {
		t.Fatalf("unexpected Go field name in wire payload: %s", data)
	}

	heartbeat := RunnerHeartbeat{
		RunnerID:     "runner-1",
		Capacity:     8,
		InFlight:     3,
		Capabilities: []RunnerCapability{{NodeType: "xflow.function", Version: 1}},
	}
	data, err = json.Marshal(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	got = map[string]any{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"runner_id", "capacity", "in_flight", "capabilities"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("expected heartbeat field %q in %s", key, data)
		}
	}
}
