package queue

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestQueuedTaskPayloadKeepsSchedulerMetadataPrivate(t *testing.T) {
	task := &engine.Task{
		ExecutionID:  types.ExecutionID("exec-1"),
		NodeName:     "Review",
		NodeIdx:      3,
		Type:         engine.TaskTypeNodeExec,
		AutoDepth:    8,
		ActivationID: 13,
	}

	payload, err := MarshalWithNamespace(task, namespace.Namespace("acme"))
	if err != nil {
		t.Fatalf("MarshalWithNamespace() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	for _, key := range []string{"auto_depth", "activation_id"} {
		if _, ok := wire[key]; ok {
			t.Fatalf("public scheduler key %q leaked into queue payload: %s", key, payload)
		}
	}
	for _, key := range []string{"_auto_depth", "_activation_id", "_namespace"} {
		if _, ok := wire[key]; !ok {
			t.Fatalf("internal queue key %q missing from payload: %s", key, payload)
		}
	}

	got, namespaceID, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.AutoDepth != task.AutoDepth || got.ActivationID != task.ActivationID {
		t.Fatalf("scheduler metadata = %d/%d, want %d/%d", got.AutoDepth, got.ActivationID, task.AutoDepth, task.ActivationID)
	}
	if namespaceID != "acme" {
		t.Fatalf("namespace = %q, want %q", namespaceID, "acme")
	}
}

func TestUnmarshalLegacyPayloadFallsBackToDefaultNamespace(t *testing.T) {
	// Simulate a payload written before the namespace field existed.
	legacy, err := json.Marshal(queuedTask{
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-legacy"),
			NodeName:    "Review",
			NodeIdx:     3,
			Type:        engine.TaskTypeNodeExec,
		},
		AutoDepth:    1,
		ActivationID: 2,
	})
	if err != nil {
		t.Fatalf("marshal legacy payload: %v", err)
	}
	_, namespaceID, err := Unmarshal(legacy)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if namespaceID != namespace.Default {
		t.Fatalf("namespace = %q, want default", namespaceID)
	}
}
