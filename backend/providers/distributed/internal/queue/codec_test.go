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

// TestQueuedTaskPayloadRoundTripsUnitIdx covers F1: a task carrying a
// non-zero UnitIdx (as a group task would) must round-trip exactly, and the
// wire payload must expose it under the internal _unit_idx key.
func TestQueuedTaskPayloadRoundTripsUnitIdx(t *testing.T) {
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "GroupA",
		NodeIdx:     2,
		UnitIdx:     7,
		Type:        engine.TaskTypeGroupExec,
	}

	payload, err := Marshal(task)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	if v, ok := wire["_unit_idx"]; !ok || int(v.(float64)) != 7 {
		t.Fatalf("_unit_idx = %v, want 7 present in payload: %s", wire["_unit_idx"], payload)
	}

	got, _, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.UnitIdx != 7 {
		t.Fatalf("UnitIdx = %d, want 7", got.UnitIdx)
	}
}

// TestUnmarshalMissingUnitIdxYieldsSentinel covers F1: a payload with no
// _unit_idx field (a genuinely old, pre-group durable payload) must decode to
// engine.UnitIdxUnknown, not silently default to 0 — 0 is a legitimate unit
// index and must not be confused with "field absent".
func TestUnmarshalMissingUnitIdxYieldsSentinel(t *testing.T) {
	legacy, err := json.Marshal(queuedTask{
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-legacy"),
			NodeName:    "Review",
			NodeIdx:     3,
			Type:        engine.TaskTypeNodeExec,
		},
		AutoDepth:    1,
		ActivationID: 2,
		// UnitIdx intentionally omitted (nil pointer): simulates a payload
		// written before this field existed.
	})
	if err != nil {
		t.Fatalf("marshal legacy payload: %v", err)
	}
	got, _, err := Unmarshal(legacy)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.UnitIdx != engine.UnitIdxUnknown {
		t.Fatalf("UnitIdx = %d, want engine.UnitIdxUnknown (%d)", got.UnitIdx, engine.UnitIdxUnknown)
	}
}

// TestMarshalUnitIdxUnknownOmitsWireField covers F1: a task that was never
// assigned a real unit index (UnitIdx == engine.UnitIdxUnknown) must not
// serialize a literal -1 that a future reader could mistake for a real unit
// index; the field must be entirely absent.
func TestMarshalUnitIdxUnknownOmitsWireField(t *testing.T) {
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "Review",
		NodeIdx:     3,
		UnitIdx:     engine.UnitIdxUnknown,
		Type:        engine.TaskTypeNodeExec,
	}
	payload, err := Marshal(task)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	if _, ok := wire["_unit_idx"]; ok {
		t.Fatalf("_unit_idx must be absent for UnitIdxUnknown, got payload: %s", payload)
	}
}
