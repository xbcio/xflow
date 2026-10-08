package queue

import (
	"encoding/json"
	"testing"
	"time"

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

// TestQueuedTaskPayloadRoundTripsDeliverableAt pins the delivery-lag stamp:
// the instant the task became deliverable must survive the transport with
// millisecond fidelity, under an internal key that never collides with the
// public runner contract, and its absence must decode as the zero time (no
// observation) rather than a fabricated epoch.
func TestQueuedTaskPayloadRoundTripsDeliverableAt(t *testing.T) {
	stamp := time.Date(2026, 10, 3, 12, 0, 0, 123_000_000, time.UTC)
	task := &engine.Task{
		ExecutionID:   types.ExecutionID("exec-1"),
		NodeName:      "Review",
		NodeIdx:       3,
		Type:          engine.TaskTypeNodeExec,
		DeliverableAt: stamp,
	}
	payload, err := MarshalWithNamespace(task, namespace.Namespace("acme"))
	if err != nil {
		t.Fatalf("MarshalWithNamespace() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	if _, ok := wire["deliverable_at"]; ok {
		t.Fatalf("public deliverable key leaked into queue payload: %s", payload)
	}
	got := wire["_deliverable_at_ms"]
	if got == nil || int64(got.(float64)) != stamp.UnixMilli() {
		t.Fatalf("_deliverable_at_ms = %v, want %d (payload: %s)", got, stamp.UnixMilli(), payload)
	}

	decoded, _, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !decoded.DeliverableAt.Equal(stamp) {
		t.Fatalf("DeliverableAt = %v, want %v", decoded.DeliverableAt, stamp)
	}

	// An unstamped (legacy) payload omits the key and decodes zero.
	legacy, err := json.Marshal(queuedTask{
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-legacy"),
			NodeName:    "Review",
			NodeIdx:     3,
			Type:        engine.TaskTypeNodeExec,
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy payload: %v", err)
	}
	unstamped, _, err := Unmarshal(legacy)
	if err != nil {
		t.Fatalf("Unmarshal(legacy) error = %v", err)
	}
	if !unstamped.DeliverableAt.IsZero() {
		t.Fatalf("legacy DeliverableAt = %v, want zero (not carried)", unstamped.DeliverableAt)
	}
}

// TestQueuedTaskPayloadRoundTripsIntentCreatedAt pins the delayed-intent age
// anchor across the transport. For a timer suspend wakeup (or a retry replay)
// the two stamps diverge — the intent was created hours before it became
// deliverable — and the classifier's provability window must be measured from
// the creation instant. If the wire dropped it, every delayed intent would
// fall back to its availability stamp and a benign cancelled execution's late
// wakeup would be misreported as gone.
func TestQueuedTaskPayloadRoundTripsIntentCreatedAt(t *testing.T) {
	deliverable := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	created := deliverable.Add(-2 * time.Hour)
	task := &engine.Task{
		ExecutionID:     types.ExecutionID("exec-delayed"),
		NodeName:        "Review",
		NodeIdx:         3,
		Type:            engine.TaskTypeNodeResume,
		DeliverableAt:   deliverable,
		IntentCreatedAt: created,
	}
	payload, err := MarshalWithNamespace(task, namespace.Namespace("acme"))
	if err != nil {
		t.Fatalf("MarshalWithNamespace() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	if _, ok := wire["intent_created_at"]; ok {
		t.Fatalf("public intent-created key leaked into queue payload: %s", payload)
	}
	got := wire["_intent_created_at_ms"]
	if got == nil || int64(got.(float64)) != created.UnixMilli() {
		t.Fatalf("_intent_created_at_ms = %v, want %d (payload: %s)", got, created.UnixMilli(), payload)
	}

	decoded, _, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !decoded.IntentCreatedAt.Equal(created) {
		t.Fatalf("IntentCreatedAt = %v, want %v", decoded.IntentCreatedAt, created)
	}
	if !decoded.DeliverableAt.Equal(deliverable) {
		t.Fatalf("DeliverableAt = %v, want %v (the age anchor must not replace the lag stamp)",
			decoded.DeliverableAt, deliverable)
	}

	// A payload written before the field existed carries no anchor and decodes
	// zero; the classifier then falls back to DeliverableAt (see
	// Task.provabilityAnchor), which is the pre-fix behavior for those tasks.
	legacy, err := json.Marshal(queuedTask{
		Task: engine.Task{
			ExecutionID:   types.ExecutionID("exec-legacy-delayed"),
			NodeName:      "Review",
			NodeIdx:       3,
			Type:          engine.TaskTypeNodeResume,
			DeliverableAt: deliverable,
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy payload: %v", err)
	}
	unstamped, _, err := Unmarshal(legacy)
	if err != nil {
		t.Fatalf("Unmarshal(legacy) error = %v", err)
	}
	if !unstamped.IntentCreatedAt.IsZero() {
		t.Fatalf("legacy IntentCreatedAt = %v, want zero (not carried)", unstamped.IntentCreatedAt)
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
