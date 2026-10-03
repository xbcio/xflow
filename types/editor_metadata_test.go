package types

import (
	"encoding/json"
	"testing"
)

func TestWorkflowEditorMetadataJSONRoundTrip(t *testing.T) {
	md := WorkflowEditorMetadata{
		Positions: map[string]Position{"n1": {X: 1, Y: 2}},
		Viewport:  &Viewport{X: 10, Y: 20, Zoom: 1.5},
		UI:        map[string]any{"n1": map[string]any{"color": "blue"}},
		Notes:     map[string]string{"n1": "a note"},
	}
	data, err := json.Marshal(md)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got WorkflowEditorMetadata
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Positions["n1"] != md.Positions["n1"] {
		t.Errorf("positions = %+v, want %+v", got.Positions, md.Positions)
	}
	if got.Viewport == nil || *got.Viewport != *md.Viewport {
		t.Errorf("viewport = %+v, want %+v", got.Viewport, md.Viewport)
	}
	if got.Notes["n1"] != "a note" {
		t.Errorf("notes = %+v", got.Notes)
	}
}

func TestWorkflowEditorMetadataZeroValueOmitsFields(t *testing.T) {
	data, err := json.Marshal(WorkflowEditorMetadata{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != "{}" {
		t.Errorf("zero-value metadata = %s, want {}", data)
	}
}

func TestWorkflowEditorMetadataJSONTags(t *testing.T) {
	md := WorkflowEditorMetadata{
		Positions: map[string]Position{"n1": {X: 1}},
		Viewport:  &Viewport{X: 1},
		UI:        map[string]any{"n1": "x"},
		Notes:     map[string]string{"n1": "x"},
	}
	data, err := json.Marshal(md)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"positions", "viewport", "ui", "notes"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("missing expected JSON key %q in %s", key, data)
		}
	}
}
