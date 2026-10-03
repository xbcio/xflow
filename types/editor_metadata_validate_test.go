package types

import "testing"

func TestValidateEditorMetadataNilReturnsNil(t *testing.T) {
	md, diags := ValidateEditorMetadata(&WorkflowDef{}, nil)
	if md != nil || diags != nil {
		t.Fatalf("got (%v, %v), want (nil, nil)", md, diags)
	}
}

func TestValidateEditorMetadataKeyedByID(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	in := &WorkflowEditorMetadata{
		Positions: map[string]Position{"node-1": {X: 1, Y: 2}},
		Notes:     map[string]string{"node-1": "note"},
	}
	out, diags := ValidateEditorMetadata(def, in)
	if len(diags) != 0 {
		t.Fatalf("diags = %v, want none for ID-keyed node", diags)
	}
	if out.Positions["node-1"] != (Position{X: 1, Y: 2}) {
		t.Errorf("positions dropped: %+v", out.Positions)
	}
	if out.Notes["node-1"] != "note" {
		t.Errorf("notes dropped: %+v", out.Notes)
	}
}

func TestValidateEditorMetadataKeyedByNameWarns(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{Name: "a"}}} // no ID
	in := &WorkflowEditorMetadata{Positions: map[string]Position{"a": {X: 1}}}
	out, diags := ValidateEditorMetadata(def, in)
	if len(diags) != 1 {
		t.Fatalf("diags = %v, want exactly one NODE_METADATA_KEYED_BY_NAME warning", diags)
	}
	if got := diags[0]; !containsCode(got, DiagNodeMetadataKeyedByName) {
		t.Errorf("diag = %q, want it to carry %q", got, DiagNodeMetadataKeyedByName)
	}
	if out.Positions["a"] != (Position{X: 1}) {
		t.Errorf("expected name-fallback key to be kept: %+v", out.Positions)
	}
}

func TestValidateEditorMetadataDropsUnmatchedKeySilently(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	in := &WorkflowEditorMetadata{
		Positions: map[string]Position{"node-1": {X: 1}, "ghost": {X: 99}},
		UI:        map[string]any{"ghost": "x"},
		Notes:     map[string]string{"ghost": "x"},
	}
	out, diags := ValidateEditorMetadata(def, in)
	if len(diags) != 0 {
		t.Fatalf("diags = %v, want none for a dropped unmatched key", diags)
	}
	if _, ok := out.Positions["ghost"]; ok {
		t.Errorf("expected ghost position key to be dropped: %+v", out.Positions)
	}
	if out.Positions["node-1"] != (Position{X: 1}) {
		t.Errorf("expected matching key kept: %+v", out.Positions)
	}
	if out.UI != nil {
		t.Errorf("expected UI to be nil after dropping its only key, got %+v", out.UI)
	}
	if out.Notes != nil {
		t.Errorf("expected Notes to be nil after dropping its only key, got %+v", out.Notes)
	}
}

func TestValidateEditorMetadataDropsNameKeyWhenNodeHasID(t *testing.T) {
	// A node that has a stable ID is addressable only by that ID; a key equal
	// to its Name must not match it.
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	in := &WorkflowEditorMetadata{Positions: map[string]Position{"a": {X: 1}}}
	out, diags := ValidateEditorMetadata(def, in)
	if len(diags) != 0 {
		t.Fatalf("diags = %v, want none", diags)
	}
	if out.Positions != nil {
		t.Errorf("expected name-collision key dropped: %+v", out.Positions)
	}
}

func TestValidateEditorMetadataViewportPassesThroughUnconditionally(t *testing.T) {
	def := &WorkflowDef{}
	in := &WorkflowEditorMetadata{Viewport: &Viewport{X: 1, Y: 2, Zoom: 3}}
	out, _ := ValidateEditorMetadata(def, in)
	if out.Viewport == nil || *out.Viewport != *in.Viewport {
		t.Errorf("viewport = %+v, want %+v", out.Viewport, in.Viewport)
	}
}

func containsCode(msg, code string) bool {
	return len(msg) >= len(code) && (msg[:len(code)] == code)
}
