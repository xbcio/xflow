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

func TestValidateEditorMetadataSectionsFilterStaleNodeKeys(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{
		{ID: "node-1", Name: "a"},
		{ID: "node-2", Name: "b"},
	}}
	in := &WorkflowEditorMetadata{Sections: []CanvasSection{{
		ID:       "section-1",
		Title:    "VIP approval",
		NodeKeys: []string{"node-2", "ghost", "node-1"},
		X:        10, Y: 20, Width: 300, Height: 120,
	}}}
	out, diags := ValidateEditorMetadata(def, in)
	if len(diags) != 0 {
		t.Fatalf("diags = %v, want none for stale keys dropped from a section", diags)
	}
	if len(out.Sections) != 1 {
		t.Fatalf("sections = %+v, want one surviving section", out.Sections)
	}
	got := out.Sections[0]
	if got.ID != "section-1" || got.Title != "VIP approval" || got.X != 10 || got.Y != 20 || got.Width != 300 || got.Height != 120 {
		t.Errorf("section fields = %+v, want id/title/geometry preserved", got)
	}
	if want := []string{"node-2", "node-1"}; !equalStrings(got.NodeKeys, want) {
		t.Errorf("node keys = %v, want %v (stale key dropped, order preserved)", got.NodeKeys, want)
	}
}

func TestValidateEditorMetadataSectionsNameKeyWarnsOnce(t *testing.T) {
	// The same ID-less node is addressed by Positions and by a section; the
	// diagnostic must still be emitted exactly once for that name.
	def := &WorkflowDef{Nodes: []NodeDef{{Name: "a"}}}
	in := &WorkflowEditorMetadata{
		Positions: map[string]Position{"a": {X: 1}},
		Sections:  []CanvasSection{{ID: "s1", NodeKeys: []string{"a"}}},
	}
	out, diags := ValidateEditorMetadata(def, in)
	if len(diags) != 1 {
		t.Fatalf("diags = %v, want exactly one %s warning across maps and sections", diags, DiagNodeMetadataKeyedByName)
	}
	if !containsCode(diags[0], DiagNodeMetadataKeyedByName) {
		t.Errorf("diag = %q, want it to carry %q", diags[0], DiagNodeMetadataKeyedByName)
	}
	if len(out.Sections) != 1 || !equalStrings(out.Sections[0].NodeKeys, []string{"a"}) {
		t.Errorf("name-keyed section key dropped: %+v", out.Sections)
	}
}

func TestValidateEditorMetadataDropsSectionWithNoSurvivingKeys(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	in := &WorkflowEditorMetadata{Sections: []CanvasSection{
		{ID: "stale", NodeKeys: []string{"ghost"}},
		{ID: "empty"},
		{ID: "kept", NodeKeys: []string{"node-1"}},
	}}
	out, _ := ValidateEditorMetadata(def, in)
	if len(out.Sections) != 1 || out.Sections[0].ID != "kept" {
		t.Fatalf("sections = %+v, want only the section with a surviving key", out.Sections)
	}
}

func TestValidateEditorMetadataDropsSectionWithEmptyID(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	in := &WorkflowEditorMetadata{Sections: []CanvasSection{
		{Title: "no id", NodeKeys: []string{"node-1"}},
	}}
	out, _ := ValidateEditorMetadata(def, in)
	if out.Sections != nil {
		t.Errorf("sections = %+v, want nil after dropping the empty-ID section", out.Sections)
	}
}

func TestValidateEditorMetadataDuplicateSectionIDKeepsFirst(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	in := &WorkflowEditorMetadata{Sections: []CanvasSection{
		{ID: "dup", Title: "first", NodeKeys: []string{"node-1"}},
		{ID: "dup", Title: "second", NodeKeys: []string{"node-1"}},
		{ID: "other", Title: "third", NodeKeys: []string{"node-1"}},
	}}
	out, _ := ValidateEditorMetadata(def, in)
	if len(out.Sections) != 2 {
		t.Fatalf("sections = %+v, want two after duplicate-ID dedupe", out.Sections)
	}
	if out.Sections[0].ID != "dup" || out.Sections[0].Title != "first" {
		t.Errorf("first section = %+v, want the first duplicate retained", out.Sections[0])
	}
	if out.Sections[1].ID != "other" {
		t.Errorf("second section = %+v, want the non-duplicate retained", out.Sections[1])
	}
}

func TestValidateEditorMetadataEmptySectionsNormalizedToNil(t *testing.T) {
	def := &WorkflowDef{Nodes: []NodeDef{{ID: "node-1", Name: "a"}}}
	for name, in := range map[string]*WorkflowEditorMetadata{
		"empty slice":    {Sections: []CanvasSection{}},
		"all stale keys": {Sections: []CanvasSection{{ID: "s", NodeKeys: []string{"ghost"}}}},
		"only empty ID":  {Sections: []CanvasSection{{NodeKeys: []string{"node-1"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			out, _ := ValidateEditorMetadata(def, in)
			if out.Sections != nil {
				t.Errorf("sections = %+v, want nil", out.Sections)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsCode(msg, code string) bool {
	return len(msg) >= len(code) && (msg[:len(code)] == code)
}
