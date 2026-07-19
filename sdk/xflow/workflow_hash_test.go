package xflow

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func baseRuntimeDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		ID:          "wf-1",
		Namespace:   "default",
		TenantID:    "tenant-a",
		Name:        "approval",
		Version:     "v1",
		Description: "desc",
		Spec:        "1.0",
		Nodes: []types.NodeDef{
			{
				ID:         "n1",
				Name:       "start",
				Type:       "xflow.start",
				Kind:       types.NodeKindAction,
				Version:    1,
				Parameters: map[string]any{"foo": "bar"},
				Position:   &types.Position{X: 10, Y: 20},
				UI:         map[string]any{"color": "red"},
				Notes:      "note-a",
			},
			{
				ID:       "n2",
				Name:     "review",
				Type:     "xflow.function",
				Kind:     types.NodeKindAction,
				Position: &types.Position{X: 100, Y: 200},
			},
		},
		Connections: types.Connections{
			"start": {"main": {{Node: "review", Input: "main"}}},
		},
		PinData: map[string]any{"start": map[string]any{"x": 1}},
	}
}

func mustRuntimeHash(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	h, err := runtimeHash(def)
	if err != nil {
		t.Fatalf("runtimeHash: %v", err)
	}
	return h
}

func mustLegacyHash(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	h, err := legacyDefinitionHash(def)
	if err != nil {
		t.Fatalf("legacyDefinitionHash: %v", err)
	}
	return h
}

func TestRuntimeHashExcludesEditorMetadata(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	moved := baseRuntimeDef()
	moved.Nodes[0].Position = &types.Position{X: 999, Y: 999}
	if got := mustRuntimeHash(t, moved); got != baseHash {
		t.Fatalf("runtime hash changed after moving Position: %s != %s", got, baseHash)
	}

	styled := baseRuntimeDef()
	styled.Nodes[0].UI = map[string]any{"color": "blue", "size": 42}
	if got := mustRuntimeHash(t, styled); got != baseHash {
		t.Fatalf("runtime hash changed after changing UI: %s != %s", got, baseHash)
	}

	noted := baseRuntimeDef()
	noted.Nodes[0].Notes = "completely different note"
	if got := mustRuntimeHash(t, noted); got != baseHash {
		t.Fatalf("runtime hash changed after changing Notes: %s != %s", got, baseHash)
	}
}

func TestLegacyHashIncludesEditorMetadata(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustLegacyHash(t, base)

	moved := baseRuntimeDef()
	moved.Nodes[0].Position = &types.Position{X: 999, Y: 999}
	if got := mustLegacyHash(t, moved); got == baseHash {
		t.Fatalf("legacy hash did not change after moving Position")
	}
}

func TestRuntimeHashIncludesRuntimeFields(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	cases := []struct {
		name string
		mut  func(*types.WorkflowDef)
	}{
		{"NodeType", func(d *types.WorkflowDef) { d.Nodes[0].Type = "xflow.merge" }},
		{"NodeVersion", func(d *types.WorkflowDef) { d.Nodes[0].Version = 2 }},
		{"NodeParameters", func(d *types.WorkflowDef) { d.Nodes[0].Parameters = map[string]any{"foo": "baz"} }},
		{"NodeKind", func(d *types.WorkflowDef) { d.Nodes[0].Kind = types.NodeKindTrigger }},
		{"NodeName", func(d *types.WorkflowDef) {
			d.Nodes[0].Name = "init"
			d.Connections = types.Connections{"init": {"main": {{Node: "review", Input: "main"}}}}
		}},
		{"Connections", func(d *types.WorkflowDef) {
			d.Connections = types.Connections{"start": {"main": {{Node: "review", Input: "alt"}}}}
		}},
		{"PinData", func(d *types.WorkflowDef) { d.PinData = map[string]any{"start": map[string]any{"x": 2}} }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := baseRuntimeDef()
			tc.mut(def)
			if got := mustRuntimeHash(t, def); got == baseHash {
				t.Fatalf("runtime hash did not change after changing %s", tc.name)
			}
		})
	}
}

func TestRuntimeHashExcludesInstanceIdentifiers(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	withID := baseRuntimeDef()
	withID.ID = "wf-2"
	if got := mustRuntimeHash(t, withID); got != baseHash {
		t.Fatalf("runtime hash changed after changing WorkflowDef.ID: %s != %s", got, baseHash)
	}

	withTenant := baseRuntimeDef()
	withTenant.TenantID = "tenant-b"
	if got := mustRuntimeHash(t, withTenant); got != baseHash {
		t.Fatalf("runtime hash changed after changing WorkflowDef.TenantID: %s != %s", got, baseHash)
	}
}

func TestRuntimeHashStableForEquivalentDefinitions(t *testing.T) {
	a := &types.WorkflowDef{
		Namespace: "default",
		Name:      "wf",
		Version:   "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
		},
	}
	b := &types.WorkflowDef{
		Version:   "v1",
		Namespace: "default",
		Name:      "wf",
		Nodes: []types.NodeDef{
			{Type: "xflow.start", Name: "start"},
		},
	}
	ha := mustRuntimeHash(t, a)
	hb := mustRuntimeHash(t, b)
	if ha != hb {
		t.Fatalf("runtime hashes differ for equivalent definitions: %s != %s", ha, hb)
	}
}

func TestRuntimeHashHasVersionMarker(t *testing.T) {
	h := mustRuntimeHash(t, baseRuntimeDef())
	if !strings.HasPrefix(h, "runtime-sha256:v1:") {
		t.Fatalf("runtime hash missing version marker: %s", h)
	}
}
