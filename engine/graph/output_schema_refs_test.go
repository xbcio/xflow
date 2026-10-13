package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func schemaRefDef(schema map[string]any, ref string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "schema-ref",
		Nodes: []types.NodeDef{
			{Name: "fetch_user", Type: "test.noop", OutputSchema: schema},
			{Name: "notify", Type: "test.noop", Parameters: map[string]any{"to": ref}},
		},
		Connections: types.Connections{
			"fetch_user": {"main": {Targets: []types.Connection{{Node: "notify", Input: "main"}}}},
		},
	}
}

var userSchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"vip_level": map[string]any{"type": "number"},
		"profile": map[string]any{
			"type":       "object",
			"properties": map[string]any{"city": map[string]any{"type": "string"}},
		},
		"meta":  map[string]any{"type": "object"},
		"items": map[string]any{"type": "array"},
	},
}

func TestOutputSchemaRefs_MissingField(t *testing.T) {
	_, err := Compile(schemaRefDef(userSchema, "${{ $nodes['fetch_user'].email }}"))
	if err == nil {
		t.Fatal("expected compile error for undeclared field")
	}
	for _, want := range []string{"notify", "fetch_user", "email"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q should mention %q", err, want)
		}
	}
}

func TestOutputSchemaRefs_MissingNestedField(t *testing.T) {
	_, err := Compile(schemaRefDef(userSchema, "$nodes['fetch_user'].profile['zip'] == 1"))
	if err == nil || !strings.Contains(err.Error(), "profile.zip") {
		t.Fatalf("expected profile.zip error, got %v", err)
	}
}

func TestOutputSchemaRefs_Allowed(t *testing.T) {
	cases := map[string]struct {
		schema map[string]any
		ref    string
	}{
		"present":       {userSchema, "$nodes['fetch_user'].vip_level > 3"},
		"nested":        {userSchema, "${{ $nodes['fetch_user'].profile.city }}"},
		"open object":   {userSchema, "${{ $nodes['fetch_user'].meta.anything }}"},
		"array":         {userSchema, "${{ $nodes['fetch_user'].items.length }}"},
		"dynamic":       {userSchema, "${{ $nodes['fetch_user'][$vars.k] }}"},
		"index":         {userSchema, "${{ $nodes['fetch_user'].items[0].x }}"},
		"whole output":  {userSchema, "${{ $nodes['fetch_user'] }}"},
		"no schema":     {nil, "${{ $nodes['fetch_user'].email }}"},
		"no properties": {map[string]any{"type": "object"}, "${{ $nodes['fetch_user'].email }}"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Compile(schemaRefDef(tc.schema, tc.ref)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
