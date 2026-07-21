package openapi

import (
	"context"
	"embed"
	"encoding/json"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/xbcio/xflow/types"
)

//go:embed xflow-v1.yaml
var specYAML []byte

//go:embed fixtures/*.json
var fixtureFS embed.FS

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	spec, err := loader.LoadFromData(specYAML)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		t.Fatalf("validate spec: %v", err)
	}
	return spec
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := fixtureFS.ReadFile("fixtures/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestSpecIsValid(t *testing.T) {
	_ = loadSpec(t)
}

func TestFixturesMatchSpec(t *testing.T) {
	spec := loadSpec(t)

	cases := []struct {
		fixture string
		schema  string
	}{
		{"workflow-draft.json", "WorkflowDraft"},
		{"workflow-version.json", "WorkflowDefinitionVersion"},
		{"validation-result.json", "ValidationResult"},
		{"execution-snapshot.json", "ExecutionSnapshot"},
	}

	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			schemaRef, ok := spec.Components.Schemas[tc.schema]
			if !ok {
				t.Fatalf("schema %q not found in spec", tc.schema)
			}

			data := loadFixture(t, tc.fixture)
			var value any
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatalf("unmarshal fixture: %v", err)
			}

			if err := schemaRef.Value.VisitJSON(value); err != nil {
				t.Fatalf("fixture does not match schema %q: %v", tc.schema, err)
			}
		})
	}
}

func TestWorkflowDraftRoundTripsThroughWorkflowDef(t *testing.T) {
	data := loadFixture(t, "workflow-draft.json")

	var draft struct {
		Definition map[string]any `json:"definition"`
	}
	if err := json.Unmarshal(data, &draft); err != nil {
		t.Fatalf("unmarshal draft fixture: %v", err)
	}

	// The OpenAPI runtime object uses "versionTag" to avoid ambiguity with the
	// published version's "version" field. The internal WorkflowDef uses
	// "version", so normalize before round-tripping.
	if vt, ok := draft.Definition["versionTag"].(string); ok {
		draft.Definition["version"] = vt
		delete(draft.Definition, "versionTag")
	}

	defJSON, err := json.Marshal(draft.Definition)
	if err != nil {
		t.Fatalf("marshal runtime definition: %v", err)
	}

	var def types.WorkflowDef
	if err := json.Unmarshal(defJSON, &def); err != nil {
		t.Fatalf("unmarshal into WorkflowDef: %v", err)
	}

	if def.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", def.Namespace, "default")
	}
	if def.Name != "health-check" {
		t.Errorf("name = %q, want %q", def.Name, "health-check")
	}
	if len(def.Nodes) != 2 {
		t.Errorf("nodes count = %d, want 2", len(def.Nodes))
	}
	if _, ok := def.Connections["Start"]; !ok {
		t.Errorf("expected connection from Start")
	}

	// Ensure editor-only fields are not present in the runtime definition of
	// the fixture (they live in editorMetadata).
	for i, n := range def.Nodes {
		if n.Position != nil {
			t.Errorf("node %d has position in runtime definition", i)
		}
		if n.UI != nil {
			t.Errorf("node %d has ui in runtime definition", i)
		}
		if n.Notes != "" {
			t.Errorf("node %d has notes in runtime definition", i)
		}
	}
}

func TestAllFixturesAreTested(t *testing.T) {
	entries, err := fixtureFS.ReadDir("fixtures")
	if err != nil {
		t.Fatalf("read fixtures dir: %v", err)
	}
	tested := map[string]bool{
		"workflow-draft.json":     true,
		"workflow-version.json":   true,
		"validation-result.json":  true,
		"execution-snapshot.json": true,
	}
	for _, e := range entries {
		if e.Type().IsRegular() && !tested[e.Name()] {
			t.Errorf("fixture %s is not covered by TestFixturesMatchSpec", e.Name())
		}
	}
}
