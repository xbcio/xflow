package xflow

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func baseRuntimeDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		ID:          "wf-1",
		Namespace:   "namespace-a",
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
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "main"}}}},
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
			d.Connections = types.Connections{"init": {"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "main"}}}}}
		}},
		{"Connections", func(d *types.WorkflowDef) {
			d.Connections = types.Connections{"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "alt"}}}}}
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

func TestRuntimeHashIncludesActivationReplicas(t *testing.T) {
	nodeBase := baseRuntimeDef()
	nodeHash := mustRuntimeHash(t, nodeBase)
	nodeReplicated := baseRuntimeDef()
	nodeReplicated.Nodes[0].ActivationReplicas = 3
	if got := mustRuntimeHash(t, nodeReplicated); got == nodeHash {
		t.Fatal("standalone node activation cardinality did not change runtime hash")
	}

	groupBase := baseRuntimeDef()
	groupBase.Groups = []types.GroupDef{{Name: "entry", Members: []string{"start", "review"}}}
	groupHash := mustRuntimeHash(t, groupBase)
	groupReplicated := baseRuntimeDef()
	groupReplicated.Groups = []types.GroupDef{{Name: "entry", Members: []string{"start", "review"}, ActivationReplicas: 3}}
	if got := mustRuntimeHash(t, groupReplicated); got == groupHash {
		t.Fatal("group activation cardinality did not change runtime hash")
	}
}

func TestRuntimeHashExcludesInstanceID(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	withID := baseRuntimeDef()
	withID.ID = "wf-2"
	if got := mustRuntimeHash(t, withID); got != baseHash {
		t.Fatalf("runtime hash changed after changing WorkflowDef.ID: %s != %s", got, baseHash)
	}
}

func TestRuntimeHashIncludesNamespace(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	withNamespace := baseRuntimeDef()
	withNamespace.Namespace = "namespace-b"
	if got := mustRuntimeHash(t, withNamespace); got == baseHash {
		t.Fatalf("runtime hash did not change after changing WorkflowDef.Namespace")
	}
}

// TestRuntimeHashExcludesDescription asserts that the workflow-level
// Description field — purely human documentation — does not factor into the
// runtime hash. Changing it must not invalidate an existing registry record.
func TestRuntimeHashExcludesDescription(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	redescribed := baseRuntimeDef()
	redescribed.Description = "completely new description text"
	if got := mustRuntimeHash(t, redescribed); got != baseHash {
		t.Fatalf("runtime hash changed after changing Description: %s != %s", got, baseHash)
	}

	blanked := baseRuntimeDef()
	blanked.Description = ""
	if got := mustRuntimeHash(t, blanked); got != baseHash {
		t.Fatalf("runtime hash changed after blanking Description: %s != %s", got, baseHash)
	}
}

// TestRuntimeHashExcludesNodeID asserts that NodeDef.ID — the stable editor
// identity — does not factor into the runtime hash. Changing it (e.g. when
// an editor re-imports a workflow with refreshed IDs) must not invalidate
// an existing registry record.
func TestRuntimeHashExcludesNodeID(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustRuntimeHash(t, base)

	renamed := baseRuntimeDef()
	renamed.Nodes[0].ID = "n1-primed"
	renamed.Nodes[1].ID = "n2-primed"
	if got := mustRuntimeHash(t, renamed); got != baseHash {
		t.Fatalf("runtime hash changed after changing NodeDef.ID: %s != %s", got, baseHash)
	}

	cleared := baseRuntimeDef()
	cleared.Nodes[0].ID = ""
	cleared.Nodes[1].ID = ""
	if got := mustRuntimeHash(t, cleared); got != baseHash {
		t.Fatalf("runtime hash changed after clearing NodeDef.ID: %s != %s", got, baseHash)
	}
}

// TestAuditFingerprintIncludesDescriptionAndNodeID asserts the inverse of the
// runtime-hash exclusion tests: the audit fingerprint (legacyDefinitionHash)
// DOES cover Description and NodeDef.ID, so audit/export traceability is
// preserved when these fields change.
func TestAuditFingerprintIncludesDescriptionAndNodeID(t *testing.T) {
	base := baseRuntimeDef()
	baseHash := mustLegacyHash(t, base)

	redescribed := baseRuntimeDef()
	redescribed.Description = "different description"
	if got := mustLegacyHash(t, redescribed); got == baseHash {
		t.Fatalf("audit fingerprint did not change after changing Description")
	}

	renamed := baseRuntimeDef()
	renamed.Nodes[0].ID = "n1-primed"
	renamed.Nodes[1].ID = "n2-primed"
	if got := mustLegacyHash(t, renamed); got == baseHash {
		t.Fatalf("audit fingerprint did not change after changing NodeDef.ID")
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
	if !strings.HasPrefix(h, runtimeHashPrefix) {
		t.Fatalf("runtime hash missing version marker: %s", h)
	}
}

func TestReconcileDefinitionHash(t *testing.T) {
	def := baseRuntimeDef()
	currentHash, err := runtimeHash(def)
	if err != nil {
		t.Fatalf("runtimeHash: %v", err)
	}

	// Already-current runtime hash → returned as-is, no upgrade.
	got, upgrade, err := reconcileDefinitionHash(currentHash, def)
	if err != nil {
		t.Fatalf("reconcile current: %v", err)
	}
	if got != currentHash || upgrade {
		t.Fatalf("reconcile current = (%q, %v), want (%q, false)", got, upgrade, currentHash)
	}

	// Legacy bare sha256: hash → recomputed, needsUpgrade=true.
	legacyHash := "sha256:deadbeef"
	got, upgrade, err = reconcileDefinitionHash(legacyHash, def)
	if err != nil {
		t.Fatalf("reconcile legacy: %v", err)
	}
	if got != currentHash || !upgrade {
		t.Fatalf("reconcile legacy = (%q, %v), want (%q, true)", got, upgrade, currentHash)
	}

	// Audit fingerprint hash → also reconciled (it is not a runtime hash).
	auditHash := "sha256:audit:v1:feedface"
	got, upgrade, err = reconcileDefinitionHash(auditHash, def)
	if err != nil {
		t.Fatalf("reconcile audit: %v", err)
	}
	if got != currentHash || !upgrade {
		t.Fatalf("reconcile audit = (%q, %v), want (%q, true)", got, upgrade, currentHash)
	}

	// Legacy hash with nil stored def → error (cannot recompute).
	if _, _, err := reconcileDefinitionHash(legacyHash, nil); err == nil {
		t.Fatalf("reconcile legacy with nil def: want error, got nil")
	}

	// Current runtime hash with nil stored def → OK (no recompute needed).
	got, upgrade, err = reconcileDefinitionHash(currentHash, nil)
	if err != nil {
		t.Fatalf("reconcile current with nil def: %v", err)
	}
	if got != currentHash || upgrade {
		t.Fatalf("reconcile current with nil def = (%q, %v), want (%q, false)", got, upgrade, currentHash)
	}
}
