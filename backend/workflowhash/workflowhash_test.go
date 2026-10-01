package workflowhash

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func mustRuntime(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	h, err := Runtime(def)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	return h
}

// TestRuntimePins carries the same literal constants as the sdk/xflow pins
// (selector_hash_pin_test.go, supply_identity_test.go): moving the algorithm
// into this package must not move a single stored hash.
func TestRuntimePins(t *testing.T) {
	tests := []struct {
		name string
		def  *types.WorkflowDef
		want string
	}{
		{
			name: "selector-bearing definition",
			def: &types.WorkflowDef{
				Namespace: "default",
				Name:      "sel-wf",
				Version:   "v1",
				RunnerSelector: &types.RunnerSelector{
					Mode:        types.RunnerSelectorModeRequired,
					MatchLabels: map[string]string{"zone": "a", "env": "prod"},
				},
				Nodes: []types.NodeDef{
					{
						Name: "trig", Type: "kafka.source", Kind: types.NodeKindTrigger,
						RunnerSelector: &types.RunnerSelector{
							Mode:        types.RunnerSelectorModeDefault,
							MatchLabels: map[string]string{"region": "cn"},
						},
					},
					{Name: "work", Type: "http.request"},
				},
				Groups: []types.GroupDef{
					{
						Name:    "g1",
						Members: []string{"trig", "work"},
						RunnerSelector: &types.RunnerSelector{
							MatchLabels: map[string]string{"pool": "x"},
						},
					},
				},
				Connections: types.Connections{
					"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
				},
			},
			want: "runtime-sha256:v1:4f770306236df9d5470bd4bc6296ea1267c7cc05683eb4cde8696385ae310ad0",
		},
		{
			name: "dependency-edge-free definition",
			def: &types.WorkflowDef{
				Namespace: "default",
				Name:      "wf",
				Version:   "v1",
				Nodes:     []types.NodeDef{{Name: "a", Type: "xflow.transform.set"}},
			},
			want: "runtime-sha256:v1:ed9b25378112181dff504e0e587e980b4e3dd30581ad6a251481626c523b822a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mustRuntime(t, tt.def); got != tt.want {
				t.Fatalf("Runtime(%s) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestRuntimePrefix(t *testing.T) {
	base := func() *types.WorkflowDef {
		return &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{{Name: "a", Type: "xflow.start"}}}
	}
	t.Run("v1 without timeout or output", func(t *testing.T) {
		if h := mustRuntime(t, base()); !strings.HasPrefix(h, RuntimePrefixV1) {
			t.Fatalf("Runtime = %q, want prefix %q", h, RuntimePrefixV1)
		}
	})
	t.Run("v2 with timeout", func(t *testing.T) {
		def := base()
		def.Nodes[0].Timeout = time.Minute
		if h := mustRuntime(t, def); !strings.HasPrefix(h, RuntimePrefixV2) {
			t.Fatalf("Runtime = %q, want prefix %q", h, RuntimePrefixV2)
		}
	})
	t.Run("v2 with private output", func(t *testing.T) {
		def := base()
		def.Nodes[0].Output = &types.NodeOutputPolicy{Private: true}
		if h := mustRuntime(t, def); !strings.HasPrefix(h, RuntimePrefixV2) {
			t.Fatalf("Runtime = %q, want prefix %q", h, RuntimePrefixV2)
		}
	})
}

func TestRuntimeExcludesMetadataAuditIncludesIt(t *testing.T) {
	base := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{{Name: "a", Type: "xflow.start"}}}
	edited := &types.WorkflowDef{
		ID: "wf-1", Name: "wf", Description: "d",
		Nodes: []types.NodeDef{{ID: "n1", Name: "a", Type: "xflow.start", Notes: "n", Position: &types.Position{X: 1}}},
	}
	if a, b := mustRuntime(t, base), mustRuntime(t, edited); a != b {
		t.Fatalf("Runtime changed with metadata: %q != %q", a, b)
	}
	a, err := Audit(base)
	if err != nil {
		t.Fatalf("Audit(base): %v", err)
	}
	b, err := Audit(edited)
	if err != nil {
		t.Fatalf("Audit(edited): %v", err)
	}
	if a == b || !strings.HasPrefix(a, AuditPrefix) {
		t.Fatalf("Audit = (%q, %q), want distinct %q-prefixed fingerprints", a, b, AuditPrefix)
	}
}

func TestRuntimePayloadOmitsEmptyDependencyEdges(t *testing.T) {
	b, err := json.Marshal(RuntimePayload{})
	if err != nil {
		t.Fatalf("marshal empty payload: %v", err)
	}
	if strings.Contains(string(b), "dependency_edges") {
		t.Fatalf("dependency_edges must be omitted when empty: %s", b)
	}
}
