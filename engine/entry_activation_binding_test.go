package engine

import (
	"encoding/json"
	"testing"
)

// TestBindingIsDeclarationRequiresBothNames pins the discriminator the runner
// branches on. A binding with only one of the two names is not a usable
// declaration: the runner keys its declaration table by the pair, so half a key
// would silently land under an empty string and be picked up by an unrelated
// node.
func TestBindingIsDeclarationRequiresBothNames(t *testing.T) {
	cases := []struct {
		name string
		b    SupplyConsumerBinding
		want bool
	}{
		{"both names", SupplyConsumerBinding{WorkflowName: "collect", NodeName: "decode", SupplyNode: "rules"}, true},
		{"workflow only", SupplyConsumerBinding{WorkflowName: "collect", SupplyNode: "rules"}, false},
		{"node only", SupplyConsumerBinding{NodeName: "decode", SupplyNode: "rules"}, false},
		{"legacy digest", SupplyConsumerBinding{ModuleDigest: "sha256:aa", SupplyNode: "rules"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.b.IsDeclaration(); got != tc.want {
				t.Fatalf("IsDeclaration() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestLegacyBindingJSONStaysByteStable is the mixed-version guard on the READ
// side: a record written by an older control plane carries only module_digest
// and supply_node, and must still decode into a usable legacy binding. The new
// fields are omitempty so a legacy record round-trips byte-for-byte.
func TestLegacyBindingJSONStaysByteStable(t *testing.T) {
	const legacy = `{"module_digest":"sha256:abc","supply_node":"rules"}`
	var b SupplyConsumerBinding
	if err := json.Unmarshal([]byte(legacy), &b); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if b.ModuleDigest != "sha256:abc" || b.SupplyNode != "rules" {
		t.Fatalf("legacy decode lost fields: %+v", b)
	}
	if b.IsDeclaration() {
		t.Fatal("a legacy record must NOT be treated as a declaration -- the runner would " +
			"record an empty (workflow, node) key and never resolve a digest")
	}
	out, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != legacy {
		t.Fatalf("legacy round-trip = %s, want %s", out, legacy)
	}
}

// TestDeclarationBindingJSONOmitsDigest: a declaration carries no module digest
// at all. If module_digest were ever populated on a declaration the runner's
// branch would take the legacy path and try to compile a template string.
func TestDeclarationBindingJSONOmitsDigest(t *testing.T) {
	b := SupplyConsumerBinding{
		SupplyNode:   "wasm_versions",
		WorkflowName: "collect",
		NodeName:     "decode",
		DigestExpr:   "${{ $supplies.wasm_versions.decode.digest }}",
	}
	out, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"supply_node":"wasm_versions","workflow_name":"collect","node_name":"decode","digest_expr":"${{ $supplies.wasm_versions.decode.digest }}"}`
	if string(out) != want {
		t.Fatalf("marshal = %s\nwant %s", out, want)
	}
}
