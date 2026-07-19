package xflow

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestWorkflowKeyUsesNamespaceNameVersion(t *testing.T) {
	def := &types.WorkflowDef{Namespace: "risk", Name: "approval", Version: "v3"}
	got := workflowKey(def)
	if got != "risk/approval@v3" {
		t.Fatalf("workflowKey = %q, want risk/approval@v3", got)
	}
}

func TestDefinitionHashStableForEquivalentDefinitions(t *testing.T) {
	a := &types.WorkflowDef{Name: "wf", Namespace: "default", Version: "v1", Nodes: []types.NodeDef{{Name: "start", Type: "xflow.start"}}}
	b := &types.WorkflowDef{Version: "v1", Namespace: "default", Name: "wf", Nodes: []types.NodeDef{{Type: "xflow.start", Name: "start"}}}
	ha, err := legacyDefinitionHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := legacyDefinitionHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("hashes differ: %s != %s", ha, hb)
	}
}
