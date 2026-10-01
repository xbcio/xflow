package apiserver

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/types"
)

// waitWorkflow is start -> wait. withMode spells out wait.mode, the builtin
// Default the SDK builder writes; without it the definition is what an editor
// or YAML author sends.
func waitWorkflow(name string, withMode bool) *types.WorkflowDef {
	params := map[string]any{"signal_name": "go"}
	if withMode {
		params["mode"] = "signal"
	}
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "wait", Type: "xflow.wait", Parameters: params},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "wait", Input: "main"}}}},
		},
	}
}

func postRegisterID(t *testing.T, base string, def *types.WorkflowDef, wantStatus int) types.WorkflowID {
	t.Helper()
	resp := postRegister(t, base, "tok-full", def)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST status = %d, want %d", resp.StatusCode, wantStatus)
	}
	if wantStatus != http.StatusCreated {
		return ""
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	return out.WorkflowID
}

// TestRegisterWorkflowStoresCanonicalRuntimeHash pins that POST stores the
// runtime hash the SDK computes, over the definition with builtin Defaults
// filled in memory only: the stored definition stays as sent.
func TestRegisterWorkflowStoresCanonicalRuntimeHash(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	id := postRegisterID(t, srv.URL, waitWorkflow("hash", false), http.StatusCreated)
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if !strings.HasPrefix(rec.DefinitionHash, workflowhash.RuntimePrefixV1) {
		t.Fatalf("DefinitionHash = %q, want %s prefix", rec.DefinitionHash, workflowhash.RuntimePrefixV1)
	}
	filled := waitWorkflow("hash", true)
	filled.Namespace = "namespaceA"
	want, err := workflowhash.Runtime(filled, nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	if rec.DefinitionHash != want {
		t.Fatalf("DefinitionHash = %q, want the hash of the Default-filled definition %q", rec.DefinitionHash, want)
	}
	if _, ok := rec.Definition.Nodes[1].Parameters["mode"]; ok {
		t.Fatal("stored definition gained wait.mode; the HTTP path must not write Defaults")
	}
}

// TestRegisterWorkflowOmittedDefaultIsIdempotent pins that an omitted builtin
// param and its spelled-out Default are one registration identity.
func TestRegisterWorkflowOmittedDefaultIsIdempotent(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	first := postRegisterID(t, srv.URL, waitWorkflow("idem", false), http.StatusCreated)
	second := postRegisterID(t, srv.URL, waitWorkflow("idem", true), http.StatusCreated)
	if first != second {
		t.Fatalf("second POST id = %q, want the existing %q", second, first)
	}

	changed := waitWorkflow("idem", false)
	changed.Nodes[1].Parameters["signal_name"] = "other"
	postRegisterID(t, srv.URL, changed, http.StatusConflict)
}
