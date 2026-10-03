package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// newEditorMetadataTwoNamespaceTestServer builds an apiserver with two bearer
// tokens in two distinct namespaces, for cross-namespace editor_metadata IDOR
// coverage (ADR-D4 D4: EditorMetadata lives on the same WorkflowRecord as the
// definition, so it must inherit the same namespace isolation).
func newEditorMetadataTwoNamespaceTestServer(t *testing.T) (string, *control.ControlPlane) {
	t.Helper()
	backend := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	cfg := Config{
		Concurrency: 1,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
			{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"workflow", "execution"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL, cp
}

// TestEditorMetadataGetCrossNamespaceIsNotFound proves a namespaceB principal
// cannot read namespaceA's editor_metadata by GETting the workflow id: the
// generic cross-namespace 404 (module_control.go's namespace-scoped registry
// read) must also hide the editor_metadata sibling, since there is no
// separate authorization path for it to leak through (D4: one record).
func TestEditorMetadataGetCrossNamespaceIsNotFound(t *testing.T) {
	srv, _ := newEditorMetadataTwoNamespaceTestServer(t)

	def := editorMetadataWorkflow("editor-ns-a")
	md := &types.WorkflowEditorMetadata{
		Positions: map[string]types.Position{"n-start": {X: 1, Y: 2}},
		Notes:     map[string]string{"n-wait": "namespaceA secret note"},
	}
	resp := postWorkflows(t, srv, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	_ = resp.Body.Close()

	// namespaceB attempts to GET namespaceA's workflow by id.
	getReq, err := http.NewRequest(http.MethodGet, srv+"/v1/workflows/"+string(out.WorkflowID), nil)
	if err != nil {
		t.Fatal(err)
	}
	getReq.Header.Set("Authorization", "Bearer tok-b")
	r, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = r.Body.Close() }()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-namespace GET status = %d, want 404", r.StatusCode)
	}

	// The response body must not leak the note text either, in case a
	// future refactor swaps the 404 mapping for something that still
	// serializes the record.
	env := decodeEnvelope(t, r, nil)
	if strings.Contains(env.Message, "namespaceA secret note") {
		t.Fatalf("cross-namespace 404 leaked editor_metadata content: %q", env.Message)
	}

	// namespaceA's own GET still works and still carries its metadata.
	ownReq, _ := http.NewRequest(http.MethodGet, srv+"/v1/workflows/"+string(out.WorkflowID), nil)
	ownReq.Header.Set("Authorization", "Bearer tok-full")
	ownResp, err := http.DefaultClient.Do(ownReq)
	if err != nil {
		t.Fatalf("owner GET: %v", err)
	}
	defer func() { _ = ownResp.Body.Close() }()
	if ownResp.StatusCode != http.StatusOK {
		t.Fatalf("owner GET status = %d, want 200", ownResp.StatusCode)
	}
}

// TestEditorMetadataPutCrossNamespaceIsNotFoundAndDoesNotMutate proves a
// namespaceB principal cannot overwrite namespaceA's editor_metadata via PUT
// on namespaceA's workflow id, and that the attempt leaves the stored record
// (definition and metadata) untouched.
func TestEditorMetadataPutCrossNamespaceIsNotFoundAndDoesNotMutate(t *testing.T) {
	srv, cp := newEditorMetadataTwoNamespaceTestServer(t)

	def := editorMetadataWorkflow("editor-ns-a-put")
	md := &types.WorkflowEditorMetadata{Positions: map[string]types.Position{"n-start": {X: 1, Y: 1}}}
	resp := postWorkflows(t, srv, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	_ = resp.Body.Close()

	before, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow before: %v", err)
	}

	attack := editorMetadataWorkflow("editor-ns-a-put")
	attackMD := &types.WorkflowEditorMetadata{Positions: map[string]types.Position{"n-start": {X: 999, Y: 999}}}
	putResp := putWorkflow(t, srv, "tok-b", string(out.WorkflowID), workflowWithMetadata{WorkflowDef: attack, EditorMetadata: attackMD})
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-namespace PUT status = %d, want 404", putResp.StatusCode)
	}

	after, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow after: %v", err)
	}
	if after.RegistryRevision != before.RegistryRevision {
		t.Fatalf("RegistryRevision changed after a denied cross-namespace PUT: %d != %d", after.RegistryRevision, before.RegistryRevision)
	}
	if after.EditorMetadata == nil || after.EditorMetadata.Positions["n-start"] != (types.Position{X: 1, Y: 1}) {
		t.Fatalf("editor_metadata mutated by a denied cross-namespace PUT: %+v", after.EditorMetadata)
	}
}

// TestEditorMetadataMalformedTypeRejected proves a structurally invalid
// editor_metadata value (wrong JSON type for a field) is rejected with 400,
// not silently coerced or ignored. "positions" must decode to
// map[string]types.Position; a scalar string in that slot is a decode error.
func TestEditorMetadataMalformedTypeRejected(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	raw := `{"name":"editor-malformed","nodes":[{"id":"n-start","name":"start","type":"xflow.start"}],` +
		`"editor_metadata":{"positions":"not-an-object"}}`
	resp := postRawWorkflowBody(t, srv.URL, "tok-full", raw)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed editor_metadata.positions status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "bad_request" {
		t.Fatalf("code = %q, want bad_request", env.Code)
	}
}

// TestEditorMetadataMalformedViewportTypeRejected covers the nested-object
// case: viewport must decode to *types.Viewport{X,Y,Zoom float64}; a string
// value for a numeric field is a decode error, not a silent zero.
func TestEditorMetadataMalformedViewportTypeRejected(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	raw := `{"name":"editor-malformed-viewport","nodes":[{"id":"n-start","name":"start","type":"xflow.start"}],` +
		`"editor_metadata":{"viewport":{"x":"not-a-number","y":0,"zoom":1}}}`
	resp := postRawWorkflowBody(t, srv.URL, "tok-full", raw)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed editor_metadata.viewport.x status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "bad_request" {
		t.Fatalf("code = %q, want bad_request", env.Code)
	}
}

// postRawWorkflowBody posts a literal JSON string body to POST /v1/workflows,
// for cases that need a shape that doesn't typecheck as Go structs (malformed
// field types).
func postRawWorkflowBody(t *testing.T, base, token, rawJSON string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/workflows", strings.NewReader(rawJSON))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/workflows: %v", err)
	}
	return resp
}
