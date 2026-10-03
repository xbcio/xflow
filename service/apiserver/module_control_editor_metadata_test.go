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

// editorMetadataWorkflow is start -> wait, with a stable NodeDef.ID on each
// node so editor_metadata can key off it without triggering the
// NODE_METADATA_KEYED_BY_NAME diagnostic.
func editorMetadataWorkflow(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{ID: "n-start", Name: "start", Type: "xflow.start"},
			{ID: "n-wait", Name: "wait", Type: "xflow.wait", Parameters: map[string]any{"signal_name": "go"}},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "wait", Input: "main"}}}},
		},
	}
}

// workflowWithMetadata is the POST/PUT wire body: WorkflowDef fields flattened
// alongside an editor_metadata sibling, matching workflowDefinitionWithMetadata.
type workflowWithMetadata struct {
	*types.WorkflowDef
	EditorMetadata *types.WorkflowEditorMetadata `json:"editor_metadata,omitempty"`
}

func TestPostWorkflowsStoresEditorMetadata(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	def := editorMetadataWorkflow("editor-post")
	md := &types.WorkflowEditorMetadata{
		Positions: map[string]types.Position{"n-start": {X: 1, Y: 2}},
		Viewport:  &types.Viewport{X: 10, Y: 20, Zoom: 1.5},
		Notes:     map[string]string{"n-wait": "ask before approving"},
	}
	resp := postWorkflows(t, srv.URL, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)

	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if rec.EditorMetadata == nil {
		t.Fatal("stored record has no EditorMetadata")
	}
	if rec.EditorMetadata.Positions["n-start"] != (types.Position{X: 1, Y: 2}) {
		t.Errorf("positions = %+v", rec.EditorMetadata.Positions)
	}
	if rec.EditorMetadata.Viewport == nil || *rec.EditorMetadata.Viewport != *md.Viewport {
		t.Errorf("viewport = %+v, want %+v", rec.EditorMetadata.Viewport, md.Viewport)
	}
	if rec.EditorMetadata.Notes["n-wait"] != "ask before approving" {
		t.Errorf("notes = %+v", rec.EditorMetadata.Notes)
	}
}

func TestGetWorkflowReturnsEditorMetadata(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	def := editorMetadataWorkflow("editor-get")
	md := &types.WorkflowEditorMetadata{Positions: map[string]types.Position{"n-start": {X: 5}}}
	resp := postWorkflows(t, srv.URL, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	_ = resp.Body.Close()

	getResp, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/workflows/"+string(out.WorkflowID), nil)
	if err != nil {
		t.Fatal(err)
	}
	getResp.Header.Set("Authorization", "Bearer tok-full")
	r, err := http.DefaultClient.Do(getResp)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = r.Body.Close() }()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", r.StatusCode)
	}
	var body workflowWithMetadata
	body.WorkflowDef = &types.WorkflowDef{}
	decodeEnvelope(t, r, &body)
	if body.EditorMetadata == nil {
		t.Fatal("GET response carries no editor_metadata")
	}
	if body.EditorMetadata.Positions["n-start"] != (types.Position{X: 5}) {
		t.Errorf("GET editor_metadata.positions = %+v", body.EditorMetadata.Positions)
	}
	if body.Name != "editor-get" {
		t.Errorf("GET definition.name = %q, want editor-get", body.Name)
	}
}

func TestPutWorkflowMetadataOnlyChangeBumpsRevisionNotRuntimeHash(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	def := editorMetadataWorkflow("editor-put-metadata-only")
	resp := postWorkflows(t, srv.URL, "tok-full", def)
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	_ = resp.Body.Close()

	before, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow before: %v", err)
	}

	md := &types.WorkflowEditorMetadata{Positions: map[string]types.Position{"n-start": {X: 1, Y: 1}}}
	putResp := putWorkflow(t, srv.URL, "tok-full", string(out.WorkflowID), workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", putResp.StatusCode)
	}

	after, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow after: %v", err)
	}
	if after.RegistryRevision <= before.RegistryRevision {
		t.Fatalf("RegistryRevision = %d, want > %d after a metadata-only PUT (D3)", after.RegistryRevision, before.RegistryRevision)
	}
	if after.DefinitionHash != before.DefinitionHash {
		t.Fatalf("DefinitionHash changed after a metadata-only PUT: %q != %q", after.DefinitionHash, before.DefinitionHash)
	}
	if after.AuditFingerprint == before.AuditFingerprint {
		t.Fatal("AuditFingerprint did not change after a metadata-only PUT")
	}
	if after.EditorMetadata == nil || after.EditorMetadata.Positions["n-start"] != (types.Position{X: 1, Y: 1}) {
		t.Fatalf("stored EditorMetadata = %+v, want the new position", after.EditorMetadata)
	}
}

func TestPutWorkflowAuthorizesAsDefinitionUpdate(t *testing.T) {
	audit := NewInMemoryAuditSink()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	api, err := New(Config{
		Concurrency: 1,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  audit,
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(api.Handler())
	t.Cleanup(httpSrv.Close)

	def := editorMetadataWorkflow("editor-authz")
	resp := postWorkflows(t, httpSrv.URL, "tok-full", def)
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	_ = resp.Body.Close()

	putResp := putWorkflow(t, httpSrv.URL, "tok-full", string(out.WorkflowID), def)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", putResp.StatusCode)
	}

	// The PUT must record OpWorkflowDefinitionUpdate (both admission and
	// outcome phases), and must NOT record a second OpWorkflowRegister
	// outcome beyond the one POST already produced above.
	var sawDefinitionUpdate bool
	registerOutcomes := 0
	for _, e := range audit.Events() {
		if e.Operation == OpWorkflowDefinitionUpdate {
			sawDefinitionUpdate = true
		}
		if e.Operation == OpWorkflowRegister && e.Phase == "outcome" {
			registerOutcomes++
		}
	}
	if !sawDefinitionUpdate {
		t.Fatalf("no audit event recorded OpWorkflowDefinitionUpdate for the PUT; events = %+v", audit.Events())
	}
	if registerOutcomes != 1 {
		t.Fatalf("OpWorkflowRegister outcome events = %d, want exactly 1 (from the POST, not the PUT)", registerOutcomes)
	}
	if !sawDefinitionUpdate {
		t.Fatal("no audit event recorded OpWorkflowDefinitionUpdate for the PUT")
	}
}

func TestEditorMetadataUnknownNodeKeyDroppedWithoutDiagnostic(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	def := editorMetadataWorkflow("editor-unknown-key")
	md := &types.WorkflowEditorMetadata{
		Positions: map[string]types.Position{
			"n-start": {X: 1},
			"ghost":   {X: 99}, // matches no node
		},
	}
	resp := postWorkflows(t, srv.URL, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	for _, w := range out.Warnings {
		if strings.Contains(w, "ghost") {
			t.Errorf("unexpected warning naming the dropped unmatched key: %q", w)
		}
	}

	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if _, ok := rec.EditorMetadata.Positions["ghost"]; ok {
		t.Error("unmatched key 'ghost' was persisted; want it dropped")
	}
	if rec.EditorMetadata.Positions["n-start"] != (types.Position{X: 1}) {
		t.Errorf("matching key dropped unexpectedly: %+v", rec.EditorMetadata.Positions)
	}
}

func TestEditorMetadataKeyedByNameWarns(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	// No stable NodeDef.ID on either node: the only addressable key is Name.
	def := &types.WorkflowDef{
		Name:  "editor-name-keyed",
		Nodes: []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	}
	md := &types.WorkflowEditorMetadata{Positions: map[string]types.Position{"start": {X: 1}}}
	resp := postWorkflows(t, srv.URL, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)

	var sawDiagnostic bool
	for _, w := range out.Warnings {
		if strings.Contains(w, types.DiagNodeMetadataKeyedByName) {
			sawDiagnostic = true
		}
	}
	if !sawDiagnostic {
		t.Fatalf("warnings = %v, want one carrying %s", out.Warnings, types.DiagNodeMetadataKeyedByName)
	}
}

func TestEditorMetadataTooLargeRejected(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	def := editorMetadataWorkflow("editor-too-large")
	huge := strings.Repeat("x", MaxEditorMetadataBytes+1)
	md := &types.WorkflowEditorMetadata{Notes: map[string]string{"n-start": huge}}
	resp := postWorkflows(t, srv.URL, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "editor_metadata_too_large" {
		t.Fatalf("code = %q, want editor_metadata_too_large", env.Code)
	}
}

func TestEditorMetadataWithinLimitAccepted(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	def := editorMetadataWorkflow("editor-within-limit")
	ok := strings.Repeat("x", MaxEditorMetadataBytes/2)
	md := &types.WorkflowEditorMetadata{Notes: map[string]string{"n-start": ok}}
	resp := postWorkflows(t, srv.URL, "tok-full", workflowWithMetadata{WorkflowDef: def, EditorMetadata: md})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
}
