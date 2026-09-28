package apiserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

type denyWorkflowAuth struct{}

func (denyWorkflowAuth) AuthenticateRequest(*http.Request) error { return ErrWorkflowUnauthenticated }

func nodeTypesMux(m *workflowControlModule) *http.ServeMux {
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux
}

func getNodeTypes(t *testing.T, h http.Handler, path string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// envelopeData decodes a success envelope and returns its data bytes.
func envelopeData(t *testing.T, rec *httptest.ResponseRecorder) json.RawMessage {
	t.Helper()
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v; body %s", err, rec.Body.String())
	}
	if !env.Success {
		t.Fatalf("envelope failure: %s", rec.Body.String())
	}
	return env.Data
}

func envelopeCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var env struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return env.Code
}

func fakeDescriptors(entries ...registry.RegisteredDescriptor) nodeDescriptorSource {
	return func() []registry.RegisteredDescriptor { return entries }
}

func customDescriptor(version int, label string) registry.RegisteredDescriptor {
	return registry.RegisteredDescriptor{Type: "custom.x", Version: version, Descriptor: types.Descriptor{
		Type: "custom.x", DisplayName: label,
		Params: []types.ParamSpec{{Name: "p", Type: types.ParamString}},
	}}
}

func TestListNodeTypes(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{nodeDescriptors: builtinDescriptors})
	rec := getNodeTypes(t, mux, "/v1/node-types", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Mode      string           `json:"param_validation_mode"`
		NodeTypes []map[string]any `json:"node_types"`
	}
	if err := json.Unmarshal(envelopeData(t, rec), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Mode != string(types.ParamValidationWarn) {
		t.Errorf("param_validation_mode = %q, want warn (the default)", resp.Mode)
	}
	if len(resp.NodeTypes) != len(builtinDescriptors()) {
		t.Errorf("node_types = %d, want %d", len(resp.NodeTypes), len(builtinDescriptors()))
	}
}

func TestListNodeTypesReflectsParamValidationMode(t *testing.T) {
	for _, mode := range []types.ParamValidationMode{types.ParamValidationOff, types.ParamValidationWarn, types.ParamValidationEnforce} {
		mux := nodeTypesMux(&workflowControlModule{paramValidation: mode, nodeDescriptors: fakeDescriptors()})
		var resp nodeTypesResponse
		if err := json.Unmarshal(envelopeData(t, getNodeTypes(t, mux, "/v1/node-types", nil)), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.ParamValidationMode != mode {
			t.Errorf("mode = %q, want %q", resp.ParamValidationMode, mode)
		}
		if resp.NodeTypes == nil {
			t.Errorf("node_types must be [] rather than null")
		}
	}
}

func TestNodeTypesThroughAPIServerConfig(t *testing.T) {
	srv, err := New(Config{ParamValidation: types.ParamValidationEnforce})
	if err != nil {
		t.Fatal(err)
	}
	rec := getNodeTypes(t, srv.Handler(), "/v1/node-types", nil)
	var resp nodeTypesResponse
	if err := json.Unmarshal(envelopeData(t, rec), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ParamValidationMode != types.ParamValidationEnforce {
		t.Errorf("mode = %q, want enforce from Config.ParamValidation", resp.ParamValidationMode)
	}
	found := false
	for _, s := range resp.NodeTypes {
		found = found || s.NodeType == "xflow.wait"
	}
	if !found {
		t.Error("server-process registry projection lacks xflow.wait")
	}
}

func TestGetNodeTypeVersionSelection(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{nodeDescriptors: fakeDescriptors(customDescriptor(1, "One"), customDescriptor(2, "Two"))})
	cases := []struct {
		path        string
		wantStatus  int
		wantVersion int
		wantCode    string
	}{
		{"/v1/node-types/custom.x", http.StatusOK, 2, ""},
		{"/v1/node-types/custom.x?version=0", http.StatusOK, 2, ""},
		{"/v1/node-types/custom.x?version=1", http.StatusOK, 1, ""},
		{"/v1/node-types/custom.x?version=2", http.StatusOK, 2, ""},
		{"/v1/node-types/custom.x?version=3", http.StatusNotFound, 0, "node_type_not_found"},
		{"/v1/node-types/custom.y", http.StatusNotFound, 0, "node_type_not_found"},
		{"/v1/node-types/custom.x?version=abc", http.StatusBadRequest, 0, "node_type_version_invalid"},
		{"/v1/node-types/custom.x?version=-1", http.StatusBadRequest, 0, "node_type_version_invalid"},
	}
	for _, tc := range cases {
		rec := getNodeTypes(t, mux, tc.path, nil)
		if rec.Code != tc.wantStatus {
			t.Errorf("%s: status = %d, want %d", tc.path, rec.Code, tc.wantStatus)
			continue
		}
		if tc.wantStatus != http.StatusOK {
			if code := envelopeCode(t, rec); code != tc.wantCode {
				t.Errorf("%s: code = %q, want %q", tc.path, code, tc.wantCode)
			}
			continue
		}
		var s nodeFormSchema
		if err := json.Unmarshal(envelopeData(t, rec), &s); err != nil {
			t.Fatal(err)
		}
		if s.NodeType != "custom.x" || s.NodeVersion != tc.wantVersion {
			t.Errorf("%s: got %s@%d, want custom.x@%d", tc.path, s.NodeType, s.NodeVersion, tc.wantVersion)
		}
	}
}

func TestGetBuiltinNodeType(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{})
	var s nodeFormSchema
	if err := json.Unmarshal(envelopeData(t, getNodeTypes(t, mux, "/v1/node-types/xflow.trigger.kafka", nil)), &s); err != nil {
		t.Fatal(err)
	}
	if s.NodeType != "xflow.trigger.kafka" || s.Kind != types.NodeKindTrigger || len(s.Fields) == 0 {
		t.Errorf("got %+v", s)
	}
}

func TestNodeTypesETag(t *testing.T) {
	src := fakeDescriptors(customDescriptor(1, "One"))
	m := &workflowControlModule{nodeDescriptors: src}
	mux := nodeTypesMux(m)
	for _, path := range []string{"/v1/node-types", "/v1/node-types/custom.x"} {
		t.Run(path, func(t *testing.T) {
			rec := getNodeTypes(t, mux, path, nil)
			etag := rec.Header().Get("ETag")
			sum := sha256.Sum256(envelopeData(t, rec))
			if want := `"sha256:` + hex.EncodeToString(sum[:]) + `"`; etag != want {
				t.Fatalf("ETag = %q, want %q (sha256 of the data payload)", etag, want)
			}
			for _, inm := range []string{etag, "W/" + etag, `"other", ` + etag, "*"} {
				notMod := getNodeTypes(t, mux, path, http.Header{"If-None-Match": {inm}})
				if notMod.Code != http.StatusNotModified || notMod.Body.Len() != 0 {
					t.Errorf("If-None-Match %s: status %d body %q, want 304 empty", inm, notMod.Code, notMod.Body.String())
				}
				if notMod.Header().Get("ETag") != etag {
					t.Errorf("304 ETag = %q", notMod.Header().Get("ETag"))
				}
			}
			if stale := getNodeTypes(t, mux, path, http.Header{"If-None-Match": {`"sha256:stale"`}}); stale.Code != http.StatusOK {
				t.Errorf("stale If-None-Match: status %d, want 200", stale.Code)
			}
		})
	}
	// The ETag follows the registry per request, with no caching.
	before := getNodeTypes(t, mux, "/v1/node-types", nil).Header().Get("ETag")
	m.nodeDescriptors = fakeDescriptors(customDescriptor(1, "Renamed"))
	after := getNodeTypes(t, mux, "/v1/node-types", nil)
	if after.Header().Get("ETag") == before {
		t.Error("ETag did not change with the registry")
	}
	if got := getNodeTypes(t, mux, "/v1/node-types", http.Header{"If-None-Match": {before}}); got.Code != http.StatusOK {
		t.Errorf("old ETag after change: status %d, want 200", got.Code)
	}
}

func TestNodeTypesRequireAuthBareBranch(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{auth: denyWorkflowAuth{}})
	for _, path := range []string{"/v1/node-types", "/v1/node-types/xflow.wait"} {
		if rec := getNodeTypes(t, mux, path, nil); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", path, rec.Code)
		}
	}
}

func TestNodeTypesAuthzBranch(t *testing.T) {
	cases := []struct {
		name string
		auth staticPrincipalAuth
		want int
	}{
		{"unauthenticated", staticPrincipalAuth{err: errors.New("no token")}, http.StatusUnauthorized},
		{"missing workflow scope", staticPrincipalAuth{principal: Principal{Subject: "bob", Scopes: []string{"execution"}}}, http.StatusForbidden},
		{"workflow scope (OpWorkflowRead)", staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"workflow"}}}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			audit := NewInMemoryAuditSink()
			m := authzModule(t, tc.auth, ScopeAuthorizer{}, audit)
			m.nodeDescriptors = fakeDescriptors(customDescriptor(1, "One"))
			mux := http.NewServeMux()
			m.registerAuthzRoutes(mux)
			for _, path := range []string{"/v1/node-types", "/v1/node-types/custom.x"} {
				if rec := getNodeTypes(t, mux, path, nil); rec.Code != tc.want {
					t.Errorf("%s: status %d, want %d: %s", path, rec.Code, tc.want, rec.Body.String())
				}
			}
			if tc.want == http.StatusOK {
				for _, e := range audit.Events() {
					if e.Operation != OpWorkflowRead {
						t.Errorf("audit op = %q, want %q", e.Operation, OpWorkflowRead)
					}
				}
			}
		})
	}
}

// nodeFormFixturePath is the cross-language fixture the xflow-editor test
// compiles with compileNodeForm (nodeForm.generated.test.ts).
var nodeFormFixturePath = filepath.Join("..", "..", "web", "packages", "xflow-editor", "src", "node-form", "testdata", "node-types.generated.json")

// TestNodeFormFixtureIsCurrent writes the GET /v1/node-types data payload for
// every builtin type (through the real handler) to the web fixture with
// -update, and fails without it when the checked-in fixture is stale.
func TestNodeFormFixtureIsCurrent(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{nodeDescriptors: builtinDescriptors})
	raw := envelopeData(t, getNodeTypes(t, mux, "/v1/node-types", nil))
	var got bytes.Buffer
	if err := json.Indent(&got, raw, "", "  "); err != nil {
		t.Fatal(err)
	}
	got.WriteByte('\n')
	if *updateNodeForm {
		if err := os.WriteFile(nodeFormFixturePath, got.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(nodeFormFixturePath)
	if err != nil {
		t.Fatalf("%v; regenerate with: go test ./service/apiserver -run TestNodeFormFixtureIsCurrent -update", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("%s is stale; regenerate with: go test ./service/apiserver -run TestNodeFormFixtureIsCurrent -update", nodeFormFixturePath)
	}
}
