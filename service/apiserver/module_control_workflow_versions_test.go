package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/getkin/kin-openapi/openapi3"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// This file covers GET /v1/workflows/{id}/versions — the version-history read
// (versioning design §2.2). The properties pinned here are the ones a mistake
// would not surface as a compile error:
//
//   - the row content of each state: the live record's current row first, then
//     retired versions newest-retirement first, each with the hash and revision
//     that version carried;
//   - a workflow that was never replaced lists exactly its single current
//     version (the zero-backfill claim: old workflows have no history);
//   - the {list,total} envelope with the shared pagination dialect, including
//     a past-the-end page that is [] rather than null;
//   - a row carries no definition body;
//   - namespace isolation and an unknown id both answer 404
//     workflow_not_found, never an empty history.

// versionListBody is the decoded data of GET /v1/workflows/{id}/versions.
type versionListBody struct {
	List  []workflowVersionItem `json:"list"`
	Total int                   `json:"total"`
}

type versionListEnvelope struct {
	Success bool             `json:"success"`
	Code    string           `json:"code"`
	Message string           `json:"message"`
	Data    *versionListBody `json:"data"`
}

// getWorkflowVersions issues GET /v1/workflows/{id}/versions with the given raw
// query string (including its leading "?") and returns the status plus the raw
// body, mirroring getWorkflowList.
func getWorkflowVersions(t *testing.T, base, token, id, query string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/v1/workflows/"+id+"/versions"+query, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/workflows/%s/versions%s: %v", id, query, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func decodeVersionListEnvelope(t *testing.T, body []byte) versionListEnvelope {
	t.Helper()
	var env versionListEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode versions envelope: %v (body=%s)", err, body)
	}
	return env
}

// versionRowKeys returns the JSON key set of the first raw row, so a test can
// pin the row shape without depending on field order.
func versionRowKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var raw struct {
		Data struct {
			List []map[string]json.RawMessage `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw list: %v (body=%s)", err, body)
	}
	if len(raw.Data.List) == 0 {
		t.Fatalf("no version rows to inspect (body=%s)", body)
	}
	keys := make([]string, 0, len(raw.Data.List[0]))
	for k := range raw.Data.List[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// publishRun replaces the workflow's definition with the same name under a new
// version label over the real PUT route. The version label is part of the
// runtime hash, so each call publishes a distinct definition hash.
func publishRun(t *testing.T, base, token, id, name, version string) {
	t.Helper()
	resp := putWorkflow(t, base, token, id, listTestWorkflow(name, version))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT %s version %s: status = %d, want 200 (body=%s)", id, version, resp.StatusCode, body)
	}
}

// TestListWorkflowVersionsListsCurrentThenRetiredNewestFirst pins the row
// content and order of the history: the current version first, then each
// retired version newest-retirement first, with the hash each version was
// published with.
func TestListWorkflowVersionsListsCurrentThenRetiredNewestFirst(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vlist", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vlist", "v2")
	publishRun(t, srv.URL, "tok-a", id, "vlist", "v3")

	status, body := getWorkflowVersions(t, srv.URL, "tok-a", id, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeVersionListEnvelope(t, body)
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success with code 200", env)
	}
	if env.Data == nil {
		t.Fatalf("data is absent on a success response (body=%s)", body)
	}
	if env.Data.Total != 3 || len(env.Data.List) != 3 {
		t.Fatalf("history = %+v, want 3 versions (v1 retired, v2 retired, v3 current)", env.Data)
	}

	rows := env.Data.List
	if rows[0].Version != "v3" || !rows[0].Current || rows[0].Archived {
		t.Fatalf("row 0 = %+v, want the current version v3 (current=true archived=false)", rows[0])
	}
	if rows[1].Version != "v2" || rows[1].Current || !rows[1].Archived {
		t.Fatalf("row 1 = %+v, want v2 retired (current=false archived=true)", rows[1])
	}
	if rows[2].Version != "v1" || rows[2].Current || !rows[2].Archived {
		t.Fatalf("row 2 = %+v, want v1 retired (current=false archived=true)", rows[2])
	}
	for _, row := range rows {
		if row.DefinitionHash == "" {
			t.Errorf("version %q carries an empty definition_hash; the row is the change detector for the version", row.Version)
		}
		if row.RegistryRevision == 0 {
			t.Errorf("version %q carries registry_revision 0; it is the version's retirement order", row.Version)
		}
	}
	// Retirement order: the newest-retired version carries the larger revision,
	// so revisions strictly decrease down the archived tail.
	if rows[1].RegistryRevision <= rows[2].RegistryRevision {
		t.Fatalf("archived revisions = %d then %d, want strictly decreasing (newest retirement first)",
			rows[1].RegistryRevision, rows[2].RegistryRevision)
	}

	// The row's key set is exact: a definition on a list row would be the
	// O(page × definition size) payload regression the summary projection exists
	// to prevent.
	wantKeys := []string{"archived", "current", "definition_hash", "registry_revision", "version"}
	got := versionRowKeys(t, body)
	if len(got) != len(wantKeys) {
		t.Fatalf("row keys = %v, want %v", got, wantKeys)
	}
	for i := range wantKeys {
		if got[i] != wantKeys[i] {
			t.Fatalf("row keys = %v, want %v", got, wantKeys)
		}
	}
	for _, forbidden := range []string{`"definition"`, `"graph"`, `"nodes"`, `"connections"`} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Errorf("versions body contains %s; the history list must not project definitions or graphs", forbidden)
		}
	}
}

// TestListWorkflowVersionsSingleVersionWhenNeverReplaced pins the zero-backfill
// claim: a workflow that was only ever registered lists exactly one row — its
// current version — with an empty archived tail.
func TestListWorkflowVersionsSingleVersionWhenNeverReplaced(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vlist-single", "v1")

	status, body := getWorkflowVersions(t, srv.URL, "tok-a", id, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeVersionListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 1 || len(env.Data.List) != 1 {
		t.Fatalf("history = %+v, want exactly the current version", env.Data)
	}
	row := env.Data.List[0]
	if row.Version != "v1" || !row.Current || row.Archived {
		t.Fatalf("row = %+v, want the current version v1 with no history", row)
	}
}

// TestListWorkflowVersionsPaginationEnvelope pins the shared pagination dialect
// on this route: 1-based pages, exact total independent of the page, order
// preserved across pages, and a past-the-end page that is [] rather than null.
func TestListWorkflowVersionsPaginationEnvelope(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vlist-page", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vlist-page", "v2")
	publishRun(t, srv.URL, "tok-a", id, "vlist-page", "v3")

	status, body := getWorkflowVersions(t, srv.URL, "tok-a", id, "?page=1&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("page 1 status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeVersionListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 3 || len(env.Data.List) != 2 {
		t.Fatalf("page 1 = %+v, want 2 rows with total 3", env.Data)
	}
	if env.Data.List[0].Version != "v3" || env.Data.List[1].Version != "v2" {
		t.Fatalf("page 1 versions = [%s %s], want [v3 v2]",
			env.Data.List[0].Version, env.Data.List[1].Version)
	}

	status, body = getWorkflowVersions(t, srv.URL, "tok-a", id, "?page=2&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("page 2 status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeVersionListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 3 || len(env.Data.List) != 1 {
		t.Fatalf("page 2 = %+v, want 1 row with total 3", env.Data)
	}
	if env.Data.List[0].Version != "v1" {
		t.Fatalf("page 2 version = %q, want v1", env.Data.List[0].Version)
	}

	status, body = getWorkflowVersions(t, srv.URL, "tok-a", id, "?page=9&page_size=5")
	if status != http.StatusOK {
		t.Fatalf("past-the-end status = %d, want 200 (body=%s)", status, body)
	}
	if !bytes.Contains(body, []byte(`"list":[]`)) {
		t.Fatalf("past-the-end body = %s, want data.list to be [] (never null)", body)
	}
	env = decodeVersionListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 3 || len(env.Data.List) != 0 {
		t.Fatalf("past-the-end data = %+v, want total 3 with an empty list", env.Data)
	}
}

// TestListWorkflowVersionsNeverRevealsAnotherNamespaceOrMissingWorkflow pins
// the failure modes: a cross-namespace id and an unknown id are both reported
// as 404 workflow_not_found — never as an empty history, which would confirm
// the id names a workflow in some other namespace.
func TestListWorkflowVersionsNeverRevealsAnotherNamespaceOrMissingWorkflow(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	idA := registerListWorkflow(t, srv.URL, "tok-a", "vlist-scoped", "v1")
	idB := registerListWorkflow(t, srv.URL, "tok-b", "vlist-scoped-b", "v1")

	// Sanity: the owner sees its own history, so the 404s below are the
	// namespace check, not a broken fixture.
	if status, body := getWorkflowVersions(t, srv.URL, "tok-a", idA, ""); status != http.StatusOK {
		t.Fatalf("owner status = %d, want 200 (body=%s)", status, body)
	}

	for _, tc := range []struct {
		name  string
		token string
		id    string
	}{
		{name: "cross-namespace id", token: "tok-b", id: idA},
		{name: "unknown id", token: "tok-a", id: "wf-does-not-exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := getWorkflowVersions(t, srv.URL, tc.token, tc.id, "")
			if status != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body=%s)", status, body)
			}
			env := decodeVersionListEnvelope(t, body)
			if env.Success || env.Code != "workflow_not_found" {
				t.Fatalf("envelope = %+v, want code workflow_not_found", env)
			}
			if env.Data != nil {
				t.Fatalf("refusal carried data = %+v, want none", env.Data)
			}
			// The foreign namespace's workflow must not leak through the refusal.
			if bytes.Contains(body, []byte(idB)) {
				t.Errorf("refusal body leaks namespaceB workflow %q", idB)
			}
		})
	}
}

// getWorkflowVersionDetail issues GET /v1/workflows/{id}/versions/{version}.
func getWorkflowVersionDetail(t *testing.T, base, token, id, version string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/v1/workflows/"+id+"/versions/"+version, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/workflows/%s/versions/%s: %v", id, version, err)
	}
	return resp
}

// TestGetWorkflowVersionServesTheArchivedDefinitionFromHistory pins the detail
// route as the way to read a retired version's definition: after v1 is replaced
// by v2, GET …/versions/v1 must return the v1 payload captured at retirement —
// not the live record, and not a 404. The same route on the current version
// resolves to the live record.
func TestGetWorkflowVersionServesTheArchivedDefinitionFromHistory(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vdetail", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vdetail", "v2")

	// The retired version reads back as itself.
	resp := getWorkflowVersionDetail(t, srv.URL, "tok-a", id, "v1")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("archived detail status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var archived types.WorkflowDef
	if env := decodeEnvelope(t, resp, &archived); !env.Success {
		t.Fatalf("archived detail envelope = %+v, want success", env)
	}
	if archived.Version != "v1" {
		t.Fatalf("archived detail version = %q, want v1 (the live record is v2; the handler must not fold them)", archived.Version)
	}
	if archived.Name != "vdetail" {
		t.Fatalf("archived detail name = %q, want vdetail", archived.Name)
	}
	if len(archived.Nodes) == 0 {
		t.Fatal("archived detail carries no nodes; the history payload must be the full record")
	}

	// The current version resolves to the live record: same version label and
	// same definition the by-id read serves.
	resp = getWorkflowVersionDetail(t, srv.URL, "tok-a", id, "v2")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("current detail status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var current types.WorkflowDef
	if env := decodeEnvelope(t, resp, &current); !env.Success {
		t.Fatalf("current detail envelope = %+v, want success", env)
	}
	if current.Version != "v2" {
		t.Fatalf("current detail version = %q, want v2", current.Version)
	}
}

// TestGetWorkflowVersionDistinguishesUnknownVersionFromUnknownWorkflow pins the
// two 404 codes apart — a caller must be able to tell "never published that
// version" from "this workflow is gone" — and that a cross-namespace id is
// reported as the workflow being gone, never as a version lookup failure.
func TestGetWorkflowVersionDistinguishesUnknownVersionFromUnknownWorkflow(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	idA := registerListWorkflow(t, srv.URL, "tok-a", "vdiff", "v1")

	cases := []struct {
		name     string
		token    string
		id       string
		version  string
		wantCode string
	}{
		{name: "unknown version on a known workflow", token: "tok-a", id: idA, version: "v9", wantCode: "workflow_version_not_found"},
		{name: "unknown workflow", token: "tok-a", id: "wf-does-not-exist", version: "v1", wantCode: "workflow_not_found"},
		{name: "cross-namespace workflow", token: "tok-b", id: idA, version: "v1", wantCode: "workflow_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := getWorkflowVersionDetail(t, srv.URL, tc.token, tc.id, tc.version)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusNotFound {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 404 (body=%s)", resp.StatusCode, body)
			}
			env := decodeEnvelope(t, resp, nil)
			if env.Success || env.Code != tc.wantCode {
				t.Fatalf("envelope = %+v, want code %q", env, tc.wantCode)
			}
		})
	}
}

// TestWorkflowVersionContractBindsToHandler is the spec↔implementation binding
// for the version routes, the same in spirit as
// TestWorkflowListContractBindsToHandler: it checks the contract declares each
// route with its operation id and envelope wiring, then marshals the REAL Go
// values the handlers serialize and validates them against the published
// schemas, so a drifted json tag or a required property that no longer exists
// fails here rather than shipping.
func TestWorkflowVersionContractBindsToHandler(t *testing.T) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	spec, err := loader.LoadFromFile("../../api/openapi/xflow-v1.yaml")
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		t.Fatalf("validate spec: %v", err)
	}

	routes := []struct {
		path        string
		operationID string
		get         *openapi3.Operation
		wantData    string
	}{
		{path: PathWorkflowVersions, operationID: "listWorkflowVersions", wantData: "WorkflowVersionListResponse"},
		{path: PathWorkflowVersionByID, operationID: "getWorkflowVersion", wantData: "WorkflowDefWithEditorMetadata"},
		{path: PathWorkflowRollback, operationID: "rollbackWorkflow", wantData: "RollbackWorkflowResponse"},
	}
	for _, tc := range routes {
		item := spec.Paths.Value(tc.path)
		if item == nil {
			t.Fatalf("path %q is missing from the contract", tc.path)
		}
		var op *openapi3.Operation
		if tc.path == PathWorkflowRollback {
			op = item.Post
		} else {
			op = item.Get
		}
		if op == nil {
			t.Fatalf("the method for %q is missing from the contract", tc.path)
		}
		if op.OperationID != tc.operationID {
			t.Errorf("%s operationId = %q, want %q", tc.path, op.OperationID, tc.operationID)
		}
		response := op.Responses.Value("200")
		if response == nil || response.Value == nil {
			t.Fatalf("%s has no 200 response", tc.path)
		}
		media := response.Value.Content.Get("application/json")
		if media == nil || media.Schema == nil || media.Schema.Value == nil || len(media.Schema.Value.AllOf) < 2 {
			t.Fatalf("%s 200 application/json schema is missing", tc.path)
		}
		data := media.Schema.Value.AllOf[1].Value.Properties["data"]
		if data == nil || data.Ref != "#/components/schemas/"+tc.wantData {
			t.Errorf("%s 200 data schema = %#v, want %s", tc.path, data, tc.wantData)
		}
	}

	realItem := workflowVersionItem{
		Version:          "v1",
		DefinitionHash:   "9f2f",
		RegistryRevision: 7,
		Current:          true,
		Archived:         false,
	}
	realPage := listPage{List: []workflowVersionItem{realItem}, Total: 3}
	realRollback := rollbackWorkflowResponse{WorkflowID: "wf-01H8XG", Version: "v1", Unchanged: false}
	for _, tc := range []struct {
		name   string
		schema string
		value  any
	}{
		{name: "WorkflowVersionListItem", schema: "WorkflowVersionListItem", value: realItem},
		{name: "WorkflowVersionListResponse", schema: "WorkflowVersionListResponse", value: realPage},
		{name: "RollbackWorkflowRequest", schema: "RollbackWorkflowRequest", value: rollbackWorkflowRequest{Version: "v1"}},
		{name: "RollbackWorkflowResponse", schema: "RollbackWorkflowResponse", value: realRollback},
		{name: "ExecuteRegisteredRequest", schema: "ExecuteRegisteredRequest", value: executeRegisteredRequest{Version: "v1"}},
	} {
		ref := spec.Components.Schemas[tc.schema]
		if ref == nil || ref.Value == nil {
			t.Fatalf("schema %q not found in spec", tc.schema)
		}
		raw, err := json.Marshal(tc.value)
		if err != nil {
			t.Fatalf("marshal %s: %v", tc.schema, err)
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("unmarshal %s: %v", tc.schema, err)
		}
		if err := ref.Value.VisitJSON(v); err != nil {
			t.Fatalf("%s: schema %q does not match marshaled %T: %v\nbody: %s", tc.name, tc.schema, tc.value, err, raw)
		}
	}
}

// TestListWorkflowVersionsEndToEndAgainstDistributedRegistry exercises the
// route over HTTP against the REAL Redis-backed registry (miniredis) rather
// than the in-memory provider, so the history hash, the CAS that appends to it,
// and the projection the handler reads are all the producing implementations.
func TestListWorkflowVersionsEndToEndAgainstDistributedRegistry(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	be, err := distributed.New(redisServer.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: NewBearerPrincipalAuthMulti(listTestPrincipals()),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	id := registerListWorkflow(t, httpSrv.URL, "tok-a", "vlist-redis", "v1")
	publishRun(t, httpSrv.URL, "tok-a", id, "vlist-redis", "v2")

	status, body := getWorkflowVersions(t, httpSrv.URL, "tok-a", id, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeVersionListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 2 || len(env.Data.List) != 2 {
		t.Fatalf("history = %+v, want [v2 current, v1 archived] (body=%s)", env.Data, body)
	}
	if env.Data.List[0].Version != "v2" || !env.Data.List[0].Current {
		t.Fatalf("row 0 = %+v, want current v2", env.Data.List[0])
	}
	if env.Data.List[1].Version != "v1" || !env.Data.List[1].Archived {
		t.Fatalf("row 1 = %+v, want archived v1", env.Data.List[1])
	}
	t.Logf("GET /v1/workflows/%s/versions (tok-a) -> %d %s", id, status, body)

	// The detail route reads the retired version's definition back out of the
	// same history hash the CAS wrote.
	resp := getWorkflowVersionDetail(t, httpSrv.URL, "tok-a", id, "v1")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(resp.Body)
		t.Fatalf("archived detail status = %d, want 200 (body=%s)", resp.StatusCode, detail)
	}
	var archived types.WorkflowDef
	if env := decodeEnvelope(t, resp, &archived); !env.Success || archived.Version != "v1" {
		t.Fatalf("archived detail = %+v (envelope %+v), want version v1", archived, env)
	}

	// The other namespace is refused, and the refusal is the by-id read's 404,
	// not an empty history.
	status, body = getWorkflowVersions(t, httpSrv.URL, "tok-b", id, "")
	if status != http.StatusNotFound {
		t.Fatalf("cross-namespace status = %d, want 404 (body=%s)", status, body)
	}
	if env := decodeVersionListEnvelope(t, body); env.Code != "workflow_not_found" {
		t.Fatalf("cross-namespace code = %q, want workflow_not_found (body=%s)", env.Code, body)
	}
}
