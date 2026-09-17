package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/getkin/kin-openapi/openapi3"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// This file covers GET /v1/workflows — the offset-paginated workflow
// collection read (spec §3.3). The properties pinned here are the ones a
// mistake would not surface as a compile error:
//
//   - the page/offset arithmetic handed to the registry (the handler never
//     slices an unbounded slice itself);
//   - the 200 page_size cap being ENFORCED on the request, not merely
//     documented;
//   - malformed pagination input degrading to defaults instead of failing;
//   - the {list,total} envelope, and that a list row is a summary (no
//     definition, no graph);
//   - namespace isolation: a principal can never observe another namespace's
//     workflows, and the namespace the handler reads is the authenticated
//     principal's, never a query parameter;
//   - refusal before the handler for unauthenticated / under-scoped callers.

// listTestPrincipals is the token registry shared by the list fixtures: two
// full workflow-scope principals in different namespaces, plus one principal in
// namespaceA that holds only the execution scope — the last one exists so a
// denial can be attributed to a missing scope rather than to a wrong namespace.
func listTestPrincipals() []TokenPrincipalMapping {
	return []TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: []string{"workflow"}},
		{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"workflow"}},
		{Token: "tok-exec-only", Subject: "op-exec-only", Namespace: "namespaceA", Scopes: []string{"execution"}},
	}
}

// recordingListRegistry wraps a real WorkflowRegistry and records every
// ListWorkflows call, so a test can assert the exact WorkflowListOptions the
// handler translated the query string into. Embedding the interface means only
// the one method under observation is overridden.
type recordingListRegistry struct {
	backend.WorkflowRegistry

	mu         sync.Mutex
	options    []backend.WorkflowListOptions
	namespaces []namespace.Namespace
}

func (r *recordingListRegistry) ListWorkflows(ctx context.Context, ns namespace.Namespace, opts backend.WorkflowListOptions) ([]types.WorkflowID, error) {
	r.mu.Lock()
	r.options = append(r.options, opts)
	r.namespaces = append(r.namespaces, ns)
	r.mu.Unlock()
	return r.WorkflowRegistry.ListWorkflows(ctx, ns, opts)
}

func (r *recordingListRegistry) recordedOptions() []backend.WorkflowListOptions {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]backend.WorkflowListOptions(nil), r.options...)
}

func (r *recordingListRegistry) recordedNamespaces() []namespace.Namespace {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]namespace.Namespace(nil), r.namespaces...)
}

func (r *recordingListRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.options = nil
	r.namespaces = nil
}

func newRecordingListRegistry() *recordingListRegistry {
	return &recordingListRegistry{WorkflowRegistry: local.New().WorkflowRegistry()}
}

// listTestWorkflow is a minimal definition that compiles, with a caller-chosen
// name so a namespace can hold several distinct registrations.
func listTestWorkflow(name, version string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    name,
		Version: version,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "work", Type: "test.work"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
}

// registerListWorkflow registers one workflow over HTTP and returns its id.
func registerListWorkflow(t *testing.T, base, token, name, version string) string {
	t.Helper()
	resp := postRegister(t, base, token, listTestWorkflow(name, version))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("register %s/%s: status = %d, want 201 (body=%s)", name, version, resp.StatusCode, body)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	if out.WorkflowID == "" {
		t.Fatalf("register %s/%s returned an empty workflow id", name, version)
	}
	return string(out.WorkflowID)
}

// listEnvelope is the decoded GET /v1/workflows body. Data is a pointer so a
// failure envelope (which omits `data` entirely) is distinguishable from a
// success envelope carrying an empty page.
type listEnvelope struct {
	Success bool          `json:"success"`
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Data    *listPageBody `json:"data"`
	TraceID string        `json:"trace_id"`
}

type listPageBody struct {
	List  []workflowListItem `json:"list"`
	Total int                `json:"total"`
}

// getWorkflowList issues GET /v1/workflows with the given raw query string
// (including its leading "?") and returns the status plus the raw body, so a
// test can assert both the decoded shape and what is literally on the wire.
func getWorkflowList(t *testing.T, base, token, query string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+PathWorkflows+query, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s%s: %v", PathWorkflows, query, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func decodeListEnvelope(t *testing.T, body []byte) listEnvelope {
	t.Helper()
	var env listEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode list envelope: %v (body=%s)", err, body)
	}
	return env
}

// listRowKeys returns the JSON key set of the first raw list row, so a test can
// pin the row shape without depending on field order.
func listRowKeys(t *testing.T, body []byte) []string {
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
		t.Fatalf("no list rows to inspect (body=%s)", body)
	}
	keys := make([]string, 0, len(raw.Data.List[0]))
	for k := range raw.Data.List[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestListWorkflowsPageOffsetArithmeticAndPageSizeCap pins the translation from
// (page, page_size) to the registry's WorkflowListOptions, including the 200
// cap the security control requires and the fallbacks the shared pageParams
// helper defines.
//
// It asserts the exact number of registry reads per request on purpose: the
// handler issues one page read (the caller's page, sliced by the registry) and
// one count read (the unbounded namespace enumeration that makes `total`
// exact). Pinning both keeps that design visible instead of implicit — and if
// the count read ever disappears, this test says so rather than letting a
// silently-degraded total ship.
func TestListWorkflowsPageOffsetArithmeticAndPageSizeCap(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	for _, name := range []string{"wf-a1", "wf-a2", "wf-a3"} {
		registerListWorkflow(t, srv.URL, "tok-a", name, "v1")
	}

	maxInt := int(^uint(0) >> 1)
	maxSafeOffset := maxInt - (maxPageSize - 1)
	cases := []struct {
		name       string
		query      string
		wantOffset int
		wantLimit  int
		wantRows   int
	}{
		{name: "defaults", query: "", wantOffset: 0, wantLimit: 20, wantRows: 3},
		{name: "second page", query: "?page=2&page_size=2", wantOffset: 2, wantLimit: 2, wantRows: 1},
		{name: "first page explicit", query: "?page=1&page_size=3", wantOffset: 0, wantLimit: 3, wantRows: 3},
		{name: "page only keeps default size", query: "?page=3", wantOffset: 40, wantLimit: 20, wantRows: 0},
		{name: "page_size over the cap is clamped", query: "?page_size=1000", wantOffset: 0, wantLimit: 200, wantRows: 3},
		{name: "one past the cap is clamped", query: "?page_size=201", wantOffset: 0, wantLimit: 200, wantRows: 3},
		{name: "the cap itself is honoured", query: "?page_size=200", wantOffset: 0, wantLimit: 200, wantRows: 3},
		{name: "huge page_size is clamped, not honoured", query: "?page_size=100000", wantOffset: 0, wantLimit: 200, wantRows: 3},
		{name: "largest parseable page saturates safely", query: "?page=" + strconv.Itoa(maxInt) + "&page_size=200", wantOffset: maxSafeOffset, wantLimit: 200, wantRows: 0},
		{name: "zero falls back to defaults", query: "?page=0&page_size=0", wantOffset: 0, wantLimit: 20, wantRows: 3},
		{name: "negative falls back to defaults", query: "?page=-3&page_size=-1", wantOffset: 0, wantLimit: 20, wantRows: 3},
		{name: "non-numeric falls back to defaults", query: "?page=abc&page_size=xyz", wantOffset: 0, wantLimit: 20, wantRows: 3},
		{name: "fractional and exponent fall back", query: "?page=1.5&page_size=1e3", wantOffset: 0, wantLimit: 20, wantRows: 3},
		{name: "blank values fall back", query: "?page=&page_size=", wantOffset: 0, wantLimit: 20, wantRows: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry.reset()

			status, body := getWorkflowList(t, srv.URL, "tok-a", tc.query)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", status, body)
			}
			env := decodeListEnvelope(t, body)
			if !env.Success || env.Code != "200" {
				t.Fatalf("envelope = %+v, want success with code 200", env)
			}
			if env.Data == nil {
				t.Fatalf("data is absent on a success response (body=%s)", body)
			}
			// total is the collection size, independent of the page requested.
			if env.Data.Total != 3 {
				t.Fatalf("total = %d, want 3 (exact namespace count) for query %q", env.Data.Total, tc.query)
			}
			if len(env.Data.List) != tc.wantRows {
				t.Fatalf("rows = %d, want %d for query %q", len(env.Data.List), tc.wantRows, tc.query)
			}

			opts := registry.recordedOptions()
			if len(opts) != 2 {
				t.Fatalf("registry reads = %d (%+v), want 2 (one page read + one count read)", len(opts), opts)
			}
			if opts[0].Offset != tc.wantOffset || opts[0].Limit != tc.wantLimit {
				t.Fatalf("page read = %+v, want offset %d limit %d (query %q)",
					opts[0], tc.wantOffset, tc.wantLimit, tc.query)
			}
			if opts[1].Offset != 0 || opts[1].Limit != 0 {
				t.Fatalf("count read = %+v, want the unbounded namespace enumeration (offset 0, limit 0)", opts[1])
			}
		})
	}
}

// TestListWorkflowsResponseShapeIsSummaryInCollectionEnvelope pins the response
// contract: the mandated {list,total} envelope (never a bare array), a row that
// is a summary rather than the stored record, and an empty page that is `[]`
// rather than `null`.
func TestListWorkflowsResponseShapeIsSummaryInCollectionEnvelope(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "wf-shape", "v2")

	status, body := getWorkflowList(t, srv.URL, "tok-a", "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 1 {
		t.Fatalf("list = %+v, want exactly one row (body=%s)", env.Data, body)
	}
	row := env.Data.List[0]
	if row.ID != id || row.Name != "wf-shape" || row.Version != "v2" {
		t.Fatalf("row = %+v, want id %q name wf-shape version v2", row, id)
	}
	if row.DefinitionHash == "" {
		t.Error("row.DefinitionHash is empty; a list row must carry a change detector")
	}
	if row.RegistryRevision == 0 {
		t.Error("row.RegistryRevision is 0; a list row must carry the CAS token a replace presents")
	}
	if env.Data.Total != 1 {
		t.Fatalf("total = %d, want 1", env.Data.Total)
	}

	// The row's key set is exact: adding the definition or the compiled graph to
	// a list row would be a payload regression (O(page × definition size) on the
	// wire), and this makes that a test failure rather than a quiet widening.
	wantKeys := []string{"definition_hash", "id", "name", "registry_revision", "version"}
	if got := listRowKeys(t, body); strings.Join(got, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("list row keys = %v, want %v", got, wantKeys)
	}
	for _, forbidden := range []string{`"definition"`, `"graph"`, `"nodes"`, `"connections"`} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Errorf("list body contains %s; a collection response must not project definitions or graphs", forbidden)
		}
	}

	// An empty page is [] and still reports the exact total.
	status, body = getWorkflowList(t, srv.URL, "tok-a", "?page=9&page_size=5")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	if !bytes.Contains(body, []byte(`"list":[]`)) {
		t.Fatalf("empty page body = %s, want data.list to be [] (never null)", body)
	}
	env = decodeListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 1 {
		t.Fatalf("empty-page data = %+v, want total 1", env.Data)
	}
}

// TestListWorkflowsNeverListsAnotherNamespace is the isolation guard. Two
// namespaces hold a workflow with the SAME name, so a listing that filtered by
// name (or that ignored the namespace entirely) would show the foreign record
// under the wrong principal.
func TestListWorkflowsNeverListsAnotherNamespace(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())

	idA := registerListWorkflow(t, srv.URL, "tok-a", "shared-name", "v1")
	idB := registerListWorkflow(t, srv.URL, "tok-b", "shared-name", "v1")
	idB2 := registerListWorkflow(t, srv.URL, "tok-b", "b-only", "v1")

	// A query parameter must not be able to move the scope: `namespace` is not a
	// recognized parameter, and the handler reads the namespace from the
	// verified principal.
	status, body := getWorkflowList(t, srv.URL, "tok-a", "?namespace=namespaceB&page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil {
		t.Fatalf("data is absent (body=%s)", body)
	}
	if env.Data.Total != 1 || len(env.Data.List) != 1 {
		t.Fatalf("namespaceA view = %+v, want exactly its own single workflow", env.Data)
	}
	if env.Data.List[0].ID != idA {
		t.Fatalf("namespaceA view listed %q, want %q", env.Data.List[0].ID, idA)
	}
	for _, foreign := range []string{idB, idB2} {
		if bytes.Contains(body, []byte(foreign)) {
			t.Errorf("namespaceA response body contains namespaceB workflow %q", foreign)
		}
	}
	if got := registry.recordedNamespaces(); len(got) == 0 || got[0] != "namespaceA" {
		t.Fatalf("registry namespaces = %v, want namespaceA first", got)
	}

	// The mirror view: namespaceB sees only its own two, and never A's.
	registry.reset()
	status, body = getWorkflowList(t, srv.URL, "tok-b", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 2 || len(env.Data.List) != 2 {
		t.Fatalf("namespaceB view = %+v, want exactly its own two workflows", env.Data)
	}
	if bytes.Contains(body, []byte(idA)) {
		t.Errorf("namespaceB response body contains namespaceA workflow %q", idA)
	}
	if got := registry.recordedNamespaces(); len(got) == 0 || got[0] != "namespaceB" {
		t.Fatalf("registry namespaces = %v, want namespaceB first", got)
	}
}

// TestListWorkflowsRefusesUnauthenticatedAndUnderScoped pins that the route is
// behind the authz wrapper and reuses OpWorkflowRead's scope: a caller that
// cannot read one workflow must not be able to enumerate them either.
func TestListWorkflowsRefusesUnauthenticatedAndUnderScoped(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	workflowID := registerListWorkflow(t, srv.URL, "tok-a", "wf-guarded", "v1")

	cases := []struct {
		name     string
		token    string
		wantCode int
	}{
		{name: "no credential", token: "", wantCode: http.StatusUnauthorized},
		{name: "unrecognized credential", token: "not-a-token", wantCode: http.StatusUnauthorized},
		// Holds the execution scope only — the operation's scope is `workflow`,
		// which is exactly what GET /v1/workflows/{id} requires.
		{name: "insufficient scope", token: "tok-exec-only", wantCode: http.StatusForbidden},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry.reset()

			status, body := getWorkflowList(t, srv.URL, tc.token, "")
			if status != tc.wantCode {
				t.Fatalf("status = %d, want %d (body=%s)", status, tc.wantCode, body)
			}
			env := decodeListEnvelope(t, body)
			if env.Success {
				t.Errorf("success = true on a refusal (body=%s)", body)
			}
			if env.Data != nil {
				t.Errorf("refusal carried data = %+v, want none", env.Data)
			}
			// A refusal must not leak the collection it refused to serve.
			if bytes.Contains(body, []byte(workflowID)) || bytes.Contains(body, []byte("wf-guarded")) {
				t.Errorf("refusal body leaks the workflow it refused (body=%s)", body)
			}
			// The refusal happens in the wrapper, so no handler ever reads the
			// registry.
			if opts := registry.recordedOptions(); len(opts) != 0 {
				t.Errorf("registry reads = %+v, want none on a refused request", opts)
			}
		})
	}
}

// TestWorkflowListContractBindsToHandler is the spec↔implementation binding for
// GET /v1/workflows. The api/openapi package owns that binding for the rest of
// the contract (TestSchemasMatchHandlerTypes), but it does so through a fixture
// table that this workstream does not own, so a rename in the two schemas this
// change introduces would be caught by nothing there. The check here is the
// same in spirit — and stronger in one respect: it marshals the REAL Go value
// the handler serializes (envelope.go's listPage, module_control.go's
// workflowListItem) and validates that against the schema the spec publishes, so
// a drifted json tag or a required property that no longer exists fails here.
func TestWorkflowListContractBindsToHandler(t *testing.T) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	spec, err := loader.LoadFromFile("../../api/openapi/xflow-v1.yaml")
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		t.Fatalf("validate spec: %v", err)
	}

	item := spec.Paths.Value(PathWorkflows)
	if item == nil || item.Get == nil {
		t.Fatalf("GET %s is missing from the contract", PathWorkflows)
	}
	if got, want := item.Get.OperationID, "listWorkflows"; got != want {
		t.Errorf("operationId = %q, want %q", got, want)
	}

	// The two pagination parameters, and the 200 cap the security control
	// requires the contract to declare.
	params := map[string]*openapi3.Parameter{}
	for _, ref := range item.Get.Parameters {
		if ref != nil && ref.Value != nil {
			params[ref.Value.Name] = ref.Value
		}
	}
	for _, name := range []string{"page", "page_size"} {
		if params[name] == nil {
			t.Fatalf("query parameter %q is missing from the contract", name)
		}
	}
	if def, ok := params["page"].Schema.Value.Default.(float64); !ok || def != 1 {
		t.Errorf("page default = %#v, want 1", params["page"].Schema.Value.Default)
	}
	size := params["page_size"].Schema.Value
	if def, ok := size.Default.(float64); !ok || def != 20 {
		t.Errorf("page_size default = %#v, want 20", size.Default)
	}
	if size.Max == nil || *size.Max != maxPageSize {
		t.Errorf("page_size maximum = %v, want the server cap %d", size.Max, maxPageSize)
	}

	// The 200 response must carry the collection envelope, not a bare array.
	response := item.Get.Responses.Value("200")
	if response == nil || response.Value == nil {
		t.Fatal("GET /v1/workflows has no 200 response")
	}
	media := response.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil || len(media.Schema.Value.AllOf) < 2 {
		t.Fatal("GET /v1/workflows 200 application/json schema is missing")
	}
	data := media.Schema.Value.AllOf[1].Value.Properties["data"]
	if data == nil || data.Ref != "#/components/schemas/WorkflowListResponse" {
		t.Fatalf("200 data schema = %#v, want WorkflowListResponse", data)
	}

	// The real serialized values against the published schemas.
	realItem := workflowListItem{
		ID:               "wf-01H8XG",
		Name:             "health-check",
		Version:          "v1",
		DefinitionHash:   "9f2f",
		RegistryRevision: 7,
	}
	realPage := listPage{List: []workflowListItem{realItem}, Total: 3}
	for _, tc := range []struct {
		name   string
		schema string
		value  any
	}{
		{name: "WorkflowListItem", schema: "WorkflowListItem", value: realItem},
		{name: "WorkflowListResponse", schema: "WorkflowListResponse", value: realPage},
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

// TestListWorkflowsEndToEndAgainstDistributedRegistry exercises the endpoint
// over HTTP against the REAL distributed registry — a miniredis instance and
// the production Redis-backed index — rather than the in-memory provider the
// tests above use. It is the end-to-end proof that the per-namespace index, the
// offset page, the exact total, and the namespace boundary all hold on the
// backend the deployment actually runs.
//
// miniredis is in-process: no shared Redis and no external infrastructure.
func TestListWorkflowsEndToEndAgainstDistributedRegistry(t *testing.T) {
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

	// Registered oldest-first, so the newest registry revision is wf-a3 and the
	// listing must lead with it (newest revision first, then id ascending).
	idA1 := registerListWorkflow(t, httpSrv.URL, "tok-a", "wf-a1", "v1")
	idA2 := registerListWorkflow(t, httpSrv.URL, "tok-a", "wf-a2", "v1")
	idA3 := registerListWorkflow(t, httpSrv.URL, "tok-a", "wf-a3", "v1")
	idB1 := registerListWorkflow(t, httpSrv.URL, "tok-b", "wf-b1", "v1")

	names := func(rows []workflowListItem) []string {
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.Name)
		}
		return out
	}

	// Page 1 of 2: newest first, and total is the whole collection.
	status, body := getWorkflowList(t, httpSrv.URL, "tok-a", "?page=1&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil {
		t.Fatalf("data is absent (body=%s)", body)
	}
	if env.Data.Total != 3 {
		t.Fatalf("total = %d, want 3", env.Data.Total)
	}
	if got := names(env.Data.List); len(got) != 2 || got[0] != "wf-a3" || got[1] != "wf-a2" {
		t.Fatalf("page 1 = %v, want [wf-a3 wf-a2] (newest registry revision first)", got)
	}
	if bytes.Contains(body, []byte(idB1)) {
		t.Errorf("namespaceA page contains namespaceB workflow %q", idB1)
	}
	// Wire-level evidence: `go test -v` shows the exact URL and body a real
	// HTTP client received from the miniredis-backed server.
	t.Logf("GET %s?page=1&page_size=2 (tok-a) -> %d %s", PathWorkflows, status, body)

	// Page 2: the remainder, same total.
	status, body = getWorkflowList(t, httpSrv.URL, "tok-a", "?page=2&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 3 {
		t.Fatalf("page 2 data = %+v, want total 3", env.Data)
	}
	if got := names(env.Data.List); len(got) != 1 || got[0] != "wf-a1" {
		t.Fatalf("page 2 = %v, want [wf-a1]", got)
	}
	if env.Data.List[0].ID != idA1 {
		t.Fatalf("page 2 row id = %q, want %q", env.Data.List[0].ID, idA1)
	}

	// A page past the end is an empty array, not an error and not null.
	status, body = getWorkflowList(t, httpSrv.URL, "tok-a", "?page=50&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	if !bytes.Contains(body, []byte(`"list":[]`)) {
		t.Fatalf("past-the-end body = %s, want data.list to be []", body)
	}
	env = decodeListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 3 || len(env.Data.List) != 0 {
		t.Fatalf("past-the-end data = %+v, want total 3 with an empty list", env.Data)
	}

	// The other namespace sees only its own workflow, with its own total, and
	// none of namespaceA's ids appear anywhere in the body.
	status, body = getWorkflowList(t, httpSrv.URL, "tok-b", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 1 || len(env.Data.List) != 1 {
		t.Fatalf("namespaceB view = %+v, want its own single workflow", env.Data)
	}
	if env.Data.List[0].ID != idB1 {
		t.Fatalf("namespaceB row id = %q, want %q", env.Data.List[0].ID, idB1)
	}
	for _, foreign := range []string{idA1, idA2, idA3} {
		if bytes.Contains(body, []byte(foreign)) {
			t.Errorf("namespaceB response body contains namespaceA workflow %q", foreign)
		}
	}
	t.Logf("GET %s?page_size=100 (tok-b) -> %d %s", PathWorkflows, status, body)
}
