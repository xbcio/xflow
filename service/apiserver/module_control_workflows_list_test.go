package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/getkin/kin-openapi/openapi3"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
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

// latestExecutionPrincipals extends the shared table with a both-scope
// principal per namespace. latest_execution is execution data behind the same
// execution.read gate as stats/recent, so the column tests need a caller that
// holds both scopes; a workflow.read-only token must see the column absent.
func latestExecutionPrincipals() []TokenPrincipalMapping {
	return append(listTestPrincipals(),
		TokenPrincipalMapping{Token: "tok-a-both", Subject: "op-a-both", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		TokenPrincipalMapping{Token: "tok-b-both", Subject: "op-b-both", Namespace: "namespaceB", Scopes: []string{"workflow", "execution"}},
	)
}

// recordingListRegistry wraps a real WorkflowRegistry and records every
// ListWorkflows and GetWorkflow call, so a test can assert the exact
// WorkflowListOptions the handler translated the query string into and how
// many record reads a request performed. Embedding the interface means only
// the methods under observation are overridden.
type recordingListRegistry struct {
	backend.WorkflowRegistry

	mu         sync.Mutex
	options    []backend.WorkflowListOptions
	namespaces []namespace.Namespace
	getIDs     []types.WorkflowID
}

func (r *recordingListRegistry) ListWorkflows(ctx context.Context, ns namespace.Namespace, opts backend.WorkflowListOptions) ([]types.WorkflowID, error) {
	r.mu.Lock()
	r.options = append(r.options, opts)
	r.namespaces = append(r.namespaces, ns)
	r.mu.Unlock()
	return r.WorkflowRegistry.ListWorkflows(ctx, ns, opts)
}

func (r *recordingListRegistry) GetWorkflow(ctx context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	r.mu.Lock()
	r.getIDs = append(r.getIDs, id)
	r.mu.Unlock()
	return r.WorkflowRegistry.GetWorkflow(ctx, id)
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

func (r *recordingListRegistry) recordedGetIDs() []types.WorkflowID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]types.WorkflowID(nil), r.getIDs...)
}

func (r *recordingListRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.options = nil
	r.namespaces = nil
	r.getIDs = nil
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
	return registerListDef(t, base, token, listTestWorkflow(name, version))
}

// registerListDef registers an arbitrary definition over HTTP and returns its
// id, so a test can exercise definitions the minimal helper cannot express
// (trigger nodes, for instance).
func registerListDef(t *testing.T, base, token string, def *types.WorkflowDef) string {
	t.Helper()
	resp := postRegister(t, base, token, def)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("register %s/%s: status = %d, want 201 (body=%s)", def.Name, def.Version, resp.StatusCode, body)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	if out.WorkflowID == "" {
		t.Fatalf("register %s/%s returned an empty workflow id", def.Name, def.Version)
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
	// triggers is always present (this workflow has none, so it is []); the
	// store-backed latest_execution is absent when no execution store is wired.
	wantKeys := []string{"definition_hash", "id", "name", "registry_revision", "triggers", "updated_at", "version"}
	if got := listRowKeys(t, body); strings.Join(got, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("list row keys = %v, want %v", got, wantKeys)
	}
	if !bytes.Contains(body, []byte(`"triggers":[]`)) {
		t.Errorf("a trigger-less workflow must serialize triggers as [], not null or omitted (body=%s)", body)
	}
	for _, forbidden := range []string{`"definition"`, `"graph"`, `"nodes"`, `"connections"`, `"params"`, `"output"`} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Errorf("list body contains %s; a collection response must not project definitions, graphs, params, or output", forbidden)
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
		// Holds the execution scope only — the operation's scope is
		// `workflow.read` (or its write half `workflow`), which is exactly what
		// GET /v1/workflows/{id} requires.
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

	// The pagination parameters, the 200 cap the security control requires, and
	// the optional name filter. The q contract is bound here as well: type,
	// length ceiling, and optionality.
	params := map[string]*openapi3.Parameter{}
	for _, ref := range item.Get.Parameters {
		if ref != nil && ref.Value != nil {
			params[ref.Value.Name] = ref.Value
		}
	}
	for _, name := range []string{"page", "page_size", "q"} {
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
	q := params["q"].Schema.Value
	if q.Type == nil || !q.Type.Is("string") {
		t.Errorf("q type = %v, want string", q.Type)
	}
	if q.MaxLength == nil || *q.MaxLength != maxWorkflowListQueryLen {
		t.Errorf("q maxLength = %v, want the server limit %d", q.MaxLength, maxWorkflowListQueryLen)
	}
	if params["q"].Required {
		t.Error("q must stay optional: an unfiltered list request omits it")
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

	// The real serialized values against the published schemas. Both new fields
	// are populated so a renamed json tag or a typo in a required property fails
	// here rather than in the UI.
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	realItem := workflowListItem{
		ID:               "wf-01H8XG",
		Name:             "health-check",
		Version:          "v1",
		DefinitionHash:   "9f2f",
		RegistryRevision: 7,
		Description:      "Health check every five minutes",
		Triggers:         []string{"cron", "timer", "webhook"},
		CronExpressions:  []string{"*/5 * * * *"},
		LatestExecution: &workflowLatestExecution{
			ExecutionID: "exec-01H8XG",
			Status:      types.ExecutionStatusSuccess,
			CreatedAt:   now,
			UpdatedAt:   now.Add(time.Second),
		},
	}
	realPage := listPage{List: []workflowListItem{realItem}, Total: 3}
	for _, tc := range []struct {
		name   string
		schema string
		value  any
	}{
		{name: "WorkflowListItem", schema: "WorkflowListItem", value: realItem},
		{name: "WorkflowLatestExecution", schema: "WorkflowLatestExecution", value: *realItem.LatestExecution},
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

	// triggers is required and always an array; latest_execution is optional
	// (absent when the server has no execution store or the read degraded).
	rowSchema := spec.Components.Schemas["WorkflowListItem"]
	required := map[string]bool{}
	for _, name := range rowSchema.Value.Required {
		required[name] = true
	}
	if !required["triggers"] {
		t.Error("WorkflowListItem.triggers must be required: the server always emits it")
	}
	if required["latest_execution"] {
		t.Error("WorkflowListItem.latest_execution must stay optional: a storeless server cannot emit it")
	}
	latestSchema := spec.Components.Schemas["WorkflowLatestExecution"]
	if latestSchema == nil || latestSchema.Value == nil {
		t.Fatal("WorkflowLatestExecution schema is missing")
	}
	for _, name := range []string{"execution_id", "status", "created_at", "updated_at"} {
		found := false
		for _, req := range latestSchema.Value.Required {
			if req == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("WorkflowLatestExecution.%s must be required", name)
		}
	}
	statusRef := latestSchema.Value.Properties["status"]
	if statusRef == nil || statusRef.Ref != "#/components/schemas/ExecutionStatus" {
		t.Errorf("WorkflowLatestExecution.status ref = %v, want the shared ExecutionStatus schema", statusRef)
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

// ---------------------------------------------------------------------------
// LS-06: the triggers summary column.
// ---------------------------------------------------------------------------

// listTestTriggerWorkflow is a definition with three trigger kinds and a
// duplicate timer, so one fixture pins deduplication AND lexical ordering.
func listTestTriggerWorkflow(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    name,
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "hook", Type: "xflow.trigger.webhook", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"method": http.MethodPost, "path": "/in"}},
			{Name: "tick-a", Type: "xflow.trigger.timer", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"interval": "1m"}},
			{Name: "tick-b", Type: "xflow.trigger.timer", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"interval": "2m"}},
			{Name: "feed", Type: "xflow.trigger.kafka", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"topic": "orders"}},
			{Name: "work", Type: "xflow.end", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"start":  {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"hook":   {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"tick-a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"tick-b": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"feed":   {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
}

// TestListWorkflowsReportsDedupedSortedTriggerKinds pins the derived triggers
// column: only xflow.trigger.* nodes count, the suffix is the kind, duplicates
// collapse, and the result is sorted rather than declaration-ordered.
func TestListWorkflowsReportsDedupedSortedTriggerKinds(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	registerListWorkflow(t, srv.URL, "tok-a", "wf-plain", "v1")
	registerListDef(t, srv.URL, "tok-a", listTestTriggerWorkflow("wf-triggers"))

	status, body := getWorkflowList(t, srv.URL, "tok-a", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 2 {
		t.Fatalf("list = %+v, want two rows (body=%s)", env.Data, body)
	}
	rows := map[string]workflowListItem{}
	for _, row := range env.Data.List {
		rows[row.Name] = row
	}

	triggered := rows["wf-triggers"]
	if got, want := strings.Join(triggered.Triggers, ","), "kafka,timer,webhook"; got != want {
		t.Fatalf("wf-triggers triggers = %q, want %q (deduped, lexically sorted)", got, want)
	}
	plain := rows["wf-plain"]
	if plain.Triggers == nil {
		t.Fatal("wf-plain triggers decoded to nil; the server must emit []")
	}
	if len(plain.Triggers) != 0 {
		t.Fatalf("wf-plain triggers = %v, want an empty array (xflow.start is not a trigger)", plain.Triggers)
	}
	if !bytes.Contains(body, []byte(`"triggers":[]`)) {
		t.Errorf("trigger-less row is not serialized as \"triggers\":[] (body=%s)", body)
	}
}

// TestWorkflowTriggerKindsNilDefinition pins the nil-definition guard directly:
// a stored record whose Definition was never set must not panic the list.
func TestWorkflowTriggerKindsNilDefinition(t *testing.T) {
	got := workflowTriggerKinds(nil)
	if got == nil {
		t.Fatal("workflowTriggerKinds(nil) = nil, want a non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("workflowTriggerKinds(nil) = %v, want []", got)
	}
}

// TestListWorkflowsNilStoredDefinitionYieldsEmptyTriggers drives a record with
// a nil Definition straight into the registry (the HTTP registration path
// always compiles a real definition, so it cannot produce this shape) and
// asserts the list still answers 200 with triggers [].
func TestListWorkflowsNilStoredDefinitionYieldsEmptyTriggers(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	if _, err := registry.AddWorkflow(context.Background(), backend.WorkflowRecord{
		Namespace:      "namespaceA",
		Key:            "nil-definition:v1",
		Name:           "nil-definition",
		Version:        "v1",
		DefinitionHash: "hash-without-definition",
		Definition:     nil,
	}); err != nil {
		t.Fatalf("seed nil-definition record: %v", err)
	}

	status, body := getWorkflowList(t, srv.URL, "tok-a", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 1 {
		t.Fatalf("list = %+v, want the seeded row (body=%s)", env.Data, body)
	}
	if got := env.Data.List[0].Triggers; got == nil || len(got) != 0 {
		t.Fatalf("nil-definition triggers = %#v, want a non-nil empty slice", got)
	}
	if !bytes.Contains(body, []byte(`"triggers":[]`)) {
		t.Errorf("nil-definition row is not serialized as \"triggers\":[] (body=%s)", body)
	}
}

// ---------------------------------------------------------------------------
// Zero-cost description and cron-expression projections.
// ---------------------------------------------------------------------------

// listTestDescribedCronWorkflow is a definition with a description and two
// cron triggers (one duplicated and one padded) so one fixture pins the
// description projection, deduplication, trimming and lexical ordering.
func listTestDescribedCronWorkflow(name, description string) *types.WorkflowDef {
	cronNode := func(nodeName, expression string) types.NodeDef {
		return types.NodeDef{
			Name: nodeName, Type: cronTriggerNodeType, Version: 1, Kind: types.NodeKindTrigger,
			Parameters: map[string]any{"expression": expression},
		}
	}
	return &types.WorkflowDef{
		Name:        name,
		Version:     "v1",
		Description: description,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			cronNode("nightly", " 0 2 * * * "),
			cronNode("nightly-duplicate", "0 2 * * *"),
			cronNode("diurnal", "0 14 * * *"),
			{Name: "work", Type: "xflow.end", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"start":             {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"nightly":           {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"nightly-duplicate": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			"diurnal":           {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
}

// TestListWorkflowsReportsDescriptionAndCronExpressions pins the two metadata
// columns added without any extra read: both come from the definition the list
// row already loaded. A workflow without them omits the keys entirely.
func TestListWorkflowsReportsDescriptionAndCronExpressions(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	registerListDef(t, srv.URL, "tok-a", listTestDescribedCronWorkflow("wf-cron", "nightly rollup"))
	registerListWorkflow(t, srv.URL, "tok-a", "wf-plain", "v1")

	status, body := getWorkflowList(t, srv.URL, "tok-a", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 2 {
		t.Fatalf("list = %+v, want two rows (body=%s)", env.Data, body)
	}
	rows := map[string]workflowListItem{}
	for _, row := range env.Data.List {
		rows[row.Name] = row
	}

	cron := rows["wf-cron"]
	if cron.Description != "nightly rollup" {
		t.Fatalf("wf-cron description = %q, want %q", cron.Description, "nightly rollup")
	}
	if got, want := strings.Join(cron.CronExpressions, ","), "0 14 * * *,0 2 * * *"; got != want {
		t.Fatalf("wf-cron cron_expressions = %q, want %q (trimmed, deduped, lexically sorted)", got, want)
	}
	if !bytes.Contains(body, []byte(`"description":"nightly rollup"`)) {
		t.Errorf("description is not on the wire (body=%s)", body)
	}

	plain := rows["wf-plain"]
	if plain.Description != "" || plain.CronExpressions != nil {
		t.Fatalf("wf-plain metadata = %q/%v, want both absent", plain.Description, plain.CronExpressions)
	}
	if bytes.Contains(body, []byte(`"description":""`)) {
		t.Errorf("empty description must be omitted, not serialized as an empty string (body=%s)", body)
	}
	if bytes.Contains(body, []byte(`"cron_expressions":null`)) || bytes.Contains(body, []byte(`"cron_expressions":[]`)) {
		t.Errorf("a workflow without cron triggers must omit cron_expressions (body=%s)", body)
	}
}

// TestWorkflowCronExpressionsSkipsUnusableParameters pins the defensive branch
// of the derivation: only a non-blank string expression is projected, and a
// nil definition yields nil rather than panicking.
func TestWorkflowCronExpressionsSkipsUnusableParameters(t *testing.T) {
	if got := workflowCronExpressions(nil); got != nil {
		t.Fatalf("workflowCronExpressions(nil) = %v, want nil", got)
	}
	def := &types.WorkflowDef{Nodes: []types.NodeDef{
		{Name: "int", Type: cronTriggerNodeType, Parameters: map[string]any{"expression": 42}},
		{Name: "blank", Type: cronTriggerNodeType, Parameters: map[string]any{"expression": "   "}},
		{Name: "missing", Type: cronTriggerNodeType},
		{Name: "not-cron", Type: "xflow.trigger.timer", Parameters: map[string]any{"expression": "1m"}},
		{Name: "real", Type: cronTriggerNodeType, Parameters: map[string]any{"expression": "@every 1m"}},
	}}
	got := workflowCronExpressions(def)
	if len(got) != 1 || got[0] != "@every 1m" {
		t.Fatalf("workflowCronExpressions = %v, want only the usable expression", got)
	}
}

// ---------------------------------------------------------------------------
// LS-07: the latest-execution column.
// ---------------------------------------------------------------------------

// latestExecutionCall is one recorded batch lookup.
type latestExecutionCall struct {
	ns  namespace.Namespace
	ids []types.WorkflowID
}

// stubLatestExecutionStore is a full store.Store whose LatestExecutions is
// programmable and records the exact scope and id set the handler passed.
// Embedding store.Store (backed by memstore) supplies the required methods.
type stubLatestExecutionStore struct {
	store.Store

	mu          sync.Mutex
	calls       []latestExecutionCall
	byNamespace map[namespace.Namespace]map[types.WorkflowID]*store.ExecutionRecord
	err         error
}

func (s *stubLatestExecutionStore) LatestExecutions(_ context.Context, ns namespace.Namespace, workflowIDs []types.WorkflowID) (map[types.WorkflowID]*store.ExecutionRecord, error) {
	s.mu.Lock()
	s.calls = append(s.calls, latestExecutionCall{ns: ns, ids: append([]types.WorkflowID(nil), workflowIDs...)})
	err := s.err
	byID := s.byNamespace[ns]
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}
	out := make(map[types.WorkflowID]*store.ExecutionRecord, len(workflowIDs))
	for _, id := range workflowIDs {
		if rec := byID[id]; rec != nil {
			out[id] = rec
		}
	}
	return out, nil
}

func (s *stubLatestExecutionStore) recordedCalls() []latestExecutionCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := make([]latestExecutionCall, 0, len(s.calls))
	for _, call := range s.calls {
		calls = append(calls, latestExecutionCall{ns: call.ns, ids: append([]types.WorkflowID(nil), call.ids...)})
	}
	return calls
}

// plainListStore is a full store whose concrete type does NOT promote
// LatestExecutions: it models a backend that does not implement the optional
// capability, independent of what *memstore.Store itself implements.
type plainListStore struct {
	store.Store
}

// newWorkflowListServerWithPrincipals builds the real APIServer with an
// execution store wired through Config.Store (the production field) and an
// explicit credential table, for tests that need a principal holding both the
// workflow and execution scopes.
func newWorkflowListServerWithPrincipals(t *testing.T, registry backend.WorkflowRegistry, st store.Store, principals []TokenPrincipalMapping) *httptest.Server {
	t.Helper()
	provider := local.New()
	if registry == nil {
		registry = provider.WorkflowRegistry()
	}
	cp, err := control.NewControlPlane(control.Config{
		Backend:          provider,
		WorkflowRegistry: registry,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{
		Concurrency:   1,
		Store:         st,
		PrincipalAuth: NewBearerPrincipalAuthMulti(principals),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv
}

// TestListWorkflowsLatestExecutionIsPerWorkflowAndScoped pins the LS-07
// contract: ONE batch call per page, scoped to the caller's namespace, with
// each row showing its own newest execution and only the four summary fields.
// It also proves the fail-closed guards: a record stamped another namespace or
// keyed under the wrong workflow id is withheld, not projected.
func TestListWorkflowsLatestExecutionIsPerWorkflowAndScoped(t *testing.T) {
	registry := newRecordingListRegistry()
	st := &stubLatestExecutionStore{
		Store:       memstore.New(),
		byNamespace: map[namespace.Namespace]map[types.WorkflowID]*store.ExecutionRecord{},
	}
	srv := newWorkflowListServerWithPrincipals(t, registry, st, latestExecutionPrincipals())

	idOne := registerListWorkflow(t, srv.URL, "tok-a-both", "wf-one", "v1")
	idTwo := registerListWorkflow(t, srv.URL, "tok-a-both", "wf-two", "v1")
	idMiskeyed := registerListWorkflow(t, srv.URL, "tok-a-both", "wf-miskeyed", "v1")
	idForeign := registerListWorkflow(t, srv.URL, "tok-a-both", "wf-foreign", "v1")
	idB := registerListWorkflow(t, srv.URL, "tok-b-both", "wf-b", "v1")

	created := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	st.byNamespace["namespaceA"] = map[types.WorkflowID]*store.ExecutionRecord{
		types.WorkflowID(idOne): {
			ExecutionID: "exec-one", Namespace: "namespaceA", WorkflowID: types.WorkflowID(idOne),
			Status: types.ExecutionStatusSuccess, CreatedAt: created, UpdatedAt: created.Add(time.Second),
			// Heavy/sensitive columns are populated on purpose: the projection
			// must not leak them onto the wire.
			WorkflowDef: []byte(`{"secret":"definition-payload"}`),
			Params:      []byte(`{"secret":"params-payload"}`),
			Runtime:     []byte(`{"secret":"runtime-payload"}`),
			Error:       "runtime-payload",
		},
		types.WorkflowID(idTwo): {
			ExecutionID: "exec-two", Namespace: "namespaceA", WorkflowID: types.WorkflowID(idTwo),
			Status: types.ExecutionStatusFailed, CreatedAt: created.Add(time.Minute), UpdatedAt: created.Add(2 * time.Minute),
		},
		// Keyed under wf-miskeyed but stamped as wf-one: a broken backend must
		// produce an omission, never one workflow showing another's run.
		types.WorkflowID(idMiskeyed): {
			ExecutionID: "exec-miskeyed", Namespace: "namespaceA", WorkflowID: types.WorkflowID(idOne),
			Status: types.ExecutionStatusSuccess, CreatedAt: created, UpdatedAt: created,
		},
		// Stamped another namespace: fail closed even if the store returned it.
		types.WorkflowID(idForeign): {
			ExecutionID: "exec-foreign", Namespace: "namespaceB", WorkflowID: types.WorkflowID(idForeign),
			Status: types.ExecutionStatusSuccess, CreatedAt: created, UpdatedAt: created,
		},
	}
	st.byNamespace["namespaceB"] = map[types.WorkflowID]*store.ExecutionRecord{
		types.WorkflowID(idB): {
			ExecutionID: "exec-b", Namespace: "namespaceB", WorkflowID: types.WorkflowID(idB),
			Status: types.ExecutionStatusRunning, CreatedAt: created, UpdatedAt: created,
		},
	}

	status, body := getWorkflowList(t, srv.URL, "tok-a-both", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 4 {
		t.Fatalf("list = %+v, want four namespaceA rows (body=%s)", env.Data, body)
	}
	rows := map[string]workflowListItem{}
	for _, row := range env.Data.List {
		rows[row.Name] = row
	}

	one := rows["wf-one"].LatestExecution
	if one == nil {
		t.Fatal("wf-one has no latest_execution")
	}
	if one.ExecutionID != "exec-one" || one.Status != types.ExecutionStatusSuccess ||
		!one.CreatedAt.Equal(created) || !one.UpdatedAt.Equal(created.Add(time.Second)) {
		t.Fatalf("wf-one latest_execution = %+v, want exec-one/success with its timestamps", one)
	}
	two := rows["wf-two"].LatestExecution
	if two == nil || two.ExecutionID != "exec-two" || two.Status != types.ExecutionStatusFailed {
		t.Fatalf("wf-two latest_execution = %+v, want exec-two/failed", two)
	}
	if got := rows["wf-miskeyed"].LatestExecution; got != nil {
		t.Fatalf("wf-miskeyed latest_execution = %+v, want withheld (record's workflow_id is wf-one)", got)
	}
	if got := rows["wf-foreign"].LatestExecution; got != nil {
		t.Fatalf("wf-foreign latest_execution = %+v, want withheld (record's namespace is namespaceB)", got)
	}

	// Exactly one batch call, carrying the caller's namespace and the page's
	// workflow ids. No per-row call.
	calls := st.recordedCalls()
	if len(calls) != 1 {
		t.Fatalf("LatestExecutions calls = %d, want exactly 1 batch call", len(calls))
	}
	if calls[0].ns != "namespaceA" {
		t.Fatalf("LatestExecutions ns = %q, want the caller's namespaceA", calls[0].ns)
	}
	requested := map[types.WorkflowID]bool{}
	for _, id := range calls[0].ids {
		requested[id] = true
	}
	for _, id := range []string{idOne, idTwo, idMiskeyed, idForeign} {
		if !requested[types.WorkflowID(id)] {
			t.Errorf("LatestExecutions did not receive %q from the page", id)
		}
	}

	// The wire projection is the four summary fields; nothing from the heavy
	// columns, and none of the withheld records' ids.
	for _, forbidden := range []string{"definition-payload", "params-payload", "runtime-payload", "exec-miskeyed", "exec-foreign", `"workflow_def"`} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Errorf("latest-execution projection leaked %q (body=%s)", forbidden, body)
		}
	}

	// The mirror view: namespaceB sees its own latest execution and none of A's.
	status, body = getWorkflowList(t, srv.URL, "tok-b-both", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 1 {
		t.Fatalf("namespaceB list = %+v, want its one row (body=%s)", env.Data, body)
	}
	if got := env.Data.List[0].LatestExecution; got == nil || got.ExecutionID != "exec-b" {
		t.Fatalf("namespaceB latest_execution = %+v, want exec-b", got)
	}
	for _, foreign := range []string{"exec-one", "exec-two"} {
		if bytes.Contains(body, []byte(foreign)) {
			t.Errorf("namespaceB body contains namespaceA execution %q", foreign)
		}
	}
	calls = st.recordedCalls()
	if len(calls) != 2 || calls[1].ns != "namespaceB" {
		t.Fatalf("latest calls = %+v, want a second call scoped to namespaceB", calls)
	}
	for _, id := range calls[1].ids {
		if id != types.WorkflowID(idB) {
			t.Errorf("namespaceB lookup included %q, which is not on its page", id)
		}
	}
}

// TestListWorkflowsLatestExecutionRequiresExecutionRead pins the second
// authorization check on the column, matching stats/recent: the list route
// requires only workflow.read, so a workflow.read-only principal must receive
// the page WITHOUT latest_execution (never a 403), and the batch store read
// must not run at all. The same page read by a principal that also holds
// execution.read still carries the column, on both the plain and the ?q path.
func TestListWorkflowsLatestExecutionRequiresExecutionRead(t *testing.T) {
	registry := newRecordingListRegistry()
	st := &stubLatestExecutionStore{
		Store:       memstore.New(),
		byNamespace: map[namespace.Namespace]map[types.WorkflowID]*store.ExecutionRecord{},
	}
	srv := newWorkflowListServerWithPrincipals(t, registry, st, latestExecutionPrincipals())

	id := registerListWorkflow(t, srv.URL, "tok-a-both", "wf-gated", "v1")
	created := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	st.byNamespace["namespaceA"] = map[types.WorkflowID]*store.ExecutionRecord{
		types.WorkflowID(id): {
			ExecutionID: "exec-gated", Namespace: "namespaceA", WorkflowID: types.WorkflowID(id),
			Status: types.ExecutionStatusSuccess, CreatedAt: created, UpdatedAt: created,
		},
	}

	cases := []struct {
		name  string
		token string
		want  bool
	}{
		{name: "workflow read only", token: "tok-a", want: false},
		{name: "workflow and execution read", token: "tok-a-both", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, query := range []string{"?page_size=100", "?q=wf-gated&page_size=100"} {
				before := len(st.recordedCalls())
				status, body := getWorkflowList(t, srv.URL, tc.token, query)
				if status != http.StatusOK {
					t.Fatalf("GET %s status = %d, want 200 (body=%s)", query, status, body)
				}
				env := decodeListEnvelope(t, body)
				if env.Data == nil || len(env.Data.List) != 1 {
					t.Fatalf("GET %s list = %+v, want the one row (body=%s)", query, env.Data, body)
				}
				got := env.Data.List[0].LatestExecution
				calls := st.recordedCalls()
				if tc.want {
					if got == nil || got.ExecutionID != "exec-gated" {
						t.Fatalf("GET %s latest_execution = %+v, want exec-gated", query, got)
					}
					if len(calls) != before+1 {
						t.Fatalf("GET %s latest reads = %d, want exactly one batch read", query, len(calls)-before)
					}
					continue
				}
				if got != nil {
					t.Fatalf("GET %s latest_execution = %+v, want it omitted for a workflow-only caller", query, got)
				}
				if bytes.Contains(body, []byte(`"latest_execution"`)) {
					t.Errorf("GET %s body still carries latest_execution (body=%s)", query, body)
				}
				if len(calls) != before {
					t.Fatalf("GET %s ran the latest-execution store read for a caller without execution.read", query)
				}
			}
		})
	}
}

// TestListWorkflowsLatestExecutionDegradesToAbsent pins the three degrade
// paths: no store, a store without the optional capability, and a failing
// read. Each must answer 200 with the field absent — never 500, never a false
// empty execution.
func TestListWorkflowsLatestExecutionDegradesToAbsent(t *testing.T) {
	cases := []struct {
		name  string
		store func() store.Store
	}{
		{name: "no execution store", store: func() store.Store { return nil }},
		{name: "store without capability", store: func() store.Store { return plainListStore{Store: memstore.New()} }},
		{name: "store read error", store: func() store.Store {
			return &stubLatestExecutionStore{
				Store:       memstore.New(),
				byNamespace: map[namespace.Namespace]map[types.WorkflowID]*store.ExecutionRecord{},
				err:         errors.New("latest execution backend unavailable"),
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := newRecordingListRegistry()
			srv := newWorkflowListServerWithPrincipals(t, registry, tc.store(), latestExecutionPrincipals())
			registerListWorkflow(t, srv.URL, "tok-a-both", "wf-degraded", "v1")

			status, body := getWorkflowList(t, srv.URL, "tok-a-both", "?page_size=100")
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 with the column absent (body=%s)", status, body)
			}
			env := decodeListEnvelope(t, body)
			if env.Data == nil || len(env.Data.List) != 1 {
				t.Fatalf("list = %+v, want the one row (body=%s)", env.Data, body)
			}
			if got := env.Data.List[0].LatestExecution; got != nil {
				t.Fatalf("latest_execution = %+v, want absent on a degraded read", got)
			}
			if bytes.Contains(body, []byte(`"latest_execution"`)) {
				t.Errorf("degraded response still carries latest_execution (body=%s)", body)
			}
			// The failure must not leak its detail to the caller.
			if bytes.Contains(body, []byte("backend unavailable")) {
				t.Errorf("degraded response leaked the store error (body=%s)", body)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// LS-03: the q name filter.
// ---------------------------------------------------------------------------

// TestListWorkflowsSearchFiltersBeforePagination pins the search semantics:
// case-insensitive substring matching, filtering BEFORE pagination, total as
// the filtered count, and the registry's enumeration order preserved.
func TestListWorkflowsSearchFiltersBeforePagination(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	// Registered oldest-first; the listing order is newest revision first, so
	// the expected order is alphabet-two, beta, Alpha-One.
	registerListWorkflow(t, srv.URL, "tok-a", "Alpha-One", "v1")
	registerListWorkflow(t, srv.URL, "tok-a", "beta", "v1")
	registerListWorkflow(t, srv.URL, "tok-a", "alphabet-two", "v1")

	cases := []struct {
		name      string
		query     string
		wantNames []string
		wantTotal int
	}{
		{name: "case-insensitive first page", query: "?q=ALP&page=1&page_size=1", wantNames: []string{"alphabet-two"}, wantTotal: 2},
		{name: "second page of the filtered set", query: "?q=ALP&page=2&page_size=1", wantNames: []string{"Alpha-One"}, wantTotal: 2},
		{name: "all matches one page", query: "?q=alpha&page_size=100", wantNames: []string{"alphabet-two", "Alpha-One"}, wantTotal: 2},
		{name: "single match", query: "?q=beta", wantNames: []string{"beta"}, wantTotal: 1},
		{name: "no match", query: "?q=zzz", wantNames: []string{}, wantTotal: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry.reset()

			status, body := getWorkflowList(t, srv.URL, "tok-a", tc.query)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", status, body)
			}
			env := decodeListEnvelope(t, body)
			if env.Data == nil {
				t.Fatalf("data is absent (body=%s)", body)
			}
			if env.Data.Total != tc.wantTotal {
				t.Fatalf("total = %d, want %d (filtered count) for %q", env.Data.Total, tc.wantTotal, tc.query)
			}
			got := make([]string, 0, len(env.Data.List))
			for _, row := range env.Data.List {
				got = append(got, row.Name)
			}
			if strings.Join(got, ",") != strings.Join(tc.wantNames, ",") {
				t.Fatalf("names = %v, want %v for %q", got, tc.wantNames, tc.query)
			}

			// The filter path enumerates the namespace once (unbounded) and
			// reads each record exactly once — three here — regardless of the
			// page requested.
			opts := registry.recordedOptions()
			if len(opts) != 1 {
				t.Fatalf("registry list calls = %+v, want exactly one unbounded enumeration", opts)
			}
			if opts[0].Offset != 0 || opts[0].Limit != 0 {
				t.Fatalf("filtered enumeration opts = %+v, want offset 0 limit 0", opts[0])
			}
			if gets := len(registry.recordedGetIDs()); gets != 3 {
				t.Fatalf("GetWorkflow calls = %d, want 3 (one read per namespace record)", gets)
			}
			if tc.wantTotal == 0 && !bytes.Contains(body, []byte(`"list":[]`)) {
				t.Errorf("no-match body = %s, want data.list to be []", body)
			}
		})
	}
}

// TestListWorkflowsEmptyQueryKeepsPageReads proves the empty-q equivalence and
// the cost property: an absent/blank q is the unfiltered path, so it must not
// turn one page into a full-namespace record scan.
func TestListWorkflowsEmptyQueryKeepsPageReads(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	for _, name := range []string{"wf-a1", "wf-a2", "wf-a3"} {
		registerListWorkflow(t, srv.URL, "tok-a", name, "v1")
	}

	for _, query := range []string{"?q=&page_size=1", "?q=%20%20%20&page_size=1", "?q=%09&page_size=1"} {
		registry.reset()
		status, body := getWorkflowList(t, srv.URL, "tok-a", query)
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200 for %q (body=%s)", status, query, body)
		}
		env := decodeListEnvelope(t, body)
		if env.Data == nil || env.Data.Total != 3 || len(env.Data.List) != 1 {
			t.Fatalf("blank-q page = %+v, want total 3 with one row for %q", env.Data, query)
		}
		// Unfiltered path: one page read + one count read, one record read.
		if opts := registry.recordedOptions(); len(opts) != 2 {
			t.Fatalf("registry reads = %+v, want page + count for %q", opts, query)
		}
		if gets := len(registry.recordedGetIDs()); gets != 1 {
			t.Fatalf("GetWorkflow calls = %d, want 1 (page-sized) for %q", gets, query)
		}
	}
}

// TestListWorkflowsSearchQueryTooLongIs400 pins the request-validation limit:
// over 128 characters is refused with the stable invalid_argument code and no
// registry read; exactly 128 is accepted.
func TestListWorkflowsSearchQueryTooLongIs400(t *testing.T) {
	registry := newRecordingListRegistry()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, nil, listTestPrincipals())
	workflowID := registerListWorkflow(t, srv.URL, "tok-a", "wf-guarded", "v1")

	registry.reset()
	status, body := getWorkflowList(t, srv.URL, "tok-a", "?q="+strings.Repeat("a", 129))
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", status, body)
	}
	env := decodeListEnvelope(t, body)
	if env.Success || env.Code != "invalid_argument" {
		t.Fatalf("envelope = %+v, want failure code invalid_argument", env)
	}
	if env.Data != nil {
		t.Errorf("refusal carried data = %+v, want none", env.Data)
	}
	if opts := registry.recordedOptions(); len(opts) != 0 {
		t.Errorf("registry reads = %+v, want none on a refused query", opts)
	}
	if bytes.Contains(body, []byte(workflowID)) {
		t.Errorf("refusal body leaks the workflow id (body=%s)", body)
	}

	// The boundary itself is valid.
	status, body = getWorkflowList(t, srv.URL, "tok-a", "?q="+strings.Repeat("a", 128))
	if status != http.StatusOK {
		t.Fatalf("128-character q: status = %d, want 200 (body=%s)", status, body)
	}
	if env := decodeListEnvelope(t, body); env.Data == nil || env.Data.Total != 0 {
		t.Fatalf("128-character q: data = %+v, want total 0", env.Data)
	}
}
