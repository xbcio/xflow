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
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// This file covers GET /v1/executions — the offset-paginated execution
// collection read (spec §3.3). It is the second of the two endpoints
// API-SPECIFICATION.md §9.6 recorded as unwired.
//
// The properties pinned here are the ones a mistake would not surface as a
// compile error:
//
//   - the page/offset arithmetic handed to the store (the handler never slices
//     an unbounded slice itself);
//   - the 200 page_size cap being ENFORCED on the request, not merely
//     documented, and malformed pagination degrading to defaults;
//   - the {list,total} envelope, and that a list row is a SUMMARY: no
//     workflow_def, no params, no runtime JSON;
//   - `total` coming from the same namespace AND the same filter as the page;
//   - namespace isolation: a principal can never observe another namespace's
//     executions, the `namespace` query parameter is not a recognized input,
//     and unattributed (`""`) rows are invisible to everyone;
//   - the filter refusal semantics: an unknown `status` is a 400, not an empty
//     page, and an unparsable time bound is a 400, not silently unbounded;
//   - refusal before the handler for unauthenticated / under-scoped callers, and
//     the Op is the READ op (OpExecutionRead), never the seed mutation that
//     shares this exact path.
//
// Fixtures are in-process only: the store is memstore, the HTTP exercise goes
// through httptest.Server. No Redis, no SQL, no external infrastructure.

// execListPrincipals is the token registry shared by the fixtures: two full
// execution-scope principals in different namespaces, plus one principal in
// namespaceA that holds only the workflow scope — the last one exists so a
// denial can be attributed to a missing scope rather than a wrong namespace.
func execListPrincipals() []TokenPrincipalMapping {
	return []TokenPrincipalMapping{
		{Token: "tok-exec-a", Subject: "op-exec-a", Namespace: "namespaceA", Scopes: []string{"execution"}},
		{Token: "tok-exec-b", Subject: "op-exec-b", Namespace: "namespaceB", Scopes: []string{"execution"}},
		{Token: "tok-workflow-only", Subject: "op-wf-only", Namespace: "namespaceA", Scopes: []string{"workflow"}},
	}
}

// recordingExecutionStore wraps a real execution store and records every
// ListExecutions / CountExecutions call, so a test can assert the exact
// (namespace, filter, ListOptions) triple the handler translated the query
// string into. Embedding the interface means only the observed methods are
// overridden; the store semantics themselves stay the real ones.
type recordingExecutionStore struct {
	store.Executions

	mu        sync.Mutex
	listCalls []recordedListCall
	ctsCalls  []recordedCountCall
}

type recordedListCall struct {
	ns     namespace.Namespace
	filter store.ExecutionFilter
	opts   store.ListOptions
}

type recordedCountCall struct {
	ns     namespace.Namespace
	filter store.ExecutionFilter
}

func (s *recordingExecutionStore) ListExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter, opts store.ListOptions) ([]*store.ExecutionRecord, error) {
	s.mu.Lock()
	s.listCalls = append(s.listCalls, recordedListCall{ns: ns, filter: filter, opts: opts})
	s.mu.Unlock()
	return s.Executions.ListExecutions(ctx, ns, filter, opts)
}

func (s *recordingExecutionStore) CountExecutions(ctx context.Context, ns namespace.Namespace, filter store.ExecutionFilter) (int64, error) {
	s.mu.Lock()
	s.ctsCalls = append(s.ctsCalls, recordedCountCall{ns: ns, filter: filter})
	s.mu.Unlock()
	return s.Executions.CountExecutions(ctx, ns, filter)
}

func (s *recordingExecutionStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls = nil
	s.ctsCalls = nil
}

func (s *recordingExecutionStore) lists() []recordedListCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedListCall(nil), s.listCalls...)
}

func (s *recordingExecutionStore) counts() []recordedCountCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedCountCall(nil), s.ctsCalls...)
}

func newRecordingExecutionStore() *recordingExecutionStore {
	return &recordingExecutionStore{Executions: memstore.New()}
}

// seedExecution writes one execution row straight into the store. Rows are
// seeded directly rather than driven through submit/invoke on purpose: the
// collection read is store-backed, and a test that first had to produce a real
// execution through the engine would be testing the engine, not the list.
func seedExecution(t *testing.T, st store.Executions, ns, workflowName string, id types.ExecutionID, status types.ExecutionStatus, createdAt time.Time) {
	t.Helper()
	rec := &store.ExecutionRecord{
		ExecutionID:  id,
		Namespace:    ns,
		WorkflowName: workflowName,
		Status:       status,
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt.Add(time.Second),
	}
	if status == types.ExecutionStatusFailed {
		rec.Error = "node \"work\" failed"
	}
	// The list endpoint must not project these three JSON payload columns. They
	// carry a recognizable marker so a test can prove their bytes are absent
	// from the response rather than merely assuming the struct omits them.
	rec.WorkflowDef = []byte(`{"marker":"workflow-definition-payload"}`)
	rec.Params = []byte(`{"marker":"params-payload"}`)
	rec.Runtime = []byte(`{"marker":"runtime-payload"}`)
	if err := st.CreateExecution(context.Background(), rec); err != nil {
		t.Fatalf("seed execution %s: %v", id, err)
	}
}

// newExecutionListServer builds the real APIServer with the given store wired
// through Config.Store (the same field production sets) and returns an
// httptest.Server over its handler.
//
// The parameter is the concrete *memstore.Store, not store.Executions: the list
// route only needs the execution half, but New takes a whole store.Store
// (readiness probes and the audit reconciler read other domains off it), so a
// narrowed parameter would only move the assertion from here into a silent
// interface satisfaction. Tests that need to observe the exact store call use
// newUnitExecutionListMux with recordingExecutionStore instead.
func newExecutionListServer(t *testing.T, st *memstore.Store) *httptest.Server {
	t.Helper()
	srv, err := New(Config{
		Concurrency:   1,
		Store:         st,
		PrincipalAuth: NewBearerPrincipalAuthMulti(execListPrincipals()),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	})
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv
}

// newUnitExecutionListMux builds the BARE branch (no PrincipalAuthenticator) as
// a plain mux, so a test can drive the handler with a stub store and observe the
// exact store call. In this branch the namespace is namespace.FromContext's
// Default, which is deterministic and needs no credential.
func newUnitExecutionListMux(st store.Executions) http.Handler {
	m := &workflowControlModule{executions: st}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux
}

// execListEnvelope is the decoded GET /v1/executions body. Data is a pointer so
// a failure envelope (which omits `data` entirely) is distinguishable from a
// success envelope carrying an empty page.
type execListEnvelope struct {
	Success bool          `json:"success"`
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Data    *execListPage `json:"data"`
	TraceID string        `json:"trace_id"`
}

type execListPage struct {
	List  []executionListItem `json:"list"`
	Total int                 `json:"total"`
}

// getExecutionList issues GET /v1/executions with the given raw query string
// (including its leading "?") and returns the status plus the raw body, so a
// test can assert both the decoded shape and what is literally on the wire.
func getExecutionList(t *testing.T, base, token, query string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+PathExecutions+query, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s%s: %v", PathExecutions, query, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func decodeExecListEnvelope(t *testing.T, body []byte) execListEnvelope {
	t.Helper()
	var env execListEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode list envelope: %v (body=%s)", err, body)
	}
	return env
}

// execListRowKeys returns the JSON key set of the first raw list row, so a test
// can pin the row shape exactly without depending on field order.
func execListRowKeys(t *testing.T, body []byte) []string {
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

// TestListExecutionsPageOffsetArithmeticAndPageSizeCap pins the translation from
// (page, page_size) to the store's ListOptions, including the 200 cap the
// security control requires and the fallbacks the shared pageParams helper
// defines.
//
// It asserts the exact number of store reads per request on purpose: the
// handler issues one page read (the caller's page, sliced by the store) and one
// count read (the filtered total). Pinning both keeps that design visible
// instead of implicit.
func TestListExecutionsPageOffsetArithmeticAndPageSizeCap(t *testing.T) {
	rec := newRecordingExecutionStore()
	mux := newUnitExecutionListMux(rec)
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		seedExecution(t, rec, string(namespace.Default), "wf-1", types.ExecutionID("exec-"+strconv.Itoa(i)),
			types.ExecutionStatusSuccess, base.Add(time.Duration(i)*time.Minute))
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
			rec.reset()

			status, body := doRaw(t, mux, http.MethodGet, PathExecutions+tc.query)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", status, body)
			}
			env := decodeExecListEnvelope(t, body)
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

			lists := rec.lists()
			counts := rec.counts()
			if len(lists) != 1 {
				t.Fatalf("ListExecutions calls = %d (%+v), want exactly 1", len(lists), lists)
			}
			if len(counts) != 1 {
				t.Fatalf("CountExecutions calls = %d (%+v), want exactly 1", len(counts), counts)
			}
			if lists[0].opts.Offset != tc.wantOffset || lists[0].opts.Limit != tc.wantLimit {
				t.Fatalf("page read = %+v, want offset %d limit %d (query %q)",
					lists[0].opts, tc.wantOffset, tc.wantLimit, tc.query)
			}
			// The count must carry the same scope and filter as the page, or the
			// two describe different sets and `total` is not a total of anything.
			if counts[0].ns != lists[0].ns || counts[0].filter != lists[0].filter {
				t.Fatalf("count (ns=%q filter=%+v) disagrees with page (ns=%q filter=%+v)",
					counts[0].ns, counts[0].filter, lists[0].ns, lists[0].filter)
			}
		})
	}
}

// TestListExecutionsResponseShapeIsSummaryInCollectionEnvelope pins the response
// contract: the mandated {list,total} envelope (never a bare array), a row that
// is a summary rather than the stored record, and an empty page that is `[]`
// rather than `null`.
func TestListExecutionsResponseShapeIsSummaryInCollectionEnvelope(t *testing.T) {
	rec := newRecordingExecutionStore()
	mux := newUnitExecutionListMux(rec)
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	seedExecution(t, rec, string(namespace.Default), "wf-shape", "exec-shape", types.ExecutionStatusFailed, base)

	status, body := doRaw(t, mux, http.MethodGet, PathExecutions)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeExecListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 1 {
		t.Fatalf("list = %+v, want exactly one row (body=%s)", env.Data, body)
	}
	row := env.Data.List[0]
	if row.ExecutionID != "exec-shape" || row.Namespace != string(namespace.Default) {
		t.Fatalf("row = %+v, want execution_id exec-shape in %q", row, namespace.Default)
	}
	if row.WorkflowName != "wf-shape" {
		t.Fatalf("row.WorkflowName = %q, want wf-shape", row.WorkflowName)
	}
	if row.Status != types.ExecutionStatusFailed {
		t.Fatalf("row.Status = %q, want failed", row.Status)
	}
	if row.Error == "" {
		t.Error("row.Error is empty; a failed execution's failure message must be projected")
	}
	if row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() {
		t.Fatalf("row timestamps = %v / %v, want both set", row.CreatedAt, row.UpdatedAt)
	}
	if env.Data.Total != 1 {
		t.Fatalf("total = %d, want 1", env.Data.Total)
	}

	// The row's key set is exact: adding the definition, the params, or the
	// runtime blob to a list row would be a payload regression
	// (O(page × workflow size) on the wire), and this makes that a test failure
	// rather than a quiet widening.
	wantKeys := []string{"created_at", "error", "execution_id", "namespace", "status", "updated_at", "workflow_name"}
	if got := execListRowKeys(t, body); strings.Join(got, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("list row keys = %v, want %v", got, wantKeys)
	}
	// Belt and braces at the byte level: the seeded payloads carry recognizable
	// markers, so their absence proves nothing leaked rather than assuming the
	// projection struct is exhaustive.
	for _, forbidden := range []string{"workflow-definition-payload", "params-payload", "runtime-payload", "workflow_def"} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Errorf("list body contains %q; a collection response must not project payload columns", forbidden)
		}
	}

	// An empty page is [] and still reports the exact total.
	status, body = doRaw(t, mux, http.MethodGet, PathExecutions+"?page=9&page_size=5")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	if !bytes.Contains(body, []byte(`"list":[]`)) {
		t.Fatalf("empty page body = %s, want data.list to be [] (never null)", body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 1 {
		t.Fatalf("empty-page data = %+v, want total 1", env.Data)
	}
}

// TestListExecutionsOrderingIsNewestFirstAndStable pins the order the store
// documents (created_at DESC, id DESC) as it reaches the wire, across pages:
// page 2 continues exactly where page 1 stopped, so the offset walk neither
// repeats nor skips a row even when several executions share a created_at tick.
func TestListExecutionsOrderingIsNewestFirstAndStable(t *testing.T) {
	rec := newRecordingExecutionStore()
	mux := newUnitExecutionListMux(rec)
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	// Four rows: one strictly newest, then three sharing the same created_at so
	// only the id tiebreak can order them.
	seedExecution(t, rec, string(namespace.Default), "wf", "exec-newest", types.ExecutionStatusSuccess, base.Add(time.Hour))
	for i := 0; i < 3; i++ {
		seedExecution(t, rec, string(namespace.Default), "wf", types.ExecutionID("exec-tie-"+strconv.Itoa(i)),
			types.ExecutionStatusSuccess, base)
	}

	seen := make([]string, 0, 4)
	for page := 1; page <= 2; page++ {
		status, body := doRaw(t, mux, http.MethodGet, PathExecutions+"?page="+strconv.Itoa(page)+"&page_size=2")
		if status != http.StatusOK {
			t.Fatalf("page %d status = %d, want 200 (body=%s)", page, status, body)
		}
		env := decodeExecListEnvelope(t, body)
		if env.Data == nil {
			t.Fatalf("page %d: data is absent (body=%s)", page, body)
		}
		if env.Data.Total != 4 {
			t.Fatalf("page %d: total = %d, want 4", page, env.Data.Total)
		}
		for _, row := range env.Data.List {
			seen = append(seen, row.ExecutionID)
		}
	}
	if len(seen) != 4 {
		t.Fatalf("walked %d rows over two pages, want 4 (%v)", len(seen), seen)
	}
	if seen[0] != "exec-newest" {
		t.Fatalf("first row = %q, want the newest (exec-newest) — %v", seen[0], seen)
	}
	unique := map[string]bool{}
	for _, id := range seen {
		if unique[id] {
			t.Fatalf("offset walk repeated row %q: %v", id, seen)
		}
		unique[id] = true
	}
}

// TestListExecutionsNeverListsAnotherNamespace is the isolation guard. Two
// namespaces hold an execution whose workflow has the SAME name, so a listing
// that filtered by name (or that ignored the namespace entirely) would show the
// foreign row under the wrong principal.
//
// It also pins the second half of the boundary: the `namespace` query parameter
// is NOT a recognized input, so passing another tenant's namespace changes
// nothing.
func TestListExecutionsNeverListsAnotherNamespace(t *testing.T) {
	st := memstore.New()
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	seedExecution(t, st, "namespaceA", "shared-name", "exec-a1", types.ExecutionStatusSuccess, base)
	seedExecution(t, st, "namespaceB", "shared-name", "exec-b1", types.ExecutionStatusSuccess, base)
	seedExecution(t, st, "namespaceB", "b-only", "exec-b2", types.ExecutionStatusFailed, base.Add(time.Minute))
	// The unattributed sentinel: a row written before the namespace column
	// existed (or by a writer that did not set it). It must be invisible to
	// EVERY caller — including one whose namespace it superficially resembles.
	seedExecution(t, st, "", "orphan", "exec-orphan", types.ExecutionStatusSuccess, base.Add(2*time.Minute))
	seedExecution(t, st, "namespaceA", "a-only", "exec-a2", types.ExecutionStatusRunning, base.Add(3*time.Minute))

	srv := newExecutionListServer(t, st)

	// A query parameter must not be able to move the scope.
	status, body := getExecutionList(t, srv.URL, "tok-exec-a", "?namespace=namespaceB&page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeExecListEnvelope(t, body)
	if env.Data == nil {
		t.Fatalf("data is absent (body=%s)", body)
	}
	if env.Data.Total != 2 || len(env.Data.List) != 2 {
		t.Fatalf("namespaceA view = %+v, want exactly its own two executions", env.Data)
	}
	for _, row := range env.Data.List {
		if row.Namespace != "namespaceA" {
			t.Fatalf("namespaceA view returned a row in %q", row.Namespace)
		}
	}
	if env.Data.List[0].ExecutionID != "exec-a2" {
		t.Fatalf("namespaceA view leads with %q, want the newest (exec-a2)", env.Data.List[0].ExecutionID)
	}
	for _, foreign := range []string{"exec-b1", "exec-b2", "exec-orphan"} {
		if bytes.Contains(body, []byte(foreign)) {
			t.Errorf("namespaceA response body contains %q, which it does not own", foreign)
		}
	}

	// The mirror view: namespaceB sees only its own two, never A's, never the
	// unattributed row.
	status, body = getExecutionList(t, srv.URL, "tok-exec-b", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 2 || len(env.Data.List) != 2 {
		t.Fatalf("namespaceB view = %+v, want exactly its own two executions", env.Data)
	}
	for _, foreign := range []string{"exec-a1", "exec-a2", "exec-orphan"} {
		if bytes.Contains(body, []byte(foreign)) {
			t.Errorf("namespaceB response body contains %q, which it does not own", foreign)
		}
	}
}

// TestListExecutionsAttributedOnlyRowsAreReachable is the direct statement of
// the unattributed rule: a namespace holding ONLY sentinel rows reports an empty
// page with total 0, and never an error and never somebody else's rows. This is
// the deliberate under-report §9.6 requires — an operator recovers those rows
// offline; the API will not guess an owner for them.
func TestListExecutionsAttributedOnlyRowsAreReachable(t *testing.T) {
	st := memstore.New()
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	seedExecution(t, st, "", "orphan-1", "exec-orphan-1", types.ExecutionStatusSuccess, base)
	seedExecution(t, st, "", "orphan-2", "exec-orphan-2", types.ExecutionStatusFailed, base.Add(time.Minute))

	srv := newExecutionListServer(t, st)
	status, body := getExecutionList(t, srv.URL, "tok-exec-a", "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeExecListEnvelope(t, body)
	if env.Data == nil {
		t.Fatalf("data is absent (body=%s)", body)
	}
	if env.Data.Total != 0 || len(env.Data.List) != 0 {
		t.Fatalf("view = %+v, want an empty page with total 0", env.Data)
	}
	if !bytes.Contains(body, []byte(`"list":[]`)) {
		t.Fatalf("body = %s, want data.list to be []", body)
	}
	for _, hidden := range []string{"orphan-1", "orphan-2"} {
		if bytes.Contains(body, []byte(hidden)) {
			t.Errorf("response body contains the unattributed row %q", hidden)
		}
	}
}

// TestListExecutionsFiltersAreAppliedToPageAndTotalTogether pins the filter
// contract end to end: each advertised filter narrows the PAGE and the TOTAL by
// the same predicate, so `total` is the size of the filtered set rather than the
// namespace's whole population.
func TestListExecutionsFiltersAreAppliedToPageAndTotalTogether(t *testing.T) {
	st := memstore.New()
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	seedExecution(t, st, "namespaceA", "wf-1", "exec-early-success", types.ExecutionStatusSuccess, base)
	seedExecution(t, st, "namespaceA", "wf-2", "exec-mid-failed", types.ExecutionStatusFailed, base.Add(time.Hour))
	seedExecution(t, st, "namespaceA", "wf-3", "exec-late-success", types.ExecutionStatusSuccess, base.Add(2*time.Hour))
	seedExecution(t, st, "namespaceA", "wf-4", "exec-late-failed", types.ExecutionStatusFailed, base.Add(3*time.Hour))
	seedExecution(t, st, "namespaceB", "wf-b", "exec-b", types.ExecutionStatusFailed, base.Add(time.Hour))

	srv := newExecutionListServer(t, st)

	cases := []struct {
		name      string
		query     string
		wantTotal int
		wantIDs   []string
	}{
		{
			name:      "status success",
			query:     "?status=success&page_size=100",
			wantTotal: 2,
			wantIDs:   []string{"exec-late-success", "exec-early-success"},
		},
		{
			name:      "status failed",
			query:     "?status=failed&page_size=100",
			wantTotal: 2,
			wantIDs:   []string{"exec-late-failed", "exec-mid-failed"},
		},
		{
			name:      "status with no matches is an empty page, not an error",
			query:     "?status=canceled&page_size=100",
			wantTotal: 0,
			wantIDs:   nil,
		},
		{
			name:      "created_after is exclusive",
			query:     "?created_after=" + urlEscapeTime(base.Add(time.Hour)) + "&page_size=100",
			wantTotal: 2,
			wantIDs:   []string{"exec-late-failed", "exec-late-success"},
		},
		{
			name:      "created_before is exclusive",
			query:     "?created_before=" + urlEscapeTime(base.Add(3*time.Hour)) + "&page_size=100",
			wantTotal: 3,
			wantIDs:   []string{"exec-late-success", "exec-mid-failed", "exec-early-success"},
		},
		{
			name:      "half-open window cannot double-count a boundary row",
			query:     "?created_after=" + urlEscapeTime(base) + "&created_before=" + urlEscapeTime(base.Add(3*time.Hour)) + "&page_size=100",
			wantTotal: 2,
			wantIDs:   []string{"exec-late-success", "exec-mid-failed"},
		},
		{
			name:      "status and window compose",
			query:     "?status=failed&created_after=" + urlEscapeTime(base) + "&page_size=100",
			wantTotal: 2,
			wantIDs:   []string{"exec-late-failed", "exec-mid-failed"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := getExecutionList(t, srv.URL, "tok-exec-a", tc.query)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", status, body)
			}
			env := decodeExecListEnvelope(t, body)
			if env.Data == nil {
				t.Fatalf("data is absent (body=%s)", body)
			}
			if env.Data.Total != tc.wantTotal {
				t.Fatalf("total = %d, want %d (query %q, body=%s)", env.Data.Total, tc.wantTotal, tc.query, body)
			}
			got := make([]string, 0, len(env.Data.List))
			for _, row := range env.Data.List {
				got = append(got, row.ExecutionID)
			}
			if strings.Join(got, ",") != strings.Join(tc.wantIDs, ",") {
				t.Fatalf("page = %v, want %v (query %q)", got, tc.wantIDs, tc.query)
			}
			// namespaceB's row must never appear under any filter.
			if bytes.Contains(body, []byte("exec-b")) {
				t.Errorf("response body contains namespaceB's execution (query %q)", tc.query)
			}
		})
	}
}

// TestListExecutionsRejectsUnusableFilters pins the refusal half of the filter
// contract: an unknown status and an unparsable time bound are 400s with stable
// snake_case codes.
//
// An empty page would be the dangerous answer in both cases — for the status it
// is indistinguishable from "no executions have that status", and for a time
// bound it silently widens the result set to the whole namespace.
func TestListExecutionsRejectsUnusableFilters(t *testing.T) {
	st := memstore.New()
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	seedExecution(t, st, "namespaceA", "wf-1", "exec-1", types.ExecutionStatusSuccess, base)

	srv := newExecutionListServer(t, st)

	cases := []struct {
		name     string
		query    string
		wantCode string
	}{
		{name: "unknown status", query: "?status=bogus", wantCode: "execution_status_invalid"},
		{name: "status typo", query: "?status=sucess", wantCode: "execution_status_invalid"},
		{name: "status case mismatch", query: "?status=SUCCESS", wantCode: "execution_status_invalid"},
		{name: "unparsable created_after", query: "?created_after=yesterday", wantCode: "execution_list_time_invalid"},
		{name: "unparsable created_before", query: "?created_before=2026-09-17", wantCode: "execution_list_time_invalid"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := getExecutionList(t, srv.URL, "tok-exec-a", tc.query)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body=%s)", status, body)
			}
			env := decodeExecListEnvelope(t, body)
			if env.Success {
				t.Errorf("success = true on a refusal (body=%s)", body)
			}
			if env.Code != tc.wantCode {
				t.Errorf("code = %q, want %q (body=%s)", env.Code, tc.wantCode, body)
			}
			// A refusal must not carry a page, and must not leak the collection
			// it refused to serve.
			if env.Data != nil {
				t.Errorf("refusal carried data = %+v, want none", env.Data)
			}
			if bytes.Contains(body, []byte("exec-1")) {
				t.Errorf("refusal body leaks an execution id (body=%s)", body)
			}
		})
	}

	// A 400 must not be a disguised empty success: the same request one value
	// over (a real status) is a 200. This is what distinguishes "rejected" from
	// "there were no rows".
	if status, body := getExecutionList(t, srv.URL, "tok-exec-a", "?status=success"); status != http.StatusOK {
		t.Fatalf("status=success status = %d, want 200 (body=%s)", status, body)
	}
}

// TestListExecutionsRefusesUnauthenticatedAndUnderScoped pins that the route is
// behind the authz wrapper and reuses the READ operation's scope — not the seed
// mutation's, which shares the same path: a caller that cannot read one
// execution must not be able to enumerate them either.
func TestListExecutionsRefusesUnauthenticatedAndUnderScoped(t *testing.T) {
	st := memstore.New()
	seedExecution(t, st, "namespaceA", "wf-guarded", "exec-guarded", types.ExecutionStatusSuccess, time.Now().UTC())
	srv := newExecutionListServer(t, st)

	cases := []struct {
		name     string
		token    string
		wantCode int
	}{
		{name: "no credential", token: "", wantCode: http.StatusUnauthorized},
		{name: "unrecognized credential", token: "not-a-token", wantCode: http.StatusUnauthorized},
		// Holds the workflow scope only — the operation's scope is `execution`,
		// which is exactly what GET /v1/executions/{id} requires.
		{name: "insufficient scope", token: "tok-workflow-only", wantCode: http.StatusForbidden},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := getExecutionList(t, srv.URL, tc.token, "")
			if status != tc.wantCode {
				t.Fatalf("status = %d, want %d (body=%s)", status, tc.wantCode, body)
			}
			env := decodeExecListEnvelope(t, body)
			if env.Success {
				t.Errorf("success = true on a refusal (body=%s)", body)
			}
			if env.Data != nil {
				t.Errorf("refusal carried data = %+v, want none", env.Data)
			}
			if bytes.Contains(body, []byte("exec-guarded")) || bytes.Contains(body, []byte("wf-guarded")) {
				t.Errorf("refusal body leaks the collection it refused (body=%s)", body)
			}
		})
	}
}

// TestListExecutionsRouteUsesReadOpNotSeedMutation pins the operation the route
// resolves to. GET and POST share the exact path /v1/executions, and the two
// carry different operations with different mutation semantics; a regression
// that made the GET resolve to OpExecutionSeed would make the collection read
// demand a MUTATION scope and write an admission audit row for a read.
//
// The check is behavioural rather than a string compare against the source: a
// principal holding only the execution-scope READ is allowed, and the audit sink
// records no admission row for it (only mutations are admitted through the
// fail-closed audit path).
func TestListExecutionsRouteUsesReadOpNotSeedMutation(t *testing.T) {
	st := memstore.New()
	seedExecution(t, st, "namespaceA", "wf-1", "exec-1", types.ExecutionStatusSuccess, time.Now().UTC())
	audit := NewInMemoryAuditSink()
	srv, err := New(Config{
		Concurrency:   1,
		Store:         st,
		PrincipalAuth: NewBearerPrincipalAuthMulti(execListPrincipals()),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     audit,
	})
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	status, body := getExecutionList(t, httpSrv.URL, "tok-exec-a", "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}

	// The mutation path is what would have demanded a write scope; the read is
	// admitted through the non-fail-closed branch. Either way the recorded
	// operation must be the read op: an admission row naming OpExecutionSeed
	// would mean the collection GET had resolved to the seed mutation.
	events := audit.Events()
	if len(events) == 0 {
		t.Fatal("no audit rows recorded for the collection read")
	}
	admitted := 0
	for _, ev := range events {
		if ev.Operation != OpExecutionRead {
			t.Fatalf("audit operation = %q, want %q (GET must not resolve to the seed mutation)",
				ev.Operation, OpExecutionRead)
		}
		if ev.Phase == "admission" {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("admission audit rows = %d, want exactly 1 for one collection read", admitted)
	}
}

// TestListExecutionsFailsClosedWithoutStoreOrScope pins the two refusal paths
// that must never degrade into a 200 with an empty list: no configured
// execution store, and a namespace scope the store would refuse.
func TestListExecutionsFailsClosedWithoutStoreOrScope(t *testing.T) {
	t.Run("no store configured", func(t *testing.T) {
		mux := newUnitExecutionListMux(nil)
		status, body := doRaw(t, mux, http.MethodGet, PathExecutions)
		if status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500 (body=%s)", status, body)
		}
		env := decodeExecListEnvelope(t, body)
		if env.Success || env.Data != nil {
			t.Fatalf("envelope = %+v, want a failure with no data", env)
		}
		if env.Code != "internal_error" {
			t.Fatalf("code = %q, want internal_error (body=%s)", env.Code, body)
		}
	})

	t.Run("store refuses the scope", func(t *testing.T) {
		// A store that answers every scoped read with the store's own
		// fail-closed refusal. It is a server-side fault from this handler's
		// point of view — the scope the authenticated principal resolved to was
		// fine, so the refusal can only have come from inside the store — and
		// the one thing it must never become is a 200 with an empty list.
		//
		// The 403 execution_namespace_invalid branch belongs to the pre-check
		// above (a principal whose namespace could not be resolved into a tenant
		// at all); this case proves the OTHER refusal path is also not a
		// disguised empty success.
		bad := &refusingScopeStore{Executions: memstore.New()}
		mux := newUnitExecutionListMux(bad)
		status, body := doRaw(t, mux, http.MethodGet, PathExecutions)
		if status == http.StatusOK {
			t.Fatalf("status = 200 for a refused scope (body=%s); a widened query must never be a 200", body)
		}
		if status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500 (body=%s)", status, body)
		}
		env := decodeExecListEnvelope(t, body)
		if env.Success || env.Data != nil {
			t.Fatalf("envelope = %+v, want a failure with no data", env)
		}
		if env.Code != "internal_error" {
			t.Fatalf("code = %q, want internal_error (body=%s)", env.Code, body)
		}
	})

	t.Run("unresolvable principal namespace is a 403, not an unscoped read", func(t *testing.T) {
		// The 403 branch. A scope that cannot be enforced must be refused rather
		// than let the store decide (and it must NEVER fall back to an unscoped
		// query). Namespace is injected here the way the authz wrapper injects
		// it, so this drives the real handler, not a stand-in.
		//
		// The scope is malformed rather than empty on purpose: FromContext maps
		// an ABSENT namespace to Default, so an empty value would exercise that
		// fallback instead of the refusal. A value carrying a key-schema
		// delimiter is what a corrupt principal or a mis-migrated row actually
		// produces, and it is the case store.ValidateNamespaceScope refuses.
		rec := newRecordingExecutionStore()
		seedExecution(t, rec, string(namespace.Default), "wf-1", "exec-1", types.ExecutionStatusSuccess, time.Now().UTC())
		m := &workflowControlModule{executions: rec}
		handler := http.HandlerFunc(m.handleListExecutions)

		req := httptest.NewRequest(http.MethodGet, PathExecutions, nil)
		req = req.WithContext(namespace.WithNamespace(req.Context(), namespace.Namespace("tenant:not-a-scope")))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		resp := w.Result()
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 (body=%s)", resp.StatusCode, body)
		}
		env := decodeExecListEnvelope(t, body)
		if env.Success || env.Data != nil {
			t.Fatalf("envelope = %+v, want a failure with no data", env)
		}
		if env.Code != "execution_namespace_invalid" {
			t.Fatalf("code = %q, want execution_namespace_invalid (body=%s)", env.Code, body)
		}
		// The decisive assertion: the store was never queried, so no unscoped
		// read could have happened.
		if lists, counts := rec.lists(), rec.counts(); len(lists) != 0 || len(counts) != 0 {
			t.Fatalf("store was queried on a refused scope: lists=%+v counts=%+v", lists, counts)
		}
	})
}

// refusingScopeStore is a store whose namespace-scoped reads fail closed, the
// way the real backends do for a scope that cannot be enforced. It exists so the
// handler's ErrInvalidNamespace mapping is exercised against a real returned
// error rather than a hand-set status.
type refusingScopeStore struct {
	store.Executions
}

func (s *refusingScopeStore) ListExecutions(context.Context, namespace.Namespace, store.ExecutionFilter, store.ListOptions) ([]*store.ExecutionRecord, error) {
	return nil, store.ErrInvalidNamespace
}

func (s *refusingScopeStore) CountExecutions(context.Context, namespace.Namespace, store.ExecutionFilter) (int64, error) {
	return 0, store.ErrInvalidNamespace
}

// TestExecutionListContractBindsToHandler is the spec↔implementation binding for
// GET /v1/executions, mirroring TestWorkflowListContractBindsToHandler for the
// sibling endpoint. The api/openapi package owns that binding for the rest of the
// contract (TestSchemasMatchHandlerTypes), but it does so through a fixture table
// this workstream does not own, so a rename in the schemas this change
// introduces would be caught by nothing there. The check here marshals the REAL
// Go values the handler serializes (envelope.go's listPage, module_control.go's
// executionListItem) and validates them against the schemas the spec publishes,
// so a drifted json tag or a required property that no longer exists fails here.
func TestExecutionListContractBindsToHandler(t *testing.T) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	spec, err := loader.LoadFromFile("../../api/openapi/xflow-v1.yaml")
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		t.Fatalf("validate spec: %v", err)
	}

	item := spec.Paths.Value(PathExecutions)
	if item == nil || item.Get == nil {
		t.Fatalf("GET %s is missing from the contract", PathExecutions)
	}
	if got, want := item.Get.OperationID, "listExecutions"; got != want {
		t.Errorf("operationId = %q, want %q", got, want)
	}
	// The POST on the same path is the runner-face seed endpoint and must NOT
	// have been pulled into the contract by this change.
	if item.Post != nil {
		t.Errorf("POST %s appeared in the user-face contract; entry-seed is runner protocol face (spec §0.1)", PathExecutions)
	}

	// The pagination parameters, the 200 cap the security control requires the
	// contract to declare, and the filter parameters that actually work.
	params := map[string]*openapi3.Parameter{}
	for _, ref := range item.Get.Parameters {
		if ref != nil && ref.Value != nil {
			params[ref.Value.Name] = ref.Value
		}
	}
	for _, name := range []string{"page", "page_size", "status", "created_after", "created_before"} {
		if params[name] == nil {
			t.Fatalf("query parameter %q is missing from the contract", name)
		}
	}
	// ...and the two the store genuinely cannot support must NOT be advertised.
	for _, forbidden := range []string{"workflow_id", "workflow_key", "workflow", "runner_id", "runner"} {
		if params[forbidden] != nil {
			t.Errorf("query parameter %q is advertised but the store has no column to filter on", forbidden)
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
	if statusParam := params["status"]; statusParam.Schema.Value.Enum == nil {
		t.Error("status parameter publishes no enum; an unknown value is a 400 the client should be able to avoid")
	}

	// The 200 response must carry the collection envelope, not a bare array.
	response := item.Get.Responses.Value("200")
	if response == nil || response.Value == nil {
		t.Fatal("GET /v1/executions has no 200 response")
	}
	media := response.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil || len(media.Schema.Value.AllOf) < 2 {
		t.Fatal("GET /v1/executions 200 application/json schema is missing")
	}
	data := media.Schema.Value.AllOf[1].Value.Properties["data"]
	if data == nil || data.Ref != "#/components/schemas/ExecutionListResponse" {
		t.Fatalf("200 data schema = %#v, want ExecutionListResponse", data)
	}
	// The filter refusal must be documented with the envelope, not just implied
	// by `default`.
	if bad := item.Get.Responses.Value("400"); bad == nil || bad.Value == nil {
		t.Error("GET /v1/executions has no documented 400 for an unusable filter")
	}

	// The real serialized values against the published schemas. The item comes
	// from the same Example* constructor the api/openapi contract fixture uses,
	// so this check and that one cannot drift into validating two different
	// shapes.
	now := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	realItem, ok := ExampleExecutionListItem(
		"exec-01H8XG", "namespaceA", "health-check",
		types.ExecutionStatusFailed, "node \"work\" failed",
		now, now.Add(time.Minute),
	).(executionListItem)
	if !ok {
		t.Fatalf("ExampleExecutionListItem returned %T, want executionListItem", ExampleExecutionListItem("a", "b", "c", types.ExecutionStatusFailed, "d", now, now))
	}
	realPage := listPage{List: []executionListItem{realItem}, Total: 3}
	for _, tc := range []struct {
		name   string
		schema string
		value  any
	}{
		{name: "ExecutionListItem", schema: "ExecutionListItem", value: realItem},
		{name: "ExecutionListResponse", schema: "ExecutionListResponse", value: realPage},
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

// TestListExecutionsEndToEndOverHTTP is the end-to-end exercise: a real
// httptest.Server over the real APIServer handler, a real (in-memory) execution
// store, and real bearer principals. Every assertion below is made against a
// response a real HTTP client received, and the raw URL + body are logged so
// `go test -v` shows the wire evidence.
//
// It is the proof that the handler's namespace resolution, paging, filter, and
// envelope work together across the full stack — the properties the unit tests
// above pin one at a time.
func TestListExecutionsEndToEndOverHTTP(t *testing.T) {
	st := memstore.New()
	base := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	// Newest first on the wire: a5, a4, a3, a2, a1.
	seedExecution(t, st, "namespaceA", "wf-a1", "exec-a1", types.ExecutionStatusSuccess, base)
	seedExecution(t, st, "namespaceA", "wf-a2", "exec-a2", types.ExecutionStatusFailed, base.Add(time.Hour))
	seedExecution(t, st, "namespaceA", "wf-a3", "exec-a3", types.ExecutionStatusRunning, base.Add(2*time.Hour))
	seedExecution(t, st, "namespaceA", "wf-a4", "exec-a4", types.ExecutionStatusSuccess, base.Add(3*time.Hour))
	seedExecution(t, st, "namespaceA", "wf-a5", "exec-a5", types.ExecutionStatusFailed, base.Add(4*time.Hour))
	seedExecution(t, st, "namespaceB", "wf-b1", "exec-b1", types.ExecutionStatusSuccess, base.Add(5*time.Hour))
	seedExecution(t, st, "", "orphan", "exec-orphan", types.ExecutionStatusSuccess, base.Add(6*time.Hour))

	srv := newExecutionListServer(t, st)

	ids := func(page *execListPage) []string {
		out := make([]string, 0, len(page.List))
		for _, row := range page.List {
			out = append(out, row.ExecutionID)
		}
		return out
	}

	// Page 1 of 3.
	status, body := getExecutionList(t, srv.URL, "tok-exec-a", "?page=1&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeExecListEnvelope(t, body)
	if env.Data == nil {
		t.Fatalf("data is absent (body=%s)", body)
	}
	if env.Data.Total != 5 {
		t.Fatalf("total = %d, want 5", env.Data.Total)
	}
	if got := ids(env.Data); strings.Join(got, ",") != "exec-a5,exec-a4" {
		t.Fatalf("page 1 = %v, want [exec-a5 exec-a4] (newest first)", got)
	}
	row := env.Data.List[0]
	if row.Namespace != "namespaceA" || row.WorkflowName != "wf-a5" || row.Status != types.ExecutionStatusFailed {
		t.Fatalf("row 1 = %+v, want namespaceA/wf-a5/failed", row)
	}
	if row.Error == "" {
		t.Error("a failed row must project its failure message")
	}
	t.Logf("GET %s?page=1&page_size=2 (tok-exec-a) -> %d %s", PathExecutions, status, body)

	// Page 3: the remainder, same total.
	status, body = getExecutionList(t, srv.URL, "tok-exec-a", "?page=3&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 5 || len(env.Data.List) != 1 {
		t.Fatalf("page 3 = %+v, want one row with total 5", env.Data)
	}
	if got := ids(env.Data); got[0] != "exec-a1" {
		t.Fatalf("page 3 = %v, want [exec-a1]", got)
	}

	// A page past the end is an empty array, not an error and not null.
	status, body = getExecutionList(t, srv.URL, "tok-exec-a", "?page=50&page_size=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	if !bytes.Contains(body, []byte(`"list":[]`)) {
		t.Fatalf("past-the-end body = %s, want data.list to be []", body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 5 || len(env.Data.List) != 0 {
		t.Fatalf("past-the-end data = %+v, want total 5 with an empty list", env.Data)
	}

	// An oversized page_size is clamped on the wire, not honoured.
	status, body = getExecutionList(t, srv.URL, "tok-exec-a", "?page_size=100000")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || len(env.Data.List) != 5 {
		t.Fatalf("clamped page = %+v, want all 5 rows and no more", env.Data)
	}
	if bytes.Contains(body, []byte("exec-b1")) || bytes.Contains(body, []byte("exec-orphan")) {
		t.Errorf("namespaceA response leaked a foreign or unattributed row (body=%s)", body)
	}

	// Filters over the wire, page and total narrowed together.
	status, body = getExecutionList(t, srv.URL, "tok-exec-a", "?status=failed&page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 2 {
		t.Fatalf("status=failed data = %+v, want total 2", env.Data)
	}
	if got := ids(env.Data); strings.Join(got, ",") != "exec-a5,exec-a2" {
		t.Fatalf("status=failed page = %v, want [exec-a5 exec-a2]", got)
	}
	t.Logf("GET %s?status=failed&page_size=100 (tok-exec-a) -> %d %s", PathExecutions, status, body)

	// The refusal, over the wire: a 400 carrying the stable code.
	status, body = getExecutionList(t, srv.URL, "tok-exec-a", "?status=not-a-status")
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", status, body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Code != "execution_status_invalid" {
		t.Fatalf("code = %q, want execution_status_invalid (body=%s)", env.Code, body)
	}
	t.Logf("GET %s?status=not-a-status (tok-exec-a) -> %d %s", PathExecutions, status, body)

	// The other namespace sees only its own single execution, with its own
	// total, and none of namespaceA's ids appear anywhere in the body.
	status, body = getExecutionList(t, srv.URL, "tok-exec-b", "?page_size=100")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env = decodeExecListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 1 || len(env.Data.List) != 1 {
		t.Fatalf("namespaceB view = %+v, want its own single execution", env.Data)
	}
	if env.Data.List[0].ExecutionID != "exec-b1" {
		t.Fatalf("namespaceB row = %q, want exec-b1", env.Data.List[0].ExecutionID)
	}
	t.Logf("GET %s?page_size=100 (tok-exec-b) -> %d %s", PathExecutions, status, body)
}

// doRaw issues a request against an in-process handler and returns the status
// and body. It is the unit-level twin of getExecutionList (no server, no
// credential) used by the bare-branch tests.
func doRaw(t *testing.T, h http.Handler, method, path string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

// urlEscapeTime renders a timestamp the way a client puts it in a query string.
func urlEscapeTime(tm time.Time) string {
	// RFC3339Nano's ":" and "+" are legal in a query value, but escaping is what
	// a real client does; use the same helper the tests assert against.
	return strings.ReplaceAll(tm.Format(time.RFC3339Nano), "+", "%2B")
}
