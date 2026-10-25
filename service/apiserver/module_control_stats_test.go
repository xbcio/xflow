package apiserver

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// This file covers the execution statistics surface: GET /v1/executions/stats
// and the ?include=stats,recent fields of GET /v1/workflows.
//
// The properties a mistake would not surface as a compile error:
//
//   - 501 when no store (or a store without the optional capability) is wired,
//     never a false 200 with zeroes;
//   - strict window validation: required bounds, ≤ 30 days, known bucket enum,
//     ≤ 168 buckets;
//   - namespace isolation: a namespace's aggregate never includes another
//     namespace's rows, even under the same workflow id;
//   - include=stats,recent is execution data: a workflow.read-only principal
//     gets the page WITHOUT those fields, not a 403;
//   - a workflow with no runs still gets a stats object (a known zero), while a
//     failed read omits it;
//   - page_size is tightened to 50 when stats are requested.

// statsListPrincipals extends the shared list fixture with a principal that
// holds BOTH scopes — the only kind of caller for which include=stats,recent
// may attach anything.
func statsListPrincipals() []TokenPrincipalMapping {
	return append(listTestPrincipals(), TokenPrincipalMapping{
		Token: "tok-both", Subject: "op-both", Namespace: "namespaceA",
		Scopes: []string{"workflow", "execution"},
	})
}

// plainStatsStore models a backend that does not implement the optional
// capability: embedding the interface narrows the method set, so the concrete
// wrapper has no ExecutionStatsReader even though memstore.Store does.
type plainStatsStore struct {
	store.Executions
}

func getExecutionStats(t *testing.T, base, token, query string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+PathExecutionStats+query, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s%s: %v", PathExecutionStats, query, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

type executionStatsEnvelope struct {
	Success bool                    `json:"success"`
	Code    string                  `json:"code"`
	Message string                  `json:"message"`
	Data    *executionStatsResponse `json:"data"`
}

func decodeExecutionStats(t *testing.T, body []byte) executionStatsEnvelope {
	t.Helper()
	var env executionStatsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode stats envelope: %v (body=%s)", err, body)
	}
	return env
}

// statsWindow builds the required query string for a window around base.
func statsWindow(base time.Time, extra string) string {
	q := "?created_after=" + base.Add(-time.Hour).Format(time.RFC3339Nano) +
		"&created_before=" + base.Add(2*time.Hour).Format(time.RFC3339Nano)
	if extra != "" {
		q += "&" + extra
	}
	return q
}

func TestExecutionStatsUnsupportedWithoutCapability(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	query := statsWindow(base, "")

	t.Run("no_store", func(t *testing.T) {
		m := &workflowControlModule{}
		mux := http.NewServeMux()
		m.RegisterHTTP(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathExecutionStats+query, nil))
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501", rec.Code)
		}
		env := decodeExecutionStats(t, rec.Body.Bytes())
		if env.Code != "execution_stats_unsupported" {
			t.Fatalf("code = %q, want execution_stats_unsupported", env.Code)
		}
	})

	t.Run("store_without_reader", func(t *testing.T) {
		mux := newUnitExecutionListMux(plainStatsStore{Executions: memstore.New()})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathExecutionStats+query, nil))
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501; a store without the optional capability must not answer zeroes", rec.Code)
		}
		if env := decodeExecutionStats(t, rec.Body.Bytes()); env.Code != "execution_stats_unsupported" {
			t.Fatalf("code = %q, want execution_stats_unsupported", env.Code)
		}
	})
}

func TestExecutionStatsValidatesWindowAndBucket(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	mux := newUnitExecutionListMux(memstore.New())

	cases := []struct {
		name      string
		query     string
		wantCode  string
		wantHTTP  int
		wantNoErr bool
	}{
		{
			name:     "missing bounds",
			query:    "",
			wantCode: "stats_window_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:     "bad lower bound",
			query:    "?created_after=yesterday&created_before=" + base.Format(time.RFC3339Nano),
			wantCode: "stats_window_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:     "bad upper bound",
			query:    "?created_after=" + base.Format(time.RFC3339Nano) + "&created_before=later",
			wantCode: "stats_window_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "inverted window",
			query: "?created_after=" + base.Add(time.Hour).Format(time.RFC3339Nano) +
				"&created_before=" + base.Format(time.RFC3339Nano),
			wantCode: "stats_window_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "window over 30 days",
			query: "?created_after=" + base.Format(time.RFC3339Nano) +
				"&created_before=" + base.Add(31*24*time.Hour).Format(time.RFC3339Nano),
			wantCode: "stats_window_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:     "unknown bucket",
			query:    statsWindow(base, "bucket=90m"),
			wantCode: "stats_bucket_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name: "more than 168 buckets",
			query: "?created_after=" + base.Format(time.RFC3339Nano) +
				"&created_before=" + base.Add(30*24*time.Hour).Format(time.RFC3339Nano) + "&bucket=5m",
			wantCode: "stats_bucket_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:     "overlong workflow id",
			query:    statsWindow(base, "workflow_id="+strings.Repeat("w", 65)),
			wantCode: "execution_workflow_id_invalid",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "valid without bucket",
			query:     statsWindow(base, ""),
			wantHTTP:  http.StatusOK,
			wantNoErr: true,
		},
		{
			name:      "valid with bucket",
			query:     statsWindow(base, "bucket=1h"),
			wantHTTP:  http.StatusOK,
			wantNoErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathExecutionStats+tc.query, nil))
			if rec.Code != tc.wantHTTP {
				t.Fatalf("status = %d, want %d (body=%s)", rec.Code, tc.wantHTTP, rec.Body.String())
			}
			if tc.wantNoErr {
				return
			}
			if env := decodeExecutionStats(t, rec.Body.Bytes()); env.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", env.Code, tc.wantCode)
			}
		})
	}
}

// TestExecutionStatsAggregatesAndIsolation proves the response shape, the
// zero-filled key set, and that a row stamped another namespace — under the
// SAME workflow id, the worst case — never enters the caller's aggregate.
func TestExecutionStatsAggregatesAndIsolation(t *testing.T) {
	st := memstore.New()
	srv := newExecutionListServer(t, st)
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	workflow := types.WorkflowID("wf-stats-a")

	seedExecutionWorkflow(t, st, "namespaceA", workflow, "wf-stats-a", "exec-a1", types.ExecutionStatusSuccess, base.Add(-30*time.Minute))
	seedExecutionWorkflow(t, st, "namespaceA", workflow, "wf-stats-a", "exec-a2", types.ExecutionStatusFailed, base.Add(-10*time.Minute))
	seedExecutionWorkflow(t, st, "namespaceA", "", "wf-stats-unattributed", "exec-a3", types.ExecutionStatusSuccess, base.Add(-5*time.Minute))
	// Another namespace's row under the same workflow id: a scoping regression
	// would show up as an extra count in namespaceA's answer.
	seedExecutionWorkflow(t, st, "namespaceB", workflow, "wf-stats-a", "exec-b1", types.ExecutionStatusSuccess, base.Add(-1*time.Minute))

	status, body := getExecutionStats(t, srv.URL, "tok-exec-a", statsWindow(base, "bucket=1h"))
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeExecutionStats(t, body)
	if env.Data == nil {
		t.Fatalf("success envelope has no data: %s", body)
	}
	data := env.Data
	if data.Total != 3 {
		t.Fatalf("total = %d, want 3 (two attributed + one unattributed workflow row in namespaceA)", data.Total)
	}
	for _, status := range store.ExecutionStatuses() {
		if _, ok := data.CountsByStatus[status]; !ok {
			t.Fatalf("counts_by_status is missing %q; every status key must be present", status)
		}
	}
	if data.CountsByStatus[types.ExecutionStatusSuccess] != 2 ||
		data.CountsByStatus[types.ExecutionStatusFailed] != 1 {
		t.Fatalf("counts_by_status = %v, want success=2 failed=1", data.CountsByStatus)
	}
	if len(data.Histogram) == 0 {
		t.Fatal("histogram is absent although bucket=1h was requested")
	}
	var histTotal int64
	for _, b := range data.Histogram {
		if b.Start.Location() != time.UTC {
			t.Fatalf("histogram bucket start %v is not UTC", b.Start)
		}
		for _, n := range b.Counts {
			histTotal += n
		}
	}
	if histTotal != data.Total {
		t.Fatalf("histogram sum = %d, total = %d; the buckets must tile the filtered set", histTotal, data.Total)
	}

	// The unscoped row's namespace has no principal and no store access here;
	// the workflow-only principal is refused by the route's execution.read gate.
	if status, _ := getExecutionStats(t, srv.URL, "tok-workflow-only", statsWindow(base, "")); status != http.StatusForbidden {
		t.Fatalf("workflow-only principal status = %d, want 403", status)
	}
}

// listRows decodes the raw wire rows of a workflow list so a test can assert
// field PRESENCE, which a typed decode would silently normalize away.
func listRows(t *testing.T, body []byte) []map[string]json.RawMessage {
	t.Helper()
	var raw struct {
		Data struct {
			List []map[string]json.RawMessage `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw workflow list: %v (body=%s)", err, body)
	}
	return raw.Data.List
}

func rowByID(t *testing.T, rows []map[string]json.RawMessage, id string) map[string]json.RawMessage {
	t.Helper()
	for _, row := range rows {
		var got string
		if err := json.Unmarshal(row["id"], &got); err != nil {
			t.Fatalf("decode row id: %v", err)
		}
		if got == id {
			return row
		}
	}
	t.Fatalf("no row with id %q in %d rows", id, len(rows))
	return nil
}

func TestListWorkflowsIncludeStatsRequiresExecutionRead(t *testing.T) {
	st := memstore.New()
	srv := newWorkflowListServerWithPrincipals(t, nil, st, statsListPrincipals())

	now := time.Now().UTC().Truncate(time.Millisecond)
	idRun := registerListWorkflow(t, srv.URL, "tok-both", "wf-stats-run", "v1")
	idEmpty := registerListWorkflow(t, srv.URL, "tok-both", "wf-stats-empty", "v1")
	idOther := registerListWorkflow(t, srv.URL, "tok-both", "wf-stats-other", "v1")

	// Outside the 24h window, inside the 7d default window.
	seedExecutionWorkflow(t, st, "namespaceA", types.WorkflowID(idRun), "wf-stats-run", "exec-old", types.ExecutionStatusSuccess, now.Add(-40*time.Hour))
	seedExecutionWorkflow(t, st, "namespaceA", types.WorkflowID(idRun), "wf-stats-run", "exec-new", types.ExecutionStatusFailed, now.Add(-time.Hour))
	seedExecutionWorkflow(t, st, "namespaceA", types.WorkflowID(idOther), "wf-stats-other", "exec-running", types.ExecutionStatusRunning, now.Add(-30*time.Minute))
	// A same-id row in another namespace must not appear in namespaceA's stats.
	seedExecutionWorkflow(t, st, "namespaceB", types.WorkflowID(idRun), "wf-stats-run", "exec-foreign", types.ExecutionStatusSuccess, now.Add(-time.Minute))

	t.Run("workflow_only_principal_gets_no_fields", func(t *testing.T) {
		status, body := getWorkflowList(t, srv.URL, "tok-a", "?include=stats,recent&window=24h")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200 (a missing secondary scope must not fail the page): %s", status, body)
		}
		for _, row := range listRows(t, body) {
			if _, ok := row["stats"]; ok {
				t.Fatalf("a workflow.read-only principal received stats: %s", row["stats"])
			}
			if _, ok := row["recent_executions"]; ok {
				t.Fatalf("a workflow.read-only principal received recent_executions: %s", row["recent_executions"])
			}
		}
	})

	t.Run("both_scopes_get_stats_and_recent", func(t *testing.T) {
		status, body := getWorkflowList(t, srv.URL, "tok-both", "?include=stats,recent&window=24h")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", status, body)
		}
		rows := listRows(t, body)

		var runStats workflowExecutionStats
		runRow := rowByID(t, rows, idRun)
		if err := json.Unmarshal(runRow["stats"], &runStats); err != nil {
			t.Fatalf("decode stats for wf-stats-run: %v (row=%v)", err, runRow)
		}
		if runStats.Window != "24h" || runStats.Runs != 1 || runStats.Succeeded != 0 || runStats.Failed != 1 {
			t.Fatalf("wf-stats-run stats = %+v, want window=24h runs=1 failed=1 (the -40h success is outside the window)", runStats)
		}
		if runStats.SuccessRate == nil || *runStats.SuccessRate != 0 {
			t.Fatalf("wf-stats-run success_rate = %v, want a non-nil 0", runStats.SuccessRate)
		}

		var recent []workflowRecentExecution
		if err := json.Unmarshal(runRow["recent_executions"], &recent); err != nil {
			t.Fatalf("decode recent_executions: %v", err)
		}
		if len(recent) != 2 {
			t.Fatalf("recent_executions = %v, want the two namespaceA rows (the foreign one must not leak)", recent)
		}
		if recent[0].ExecutionID != "exec-new" || recent[1].ExecutionID != "exec-old" {
			t.Fatalf("recent order = %v, want newest first (exec-new, exec-old)", recent)
		}
		if recent[0].DurationMS == nil || *recent[0].DurationMS != 1000 {
			t.Fatalf("terminal row duration_ms = %v, want 1000", recent[0].DurationMS)
		}

		// A workflow with no executions has a KNOWN zero, not an absent field.
		var emptyStats workflowExecutionStats
		emptyRow := rowByID(t, rows, idEmpty)
		if err := json.Unmarshal(emptyRow["stats"], &emptyStats); err != nil {
			t.Fatalf("decode stats for wf-stats-empty: %v (row=%v)", err, emptyRow)
		}
		if emptyStats.Runs != 0 || emptyStats.SampleSize != 0 || emptyStats.SuccessRate != nil {
			t.Fatalf("wf-stats-empty stats = %+v, want runs=0 sample_size=0 success_rate=null", emptyStats)
		}
		if _, ok := emptyRow["recent_executions"]; ok {
			t.Fatal("wf-stats-empty carries recent_executions although it has no executions")
		}

		// A running (non-terminal) row has no duration: absence, not zero.
		var otherRecent []workflowRecentExecution
		otherRow := rowByID(t, rows, idOther)
		if err := json.Unmarshal(otherRow["recent_executions"], &otherRecent); err != nil {
			t.Fatalf("decode wf-stats-other recent_executions: %v", err)
		}
		if len(otherRecent) != 1 || otherRecent[0].DurationMS != nil {
			t.Fatalf("non-terminal recent row = %+v, want a nil duration", otherRecent)
		}
	})

	t.Run("default_window_is_7d", func(t *testing.T) {
		status, body := getWorkflowList(t, srv.URL, "tok-both", "?include=stats")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", status, body)
		}
		var stats workflowExecutionStats
		if err := json.Unmarshal(rowByID(t, listRows(t, body), idRun)["stats"], &stats); err != nil {
			t.Fatalf("decode stats: %v", err)
		}
		if stats.Window != "7d" || stats.Runs != 2 || stats.Succeeded != 1 || stats.Failed != 1 {
			t.Fatalf("default-window stats = %+v, want window=7d runs=2 succeeded=1 failed=1", stats)
		}
	})

	t.Run("invalid_include_and_window_are_400", func(t *testing.T) {
		for _, tc := range []struct {
			query string
			code  string
		}{
			{query: "?include=everything", code: "workflow_include_invalid"},
			{query: "?include=stats,,recent", code: "workflow_include_invalid"},
			{query: "?include=stats&window=1y", code: "workflow_window_invalid"},
			{query: "?window=1y", code: "workflow_window_invalid"},
		} {
			status, body := getWorkflowList(t, srv.URL, "tok-both", tc.query)
			if status != http.StatusBadRequest {
				t.Fatalf("%s status = %d, want 400 (body=%s)", tc.query, status, body)
			}
			env := decodeListEnvelope(t, body)
			if env.Code != tc.code {
				t.Fatalf("%s code = %q, want %q", tc.query, env.Code, tc.code)
			}
		}
	})
}

func TestListWorkflowsIncludeStatsTightensPageSize(t *testing.T) {
	registry := newRecordingListRegistry()
	st := memstore.New()
	srv := newWorkflowListServerWithPrincipals(t, registry, st, statsListPrincipals())
	registerListWorkflow(t, srv.URL, "tok-both", "wf-page-stats", "v1")

	status, body := getWorkflowList(t, srv.URL, "tok-both", "?page_size=200&include=stats")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", status, body)
	}
	options := registry.recordedOptions()
	if len(options) == 0 {
		t.Fatal("registry recorded no ListWorkflows call")
	}
	// Every list request makes two registry calls: the page read (with a limit)
	// and countWorkflows (no options). The page read is the one under test.
	if got := lastPageLimit(t, options); got != maxWorkflowListPageSizeWithStats {
		t.Fatalf("page limit with include=stats = %d, want %d", got, maxWorkflowListPageSizeWithStats)
	}

	// Without include the normal 200 cap applies.
	if status, _ := getWorkflowList(t, srv.URL, "tok-both", "?page_size=200"); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if got := lastPageLimit(t, registry.recordedOptions()); got != 200 {
		t.Fatalf("page limit without include = %d, want 200", got)
	}
}

// lastPageLimit returns the limit of the newest ListWorkflows call that carried
// one, skipping countWorkflows' optionless enumeration.
func lastPageLimit(t *testing.T, options []backend.WorkflowListOptions) int {
	t.Helper()
	for i := len(options) - 1; i >= 0; i-- {
		if options[i].Limit > 0 {
			return options[i].Limit
		}
	}
	t.Fatalf("no ListWorkflows call with a page limit in %v", options)
	return 0
}
