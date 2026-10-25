package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xbackend "github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// The schedule API's tests. What they defend, in one line each:
//
//   - the three schedule scopes are three grants, and holding one is not holding
//     another (schedule.write especially must not imply schedule.backfill);
//   - another namespace's workflow id resolves to 404, before any store read;
//   - a conditional write's If-Match is the revision vocabulary, and an
//     unusable value is refused rather than guessed at;
//   - every mutation is in the audit ledger with the node it targeted;
//   - a deployment with no overlay store says so (501) rather than pretending
//     the route does not exist (404);
//   - deleting a workflow takes its overlays with it.
//
// The manager's own behaviour (validation, view derivation, backfill planning)
// is tested in service/control; these tests are about the HTTP contract on top
// of it, so where the two could disagree the assertion is on the wire shape.

const (
	scheduleTokenRead     = "tok-schedule-read"
	scheduleTokenWrite    = "tok-schedule-write"
	scheduleTokenBackfill = "tok-schedule-backfill"
	scheduleTokenFull     = "tok-schedule-full"
	scheduleTokenOtherNS  = "tok-schedule-other-ns"
)

const (
	scheduleWorkflowID = "wf-sched"
	scheduleNamespace  = "namespaceA"
	scheduleOtherNS    = "namespaceB"
)

// scheduleResponse is one decoded response: the status, the envelope fields,
// and the raw body (so a test can assert on the bytes a client would see, and
// on what a failure must NOT contain).
type scheduleResponse struct {
	Status  int
	Success bool
	Code    string
	Message string
	Data    json.RawMessage
	Body    string
}

// want asserts a success response and returns it for further decoding.
func (r scheduleResponse) want(t *testing.T, status int) scheduleResponse {
	t.Helper()
	if r.Status != status || !r.Success {
		t.Fatalf("status=%d success=%v body=%s, want %d/success", r.Status, r.Success, r.Body, status)
	}
	return r
}

// wantFail asserts a refusal: the status, the stable code, and that no payload
// came with it. A failure body carrying data is how a client ends up acting on
// a plan the server refused to honour.
func (r scheduleResponse) wantFail(t *testing.T, status int, code string) scheduleResponse {
	t.Helper()
	if r.Status != status || r.Code != code || r.Success {
		t.Fatalf("status=%d code=%q success=%v body=%s, want %d/%q/failure", r.Status, r.Code, r.Success, r.Body, status, code)
	}
	if len(r.Data) > 0 && string(r.Data) != "null" {
		t.Fatalf("refusal %s carried a data payload: %s", code, r.Data)
	}
	return r
}

func (r scheduleResponse) view(t *testing.T) scheduleViewResponse {
	t.Helper()
	var out scheduleViewResponse
	if err := json.Unmarshal(r.Data, &out); err != nil {
		t.Fatalf("decode schedule view: %v (data=%s)", err, r.Data)
	}
	return out
}

func (r scheduleResponse) list(t *testing.T) ([]scheduleViewResponse, int) {
	t.Helper()
	var page struct {
		List  []scheduleViewResponse `json:"list"`
		Total int                    `json:"total"`
	}
	if err := json.Unmarshal(r.Data, &page); err != nil {
		t.Fatalf("decode schedule list: %v (data=%s)", err, r.Data)
	}
	return page.List, page.Total
}

func (r scheduleResponse) backfill(t *testing.T) scheduleBackfillResponse {
	t.Helper()
	var out scheduleBackfillResponse
	if err := json.Unmarshal(r.Data, &out); err != nil {
		t.Fatalf("decode backfill result: %v (data=%s)", err, r.Data)
	}
	return out
}

// scheduleRequest drives one request through a handler and decodes the reply.
// body is the raw JSON a client would send, so a test can send a malformed one
// or none at all.
func scheduleRequest(h http.Handler, method, token, path, body string, headers map[string]string) scheduleResponse {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	res := scheduleResponse{Status: rec.Code, Body: rec.Body.String()}
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err == nil {
		res.Success, res.Code, res.Message, res.Data = env.Success, env.Code, env.Message, env.Data
	}
	return res
}

// scheduleAPIFixture is the schedule API over an in-memory overlay store, wired
// the way apiserver.New wires it (the module's post-construction schedules
// field, the principal authenticator, the namespace-aware authorizer and the
// audit sink), so the tests exercise the real authz wrapper rather than a stub.
type scheduleAPIFixture struct {
	mux   *http.ServeMux
	audit *InMemoryAuditSink
	be    *local.Backend
	mem   *memstore.Store
	mgr   *control.ScheduleManager
	// now is the fixed clock the manager plans against and derives next_fire
	// from, so a view is a function of the schedule and not of the wall clock.
	now time.Time
}

func newScheduleAPIFixture(t *testing.T, emitter control.ScheduleEmitter) *scheduleAPIFixture {
	t.Helper()
	be := local.New()
	registerScheduleWorkflow(t, be, scheduleWorkflowDef(scheduleWorkflowID))
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	mem := memstore.New()
	now := time.Date(2026, 5, 4, 12, 10, 0, 0, time.UTC)
	mgr := control.NewScheduleManager(control.ScheduleManagerConfig{
		Overrides: mem,
		Emitter:   emitter,
		Now:       func() time.Time { return now },
	})
	audit := NewInMemoryAuditSink()
	m := newWorkflowControlModule(cp, nil, nil, nil)
	m.principalAuth = NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		// The scopes are deliberately disjoint per token: the scope table below
		// proves that schedule.read, schedule.write and schedule.backfill are
		// separate grants, which a token holding all three could not.
		{Token: scheduleTokenRead, Subject: "op-read", Namespace: scheduleNamespace, Scopes: []string{"schedule.read"}},
		{Token: scheduleTokenWrite, Subject: "op-write", Namespace: scheduleNamespace, Scopes: []string{"schedule.write"}},
		{Token: scheduleTokenBackfill, Subject: "op-backfill", Namespace: scheduleNamespace, Scopes: []string{"schedule.backfill"}},
		// The full token also carries "workflow" because one test deletes the
		// workflow through this same server (the overlay cascade rides the
		// deregistration route, which is OpWorkflowRegister).
		{Token: scheduleTokenFull, Subject: "op-full", Namespace: scheduleNamespace, Scopes: []string{"workflow", "schedule.read", "schedule.write", "schedule.backfill"}},
		{Token: scheduleTokenOtherNS, Subject: "op-other", Namespace: scheduleOtherNS, Scopes: []string{"schedule.read", "schedule.write", "schedule.backfill"}},
	})
	m.authorizer = NamespaceAwareAuthorizer{}
	m.audit = audit
	m.schedules = mgr

	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)
	return &scheduleAPIFixture{mux: mux, audit: audit, be: be, mem: mem, mgr: mgr, now: now}
}

func (f *scheduleAPIFixture) do(method, token, path, body string, headers map[string]string) scheduleResponse {
	return scheduleRequest(f.mux, method, token, path, body, headers)
}

// schedulesPath is the collection URL of the fixture's workflow.
func (f *scheduleAPIFixture) schedulesPath() string {
	return "/v1/workflows/" + scheduleWorkflowID + "/schedules"
}

func (f *scheduleAPIFixture) nodePath(node, verb string) string {
	path := f.schedulesPath() + "/" + node
	if verb != "" {
		path += "/" + verb
	}
	return path
}

// overlays reads the overlay store directly, bypassing the API, so a test can
// prove what was actually persisted rather than what the API reported.
func (f *scheduleAPIFixture) overlays(t *testing.T) []string {
	t.Helper()
	rows, err := f.mem.ListScheduleOverrides(context.Background(), scheduleNamespace, scheduleWorkflowID)
	if err != nil {
		t.Fatalf("ListScheduleOverrides: %v", err)
	}
	nodes := make([]string, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, row.Node)
	}
	return nodes
}

// scheduleWorkflowDef is the API fixture's workflow: a cron schedule, a timer
// schedule, and an ordinary action node that is not a schedule at all (so the
// "not a schedule" refusal has something to point at).
func scheduleWorkflowDef(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    name,
		Version: "v1",
		Nodes: []types.NodeDef{
			{
				Name: "nightly", Type: control.NodeTypeCronTrigger, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "0 2 * * *"},
			},
			{
				Name: "poll", Type: control.NodeTypeTimerTrigger, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"interval": "1m"},
			},
			{Name: "job", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"nightly": {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
			"poll":    {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
		},
	}
}

// scheduleWorkflowDefWithoutPoll is the same workflow with the timer node gone:
// re-registering it is how a test makes an existing overlay orphaned.
func scheduleWorkflowDefWithoutPoll(name string) *types.WorkflowDef {
	def := scheduleWorkflowDef(name)
	kept := make([]types.NodeDef, 0, len(def.Nodes))
	for _, n := range def.Nodes {
		if n.Name != "poll" {
			kept = append(kept, n)
		}
	}
	def.Nodes = kept
	delete(def.Connections, "poll")
	return def
}

func registerScheduleWorkflow(t *testing.T, be *local.Backend, def *types.WorkflowDef) {
	t.Helper()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile %s: %v", def.Name, err)
	}
	if _, err := be.WorkflowRegistry().AddWorkflow(context.Background(), xbackend.WorkflowRecord{
		ID:             types.WorkflowID(def.Name),
		Key:            def.Name + "@" + def.Version,
		Namespace:      scheduleNamespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: "sha256:" + def.Name,
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("AddWorkflow %s: %v", def.Name, err)
	}
}

// reregisterWorkflow replaces the fixture's workflow with a different graph
// under the same id. The registry has no replace, so it is remove-then-add —
// which is also the sequence that leaves existing overlays behind, and a
// leftover overlay is exactly what an orphan is.
func (f *scheduleAPIFixture) reregisterWorkflow(t *testing.T, def *types.WorkflowDef) {
	t.Helper()
	if err := f.be.WorkflowRegistry().RemoveWorkflow(context.Background(), types.WorkflowID(def.Name)); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	registerScheduleWorkflow(t, f.be, def)
}

// TestScheduleAPIListProjectsScheduleNodesAndOverlay pins the collection read:
// one row per schedule the definition declares (not per trigger node — the
// action node is absent), each carrying the effective timing, the two nullable
// instants, and the overlay's revision.
func TestScheduleAPIListProjectsScheduleNodesAndOverlay(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)

	res := f.do(http.MethodGet, scheduleTokenRead, f.schedulesPath(), "", nil).want(t, http.StatusOK)
	items, total := res.list(t)
	if total != 2 || len(items) != 2 {
		t.Fatalf("list total=%d len=%d, want 2/2 (one per schedule; the action node is not one)", total, len(items))
	}
	byNode := make(map[string]scheduleViewResponse, len(items))
	for _, item := range items {
		byNode[item.Node] = item
	}
	if _, ok := byNode["job"]; ok {
		t.Errorf("the action node appears in the schedule list: %+v", byNode["job"])
	}

	cron, ok := byNode["nightly"]
	if !ok {
		t.Fatalf("no row for the cron node; got %+v", items)
	}
	if cron.WorkflowID != scheduleWorkflowID || cron.Kind != control.ScheduleKindCron {
		t.Errorf("cron row = %+v, want workflow %s kind %s", cron, scheduleWorkflowID, control.ScheduleKindCron)
	}
	if cron.Expression != "0 2 * * *" {
		t.Errorf("expression = %q, want the definition's", cron.Expression)
	}
	// The definition declares no timezone and the workflow has no
	// settings.timezone, so the effective zone is UTC — what will happen, not
	// what the definition spells out.
	if cron.Timezone != "UTC" || cron.Misfire != "skip" || cron.Overlap != "allow" {
		t.Errorf("cron defaults = %q/%q/%q, want UTC/skip/allow", cron.Timezone, cron.Misfire, cron.Overlap)
	}
	if cron.Overridden || cron.Orphaned || cron.Paused || cron.Revision != 0 {
		t.Errorf("cron row = %+v, want a plain definition-backed schedule", cron)
	}
	if cron.LastScheduled != nil {
		t.Errorf("last_scheduled = %v, want nil (no trigger host can be asked)", cron.LastScheduled)
	}
	wantNext := time.Date(2026, 5, 5, 2, 0, 0, 0, time.UTC)
	if cron.NextFire == nil || !cron.NextFire.Equal(wantNext) {
		t.Errorf("next_fire = %v, want %v", cron.NextFire, wantNext)
	}
	// The key must be present-and-null rather than absent: the frontend renders
	// the column either way, and "unknown" has to be distinguishable from "the
	// server did not answer at all".
	if !strings.Contains(string(res.Data), `"last_scheduled":null`) {
		t.Errorf("last_scheduled is not an explicit null: %s", res.Data)
	}

	timer, ok := byNode["poll"]
	if !ok {
		t.Fatalf("no row for the timer node; got %+v", items)
	}
	wantTick := time.Date(2026, 5, 4, 12, 11, 0, 0, time.UTC)
	if timer.Kind != control.ScheduleKindTimer || timer.Expression != "1m" {
		t.Errorf("timer row = %+v, want kind timer expression 1m", timer)
	}
	if timer.NextFire == nil || !timer.NextFire.Equal(wantTick) {
		t.Errorf("timer next_fire = %v, want %v (the interval grid's next point)", timer.NextFire, wantTick)
	}

	// One node, one row.
	one := f.do(http.MethodGet, scheduleTokenRead, f.nodePath("nightly", ""), "", nil).want(t, http.StatusOK).view(t)
	if one.Node != "nightly" || one.WorkflowID != scheduleWorkflowID {
		t.Errorf("single read = %+v, want the nightly schedule of %s", one, scheduleWorkflowID)
	}

	// A node that is not a schedule and a node that does not exist are the SAME
	// refusal: the difference would map out which names a workflow declares.
	f.do(http.MethodGet, scheduleTokenRead, f.nodePath("job", ""), "", nil).
		wantFail(t, http.StatusNotFound, "schedule_not_found")
	f.do(http.MethodGet, scheduleTokenRead, f.nodePath("ghost", ""), "", nil).
		wantFail(t, http.StatusNotFound, "schedule_not_found")
	// An unknown workflow is its own code: the caller addressed something else.
	f.do(http.MethodGet, scheduleTokenRead, "/v1/workflows/ghost/schedules", "", nil).
		wantFail(t, http.StatusNotFound, "workflow_not_found")
}

// TestScheduleAPIScopesAreSeparateGrants is the scope table. Each token holds
// exactly one of the three schedule scopes, so a row that succeeds with the
// wrong token is a hole in scopeForOperation, not a test detail.
func TestScheduleAPIScopesAreSeparateGrants(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)
	patch := `{"expression":"0 3 * * *"}`
	dryRun := `{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:03:00Z","dry_run":true}`

	cases := []struct {
		name   string
		token  string
		method string
		verb   string
		body   string
		status int
		code   string
	}{
		{"read lists", scheduleTokenRead, http.MethodGet, "", "", http.StatusOK, "200"},
		{"read cannot patch", scheduleTokenRead, http.MethodPatch, "", patch, http.StatusForbidden, "forbidden"},
		{"read cannot pause", scheduleTokenRead, http.MethodPost, "pause", "", http.StatusForbidden, "forbidden"},
		{"read cannot backfill", scheduleTokenRead, http.MethodPost, "backfill", dryRun, http.StatusForbidden, "forbidden"},

		{"write cannot list", scheduleTokenWrite, http.MethodGet, "", "", http.StatusForbidden, "forbidden"},
		{"write patches", scheduleTokenWrite, http.MethodPatch, "", patch, http.StatusOK, "200"},
		{"write pauses", scheduleTokenWrite, http.MethodPost, "pause", "", http.StatusOK, "200"},
		// The one that matters most: a principal trusted to retime a schedule is
		// not thereby trusted to run its past instants.
		{"write cannot backfill", scheduleTokenWrite, http.MethodPost, "backfill", dryRun, http.StatusForbidden, "forbidden"},

		{"backfill cannot list", scheduleTokenBackfill, http.MethodGet, "", "", http.StatusForbidden, "forbidden"},
		{"backfill cannot patch", scheduleTokenBackfill, http.MethodPatch, "", patch, http.StatusForbidden, "forbidden"},
		{"backfill backfills", scheduleTokenBackfill, http.MethodPost, "backfill", dryRun, http.StatusOK, "200"},

		{"no credential", "", http.MethodGet, "", "", http.StatusUnauthorized, "unauthenticated"},
		{"no credential cannot backfill", "", http.MethodPost, "backfill", dryRun, http.StatusUnauthorized, "unauthenticated"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := f.do(c.method, c.token, f.nodePath("nightly", c.verb), c.body, nil)
			if c.status == http.StatusOK {
				res.want(t, c.status)
				return
			}
			res.wantFail(t, c.status, c.code)
		})
	}
}

// TestScheduleAPICrossNamespaceIsNotFound pins the tenant boundary: a workflow
// id from another namespace is not-found, never forbidden. A 403 would confirm
// the id exists, and the caller must not be able to learn that. It also proves
// the refusal happens before any store access, so no read is attempted against
// the wrong tenant's overlay.
func TestScheduleAPICrossNamespaceIsNotFound(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)
	dryRun := `{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:03:00Z","dry_run":true}`

	attempts := []struct {
		method, node, verb, body string
	}{
		{http.MethodGet, "", "", ""},
		{http.MethodGet, "nightly", "", ""},
		{http.MethodPatch, "nightly", "", `{"expression":"0 3 * * *"}`},
		{http.MethodPost, "nightly", "pause", ""},
		{http.MethodPost, "nightly", "resume", ""},
		{http.MethodDelete, "nightly", "override", ""},
		{http.MethodPost, "nightly", "backfill", dryRun},
	}
	for _, a := range attempts {
		path := f.schedulesPath()
		if a.node != "" {
			path = f.nodePath(a.node, a.verb)
		}
		res := f.do(a.method, scheduleTokenOtherNS, path, a.body, nil)
		res.wantFail(t, http.StatusNotFound, "workflow_not_found")
		if strings.Contains(strings.ToLower(res.Message), "forbidden") {
			t.Errorf("%s %s leaked a forbidden verdict: %s", a.method, path, res.Message)
		}
	}

	// Nothing was written under either tenant: the PATCH above never reached the
	// store, and the same request from the owning namespace does write — so the
	// refusal is the tenant boundary and not some unrelated failure.
	if got := f.overlays(t); len(got) != 0 {
		t.Fatalf("overlays after cross-namespace attempts = %v, want none", got)
	}
	f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"expression":"0 3 * * *"}`, nil).want(t, http.StatusOK)
	if got := f.overlays(t); len(got) != 1 || got[0] != "nightly" {
		t.Fatalf("overlays after the owning namespace's patch = %v, want [nightly]", got)
	}
}

// TestScheduleAPIConditionalWritesUseTheRevision pins If-Match as the store's
// own vocabulary: 0 means "only if no overlay exists", N means "only at this
// revision", and anything else is refused rather than guessed at.
func TestScheduleAPIConditionalWritesUseTheRevision(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)
	url := f.nodePath("nightly", "")
	first := `{"expression":"0 3 * * *"}`

	created := f.do(http.MethodPatch, scheduleTokenFull, url, first, map[string]string{"If-Match": "0"}).want(t, http.StatusOK).view(t)
	if !created.Overridden || created.Revision != 1 || created.Expression != "0 3 * * *" {
		t.Fatalf("create = %+v, want overridden revision 1", created)
	}

	// The row now exists, so a second create-only write must lose.
	f.do(http.MethodPatch, scheduleTokenFull, url, first, map[string]string{"If-Match": "0"}).
		wantFail(t, http.StatusConflict, "schedule_revision_conflict")

	updated := f.do(http.MethodPatch, scheduleTokenFull, url, `{"expression":"0 4 * * *"}`, map[string]string{"If-Match": "1"}).want(t, http.StatusOK).view(t)
	if updated.Revision != 2 || updated.Expression != "0 4 * * *" {
		t.Fatalf("update = %+v, want revision 2 with the new expression", updated)
	}

	// Stale revision: the write must not land, and the refusal must not have
	// changed what the schedule says.
	f.do(http.MethodPatch, scheduleTokenFull, url, `{"expression":"0 5 * * *"}`, map[string]string{"If-Match": "1"}).
		wantFail(t, http.StatusConflict, "schedule_revision_conflict")
	after := f.do(http.MethodGet, scheduleTokenFull, url, "", nil).want(t, http.StatusOK).view(t)
	if after.Revision != 2 || after.Expression != "0 4 * * *" {
		t.Fatalf("after the refused write = %+v, want the revision-2 state", after)
	}

	// An unusable precondition is a 400 that does not echo what was sent: the
	// value is unvalidated caller input with nothing to learn from it.
	for _, bad := range []string{"abc", "*", "1.0", "-1"} {
		res := f.do(http.MethodPatch, scheduleTokenFull, url, first, map[string]string{"If-Match": bad}).
			wantFail(t, http.StatusBadRequest, "invalid_if_match")
		if strings.Contains(res.Body, bad) {
			t.Errorf("If-Match %q was echoed back: %s", bad, res.Body)
		}
	}

	// No precondition is an unconditional write.
	last := f.do(http.MethodPatch, scheduleTokenFull, url, first, nil).want(t, http.StatusOK).view(t)
	if last.Revision != 3 {
		t.Errorf("unconditional write revision = %d, want 3", last.Revision)
	}
}

// TestScheduleAPIPatchValidatesAgainstTheNodeKind pins that a patch is judged
// against the node it targets before it is stored — a field the kind cannot
// apply is refused, not accepted as a silent no-op — and that an explicit null
// reverts a field to the definition rather than dropping the overlay.
func TestScheduleAPIPatchValidatesAgainstTheNodeKind(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)

	// A timer has an interval and nothing else: timezone, misfire and overlap
	// are the cron handler's parameters.
	for _, c := range []struct{ field, body string }{
		{"timezone", `{"timezone":"UTC"}`},
		{"misfire", `{"misfire":"fire_all"}`},
		{"overlap", `{"overlap":"skip"}`},
	} {
		res := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("poll", ""), c.body, nil).
			wantFail(t, http.StatusBadRequest, "invalid_schedule_field")
		// The message names the field the caller used, so a client with four
		// optional fields knows which one to fix.
		if !strings.Contains(res.Message, c.field) {
			t.Errorf("message %q does not name %s", res.Message, c.field)
		}
	}

	// A malformed expression for the kind it targets.
	if res := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"expression":"not a cron"}`, nil).
		wantFail(t, http.StatusBadRequest, "invalid_schedule_field"); !strings.Contains(res.Message, "expression") {
		t.Errorf("message %q does not name expression", res.Message)
	}
	if res := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"expression":"0 0 0 0 0"}`, nil).
		wantFail(t, http.StatusBadRequest, "invalid_schedule_field"); !strings.Contains(res.Message, "expression") {
		t.Errorf("message %q does not name expression", res.Message)
	}
	if res := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"timezone":"Mars/Olympus"}`, nil).
		wantFail(t, http.StatusBadRequest, "invalid_schedule_field"); !strings.Contains(res.Message, "timezone") {
		t.Errorf("message %q does not name timezone", res.Message)
	}
	// An unknown policy and an unknown field are both refusals, not silently
	// ignored keys.
	if res := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"misfire":"fire_always"}`, nil).
		wantFail(t, http.StatusBadRequest, "invalid_schedule_field"); !strings.Contains(res.Message, "misfire") {
		t.Errorf("message %q does not name misfire", res.Message)
	}
	if res := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"nope":"x"}`, nil).
		wantFail(t, http.StatusBadRequest, "invalid_schedule_field"); !strings.Contains(res.Message, "nope") {
		t.Errorf("message %q does not name the unknown field", res.Message)
	}
	// A body that is not JSON at all is a different refusal than a bad field.
	f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"expression":`, nil).
		wantFail(t, http.StatusBadRequest, "bad_request")

	// Override the cron timezone, then revert that one field with null: the
	// overlay survives (still overridden) and the value follows the definition
	// again — the case DELETE .../override cannot express.
	pinned := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"Timezone":"Europe/Berlin"}`, nil).want(t, http.StatusOK).view(t)
	if pinned.Timezone != "Europe/Berlin" || !pinned.Overridden || pinned.Revision != 1 {
		t.Fatalf("timezone patch = %+v, want Europe/Berlin on revision 1", pinned)
	}
	reverted := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"timezone":null}`, nil).want(t, http.StatusOK).view(t)
	if reverted.Timezone != "UTC" || !reverted.Overridden || reverted.Revision != 2 {
		t.Fatalf("revert = %+v, want UTC on revision 2 with the overlay still present", reverted)
	}

	// A patch that names nothing is a read with a precondition: it answers with
	// the current view and writes nothing, which is what makes If-Match usable
	// without inventing a body.
	empty := f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), "", nil).want(t, http.StatusOK).view(t)
	if empty.Revision != 2 {
		t.Errorf("empty patch revision = %d, want 2 (no write)", empty.Revision)
	}
}

// TestScheduleAPIPauseResumeAreIdempotent pins that pausing is a state, not a
// counter: a repeated pause writes nothing and bumps no revision, so a retried
// pause cannot invalidate another operator's If-Match.
func TestScheduleAPIPauseResumeAreIdempotent(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)

	paused := f.do(http.MethodPost, scheduleTokenFull, f.nodePath("nightly", "pause"), "", nil).want(t, http.StatusOK).view(t)
	if !paused.Paused || paused.Revision != 1 {
		t.Fatalf("pause = %+v, want paused on revision 1", paused)
	}
	again := f.do(http.MethodPost, scheduleTokenFull, f.nodePath("nightly", "pause"), "", nil).want(t, http.StatusOK).view(t)
	if !again.Paused || again.Revision != 1 {
		t.Errorf("repeated pause = %+v, want the same revision", again)
	}

	resumed := f.do(http.MethodPost, scheduleTokenFull, f.nodePath("nightly", "resume"), "", nil).want(t, http.StatusOK).view(t)
	if resumed.Paused || resumed.Revision != 2 {
		t.Fatalf("resume = %+v, want not-paused on revision 2", resumed)
	}
	again = f.do(http.MethodPost, scheduleTokenFull, f.nodePath("nightly", "resume"), "", nil).want(t, http.StatusOK).view(t)
	if again.Paused || again.Revision != 2 {
		t.Errorf("repeated resume = %+v, want the same revision", again)
	}
}

// TestScheduleAPIDeleteOverrideAndOrphan covers the one route that exists for
// leftovers: an overlay whose node the current version no longer declares is
// listed (so it can be seen) and deletable (so it can be cleaned up), which is
// not a state the patch route can reach.
func TestScheduleAPIDeleteOverrideAndOrphan(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)

	f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("poll", ""), `{"expression":"5m"}`, nil).want(t, http.StatusOK)
	dropped := f.do(http.MethodDelete, scheduleTokenFull, f.nodePath("poll", "override"), "", nil).want(t, http.StatusOK).view(t)
	if dropped.Overridden || dropped.Revision != 0 || dropped.Expression != "1m" {
		t.Fatalf("after delete = %+v, want the definition's 1m and no overlay", dropped)
	}
	// Deleting an overlay that was never written is not "already done": the
	// caller addressed something that does not exist.
	f.do(http.MethodDelete, scheduleTokenFull, f.nodePath("poll", "override"), "", nil).
		wantFail(t, http.StatusNotFound, "schedule_not_found")

	// Re-register the workflow without the timer node: the overlay written below
	// survives by node name, and its node is gone.
	f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("poll", ""), `{"expression":"5m"}`, nil).want(t, http.StatusOK)
	f.reregisterWorkflow(t, scheduleWorkflowDefWithoutPoll(scheduleWorkflowID))

	orphan := f.do(http.MethodGet, scheduleTokenFull, f.nodePath("poll", ""), "", nil).want(t, http.StatusOK).view(t)
	if !orphan.Orphaned || !orphan.Overridden || orphan.Kind != "" || orphan.Expression != "5m" {
		t.Fatalf("orphan = %+v, want an orphaned overlay keeping its own expression and no kind", orphan)
	}
	if orphan.NextFire != nil {
		t.Errorf("orphan next_fire = %v, want nil (there is no live rule to evaluate)", orphan.NextFire)
	}

	items, total := f.do(http.MethodGet, scheduleTokenFull, f.schedulesPath(), "", nil).want(t, http.StatusOK).list(t)
	if total != 2 || len(items) != 2 {
		t.Fatalf("list = %d rows (total %d), want the live cron node plus the orphan", len(items), total)
	}
	seen := map[string]bool{}
	for _, item := range items {
		seen[item.Node] = item.Orphaned
	}
	if orphaned, ok := seen["poll"]; !ok || !orphaned {
		t.Errorf("list = %+v, want poll listed as orphaned", items)
	}

	// The orphan is the only thing delete can still act on.
	cleaned := f.do(http.MethodDelete, scheduleTokenFull, f.nodePath("poll", "override"), "", nil).want(t, http.StatusOK).view(t)
	if cleaned.Overridden || !cleaned.Orphaned {
		t.Errorf("after cleaning the orphan = %+v, want no overlay on a still-missing node", cleaned)
	}
	if got := f.overlays(t); len(got) != 0 {
		t.Errorf("overlays after cleanup = %v, want none", got)
	}
	// A patch cannot target the orphan at all: there is no kind left to validate
	// it against, so the refusal points the caller at delete.
	f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("poll", ""), `{"expression":"5m"}`, nil).
		wantFail(t, http.StatusNotFound, "schedule_not_found")
}

// stubScheduleEmitter records what a backfill fired and answers per instant, so
// the test can drive each classification (emitted, duplicate, refused) without
// an engine behind it.
type stubScheduleEmitter struct {
	calls []time.Time
	emit  func(instant time.Time) (types.ExecutionID, bool, error)
}

func (e *stubScheduleEmitter) EmitScheduleInstant(_ context.Context, _ namespace.Namespace, _ types.WorkflowID, _ string, instant time.Time) (types.ExecutionID, bool, error) {
	e.calls = append(e.calls, instant)
	return e.emit(instant)
}

// TestScheduleAPIBackfillWithoutAHostStillPlans pins the split a deployment
// without trigger hosting gets: the plan is answerable (a dry run costs nothing
// and needs no host), and the firing is refused by name rather than reported as
// a missing route.
func TestScheduleAPIBackfillWithoutAHostStillPlans(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)
	url := f.nodePath("poll", "backfill")

	dry := f.do(http.MethodPost, scheduleTokenFull, url,
		`{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:03:00Z","dry_run":true}`, nil).
		want(t, http.StatusOK).backfill(t)
	if !dry.DryRun || len(dry.Planned) != 3 || len(dry.Emitted) != 0 || len(dry.Skipped) != 0 {
		t.Fatalf("dry run = %+v, want 3 planned instants and no emissions", dry)
	}
	want := []time.Time{
		time.Date(2026, 5, 4, 12, 1, 0, 0, time.UTC),
		time.Date(2026, 5, 4, 12, 2, 0, 0, time.UTC),
		time.Date(2026, 5, 4, 12, 3, 0, 0, time.UTC),
	}
	for i, instant := range dry.Planned {
		if !instant.Equal(want[i]) {
			t.Errorf("planned[%d] = %v, want %v", i, instant, want[i])
		}
	}
	if !dry.From.Equal(time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)) || !dry.To.Equal(want[2]) {
		t.Errorf("range echoed as %v..%v, want the requested (from, to]", dry.From, dry.To)
	}

	// The same range for real: 501, not 404 — the route exists on this server and
	// what is missing is the trigger host.
	f.do(http.MethodPost, scheduleTokenFull, url,
		`{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:03:00Z"}`, nil).
		wantFail(t, http.StatusNotImplemented, "backfill_unavailable")

	// Range refusals are all 400 with their own code, and none of them carries a
	// plan: a body returned beside a refused request would be read as "these ran".
	for _, c := range []struct{ name, body string }{
		{"inverted", `{"from":"2026-05-04T12:03:00Z","to":"2026-05-04T12:00:00Z"}`},
		{"empty", `{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:00:00Z"}`},
		{"future", `{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T13:00:00Z"}`},
		{"over the cap", `{"from":"2026-05-03T19:29:00Z","to":"2026-05-04T12:10:00Z"}`},
		{"missing to", `{"from":"2026-05-04T12:00:00Z"}`},
		{"not a timestamp", `{"from":"yesterday","to":"2026-05-04T12:03:00Z"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			f.do(http.MethodPost, scheduleTokenFull, url, c.body, nil).
				wantFail(t, http.StatusBadRequest, "invalid_backfill_range")
		})
	}
	// A missing or unparseable timestamp names the field, so a client with two
	// of them knows which one to fix.
	res := f.do(http.MethodPost, scheduleTokenFull, url, `{"to":"2026-05-04T12:03:00Z"}`, nil).
		wantFail(t, http.StatusBadRequest, "invalid_backfill_range")
	if !strings.Contains(res.Message, "from") {
		t.Errorf("message %q does not name from", res.Message)
	}
}

// TestScheduleAPIBackfillReportsEveryInstant pins the per-instant report: each
// planned instant lands in exactly one of emitted or skipped with a reason from
// the wire vocabulary, so a partly successful range is honestly reported rather
// than summarised into a count.
func TestScheduleAPIBackfillReportsEveryInstant(t *testing.T) {
	emitter := &stubScheduleEmitter{}
	emitter.emit = func(instant time.Time) (types.ExecutionID, bool, error) {
		switch instant.Minute() {
		case 2:
			// The instant already fired: a duplicate is not a failure.
			return "", true, nil
		case 3:
			// The workflow's own admission refused it.
			return "", false, fmt.Errorf("admission: %w", engine.ErrConcurrencyLimit)
		default:
			return types.ExecutionID(fmt.Sprintf("ex-%d", instant.Minute())), false, nil
		}
	}
	f := newScheduleAPIFixture(t, emitter)

	res := f.do(http.MethodPost, scheduleTokenFull, f.nodePath("poll", "backfill"),
		`{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:03:00Z"}`, nil).want(t, http.StatusOK).backfill(t)

	if res.DryRun || len(res.Planned) != 3 {
		t.Fatalf("result = %+v, want a real run over 3 instants", res)
	}
	if len(res.Emitted) != 1 || len(res.Skipped) != 2 {
		t.Fatalf("emitted=%+v skipped=%+v, want 1 emitted and 2 skipped", res.Emitted, res.Skipped)
	}
	if res.Emitted[0].ExecutionID != "ex-1" ||
		!res.Emitted[0].Instant.Equal(time.Date(2026, 5, 4, 12, 1, 0, 0, time.UTC)) {
		t.Errorf("emitted[0] = %+v, want the 12:01 instant and its execution id", res.Emitted[0])
	}
	if res.Skipped[0].Reason != control.ScheduleBackfillDuplicate || res.Skipped[0].Detail != "" {
		t.Errorf("skipped[0] = %+v, want a duplicate with no detail (nothing failed)", res.Skipped[0])
	}
	if res.Skipped[1].Reason != control.ScheduleBackfillRefused || res.Skipped[1].Detail == "" {
		t.Errorf("skipped[1] = %+v, want a refusal carrying what the engine said", res.Skipped[1])
	}
	if len(emitter.calls) != 3 {
		t.Errorf("emitter calls = %v, want every planned instant offered once, oldest first", emitter.calls)
	}
}

// TestScheduleAPIMutationsAreAuditedInTheWrapper pins where the ledger rows come
// from: the authz wrapper, with the operation, the node the caller targeted and
// the verified principal. The handler adds no rows of its own, and the actor
// the overlay records is the same principal.
func TestScheduleAPIMutationsAreAuditedInTheWrapper(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)
	url := f.nodePath("nightly", "")
	f.do(http.MethodPatch, scheduleTokenFull, url, `{"expression":"0 3 * * *"}`, nil).want(t, http.StatusOK)

	events := f.audit.Events()
	if len(events) != 2 {
		t.Fatalf("audit = %+v, want the admission and outcome rows of one mutation", events)
	}
	admission, outcome := events[0], events[1]
	if admission.Phase != "admission" || admission.Decision != DecisionAllow || admission.Outcome != "admitted" {
		t.Errorf("admission row = %+v, want an allow/admitted admission", admission)
	}
	if admission.Operation != OpScheduleWrite {
		t.Errorf("operation = %q, want %q", admission.Operation, OpScheduleWrite)
	}
	if admission.Resource != "workflow/"+scheduleWorkflowID+"/schedule/nightly" {
		t.Errorf("resource = %q, want the workflow-and-node address the caller acted on", admission.Resource)
	}
	if admission.Principal != "op-full" || admission.Namespace != scheduleNamespace {
		t.Errorf("principal = %q/%q, want the verified op-full in %s", admission.Principal, admission.Namespace, scheduleNamespace)
	}
	if outcome.Phase != "outcome" || outcome.Outcome != "reconciled" {
		t.Errorf("outcome row = %+v, want reconciled", outcome)
	}
	if admission.RequestID == "" || admission.RequestID != outcome.RequestID {
		t.Errorf("request ids %q/%q, want one shared id so the rows can be joined", admission.RequestID, outcome.RequestID)
	}

	// The overlay records the same actor the ledger names.
	view := f.do(http.MethodGet, scheduleTokenFull, url, "", nil).want(t, http.StatusOK).view(t)
	if view.UpdatedBy != "op-full" {
		t.Errorf("updated_by = %q, want the principal the wrapper authenticated", view.UpdatedBy)
	}

	// A denied mutation is recorded too — one row, naming the same resource, so
	// the attempt to reach another team's schedule is visible even though
	// nothing was written.
	before := len(f.audit.Events())
	f.do(http.MethodPatch, scheduleTokenRead, url, `{"expression":"0 5 * * *"}`, nil).
		wantFail(t, http.StatusForbidden, "forbidden")
	denied := f.audit.Events()[before:]
	if len(denied) != 1 {
		t.Fatalf("deny rows = %+v, want exactly one", denied)
	}
	if denied[0].Decision != DecisionDeny || denied[0].Outcome != "denied" || denied[0].Reason == "" {
		t.Errorf("deny row = %+v, want a deny with a reason code", denied[0])
	}
	if denied[0].Resource != "workflow/"+scheduleWorkflowID+"/schedule/nightly" {
		t.Errorf("deny resource = %q, want the targeted node", denied[0].Resource)
	}
	// A refusal must not explain which scope was missing: that maps out the
	// permission model.
	if strings.Contains(denied[0].Reason, "schedule.read") || strings.Contains(denied[0].Reason, "schedule.write") {
		t.Errorf("deny reason %q names a scope", denied[0].Reason)
	}
}

// TestScheduleAPIWithoutAnOverlayStoreIsNotConfigured pins the registration
// policy: the routes exist even where they cannot be served, and the answer is
// 501. A 404 would read as "this server is older than the route", which is a
// different and false statement.
func TestScheduleAPIWithoutAnOverlayStoreIsNotConfigured(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: scheduleTokenFull, Subject: "op-full", Namespace: scheduleNamespace,
				Scopes: []string{"workflow", "schedule.read", "schedule.write", "schedule.backfill"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	h := srv.Handler()
	base := "/v1/workflows/" + scheduleWorkflowID + "/schedules"

	cases := []struct{ method, path, body string }{
		{http.MethodGet, base, ""},
		{http.MethodGet, base + "/nightly", ""},
		{http.MethodPatch, base + "/nightly", `{"expression":"0 3 * * *"}`},
		{http.MethodDelete, base + "/nightly/override", ""},
		{http.MethodPost, base + "/nightly/pause", ""},
		{http.MethodPost, base + "/nightly/resume", ""},
		{http.MethodPost, base + "/nightly/backfill", `{"from":"2026-05-04T12:00:00Z","to":"2026-05-04T12:03:00Z"}`},
	}
	for _, c := range cases {
		res := scheduleRequest(h, c.method, scheduleTokenFull, c.path, c.body, nil)
		if res.Status == http.StatusNotFound {
			t.Errorf("%s %s = 404; the route must be registered even with no overlay store", c.method, c.path)
			continue
		}
		res.wantFail(t, http.StatusNotImplemented, "schedules_unavailable")
	}
}

// TestDeregisterWorkflowClearsScheduleOverrides pins the cascade: an overlay has
// no meaning without its workflow, so deleting the workflow must not leave rows
// a later re-registration of the same id would silently inherit.
func TestDeregisterWorkflowClearsScheduleOverrides(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)
	f.do(http.MethodPatch, scheduleTokenFull, f.nodePath("nightly", ""), `{"expression":"0 3 * * *"}`, nil).want(t, http.StatusOK)
	if got := f.overlays(t); len(got) != 1 {
		t.Fatalf("overlays before delete = %v, want one", got)
	}

	// A workflow with no overlays must delete just as cleanly: removing zero rows
	// is the state the caller asked for, not a failure.
	registerScheduleWorkflow(t, f.be, scheduleWorkflowDef("wf-plain"))
	f.do(http.MethodDelete, scheduleTokenFull, "/v1/workflows/wf-plain", "", nil).want(t, http.StatusOK)

	removed := f.do(http.MethodDelete, scheduleTokenFull, "/v1/workflows/"+scheduleWorkflowID, "", nil).want(t, http.StatusOK)
	var body struct {
		Removed bool `json:"removed"`
	}
	if err := json.Unmarshal(removed.Data, &body); err != nil || !body.Removed {
		t.Fatalf("delete workflow data = %s, want {removed:true}", removed.Data)
	}
	if got := f.overlays(t); len(got) != 0 {
		t.Errorf("overlays after deleting the workflow = %v, want none", got)
	}
	// And the schedule API now reports the workflow itself as gone.
	f.do(http.MethodGet, scheduleTokenFull, f.schedulesPath(), "", nil).
		wantFail(t, http.StatusNotFound, "workflow_not_found")
}

// schedulePatchBodyOfSize builds a syntactically valid PATCH body of exactly n
// bytes by padding the expression override's value. A valid body matters: a
// merely malformed one would also be refused, and the point is to show the size
// cap fired rather than the decoder tripping over garbage.
func schedulePatchBodyOfSize(t *testing.T, n int64) string {
	t.Helper()
	const overhead = int64(len(`{"expression":""}`))
	if n <= overhead {
		t.Fatalf("requested schedule patch size %d does not fit the JSON overhead", n)
	}
	body := `{"expression":"` + strings.Repeat("a", int(n-overhead)) + `"}`
	if int64(len(body)) != n {
		t.Fatalf("built schedule patch body is %d bytes, want exactly %d", len(body), n)
	}
	return body
}

// TestPatchScheduleBodyLimit pins the size cap on PATCH
// /v1/workflows/{id}/schedules/{node}. The patch decoder used to read the body
// with no bound, so a caller could make the server buffer arbitrary input
// before any validation ran. This asserts the observable contract: an ordinary
// patch still lands, an over-limit one answers decodeJSON's exact 413, and the
// counting body shows the read stopped at the cap instead of buffering the
// whole thing.
func TestPatchScheduleBodyLimit(t *testing.T) {
	f := newScheduleAPIFixture(t, nil)

	// Regression: an ordinary (well under the cap) patch is still accepted.
	f.do(http.MethodPatch, scheduleTokenWrite, f.nodePath("nightly", ""), `{"expression":"0 3 * * *"}`, nil).
		want(t, http.StatusOK)

	const total = MaxRequestBodyBytes + (1 << 20) // 1 MiB over the cap
	body := &countingReadCloser{r: strings.NewReader(schedulePatchBodyOfSize(t, total))}
	req := httptest.NewRequest(http.MethodPatch, f.nodePath("nightly", ""), nil)
	req.Header.Set("Authorization", "Bearer "+scheduleTokenWrite)
	req.Body = body
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)

	// http.MaxBytesReader may consume one probe byte past the limit to detect
	// overflow; buffering the body would show up as a count near total here.
	if body.n > MaxRequestBodyBytes+1 {
		t.Fatalf("PATCH read %d bytes, want at most %d", body.n, MaxRequestBodyBytes+1)
	}
	if body.n >= total {
		t.Fatalf("PATCH buffered the entire %d-byte body", total)
	}
	t.Logf("PATCH schedule read %d bytes of the %d-byte body", body.n, total)

	raw := rec.Body.Bytes()
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body=%s)", rec.Code, raw)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode response envelope: %v (body=%s)", err, raw)
	}
	if env.Success || env.Code != "request_too_large" || env.Message != "request body too large" {
		t.Fatalf("envelope = %+v, want 413 request_too_large with the fixed message", env)
	}
	assertNo413Internals(t, raw)
}
