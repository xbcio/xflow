package apiserver

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store/memstore"
)

// The route-level admission tests drive real servers built by New, because what
// is under test is the wiring: which routes the gates cover, on both auth paths,
// and that a refusal happens before the audit admission.
//
// The covered set is not a hand-written list of "gated routes". It is the
// admissionRoutes table below, and TestAdmissionClassifiesEveryRegisteredMutatingRoute
// forces that table to classify EVERY POST/PUT/PATCH/DELETE-reachable route on
// the production mux, either as covered (with the concrete request the behavior
// tests then drive) or as exempt with a one-line reason. A new mutating route
// that is neither covered nor explicitly exempted fails that test.
//
// The rate is set so low (one token per 1000s) that nothing refills during a
// test: "exhausted" is a fact established by one request, not a race against the
// clock.
const admissionTestRate = 0.001

// admissionRoute is one HTTP call against a route.
type admissionRoute struct {
	name, method, path string
}

// admissionRouteRow classifies one route registration found on the production
// mux. method is the registration's method qualifier and pattern its registered
// pattern; method is "" for a registration with no qualifier, which the mux
// dispatches every method to (so it is mutating-reachable). class is the
// admission route class that covers the route, "" when the route is exempt —
// and then reason carries the exemption in one line.
//
// A covered row must also say how its class is proved: sample is the concrete
// request ("METHOD /path", or just "/path" to reuse the row's method) the
// behavior tests drive and require a refusal from. A row whose route cannot be
// driven by a static sample — the webhook needs a server-registered workflow id
// — sets provedBy to the test that proves the class instead.
type admissionRouteRow struct {
	name     string
	method   string
	pattern  string
	class    string
	reason   string
	sample   string
	provedBy string
	// bare marks a covered row that is also reachable, and gated, on the legacy
	// no-PrincipalAuth path. Dead-letter replay is not: without an authenticator
	// the prefix handler answers 404 before the gates run.
	bare bool
}

// admissionRoutes classifies every mutating route the production mux serves.
var admissionRoutes = append([]admissionRouteRow{
	// ---- covered: the execute class ----
	{
		name: "execute", method: http.MethodPost, pattern: PathWorkflowExecute,
		class: routeClassExecute, sample: "POST /v1/workflows/execute", bare: true,
	},
	{
		name: "execute-by-id", method: http.MethodPost, pattern: PathWorkflowExecuteByID,
		class: routeClassExecute, sample: "POST /v1/workflows/wf-missing/execute", bare: true,
	},

	// ---- covered: the execution class ----
	{
		name: "signal", method: http.MethodPost, pattern: PathExecutionSignals,
		class: routeClassExecution, sample: "POST /v1/executions/exec-missing/signals", bare: true,
	},
	{
		name: "revoke", method: http.MethodDelete, pattern: PathExecutionSignalByID,
		class: routeClassExecution, sample: "DELETE /v1/executions/exec-missing/signals/sig", bare: true,
	},
	{
		name: "cancel", method: http.MethodPost, pattern: PathExecutionCancel,
		class: routeClassExecution, sample: "POST /v1/executions/exec-missing/cancel", bare: true,
	},
	{
		name: "pause", method: http.MethodPost, pattern: PathExecutionPause,
		class: routeClassExecution, sample: "POST /v1/executions/exec-missing/pause", bare: true,
	},
	{
		name: "resume", method: http.MethodPost, pattern: PathExecutionResume,
		class: routeClassExecution, sample: "POST /v1/executions/exec-missing/resume", bare: true,
	},
	{
		name: "rerun", method: http.MethodPost, pattern: PathExecutionRerun,
		class: routeClassExecution, sample: "POST /v1/executions/exec-missing/rerun", bare: true,
	},
	// Backfill starts executions from a schedule's missed occurrences, so it
	// creates the same kind of work cancel/pause/rerun do.
	{
		name: "schedule-backfill", method: http.MethodPost, pattern: PathWorkflowScheduleBackfill,
		class: routeClassExecution, sample: "POST /v1/workflows/wf-missing/schedules/nightly/backfill", bare: true,
	},
	// The dead-letters prefix registration dispatches two shapes: GET
	// {execID} (a read, not covered) and POST {execID}/replay (covered — a
	// replay can start a new attempt). The sample names the covered shape.
	{
		name: "dead-letter-replay", method: "", pattern: "/v1/management/dead-letters/",
		class: routeClassExecution, sample: "POST /v1/management/dead-letters/exec-missing/replay",
	},

	// ---- covered: the webhook class ----
	// The rate is charged per namespace+workflow and only to signed requests.
	// The route cannot be sampled statically: the path needs a server-assigned
	// workflow id, so the class is proved by its own test.
	{
		name: "webhook", method: "", pattern: PathWebhookEntry,
		class: routeClassWebhook, provedBy: "TestAPIAdmissionWebhookEntry",
	},

	// ---- exempt: registration ----
	{
		name: "register", method: http.MethodPost, pattern: PathWorkflows,
		reason: "definition write: registering a workflow starts no execution",
	},
	{
		name: "replace", method: http.MethodPut, pattern: PathWorkflowByID,
		reason: "definition write: replacing a version starts no execution",
	},
	{
		name: "deregister", method: http.MethodDelete, pattern: PathWorkflowByID,
		reason: "definition write: deregistering starts no execution",
	},
	{
		name: "rollback", method: http.MethodPost, pattern: PathWorkflowRollback,
		reason: "definition write: installing an archived version starts no execution",
	},

	// ---- exempt: runner admission and fallbacks ----
	{
		name: "entry-seed", method: http.MethodPost, pattern: PathExecutions,
		reason: "runner admission: the trigger already backs off on the 429 it gets",
	},
	{
		name: "execution-verb-catchall", method: "", pattern: "/v1/executions/{id}/",
		reason: "unknown-verb fallback: answers 404 without touching a handler",
	},

	// ---- exempt: schedule overlays (they retime, they do not create work) ----
	{
		name: "schedule-write", method: http.MethodPatch, pattern: PathWorkflowScheduleByNode,
		reason: "schedule overlay retiming: no execution is created",
	},
	{
		name: "schedule-override-drop", method: http.MethodDelete, pattern: PathWorkflowScheduleOverride,
		reason: "schedule overlay drop: no execution is created",
	},
	{
		name: "schedule-pause", method: http.MethodPost, pattern: PathWorkflowSchedulePause,
		reason: "schedule pause: it suppresses work rather than creating it",
	},
	{
		name: "schedule-resume", method: http.MethodPost, pattern: PathWorkflowScheduleResume,
		reason: "schedule resume: it re-enables the declared cadence, creating no work by itself",
	},

	// ---- exempt: content stores ----
	{
		name: "artifact-upload", method: "", pattern: "/v1/artifacts/",
		reason: "artifact blob write: a content store, not the control plane",
	},
	{
		name: "supply-write", method: "", pattern: "/v1/supplies/",
		reason: "supply content write: a content store, not the control plane",
	},

	// ---- exempt: platform management (operator-scope writes, no tenant work) ----
	{
		name: "runner-pool-create", method: http.MethodPost, pattern: PathManagementRunnerPools,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-pool-update", method: http.MethodPatch, pattern: PathManagementRunnerPoolByID,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-pool-delete", method: http.MethodDelete, pattern: PathManagementRunnerPoolByID,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-pool-token-create", method: http.MethodPost, pattern: PathManagementRunnerPoolTokens,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-pool-token-delete", method: http.MethodDelete, pattern: PathManagementRunnerPoolTokenByID,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-revoke-identity", method: http.MethodPost, pattern: PathManagementRunnerRevokeIdentity,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-drain", method: http.MethodPost, pattern: PathManagementRunnerDrain,
		reason: "platform management: an operator-scope write, not tenant work",
	},
	{
		name: "runner-resume", method: http.MethodPost, pattern: PathManagementRunnerResume,
		reason: "platform management: an operator-scope write, not tenant work",
	},
}, runnerFaceAdmissionRows()...)

// runnerFaceAdmissionRows exempts the runner protocol face in one loop rather
// than ten copies: those routes are machine traffic authenticated by runner
// identity, not a caller quota surface. They are exempt as a family, but each
// route still has to be enumerated, so a runner path that stops being
// registered shows up as a stale row.
func runnerFaceAdmissionRows() []admissionRouteRow {
	rows := make([]admissionRouteRow, 0, len(protocol.RunnerFacingPaths))
	for _, p := range protocol.RunnerFacingPaths {
		rows = append(rows, admissionRouteRow{
			name: "runner-protocol " + p, method: "", pattern: p,
			reason: "runner protocol face: machine traffic behind runner identity, not caller quota",
		})
	}
	return rows
}

// exemptProbes are behavioral spot checks that an exempt route really is not
// gated: driven with the caller's budget exhausted, none of them may answer 429
// or 503. This list is deliberately partial — the classification table above is
// what proves completeness; these cases keep the exemption honest for the
// routes a caller actually exercises.
func exemptProbes() []admissionRoute {
	return []admissionRoute{
		{"entry-seed", http.MethodPost, PathExecutions},
		{"wait", http.MethodGet, "/v1/executions/exec-missing/wait?timeout=1ms"},
		{"inspect", http.MethodGet, "/v1/executions/exec-missing"},
		{"list-executions", http.MethodGet, PathExecutions},
		{"list-workflows", http.MethodGet, PathWorkflows},
		{"dead-letter-list", http.MethodGet, "/v1/management/dead-letters/exec-missing"},
		{"schedule-write", http.MethodPatch, "/v1/workflows/wf-missing/schedules/nightly"},
	}
}

// coveredSamples returns the concrete requests the behavior tests drive: one per
// covered row, parsed from the row's sample. Rows without a sample (the webhook,
// which is proved by its own test) are skipped.
func coveredSamples(t *testing.T, bare bool) []admissionRoute {
	t.Helper()
	var out []admissionRoute
	for _, row := range admissionRoutes {
		if row.class == "" || row.sample == "" {
			continue
		}
		if bare && !row.bare {
			continue
		}
		out = append(out, sampleRequest(t, row))
	}
	if len(out) == 0 {
		t.Fatal("no covered samples: the classification table lost its covered rows")
	}
	return out
}

func sampleRequest(t *testing.T, row admissionRouteRow) admissionRoute {
	t.Helper()
	fields := strings.Fields(row.sample)
	switch len(fields) {
	case 1:
		return admissionRoute{name: row.name, method: row.method, path: fields[0]}
	case 2:
		return admissionRoute{name: row.name, method: fields[0], path: fields[1]}
	default:
		t.Fatalf("row %s: sample %q is not \"METHOD /path\" or \"/path\"", row.name, row.sample)
		return admissionRoute{}
	}
}

// mutatingMethods are the methods this file's coverage claim is about: the ones
// that create or change work.
var mutatingMethods = []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}

// TestAdmissionClassifiesEveryRegisteredMutatingRoute is the route-coverage
// guard: every route a mutating method reaches on the production mux must be
// either covered by an admission class or explicitly exempted with a reason.
// The route set is discovered by probing the real mux, not hand-written, so a
// new mutating route fails this test until its author classifies it.
//
// ServeMux exposes no pattern enumeration, so discovery works in two steps:
// candidate paths are harvested from the sources (every "/v1/..." string
// literal in this package's non-test files, plus the two exported path lists,
// each with its {placeholders} replaced by a real segment), and each candidate
// is probed with mux.Handler, whose second return is the matched pattern ("" for
// no route). The probe never invokes a handler.
func TestAdmissionClassifiesEveryRegisteredMutatingRoute(t *testing.T) {
	mux := newFullGuardMux(t)
	discovered := registeredMutatingRoutes(t, mux)

	classified := make(map[string]admissionRouteRow, len(admissionRoutes))
	for _, row := range admissionRoutes {
		key := routeKey(row.method, row.pattern)
		if prev, dup := classified[key]; dup {
			t.Fatalf("two classification rows for %q: %s and %s", key, prev.name, row.name)
		}
		if row.class == "" && row.reason == "" {
			t.Fatalf("row %s (%s): exempt rows must carry a one-line reason", row.name, key)
		}
		if row.class != "" && row.sample == "" && row.provedBy == "" {
			t.Fatalf("row %s (%s): covered rows must sample the concrete request the behavior tests drive, or name the test that proves the class", row.name, key)
		}
		classified[key] = row
	}

	var unclassified []string
	for _, rt := range discovered {
		key := routeKey(rt.method, rt.path)
		if _, ok := classified[key]; !ok {
			unclassified = append(unclassified, key)
		}
	}
	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("registered mutating routes with no admission classification (add a covered row or an exempt row with a reason):\n\t%s", strings.Join(unclassified, "\n\t"))
	}

	// The reverse direction: a stale row (a route that no longer exists) would
	// otherwise keep claiming coverage for a registration that was removed.
	registered := make(map[string]bool, len(discovered))
	for _, rt := range discovered {
		registered[routeKey(rt.method, rt.path)] = true
	}
	var stale []string
	for key := range classified {
		if !registered[key] {
			stale = append(stale, key)
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Fatalf("classification rows matching no registered route (remove them):\n\t%s", strings.Join(stale, "\n\t"))
	}
}

func routeKey(method, pattern string) string {
	if method == "" {
		return pattern + " (any method)"
	}
	return method + " " + pattern
}

// registeredMutatingRoutes probes mux with every candidate path and mutating
// method, and returns the distinct (method, pattern) pairs that resolve. A
// pattern with no method qualifier is returned once, since the registration has
// one method-agnostic entry rather than one per method.
func registeredMutatingRoutes(t *testing.T, mux *http.ServeMux) []admissionRoute {
	t.Helper()
	candidates := map[string]bool{}
	for _, p := range UserFacingPaths {
		candidates[p] = true
	}
	// The runner protocol face registers from service/protocol's constants,
	// which are not literals in this package; the exported list is the same
	// enumeration the runner-face dead-constant guard uses.
	for _, p := range protocol.RunnerFacingPaths {
		candidates[p] = true
	}
	for _, p := range pathCandidateLiterals(t) {
		candidates[p] = true
	}

	seen := map[string]bool{}
	var out []admissionRoute
	for candidate := range candidates {
		probePath := pathPlaceholder.ReplaceAllLiteralString(candidate, "x")
		for _, probeMethod := range mutatingMethods {
			req := httptest.NewRequest(probeMethod, probePath, nil)
			_, raw := mux.Handler(req)
			if raw == "" {
				continue
			}
			method, pattern := splitMuxPattern(raw)
			if method != "" && method != probeMethod {
				continue
			}
			key := routeKey(method, pattern)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, admissionRoute{name: key, method: method, path: pattern})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// pathPlaceholder matches one ServeMux {wildcard} segment.
var pathPlaceholder = regexp.MustCompile(`\{[^}]*\}`)

// splitMuxPattern separates a Handler pattern's method qualifier, when it has
// one, from the path.
func splitMuxPattern(raw string) (string, string) {
	for _, m := range append([]string{http.MethodGet}, mutatingMethods...) {
		if p, ok := strings.CutPrefix(raw, m+" "); ok {
			return m, p
		}
	}
	return "", raw
}

// pathCandidateLiterals reads every string literal containing "/v1/" from this
// package's non-test sources, keeping the path part when the literal carries a
// method prefix ("POST /v1/x" contributes "/v1/x"). Path constants, prefix
// registrations and route fragments all appear as literals where they are
// declared or used, so the candidate set is a superset of the registered paths
// as long as a registered /v1 path names itself in this package. A path that
// appears ONLY as a fragment of a runtime-built string would be missed — that is
// why the discovery also intersects with the two exported path lists, and why
// the guard reports its failure by listing the discovered routes.
func pathCandidateLiterals(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir .: %v", err)
	}
	seen := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			v, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			if i := strings.Index(v, "/v1/"); i >= 0 {
				seen[v[i:]] = true
			}
			return true
		})
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func newAdmissionAuthzServer(t *testing.T, limits APILimits) (*APIServer, *InMemoryAuditSink) {
	t.Helper()
	audit := NewInMemoryAuditSink()
	scopes := []string{
		"workflow", "execution", "execution.control", "execution.rerun",
		OpScheduleBackfill, OpDeadLetterReplay,
	}
	srv, err := New(Config{
		APILimits: limits,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: scopes},
			{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: scopes},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  audit,
		Supplies:   memstore.New(),
	}, WithManagement(), WithControlPlane(newMemoryControlPlane(t, 1)))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv, audit
}

func serveAdmissionRoute(t *testing.T, srv *APIServer, token string, rt admissionRoute) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(rt.method, rt.path, bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// assertRefusal checks one refusal's status, code and Retry-After.
func assertRefusal(t *testing.T, name string, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("%s: status = %d (%s), want %d", name, rec.Code, rec.Body.String(), status)
	}
	retry, err := strconv.Atoi(rec.Header().Get("Retry-After"))
	if err != nil || retry < 1 {
		t.Fatalf("%s: Retry-After = %q, want an integer >= 1", name, rec.Header().Get("Retry-After"))
	}
	env := decodeEnvelope(t, rec.Result(), nil)
	if env.Success || env.Code != code {
		t.Fatalf("%s: envelope = %+v, want %s failure", name, env, code)
	}
}

// TestAPIAdmissionRateLimitsCoveredRoutesPerNamespace is the authz-path rate
// case: one admitted request spends namespaceA's burst, after which every
// covered route answers 429 rate_limited for namespaceA, writes no audit row,
// leaves namespaceB's budget alone, and leaves the exempt routes reachable.
func TestAPIAdmissionRateLimitsCoveredRoutesPerNamespace(t *testing.T) {
	srv, audit := newAdmissionAuthzServer(t, APILimits{Rate: admissionTestRate, Burst: 1})
	covered := coveredSamples(t, false)

	if rec := serveAdmissionRoute(t, srv, "tok-a", sampleNamed(t, covered, "cancel")); rec.Code == http.StatusTooManyRequests {
		t.Fatalf("first request refused inside the burst: %s", rec.Body.String())
	}
	before := len(audit.Events())
	for _, rt := range covered {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "tok-a", rt), http.StatusTooManyRequests, "rate_limited")
	}
	if got := len(audit.Events()); got != before {
		t.Fatalf("rate-limited requests wrote %d audit rows; a refusal must precede the audit admission", got-before)
	}
	for _, rt := range exemptProbes() {
		if rec := serveAdmissionRoute(t, srv, "tok-a", rt); rec.Code == http.StatusTooManyRequests || rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("exempt %s answered %d (%s) with namespaceA exhausted", rt.name, rec.Code, rec.Body.String())
		}
	}
	if rec := serveAdmissionRoute(t, srv, "tok-b", sampleNamed(t, covered, "cancel")); rec.Code == http.StatusTooManyRequests {
		t.Fatal("namespaceB was charged for namespaceA's traffic")
	}
	// An unauthenticated request is refused by authentication, not charged.
	if rec := serveAdmissionRoute(t, srv, "", sampleNamed(t, covered, "cancel")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", rec.Code)
	}
}

func sampleNamed(t *testing.T, routes []admissionRoute, name string) admissionRoute {
	t.Helper()
	for _, rt := range routes {
		if rt.name == name {
			return rt
		}
	}
	t.Fatalf("no covered sample named %q; the classification table lost it", name)
	return admissionRoute{}
}

// TestAPIAdmissionInflightBoundCoversRoutes fills the replica's bound by holding
// its slots directly, so "full" is a fact of the test, then checks every covered
// route answers 503 server_overloaded while the exempt ones still serve.
func TestAPIAdmissionInflightBoundCoversRoutes(t *testing.T) {
	srv, audit := newAdmissionAuthzServer(t, APILimits{MaxInflight: 2})
	a := srv.ctrl.admission
	var releases []func()
	for i := 0; i < 2; i++ {
		release, ok := a.acquire(routeClassExecute)
		if !ok {
			t.Fatalf("slot %d refused below the bound", i)
		}
		releases = append(releases, release)
	}

	covered := coveredSamples(t, false)
	before := len(audit.Events())
	for _, rt := range covered {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "tok-a", rt), http.StatusServiceUnavailable, "server_overloaded")
	}
	if got := len(audit.Events()); got != before {
		t.Fatalf("overloaded requests wrote %d audit rows", got-before)
	}
	for _, rt := range exemptProbes() {
		if rec := serveAdmissionRoute(t, srv, "tok-a", rt); rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("exempt %s answered 503 with the bound full", rt.name)
		}
	}

	for _, release := range releases {
		release()
	}
	if rec := serveAdmissionRoute(t, srv, "tok-a", sampleNamed(t, covered, "cancel")); rec.Code == http.StatusServiceUnavailable {
		t.Fatalf("still 503 after the slots were released: %s", rec.Body.String())
	}
	if got := a.inflight.inUse(); got != 0 {
		t.Fatalf("in use after the requests returned = %d, want 0", got)
	}
}

// TestAPIAdmissionBareModeCoversRoutes is the same coverage on the legacy
// bearer-only path, whose op labels differ from the authz Op constants for the
// two execute routes. Rows marked bare-only-reachable (dead-letter replay, which
// 404s without an authenticator) are excluded by the table's bare field.
func TestAPIAdmissionBareModeCoversRoutes(t *testing.T) {
	srv, err := New(Config{APILimits: APILimits{Rate: admissionTestRate, Burst: 1}}, WithControlPlane(newMemoryControlPlane(t, 1)))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	covered := coveredSamples(t, true)
	if rec := serveAdmissionRoute(t, srv, "", sampleNamed(t, covered, "cancel")); rec.Code == http.StatusTooManyRequests {
		t.Fatal("first bare request refused inside the burst")
	}
	for _, rt := range covered {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "", rt), http.StatusTooManyRequests, "rate_limited")
	}
	for _, rt := range exemptProbes() {
		if rec := serveAdmissionRoute(t, srv, "", rt); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("exempt bare %s answered 429", rt.name)
		}
	}

	srv, err = New(Config{APILimits: APILimits{MaxInflight: 1}}, WithControlPlane(newMemoryControlPlane(t, 1)))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	release, _ := srv.ctrl.admission.acquire(routeClassExecution)
	defer release()
	for _, rt := range covered {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "", rt), http.StatusServiceUnavailable, "server_overloaded")
	}
}

// TestAPIAdmissionDefaultConfigIsUnchanged is the regression case: with the
// zero APILimits no request is ever rate limited, and the only active gate is
// the default inflight bound, which a sequential caller never reaches.
func TestAPIAdmissionDefaultConfigIsUnchanged(t *testing.T) {
	srv, _ := newAdmissionAuthzServer(t, APILimits{})
	a := srv.ctrl.admission
	if a.limiter != nil {
		t.Fatal("a rate limiter was installed with no rate configured")
	}
	if a.inflight == nil || a.inflight.max != DefaultAPIMaxInflight {
		t.Fatalf("default inflight bound not installed at %d", DefaultAPIMaxInflight)
	}
	covered := coveredSamples(t, false)
	for i := 0; i < 50; i++ {
		for _, rt := range covered {
			if rec := serveAdmissionRoute(t, srv, "tok-a", rt); rec.Code == http.StatusTooManyRequests || rec.Code == http.StatusServiceUnavailable {
				t.Fatalf("default config: %s answered %d (%s)", rt.name, rec.Code, rec.Body.String())
			}
		}
	}
}

func TestAPIAdmissionRedisModeRequiresRedis(t *testing.T) {
	limits := APILimits{Rate: 1, Mode: APIRateLimitModeRedis}
	if _, err := New(Config{APILimits: limits}); !errors.Is(err, errAPIRateLimitNoRedis) {
		t.Fatalf("built control plane without Redis: err = %v, want errAPIRateLimitNoRedis", err)
	}
	if _, err := New(Config{APILimits: limits}, WithControlPlane(newMemoryControlPlane(t, 1))); !errors.Is(err, errAPIRateLimitNoRedis) {
		t.Fatalf("injected in-memory control plane: err = %v, want errAPIRateLimitNoRedis", err)
	}
	if _, err := New(Config{APILimits: APILimits{Rate: -1}}); err == nil {
		t.Fatal("a negative rate was accepted")
	}

	mr := miniredis.RunT(t)
	backend, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{APILimits: limits}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("Redis-backed control plane: %v", err)
	}
	if _, ok := srv.ctrl.admission.limiter.(*sharedRateLimiter); !ok {
		t.Fatalf("limiter = %T, want the shared Redis limiter", srv.ctrl.admission.limiter)
	}
}
