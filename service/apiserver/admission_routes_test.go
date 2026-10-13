package apiserver

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/service/control"
)

// The route-level admission tests drive real servers built by New, because what
// is under test is the wiring: which routes the gates cover, on both auth paths,
// and that a refusal happens before the audit admission.
//
// The rate is set so low (one token per 1000s) that nothing refills during a
// test: "exhausted" is a fact established by one request, not a race against the
// clock.
const admissionTestRate = 0.001

// admissionRoute is one HTTP call against the control routes.
type admissionRoute struct {
	name, method, path string
}

// coveredAdmissionRoutes are every route the admission layer gates. The ids need
// not exist: a gated request is refused before any handler resolves them.
func coveredAdmissionRoutes() []admissionRoute {
	return []admissionRoute{
		{"execute", http.MethodPost, PathWorkflowExecute},
		{"execute-by-id", http.MethodPost, "/v1/workflows/wf-missing/execute"},
		{"signal", http.MethodPost, "/v1/executions/exec-missing/signals"},
		{"revoke", http.MethodDelete, "/v1/executions/exec-missing/signals/sig"},
		{"cancel", http.MethodPost, "/v1/executions/exec-missing/cancel"},
		{"pause", http.MethodPost, "/v1/executions/exec-missing/pause"},
		{"resume", http.MethodPost, "/v1/executions/exec-missing/resume"},
		{"rerun", http.MethodPost, "/v1/executions/exec-missing/rerun"},
	}
}

// uncoveredAdmissionRoutes are routes the design deliberately leaves ungated:
// entry seed (the trigger already backs off on its own 429), the long-poll wait
// (it would pin inflight slots), and reads.
func uncoveredAdmissionRoutes() []admissionRoute {
	return []admissionRoute{
		{"entry-seed", http.MethodPost, PathExecutions},
		{"wait", http.MethodGet, "/v1/executions/exec-missing/wait?timeout=1ms"},
		{"inspect", http.MethodGet, "/v1/executions/exec-missing"},
		{"list-executions", http.MethodGet, PathExecutions},
		{"list-workflows", http.MethodGet, PathWorkflows},
	}
}

func newAdmissionAuthzServer(t *testing.T, limits APILimits) (*APIServer, *InMemoryAuditSink) {
	t.Helper()
	audit := NewInMemoryAuditSink()
	scopes := []string{"workflow", "execution", "execution.control", "execution.rerun"}
	srv, err := New(Config{
		APILimits: limits,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: scopes},
			{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: scopes},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  audit,
	}, WithControlPlane(newMemoryControlPlane(t, 1)))
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
// leaves namespaceB's budget alone, and leaves the uncovered routes reachable.
func TestAPIAdmissionRateLimitsCoveredRoutesPerNamespace(t *testing.T) {
	srv, audit := newAdmissionAuthzServer(t, APILimits{Rate: admissionTestRate, Burst: 1})
	covered := coveredAdmissionRoutes()

	if rec := serveAdmissionRoute(t, srv, "tok-a", covered[4]); rec.Code == http.StatusTooManyRequests {
		t.Fatalf("first request refused inside the burst: %s", rec.Body.String())
	}
	before := len(audit.Events())
	for _, rt := range covered {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "tok-a", rt), http.StatusTooManyRequests, "rate_limited")
	}
	if got := len(audit.Events()); got != before {
		t.Fatalf("rate-limited requests wrote %d audit rows; a refusal must precede the audit admission", got-before)
	}
	for _, rt := range uncoveredAdmissionRoutes() {
		if rec := serveAdmissionRoute(t, srv, "tok-a", rt); rec.Code == http.StatusTooManyRequests || rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("uncovered %s answered %d (%s) with namespaceA exhausted", rt.name, rec.Code, rec.Body.String())
		}
	}
	if rec := serveAdmissionRoute(t, srv, "tok-b", covered[4]); rec.Code == http.StatusTooManyRequests {
		t.Fatal("namespaceB was charged for namespaceA's traffic")
	}
	// An unauthenticated request is refused by authentication, not charged.
	if rec := serveAdmissionRoute(t, srv, "", covered[4]); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", rec.Code)
	}
}

// TestAPIAdmissionInflightBoundCoversRoutes fills the replica's bound by holding
// its slots directly, so "full" is a fact of the test, then checks every covered
// route answers 503 server_overloaded while the uncovered ones still serve.
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

	before := len(audit.Events())
	for _, rt := range coveredAdmissionRoutes() {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "tok-a", rt), http.StatusServiceUnavailable, "server_overloaded")
	}
	if got := len(audit.Events()); got != before {
		t.Fatalf("overloaded requests wrote %d audit rows", got-before)
	}
	for _, rt := range uncoveredAdmissionRoutes() {
		if rec := serveAdmissionRoute(t, srv, "tok-a", rt); rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("uncovered %s answered 503 with the bound full", rt.name)
		}
	}

	for _, release := range releases {
		release()
	}
	if rec := serveAdmissionRoute(t, srv, "tok-a", coveredAdmissionRoutes()[4]); rec.Code == http.StatusServiceUnavailable {
		t.Fatalf("still 503 after the slots were released: %s", rec.Body.String())
	}
	if got := a.inflight.inUse(); got != 0 {
		t.Fatalf("in use after the requests returned = %d, want 0", got)
	}
}

// TestAPIAdmissionBareModeCoversRoutes is the same coverage on the legacy
// bearer-only path, whose op labels differ from the authz Op constants for the
// two execute routes.
func TestAPIAdmissionBareModeCoversRoutes(t *testing.T) {
	srv, err := New(Config{APILimits: APILimits{Rate: admissionTestRate, Burst: 1}}, WithControlPlane(newMemoryControlPlane(t, 1)))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	covered := coveredAdmissionRoutes()
	if rec := serveAdmissionRoute(t, srv, "", covered[4]); rec.Code == http.StatusTooManyRequests {
		t.Fatal("first bare request refused inside the burst")
	}
	for _, rt := range covered {
		assertRefusal(t, rt.name, serveAdmissionRoute(t, srv, "", rt), http.StatusTooManyRequests, "rate_limited")
	}
	for _, rt := range uncoveredAdmissionRoutes() {
		if rec := serveAdmissionRoute(t, srv, "", rt); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("uncovered bare %s answered 429", rt.name)
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
	for i := 0; i < 50; i++ {
		for _, rt := range coveredAdmissionRoutes() {
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
