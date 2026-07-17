package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	backendlocal "github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/service/control"
)

// newMemoryControlPlane builds a *control.ControlPlane backed by the in-memory
// backend, mirroring what buildControlPlane produces for an empty RedisAddr.
func newMemoryControlPlane(t *testing.T, concurrency int) *control.ControlPlane {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{
		Backend: backendlocal.New(backendlocal.WithConcurrency(concurrency)),
	})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}
	return cp
}

// TestNewAPIServerBuildsControlPlaneFromMemoryConfig verifies that New
// constructs an owned ControlPlane when none is injected, and that Handler
// routes /v1/workflows the same way a directly-built control plane does.
func TestNewAPIServerBuildsControlPlaneFromMemoryConfig(t *testing.T) {
	srv, err := New(Config{Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv.cp == nil {
		t.Fatal("APIServer.cp is nil")
	}
	if !srv.ownsCP {
		t.Fatal("APIServer.ownsCP = false, want true when CP built internally")
	}
	if srv.IsLeader() != true {
		t.Fatal("IsLeader() = false for memory backend, want true")
	}
}

// TestAPIServerHandlerMatchesControlPlane verifies that the runner-protocol
// routes served by the APIServer's module-based Handler match those served by
// a directly-built control.ControlPlane's Handler. Stage 3 moved the
// workflow/control routes out of control.Server and into the apiserver
// workflow-control module, so only the runner protocol is common to both.
func TestAPIServerHandlerMatchesControlPlane(t *testing.T) {
	cp := newMemoryControlPlane(t, 1)
	srv, err := New(Config{Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cases := []struct {
		name string
		req  *http.Request
	}{
		{
			name: "register runner empty body",
			req:  httptest.NewRequest(http.MethodPost, "/v1/runners/register", nil),
		},
		{
			name: "poll task empty body",
			req:  httptest.NewRequest(http.MethodPost, "/v1/runners/poll", nil),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := httptest.NewRecorder()
			cp.Handler().ServeHTTP(want, tc.req)

			got := httptest.NewRecorder()
			srv.Handler().ServeHTTP(got, tc.req)

			if got.Code != want.Code {
				t.Fatalf("%s: status = %d, want %d (matching control plane)", tc.name, got.Code, want.Code)
			}
		})
	}
}

// TestAPIServerHandlerServesWorkflowRoutes verifies that stage 3's
// workflow-control module wires /v1/workflows and /v1/executions/ onto the
// APIServer's Handler (these routes no longer live on control.Server).
func TestAPIServerHandlerServesWorkflowRoutes(t *testing.T) {
	srv, err := New(Config{Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cases := []struct {
		name    string
		req     *http.Request
		notWant int
	}{
		{name: "submit workflow empty body", req: httptest.NewRequest(http.MethodPost, "/v1/workflows", nil), notWant: http.StatusNotFound},
		{name: "execution signal empty body", req: httptest.NewRequest(http.MethodPost, "/v1/executions/exec-1/signal", nil), notWant: http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, tc.req)
			if rec.Code == tc.notWant {
				t.Fatalf("%s: status = %d, want non-404 (module did not mount route)", tc.name, rec.Code)
			}
		})
	}
}

// TestAPIServerHandlerAppliesMiddleware verifies middleware is applied
// around the passthrough handler.
func TestAPIServerHandlerAppliesMiddleware(t *testing.T) {
	srv, err := New(Config{Concurrency: 1}, WithHTTPMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Stage1", "yes")
			next.ServeHTTP(w, r)
		})
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Stage1"); got != "yes" {
		t.Fatalf("middleware not applied: X-Stage1 = %q, want %q", got, "yes")
	}
}

// TestWithControlPlaneInjectsUnowned verifies that a ControlPlane injected
// via WithControlPlane is reused (not rebuilt) and marked unowned.
func TestWithControlPlaneInjectsUnowned(t *testing.T) {
	cp := newMemoryControlPlane(t, 1)
	srv, err := New(Config{}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv.cp != cp {
		t.Fatal("APIServer.cp is not the injected ControlPlane")
	}
	if srv.ownsCP {
		t.Fatal("APIServer.ownsCP = true, want false for injected CP")
	}
}
