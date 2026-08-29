package xflow

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// TestSDKServerParityWithAPIServer asserts that an SDK Server exposes the
// same HTTP route surface as a default apiserver.APIServer (stage 4 SDK
// convergence contract). For each representative path of the runner-protocol
// and workflow-control modules, both handlers must agree on the response
// status code and — critically — neither may return 404 (which would mean
// the route was never registered).
func TestSDKServerParityWithAPIServer(t *testing.T) {
	sdkSrv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdkSrv.Shutdown(t.Context()) }()

	apiSrv, err := apiserver.New(apiserver.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = apiSrv.Shutdown(t.Context()) }()

	// routeMustExist=true means a 404 would indicate the route itself was
	// never registered (top-level endpoints). For execution-scoped paths a
	// 404 is a legitimate "execution not found" response from the engine,
	// so only parity (same code on both servers) is asserted there.
	cases := []struct {
		name           string
		method         string
		path           string
		routeMustExist bool
	}{
		{"submit workflow", http.MethodPost, control.SubmitWorkflowPath, true},
		// invoke merged into POST /v1/workflows/execute (§9.1); the registered
		// execute route is exercised with a nonexistent id, which both handlers
		// answer with 404 (resource not found), so routeMustExist stays false.
		{"execute registered workflow", http.MethodPost, "/v1/workflows/nonexistent/execute", false},
		// inspect/wait/cancel return 404 for a nonexistent id because the engine
		// lookup itself reports not-found; a 404 here is not proof the route is
		// missing, so routeMustExist stays false. They assert parity only.
		{"inspect execution", http.MethodGet, "/v1/executions/nonexistent", false},
		{"wait execution", http.MethodGet, "/v1/executions/nonexistent/wait", false},
		{"cancel execution", http.MethodPost, "/v1/executions/nonexistent/cancel", false},
		// signal rejects a nil body before the engine lookup (signal via
		// decodeJSON's required-name check), so it returns 400, not 404 — a
		// missing registration would fall to the /v1/executions/{id}/ catch and
		// 404, which routeMustExist catches.
		{"deliver signal", http.MethodPost, "/v1/executions/nonexistent/signals", true},
		// revoke is now DELETE /v1/executions/{id}/signals/{name} (spec §9.1).
		// The name travels in the path, so a nonexistent id reaches the engine
		// lookup and returns 404 (execution not found) — a legitimate 404, so
		// routeMustExist stays false and only parity is asserted. Parity is
		// guaranteed because both servers share the same workflowControlModule.
		{"revoke signal", http.MethodDelete, "/v1/executions/nonexistent/signals/foo", false},
		{"runner register", http.MethodPost, "/v1/runners/register", true},
	}

	sdkHandler := sdkSrv.Handler()
	apiHandler := apiSrv.Handler()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sdkCode := doRequest(t, sdkHandler, tc.method, tc.path)
			apiCode := doRequest(t, apiHandler, tc.method, tc.path)

			if tc.routeMustExist {
				if sdkCode == http.StatusNotFound {
					t.Fatalf("SDK Handler returned 404 for %s %s — route not registered", tc.method, tc.path)
				}
				if apiCode == http.StatusNotFound {
					t.Fatalf("apiserver Handler returned 404 for %s %s — route not registered", tc.method, tc.path)
				}
			}
			if sdkCode != apiCode {
				t.Fatalf("status mismatch for %s %s: SDK=%d apiserver=%d", tc.method, tc.path, sdkCode, apiCode)
			}
		})
	}
}

func doRequest(t *testing.T, h http.Handler, method, path string) int {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}
