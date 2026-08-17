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
	sdkSrv, err := NewServer(ServerConfig{})
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
		{"invoke workflow", http.MethodPost, "/v1/workflows/invoke", true},
		{"inspect execution", http.MethodGet, "/v1/executions/nonexistent", false},
		{"wait execution", http.MethodGet, "/v1/executions/nonexistent/wait", false},
		{"deliver signal", http.MethodPost, "/v1/executions/nonexistent/signals", false},
		{"revoke signal", http.MethodPost, "/v1/executions/nonexistent/revoke-signal", false},
		{"cancel execution", http.MethodPost, "/v1/executions/nonexistent/cancel", false},
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
