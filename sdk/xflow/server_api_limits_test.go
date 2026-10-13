package xflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
)

// TestWithServerAPILimitsSetsTheConfig pins the translation into
// apiserver.Config: an option that never reaches the admission layer would
// leave every embedder on the default posture without any visible error.
func TestWithServerAPILimitsSetsTheConfig(t *testing.T) {
	want := apiserver.APILimits{Rate: 5, Burst: 10, MaxInflight: 64, Mode: apiserver.APIRateLimitModeLocal}
	var c serverConfig
	WithServerAPILimits(want)(&c)
	if got := buildServerAPIConfig(ServerConfig{}, &c).APILimits; got != want {
		t.Fatalf("APILimits = %+v, want %+v", got, want)
	}
	if got := buildServerAPIConfig(ServerConfig{}, &serverConfig{}).APILimits; got != (apiserver.APILimits{}) {
		t.Fatalf("APILimits without the option = %+v, want the zero default", got)
	}
}

// TestWithServerAPILimitsReachesTheRoutes drives a real server: past the burst
// a covered route answers 429 rate_limited.
func TestWithServerAPILimitsReachesTheRoutes(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth(),
		WithServerAPILimits(apiserver.APILimits{Rate: 0.001, Burst: 1}))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(ctx) }()

	cancel := func() int {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/executions/exec-missing/cancel", strings.NewReader(`{}`)))
		return rec.Code
	}
	if got := cancel(); got == http.StatusTooManyRequests {
		t.Fatal("first request refused inside the burst")
	}
	if got := cancel(); got != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", got)
	}
}

func TestWithServerAPILimitsRejectsInvalidConfig(t *testing.T) {
	if _, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth(),
		WithServerAPILimits(apiserver.APILimits{Rate: -1})); err == nil {
		t.Fatal("a negative rate was accepted")
	}
	if _, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth(),
		WithServerAPILimits(apiserver.APILimits{Rate: 1, Mode: apiserver.APIRateLimitModeRedis})); err == nil {
		t.Fatal("the shared mode was accepted on a server with no Redis")
	}
}
