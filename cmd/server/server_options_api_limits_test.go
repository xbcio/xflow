package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

func TestParseServerConfigAPILimitFlags(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-memory", "-api-rate", "2.5", "-api-burst", "7", "-api-max-inflight", "64", "-api-rate-limit-mode", "local"})
	if err != nil {
		t.Fatal(err)
	}
	want := apiserver.APILimits{Rate: 2.5, Burst: 7, MaxInflight: 64, Mode: apiserver.APIRateLimitModeLocal}
	if cfg.apiLimits != want {
		t.Fatalf("apiLimits = %+v, want %+v", cfg.apiLimits, want)
	}
	// Absent flags are the zero value: no rate limit, the default inflight bound.
	cfg, err = parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.apiLimits != (apiserver.APILimits{}) {
		t.Fatalf("apiLimits = %+v, want the zero default when no flag is passed", cfg.apiLimits)
	}
}

// TestParseServerConfigAPILimitEnvOverridesFlags pins the precedence: a set
// XFLOW_API_* variable wins over the flag, and an empty one leaves it alone.
func TestParseServerConfigAPILimitEnvOverridesFlags(t *testing.T) {
	t.Setenv(envAPIRate, "9")
	t.Setenv(envAPIBurst, "")
	t.Setenv(envAPIMaxInflight, "-1")
	t.Setenv(envAPIRateLimitMode, "redis")
	cfg, err := parseServerConfig([]string{"-redis", "127.0.0.1:1", "-api-rate", "1", "-api-burst", "3", "-api-max-inflight", "10"})
	if err != nil {
		t.Fatal(err)
	}
	want := apiserver.APILimits{Rate: 9, Burst: 3, MaxInflight: -1, Mode: apiserver.APIRateLimitModeRedis}
	if cfg.apiLimits != want {
		t.Fatalf("apiLimits = %+v, want %+v", cfg.apiLimits, want)
	}
}

func TestParseServerConfigRejectsInvalidAPILimits(t *testing.T) {
	for _, args := range [][]string{
		{"-memory", "-api-rate", "-1"},
		{"-memory", "-api-rate", "NaN"},
		{"-memory", "-api-burst", "-2"},
		{"-memory", "-api-rate-limit-mode", "cluster"},
		{"-memory", "-api-rate", "5", "-api-rate-limit-mode", "redis"},
	} {
		if _, err := parseServerConfig(args); err == nil {
			t.Errorf("parseServerConfig(%v) accepted an invalid limit", args)
		}
	}
	for env, val := range map[string]string{envAPIRate: "fast", envAPIBurst: "1.5", envAPIMaxInflight: "many"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, val)
			if _, err := parseServerConfig([]string{"-memory"}); err == nil {
				t.Fatalf("%s=%q accepted", env, val)
			}
		})
	}
}

// TestBuildServerOptionsReachesAPILimits drives the flags through
// buildServerOptions to a live server: past the burst a covered route answers
// 429 rate_limited.
func TestBuildServerOptionsReachesAPILimits(t *testing.T) {
	path := writeTokenFile(t, "tokens.json",
		`[{"token":"tok-a","subject":"op","namespace":"namespaceA","scopes":["workflow","execution","execution.control"]}]`, 0600)
	cfg, err := parseServerConfig([]string{"-mode", "dev", "-memory", "-auth-tokens-file", path, "-api-rate", "0.001", "-api-burst", "1"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	mappings, err := loadAuthTokenMappings(cfg)
	if err != nil {
		t.Fatalf("loadAuthTokenMappings: %v", err)
	}
	auth := apiserver.NewBearerPrincipalAuthMulti(mappings)
	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, buildServerOptions(cfg, serverDeps{
		workflowAuth:          auth,
		principalAuth:         auth,
		audit:                 apiserver.NewInMemoryAuditSink(),
		registrationCodeStore: control.NewMemoryRegistrationCodeStore(),
		issuedIdentityStore:   control.NewMemoryIssuedIdentityStore(),
	})...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	cancel := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-missing/cancel", strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer tok-a")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := cancel(); rec.Code == http.StatusTooManyRequests {
		t.Fatal("first request refused inside the burst")
	}
	rec := cancel()
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second request = %d Retry-After %q, want 429 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
}
