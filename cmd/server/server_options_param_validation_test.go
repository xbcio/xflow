package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

func TestParseServerConfigParamValidationFlag(t *testing.T) {
	for _, mode := range []string{"off", "warn", "enforce"} {
		cfg, err := parseServerConfig([]string{"-memory", "-param-validation", mode})
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if string(cfg.paramValidation) != mode {
			t.Fatalf("paramValidation = %q, want %q", cfg.paramValidation, mode)
		}
	}
	// Absent flag stays empty, which the SDK resolves to warn: an upgrade of
	// this binary must not start rejecting definitions it used to accept.
	cfg, err := parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.paramValidation != "" {
		t.Fatalf("paramValidation = %q, want empty when the flag is absent", cfg.paramValidation)
	}
}

func TestParseServerConfigRejectsUnknownParamValidation(t *testing.T) {
	if _, err := parseServerConfig([]string{"-memory", "-param-validation", "strict"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for --param-validation=strict")
	}
}

// paramValidationModeServed drives argv through buildServerOptions to a live
// server and returns the param_validation_mode GET /v1/node-types reports,
// which is the value the apiserver actually applies.
func paramValidationModeServed(t *testing.T, args ...string) types.ParamValidationMode {
	t.Helper()
	path := writeTokenFile(t, "tokens.json",
		`[{"token":"tok-a","subject":"op","namespace":"namespaceA","scopes":["workflow"]}]`, 0600)
	cfg, err := parseServerConfig(append([]string{"-mode", "dev", "-memory", "-auth-tokens-file", path}, args...))
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	mappings, err := loadAuthTokenMappings(cfg)
	if err != nil {
		t.Fatalf("loadAuthTokenMappings: %v", err)
	}
	auth := apiserver.NewBearerPrincipalAuthMulti(mappings)
	deps := serverDeps{
		workflowAuth:          auth,
		principalAuth:         auth,
		audit:                 apiserver.NewInMemoryAuditSink(),
		registrationCodeStore: control.NewMemoryRegistrationCodeStore(),
		issuedIdentityStore:   control.NewMemoryIssuedIdentityStore(),
	}
	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, buildServerOptions(cfg, deps)...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+apiserver.PathNodeTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer tok-a")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d", apiserver.PathNodeTypes, resp.StatusCode)
	}
	var body struct {
		Data struct {
			Mode types.ParamValidationMode `json:"param_validation_mode"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Data.Mode
}

// TestBuildServerOptionsReachesParamValidationEndToEnd proves the flag
// survives every hop to the apiserver; the default case is its negative twin
// (a hardcoded mode would fail one of the two).
func TestBuildServerOptionsReachesParamValidationEndToEnd(t *testing.T) {
	if got := paramValidationModeServed(t, "-param-validation", "enforce"); got != types.ParamValidationEnforce {
		t.Fatalf("served mode = %q, want enforce", got)
	}
	if got := paramValidationModeServed(t); got != types.ParamValidationWarn {
		t.Fatalf("served mode = %q, want warn when the flag is absent", got)
	}
}
