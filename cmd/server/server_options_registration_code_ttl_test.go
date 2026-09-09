package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

func TestParseServerConfigRegistrationCodeTTLFlag(t *testing.T) {
	// Positive: flag set to 24h -> cfg field must be 24h.
	cfg, err := parseServerConfig([]string{"-memory", "-registration-code-ttl", "24h"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.registrationCodeTTL != 24*time.Hour {
		t.Fatalf("registrationCodeTTL = %v, want 24h", cfg.registrationCodeTTL)
	}

	// Negative: flag absent -> cfg field must be 0 (no ceiling, the default).
	// This is the property an upgrade depends on: a new binary with no new
	// flag must not start putting deadlines on codes an operator mints the
	// day they deploy it.
	cfg2, err := parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.registrationCodeTTL != 0 {
		t.Fatalf("registrationCodeTTL = %v, want 0 when the flag is absent", cfg2.registrationCodeTTL)
	}
}

func TestParseServerConfigRejectsNegativeRegistrationCodeTTL(t *testing.T) {
	// A negative ceiling would mint codes that are already expired — every
	// enroll rejected, with the enroll face deliberately unable to say why.
	// Reject it at parse time, where the operator is still looking at their
	// own command line.
	if _, err := parseServerConfig([]string{"-memory", "-registration-code-ttl", "-1h"}); err == nil {
		t.Fatal("parseServerConfig() error = nil, want error for negative --registration-code-ttl")
	}
}

// newRegistrationCodeTTLBinary drives the real flag-to-server chain and
// returns a live test server plus the bearer token that may mint codes.
//
// It exists because the middle hop — buildServerOptions attaching
// cfg.registrationCodeTTL rather than a same-typed sibling such as
// cfg.runnerIdentityTTL — has no other observer: sdk/xflow's serverConfig is
// unexported and *Server exposes no accessor, so behaviour through a real
// HTTP request is the only way in. Same shape, and the same reason, as
// TestBuildServerOptionsReachesIdentityTTLEndToEnd above.
func newRegistrationCodeTTLBinary(t *testing.T, ttlFlag string) (*httptest.Server, string) {
	t.Helper()
	path := writeTokenFile(t, "tokens.json",
		`[{"token":"tok-ops","subject":"op","namespace":"namespaceA","scopes":`+
			`["management.registration_code.create","management.registration_code.list"]}]`,
		0600)
	// -mode dev for the same reason the identity-TTL test uses it: this
	// in-memory, no-MySQL setup fails the production gate on grounds that have
	// nothing to do with the ceiling. It is the posture flag this binary
	// already exposes, not a weakening of anything under test.
	cfg, err := parseServerConfig([]string{
		"-mode", "dev", "-memory", "-enroll", "-management",
		"-auth-tokens-file", path, "-registration-code-ttl", ttlFlag,
	})
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
	return ts, "tok-ops"
}

func createRegistrationCodeStatus(t *testing.T, ts *httptest.Server, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		ts.URL+"/v1/management/registration-codes", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestBuildServerOptionsReachesRegistrationCodeTTLEndToEnd walks argv all the
// way to a refusal. The ceiling is only real if it survives every hop:
// parseServerConfig -> buildServerOptions -> WithServerRegistrationCodeTTL ->
// apiserver.Config -> managementModule. Each hop has its own unit test, but
// only this one proves they are connected to each other rather than each to a
// hand-set field.
func TestBuildServerOptionsReachesRegistrationCodeTTLEndToEnd(t *testing.T) {
	ts, token := newRegistrationCodeTTLBinary(t, "1h")
	const body = `{"allowed_namespaces":["namespaceA"],"expires_in_seconds":`

	if got := createRegistrationCodeStatus(t, ts, token, body+`60}`); got != http.StatusOK {
		t.Fatalf("status = %d for a 60s request under a 1h ceiling, want 200", got)
	}
	if got := createRegistrationCodeStatus(t, ts, token, body+`7200}`); got != http.StatusBadRequest {
		t.Fatalf("status = %d for a 2h request under a 1h ceiling, want 400; "+
			"--registration-code-ttl did not reach the handler", got)
	}
}

// TestBuildServerOptionsWithoutRegistrationCodeTTLLeavesCodesUnbounded is the
// negative twin. Without it, a buildServerOptions that hardcoded a duration
// into WithServerRegistrationCodeTTL instead of forwarding the flag would
// still pass the test above — and every deployment that never set the flag
// would quietly acquire a ceiling.
func TestBuildServerOptionsWithoutRegistrationCodeTTLLeavesCodesUnbounded(t *testing.T) {
	ts, token := newRegistrationCodeTTLBinary(t, "0")
	// 0 asks for a code that never expires. Under a ceiling that is a 400;
	// with no ceiling it is the whole point of the pre-feature behavior.
	if got := createRegistrationCodeStatus(t, ts, token,
		`{"allowed_namespaces":["namespaceA"],"expires_in_seconds":0}`); got != http.StatusOK {
		t.Fatalf("status = %d for a never-expires request with no ceiling, want 200", got)
	}
}
