package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// TestBuildServerOptionsReachesIdentityTTLEndToEnd is the reachability proof
// runServer itself has never had: cmd/server/main.go's own comment (next to
// resolveBackendTarget) records that "cmd/server has no test that calls
// runServer at all". Coverage on either side of that gap already exists —
// TestParseServerConfigRunnerIdentityTTLFlag pins --runner-identity-ttl into
// cfg.runnerIdentityTTL, and sdk/xflow's TestNewServerReachesIdentityTTLEndToEnd
// pins WithServerIdentityTTL into EnrollResponse.ExpiresAt — but nothing
// checked the middle hop: buildServerOptions actually attaching
// cfg.runnerIdentityTTL to WithServerIdentityTTL rather than, say, a
// same-typed sibling field such as cfg.runnerMetricsInterval.
//
// This walks the real chain from argv: parseServerConfig (not a hand-built
// serverConfig{} literal, which would hide a wrong flag name, a wrong
// default, or a missing validation) -> buildServerOptions -> a real
// xflowsdk.NewServer (in-memory, no listener) -> an HTTP POST to
// protocol.EnrollPath -> the decoded protocol.EnrollResponse.ExpiresAt. Only
// observing the response proves the options this test's own call to
// buildServerOptions produced are the ones NewServer actually used; there is
// no exported accessor on *xflowsdk.Server to inspect them directly (sdk/xflow's
// serverConfig is unexported), so behavioural observation is the only way in.
//
// -mode dev is added on top of the brief's minimal --enroll --memory config:
// cfg.mode defaults to "production", which makes buildServerOptions attach
// WithServerProduction, and this test's in-memory, unauthenticated,
// no-MySQL setup fails that gate on six separate requirements (principal
// auth, audit sink, durable audit, supply encryption, ...). --mode dev is
// the flag this binary already exposes for exactly that posture; it is not
// a change to the gate itself, so this does not weaken anything the SUT
// enforces.
func TestBuildServerOptionsReachesIdentityTTLEndToEnd(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-mode", "dev", "-memory", "-enroll", "-runner-identity-ttl", "24h",
	})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}

	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
	deps := serverDeps{
		registrationCodeStore: codes,
		issuedIdentityStore:   ids,
	}

	opts := buildServerOptions(cfg, deps)

	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(context.Background(), control.RegistrationCode{
		ID: id, CodeHash: control.HashSecret(plaintext),
		AllowedNamespaces: []string{"*"}, AllowedNodeTypes: []string{"*"},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	const ttl = 24 * time.Hour
	before := time.Now().UTC()
	resp, err := ts.Client().Post(ts.URL+protocol.EnrollPath, "application/json",
		strings.NewReader(`{"registration_code":"`+plaintext+`","namespaces":["sas"]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %q, want 200", resp.StatusCode, body)
	}
	var got protocol.EnrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ExpiresAt == "" {
		t.Fatal("EnrollResponse.ExpiresAt is empty; want it stamped from " +
			"--runner-identity-ttl via buildServerOptions -> WithServerIdentityTTL")
	}
	expiresAt, err := time.Parse(time.RFC3339, got.ExpiresAt)
	if err != nil {
		t.Fatalf("ExpiresAt = %q not RFC3339: %v", got.ExpiresAt, err)
	}
	after := time.Now().UTC()
	// RFC3339 truncates to whole seconds, so widen the window by a second on
	// each side rather than asserting sub-second precision the wire format
	// cannot carry (mirrors sdk/xflow's TestNewServerReachesIdentityTTLEndToEnd).
	if expiresAt.Before(before.Add(ttl).Add(-time.Second)) || expiresAt.After(after.Add(ttl).Add(time.Second)) {
		t.Fatalf("ExpiresAt = %v, want within [%v, %v]", expiresAt, before.Add(ttl), after.Add(ttl))
	}
}

// TestBuildServerOptionsWithoutIdentityTTLLeavesExpiresAtEmpty is the negative
// twin of the test above: --runner-identity-ttl absent (cfg.runnerIdentityTTL
// stays at its zero-value default) must leave EnrollResponse.ExpiresAt empty.
// Without this case, an implementation of buildServerOptions that hardcoded
// 24*time.Hour into WithServerIdentityTTL instead of forwarding
// cfg.runnerIdentityTTL would still pass the main case above.
func TestBuildServerOptionsWithoutIdentityTTLLeavesExpiresAtEmpty(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-mode", "dev", "-memory", "-enroll"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if cfg.runnerIdentityTTL != 0 {
		t.Fatalf("runnerIdentityTTL = %v, want 0 when --runner-identity-ttl is absent", cfg.runnerIdentityTTL)
	}

	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
	deps := serverDeps{
		registrationCodeStore: codes,
		issuedIdentityStore:   ids,
	}

	opts := buildServerOptions(cfg, deps)

	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(context.Background(), control.RegistrationCode{
		ID: id, CodeHash: control.HashSecret(plaintext),
		AllowedNamespaces: []string{"*"}, AllowedNodeTypes: []string{"*"},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	resp, err := ts.Client().Post(ts.URL+protocol.EnrollPath, "application/json",
		strings.NewReader(`{"registration_code":"`+plaintext+`","namespaces":["sas"]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %q, want 200", resp.StatusCode, body)
	}
	var got protocol.EnrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ExpiresAt != "" {
		t.Fatalf("ExpiresAt = %q, want empty when --runner-identity-ttl is absent "+
			"(zero must mean never-expires, not a hidden default)", got.ExpiresAt)
	}
}
