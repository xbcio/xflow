package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func enrollTestConfig(srvURL string) runnerConfig {
	cfg := defaultRunnerConfig()
	cfg.transport = transportHTTP
	cfg.serverURL = srvURL
	cfg.allowPlaintext = true
	cfg.runnerID = "proposed-1"
	cfg.capabilities = parseCapabilities("xflow.function")
	return cfg
}

func TestResolveRunnerIdentityPrefersTheStoredIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected call to %s: a stored identity must not trigger enrollment", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	store := &ephemeralIdentityStore{}
	if err := store.Save(identity{RunnerID: "stored-9", Token: "stored-token"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg := enrollTestConfig(srv.URL)
	cfg.registrationCode = "code-that-must-not-be-used"

	got, err := resolveRunnerIdentity(context.Background(), cfg, store)
	if err != nil {
		t.Fatalf("resolveRunnerIdentity: %v", err)
	}
	if got.runnerID != "stored-9" || got.token != "stored-token" {
		t.Fatalf("resolved identity = (%q, %q), want (stored-9, stored-token)", got.runnerID, got.token)
	}
}

func TestResolveRunnerIdentityEnrollsAndPersists(t *testing.T) {
	var seen protocol.EnrollRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.EnrollPath {
			t.Errorf("path = %q, want %q", r.URL.Path, protocol.EnrollPath)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("enroll carried Authorization %q; the registration code is the only credential it has", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{RunnerID: "issued-3", Token: "issued-token"})
	}))
	defer srv.Close()

	store := &ephemeralIdentityStore{}
	cfg := enrollTestConfig(srv.URL)
	cfg.registrationCode = "rc-abc"

	got, err := resolveRunnerIdentity(context.Background(), cfg, store)
	if err != nil {
		t.Fatalf("resolveRunnerIdentity: %v", err)
	}
	if got.runnerID != "issued-3" || got.token != "issued-token" {
		t.Fatalf("resolved identity = (%q, %q), want (issued-3, issued-token)", got.runnerID, got.token)
	}
	if seen.RegistrationCode != "rc-abc" || seen.ProposedRunnerID != "proposed-1" {
		t.Fatalf("enroll request = %+v, want the code and the proposed ID carried through", seen)
	}
	if len(seen.NodeTypes) != 1 || seen.NodeTypes[0] != "xflow.function" {
		t.Fatalf("enroll node types = %v, want [xflow.function]", seen.NodeTypes)
	}

	stored, ok, err := store.Load()
	if err != nil || !ok {
		t.Fatalf("store.Load after enrollment = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if stored.RunnerID != "issued-3" || stored.Token != "issued-token" {
		t.Fatalf("stored identity = %+v, want the issued one: a restart must not burn a second code", stored)
	}
}

func TestResolveRunnerIdentityWithoutCodeLeavesTheStaticConfigAlone(t *testing.T) {
	cfg := enrollTestConfig("http://unreachable.invalid")
	cfg.token = "static-token"

	got, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{})
	if err != nil {
		t.Fatalf("resolveRunnerIdentity with no stored identity and no code: %v", err)
	}
	if got.runnerID != "proposed-1" || got.token != "static-token" {
		t.Fatalf("config was rewritten to (%q, %q); with no registration code the static --token path must be untouched",
			got.runnerID, got.token)
	}
}

func TestResolveRunnerIdentitySurfacesEnrollFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("registration code already used"))
	}))
	defer srv.Close()

	cfg := enrollTestConfig(srv.URL)
	cfg.registrationCode = "spent"

	if _, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{}); err == nil {
		t.Fatal("resolveRunnerIdentity returned nil error on a rejected registration code")
	}
}

func grpcEnrollTestConfig(srvURL string) runnerConfig {
	cfg := defaultRunnerConfig()
	cfg.transport = transportGRPC
	cfg.grpcTarget = "localhost:9090"
	cfg.serverURL = srvURL
	cfg.runnerID = "proposed-1"
	cfg.capabilities = parseCapabilities("xflow.function")
	return cfg
}

func TestResolveRunnerIdentityRefusesPlaintextEnrollUnderGRPCTransport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected call to %s: a plaintext enroll target must be refused before any request", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := grpcEnrollTestConfig(srv.URL)
	cfg.registrationCode = "rc-abc"

	_, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{})
	if err == nil {
		t.Fatal("resolveRunnerIdentity returned nil error for a plaintext enroll target with no opt-in")
	}
	if !strings.Contains(err.Error(), "allow-plaintext") {
		t.Fatalf("error %q does not name --allow-plaintext", err.Error())
	}
	if strings.Contains(err.Error(), "rc-abc") {
		t.Fatalf("error %q echoes the registration code", err.Error())
	}
}

func TestResolveRunnerIdentityAllowsPlaintextEnrollUnderGRPCTransportWithOptIn(t *testing.T) {
	var seen protocol.EnrollRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.EnrollResponse{RunnerID: "issued-5", Token: "issued-token-5"})
	}))
	defer srv.Close()

	cfg := grpcEnrollTestConfig(srv.URL)
	cfg.registrationCode = "rc-abc"
	cfg.allowPlaintext = true

	got, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{})
	if err != nil {
		t.Fatalf("resolveRunnerIdentity: %v", err)
	}
	if got.runnerID != "issued-5" || got.token != "issued-token-5" {
		t.Fatalf("resolved identity = (%q, %q), want (issued-5, issued-token-5)", got.runnerID, got.token)
	}
	if seen.RegistrationCode != "rc-abc" {
		t.Fatalf("enroll request = %+v, want the code carried through", seen)
	}
}

func TestResolveRunnerIdentityGRPCWithStaticTokenIgnoresPlaintextServerURL(t *testing.T) {
	cfg := grpcEnrollTestConfig("http://unreachable.invalid")
	cfg.token = "static-token"

	got, err := resolveRunnerIdentity(context.Background(), cfg, &ephemeralIdentityStore{})
	if err != nil {
		t.Fatalf("resolveRunnerIdentity with a static token and no registration code: %v", err)
	}
	if got.runnerID != "proposed-1" || got.token != "static-token" {
		t.Fatalf("config was rewritten to (%q, %q); with no registration code the gate must not fire",
			got.runnerID, got.token)
	}
}
