package runner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
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
	if len(seen.NodeTypes) != 2 || seen.NodeTypes[0] != "xflow.function" || seen.NodeTypes[1] != "xflow.group" {
		t.Fatalf("enroll node types = %v, want [xflow.function xflow.group]", seen.NodeTypes)
	}

	stored, ok, err := store.Load()
	if err != nil || !ok {
		t.Fatalf("store.Load after enrollment = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if stored.RunnerID != "issued-3" || stored.Token != "issued-token" {
		t.Fatalf("stored identity = %+v, want the issued one: a restart must not burn a second code", stored)
	}
}

func TestCapabilityNodeTypesAddsGroupOnceInStableOrder(t *testing.T) {
	got := capabilityNodeTypes([]protocol.Capability{
		{NodeType: " xflow.function "},
		{NodeType: "xflow.function"},
		{NodeType: engine.GroupNodeType},
		{NodeType: ""},
	})
	want := []string{"xflow.function", engine.GroupNodeType}
	if len(got) != len(want) {
		t.Fatalf("capabilityNodeTypes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("capabilityNodeTypes = %v, want %v", got, want)
		}
	}
}

// TestEnrollmentIssuedPolicyAllowsSDKGroupRegistration is the cross-boundary
// regression for the SDK's synthetic group capability: enrollment must request
// xflow.group too, or the issued policy rejects the same capability NewRunner
// automatically advertises at registration.
func TestEnrollmentIssuedPolicyAllowsSDKGroupRegistration(t *testing.T) {
	ctx := context.Background()
	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
	codeID, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(ctx, control.RegistrationCode{
		ID: codeID, CodeHash: control.HashSecret(plaintext), CreatedAt: time.Now().UTC(),
		AllowedNamespaces: []string{"default"},
		AllowedNodeTypes:  []string{"xflow.function", engine.GroupNodeType},
	}); err != nil {
		t.Fatalf("codes.Create: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{
		Backend:           backendlocal.New(),
		RegistrationCodes: codes,
		IssuedIdentities:  ids,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	ts := httptest.NewServer(cp.Handler())
	defer ts.Close()

	cfg := enrollTestConfig(ts.URL)
	cfg.registrationCode = plaintext
	resolved, err := resolveRunnerIdentity(ctx, cfg, &ephemeralIdentityStore{})
	if err != nil {
		t.Fatalf("resolveRunnerIdentity: %v", err)
	}
	issued, ok, err := ids.Lookup(ctx, resolved.runnerID)
	if err != nil || !ok {
		t.Fatalf("issued identity lookup = (%v, %v), want (true, nil)", ok, err)
	}
	if !issued.Scope.Allows(engine.GroupNodeType) {
		t.Fatalf("issued policy %v does not allow %q", issued.Scope.AllowedNodeTypes, engine.GroupNodeType)
	}

	_, err = protocol.NewClient(ts.URL, ts.Client()).WithToken(resolved.token).Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:    resolved.runnerID,
		Concurrency: 1,
		Namespaces:  []string{"default"},
		Capabilities: []protocol.Capability{
			{NodeType: "xflow.function"},
			{NodeType: engine.GroupNodeType, Features: []string{engine.FeatureGroupExecV1}},
		},
	})
	if err != nil {
		t.Fatalf("register SDK group capability with issued identity: %v", err)
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
