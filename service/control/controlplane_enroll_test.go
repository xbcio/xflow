package control

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/protocol"
)

// TestControlPlaneMountsEnrollWhenStoresConfigured is the wiring test: the unit
// tests prove Core.Enroll is correct, this one proves anything ever calls it.
// A correct-but-unreachable handler is the failure mode this catches.
func TestControlPlaneMountsEnrollWhenStoresConfigured(t *testing.T) {
	codes := NewMemoryRegistrationCodeStore()
	ids := NewMemoryIssuedIdentityStore()
	id, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(context.Background(), RegistrationCode{
		ID: id, CodeHash: HashSecret(plaintext),
		AllowedNamespaces: []string{"*"}, AllowedNodeTypes: []string{"*"},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	cp := newTestControlPlaneWithEnroll(t, codes, ids)
	mux := http.NewServeMux()
	protocol.RegisterRunnerRoutes(mux, cp.RunnerHTTPHandler())
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL+protocol.EnrollPath, "application/json",
		strings.NewReader(`{"registration_code":"`+plaintext+`","namespaces":["sas"]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %q, want 200 (the route must be mounted and reach Core.Enroll)",
			resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"runner_id"`) || !strings.Contains(string(body), `"token"`) {
		t.Fatalf("body = %q, want an issued identity", body)
	}
}

func TestControlPlaneRejectsEnrollWhenStoresAbsent(t *testing.T) {
	cp := newTestControlPlaneWithEnroll(t, nil, nil)
	mux := http.NewServeMux()
	protocol.RegisterRunnerRoutes(mux, cp.RunnerHTTPHandler())
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL+protocol.EnrollPath, "application/json",
		strings.NewReader(`{"registration_code":"anything"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 when enroll is not configured", resp.StatusCode)
	}
}

// The composition must happen before the IsConfigured gate. If it moves below,
// an enroll-only server cannot start at all, and the failure is a startup error
// that no unit test of Core.Enroll would ever see.
func TestEnrollOnlyServerStartsUnderRequireRunnerAuth(t *testing.T) {
	cp, err := NewControlPlane(Config{
		Backend:           backendlocal.New(),
		RequireRunnerAuth: true,
		RegistrationCodes: NewMemoryRegistrationCodeStore(),
		IssuedIdentities:  NewMemoryIssuedIdentityStore(),
		// Auth deliberately unset: enrollment is the only runner auth here.
	})
	if err != nil {
		t.Fatalf("NewControlPlane(enroll-only, RequireRunnerAuth) error = %v, want success", err)
	}
	if cp == nil {
		t.Fatal("NewControlPlane returned nil")
	}
}

// TestEnrollCompositionIgnoresDisabledAuthenticator pins a composition bug
// found while wiring cmd/server through to enrollment: cfg.Auth is frequently
// DisabledAuthenticator{} (cmd/server's buildAuthenticator default when
// --auth-policy is empty — exactly the case an operator running --enroll
// alone hits), and DisabledAuthenticator{} is a non-nil Authenticator that
// always succeeds. MultiAuthenticator.dispatch returns the first member that
// succeeds, so composing NewMultiAuthenticator(DisabledAuthenticator{}, issued)
// — which a plain `cfg.Auth != nil` check would do — produces an authenticator
// that accepts every runner regardless of enrollment, silently defeating the
// whole feature. The composition must use IsConfigured(cfg.Auth), which is
// false for DisabledAuthenticator{}, so an enroll-only deployment ends up with
// the issued-identity authenticator alone.
func TestEnrollCompositionIgnoresDisabledAuthenticator(t *testing.T) {
	ids := NewMemoryIssuedIdentityStore()
	cp, err := NewControlPlane(Config{
		Backend:           backendlocal.New(),
		Auth:              DisabledAuthenticator{},
		RegistrationCodes: NewMemoryRegistrationCodeStore(),
		IssuedIdentities:  ids,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	auth := cp.Authenticator()
	if _, ok := auth.(*IssuedIdentityAuthenticator); !ok {
		t.Fatalf("Authenticator() = %T, want *IssuedIdentityAuthenticator (DisabledAuthenticator must not be composed in)", auth)
	}
	// A runner that was never enrolled must be rejected, not waved through by
	// a leftover DisabledAuthenticator member.
	if _, err := auth.AuthenticateRegister("never-enrolled", "some-token", TransportInfo{}); err == nil {
		t.Fatal("an unenrolled runner was authenticated; DisabledAuthenticator leaked into the composed authenticator")
	}
}


// NewControlPlane, wiring codes/ids into Config so the exercised paths are
// production wiring rather than field assignment on a test-only struct. Either
// argument may be nil to exercise the "enroll not configured" behavior.
func newTestControlPlaneWithEnroll(t *testing.T, codes RegistrationCodeStore, ids IssuedIdentityStore) *ControlPlane {
	t.Helper()
	cp, err := NewControlPlane(Config{
		Backend:           backendlocal.New(),
		RegistrationCodes: codes,
		IssuedIdentities:  ids,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	return cp
}
