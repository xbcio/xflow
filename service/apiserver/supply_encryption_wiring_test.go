package apiserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store/memstore"
)

// TestSupplyEncryptionWiredThroughInjectedControlPlane proves the real
// assembly path: apiserver.New with a ControlPlane injected via
// WithControlPlane (the same construction the server-runner e2e harness uses)
// and EnableSupplyEncryption set on the control plane's own Config. Nothing
// here touches sup.encryptor or cfg.SupplyEncryptor directly -- if the wiring
// in New (service/apiserver/apiserver.go) that reads cp.SupplyEncryptor() is
// removed or broken, this test must fail.
func TestSupplyEncryptionWiredThroughInjectedControlPlane(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{
		Backend:                local.New(),
		EnableSupplyEncryption: true,
	})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}
	enc := cp.SupplyEncryptor()
	if enc == nil {
		t.Fatal("control plane built with EnableSupplyEncryption=true has a nil SupplyEncryptor")
	}

	supplies := memstore.New()
	cfg := Config{
		Supplies: supplies,
		PrincipalAuth: staticPrincipalAuth{principal: Principal{
			Subject: "test-user", Namespace: "ns1", Scopes: []string{"supply.write", "supply.read"},
		}},
		Authorizer: ScopeAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	mux := srv.Handler()

	plain := []byte(`{"rules":[{"field":"authorization","action":"redact"}]}`)
	putReq := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader(plain))
	putReq.Header.Set("Content-Type", "application/json")
	putRec := httptest.NewRecorder()
	mux.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT = %d, body=%s", putRec.Code, putRec.Body)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil)
	getReq.Header.Set("Accept", AcceptEncrypted)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET = %d, body=%s", getRec.Code, getRec.Body)
	}

	body := getRec.Body.Bytes()
	if bytes.Contains(body, []byte("authorization")) {
		t.Fatal("response body contains plaintext; the transport-encryption wiring did not run")
	}
	// Decrypt with the key the control plane itself holds -- the key a runner
	// would receive via register/heartbeat -- not one the test fabricated.
	got, err := supplyenc.NewKeyring(keyFromRunnerString(t, enc.KeyForRunner())).Decrypt(body)
	if err != nil {
		t.Fatalf("decrypt response with the control plane's own key: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("decrypted body = %q, want %q", got, plain)
	}
}

func keyFromRunnerString(t *testing.T, b64 string) *supplyenc.Key {
	t.Helper()
	k, err := supplyenc.KeyFromBase64(b64)
	if err != nil {
		t.Fatalf("KeyFromBase64: %v", err)
	}
	return k
}

// TestSupplyEncryptorForGuardsTypedNil proves supplyEncryptorFor does not fall
// into the typed-nil-interface trap: cp.SupplyEncryptor() returns a nil
// *control.SupplyEncryptor when encryption was never enabled on that control
// plane, and assigning that nil pointer straight into the
// SupplyContentEncryptor interface would produce a NON-nil interface value --
// module_supply.go's `m.encryptor != nil` guard would then pass and Encrypt
// would be called on a nil receiver.
func TestSupplyEncryptorForGuardsTypedNil(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}
	if enc := cp.SupplyEncryptor(); enc != nil {
		t.Fatalf("expected nil SupplyEncryptor on a plane without EnableSupplyEncryption, got %v", enc)
	}

	got := supplyEncryptorFor(Config{}, cp)
	if got != nil {
		t.Fatalf("supplyEncryptorFor returned a non-nil interface (%#v) for a control plane with no encryptor -- typed-nil trap", got)
	}
}

// TestSupplyGETStaysPlaintextThroughInjectedControlPlaneWithoutEncryption is
// the end-to-end companion of TestSupplyEncryptorForGuardsTypedNil: it drives
// the same New()+WithControlPlane path as
// TestSupplyEncryptionWiredThroughInjectedControlPlane, but against a control
// plane that never enabled supply encryption. A GET with the encrypted-Accept
// header must return ordinary plaintext (not panic, not 500) -- proving the
// nil guard holds all the way through the real assembly path, not just in the
// unit-level check above.
func TestSupplyGETStaysPlaintextThroughInjectedControlPlaneWithoutEncryption(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}

	supplies := memstore.New()
	cfg := Config{
		Supplies: supplies,
		PrincipalAuth: staticPrincipalAuth{principal: Principal{
			Subject: "test-user", Namespace: "ns1", Scopes: []string{"supply.write", "supply.read"},
		}},
		Authorizer: ScopeAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	mux := srv.Handler()

	plain := []byte(`{"rules":[1]}`)
	putReq := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader(plain))
	putReq.Header.Set("Content-Type", "application/json")
	putRec := httptest.NewRecorder()
	mux.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT = %d, body=%s", putRec.Code, putRec.Body)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil)
	getReq.Header.Set("Accept", AcceptEncrypted)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET = %d, body=%s (expected plaintext passthrough, not a panic/500 from a nil encryptor)", getRec.Code, getRec.Body)
	}
	if !bytes.Equal(getRec.Body.Bytes(), plain) {
		t.Errorf("GET body = %q, want plaintext %q", getRec.Body.Bytes(), plain)
	}
}
