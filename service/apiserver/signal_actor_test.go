package apiserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/types"
)

// signalAuthzModule wires the real authz route path with a capturing engine
// facade, so a delivered signal's payload can be inspected after the fact.
func signalAuthzModule(t *testing.T, subject string, scopes []string) (*fakeControlFacade, http.Handler) {
	t.Helper()
	auth := staticPrincipalAuth{principal: Principal{Subject: subject, Scopes: scopes}}
	m := authzModule(t, auth, ScopeAuthorizer{}, NewInMemoryAuditSink())
	f := &fakeControlFacade{}
	m.eng = f
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)
	return f, mux
}

// The delivered payload must carry the authenticated subject, so a handler that
// decides on a person's behalf reads a server-verified identity. This is the
// binding that the approval node relies on.
func TestSignalDeliversTheVerifiedActor(t *testing.T) {
	f, mux := signalAuthzModule(t, "alice", []string{"execution"})

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/signals", signalRequest{
		Name: "countersign/approval/alice",
		Data: map[string]any{"action": "approve", "comment": "lgtm"},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signal status = %d, want 200", resp.StatusCode)
	}
	if got := f.signalData[types.VerifiedActorKey]; got != "alice" {
		t.Fatalf("%s = %v, want alice (the authenticated subject)", types.VerifiedActorKey, got)
	}
	// The caller's own payload survives; the actor is additive.
	if f.signalData["action"] != "approve" || f.signalData["comment"] != "lgtm" {
		t.Fatalf("signal data = %v, want the caller payload preserved", f.signalData)
	}
}

// A caller that supplies the reserved key is claiming a server-established
// fact. Overwriting it silently would repair a future dropped-injection
// regression instead of surfacing it.
func TestSignalRejectsClientSuppliedActor(t *testing.T) {
	f, mux := signalAuthzModule(t, "bob", []string{"execution"})

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/signals", signalRequest{
		Name: "countersign/approval/alice",
		Data: map[string]any{"action": "approve", types.VerifiedActorKey: "alice"},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (reserved key refused)", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "signal_invalid" {
		t.Fatalf("code = %q, want signal_invalid", env.Code)
	}
	// The refusal happens before delivery: the engine must not see the claim.
	if f.signalName != "" || f.signalData != nil {
		t.Fatalf("engine received %q %v; a refused signal must not be delivered", f.signalName, f.signalData)
	}
	// The error message names no field: an internal-reserved-key leak would map
	// out the payload contract for a caller probing the endpoint.
	if msg := env.Message; msg != "signal payload is invalid" {
		t.Fatalf("message = %q, want a generic refusal", msg)
	}
}

// Without principal authentication there is no actor to establish. The signal
// is still delivered — signal-based nodes other than approvals do not need an
// actor — but nothing is fabricated in its place, so an approval cannot be
// signed on a deployment that cannot prove who is signing.
func TestSignalWithoutPrincipalCarriesNoActor(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/signals", signalRequest{
		Name: "countersign/approval/alice",
		Data: map[string]any{"action": "approve"},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signal status = %d, want 200", resp.StatusCode)
	}
	if _, present := f.signalData[types.VerifiedActorKey]; present {
		t.Fatalf("signal data = %v, want no %s without a verified principal", f.signalData, types.VerifiedActorKey)
	}
}

// The actor value is the subject the authenticator verified, never anything the
// request carried: a caller cannot pick their subject by header.
func TestSignalActorIgnoresCallerControlledHeaders(t *testing.T) {
	f, mux := signalAuthzModule(t, "op-bob", []string{"execution"})

	var buf httptest.ResponseRecorder
	req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-1/signals",
		bytes.NewReader([]byte(`{"name":"countersign/approval/op-bob"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-alice")
	req.Header.Set("X-PRINCIPAL", "alice")
	req.Header.Set("X-Actor", "alice")
	mux.ServeHTTP(&buf, req)

	if buf.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", buf.Code)
	}
	if got := f.signalData[types.VerifiedActorKey]; got != "op-bob" {
		t.Fatalf("%s = %v, want op-bob (the verified subject, not the header claim)", types.VerifiedActorKey, got)
	}
}
