package xflow

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// TestNewServerReachesEnrollEndToEnd is the reachability proof for the full
// wiring chain: cmd/server -> xflowsdk.WithServerEnroll -> buildServerAPIConfig
// (sdk/xflow/server.go) -> apiserver.Config -> apiserver.buildControlPlane's
// ccfg -> control.NewControlPlane's enrollConfigured gate -> the mounted
// /v1/runners/enroll route -> Core.Enroll's registrationCodes/issuedIdentities.
//
// It POSTs a REAL, valid registration code — not a nonexistent one — and
// requires exactly 200 with an issued identity in the body. A nonexistent
// code cannot do this job here: control.Core.Enroll deliberately collapses
// "enroll not configured at all" and "code unknown/invalid" into the same
// ErrEnrollRejected -> 403 response (spec §2.3.4's indistinguishable-rejection
// requirement, pinned by control.TestEnrollRejectionsAreIndistinguishable), so
// asserting 403 on a bad code would stay green whether or not the stores this
// test constructs ever reach the Core that receives the HTTP request — the
// request could be served by a Core with its own nil stores and still answer
// 403. Only a code that is valid IN THE STORE THIS TEST BUILT can prove the
// HTTP request landed on a Core sharing that exact store: if any joint in the
// chain drops the passthrough, the serving Core's registrationCodes is nil (or
// a different store), so even this valid code resolves to the generic 403.
//
// Deliberately built with WithServerEnroll ALONE — no WithServerAuth, no
// WithServerInsecureNoRunnerAuth. That is the point: an enroll-only server is
// meant to be a supported deployment (cmd/server's runnerAuthConfigured has
// said so since the --enroll flag was added), and this is the test that goes
// red if NewServer's posture gate stops believing it.
func TestNewServerReachesEnrollEndToEnd(t *testing.T) {
	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
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

	srv, err := NewServer(ServerConfig{}, WithServerEnroll(codes, ids))
	if err != nil {
		t.Fatalf("NewServer(WithServerEnroll only) error = %v, want success — "+
			"enrollment alone must count as a declared runner-auth posture", err)
	}

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+protocol.EnrollPath, "application/json",
		strings.NewReader(`{"registration_code":"`+plaintext+`","namespaces":["sas"]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %q, want 200 — a rejection here means the "+
			"Core serving this request did not receive the same registration-code "+
			"store this test built, i.e. the passthrough chain is broken",
			resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"runner_id"`) || !strings.Contains(string(body), `"token"`) {
		t.Fatalf("body = %q, want an issued identity", body)
	}
}

// TestNewServerRejectsEnrollWithMismatchedStores pins that WithServerEnroll
// requires BOTH stores: passing only one is equivalent to not calling it, so
// NewServer must still demand a posture from elsewhere.
func TestNewServerRejectsEnrollWithMismatchedStores(t *testing.T) {
	codes := control.NewMemoryRegistrationCodeStore()
	_, err := NewServer(ServerConfig{}, WithServerEnroll(codes, nil))
	if err == nil {
		t.Fatal("NewServer accepted WithServerEnroll with only one store set")
	}
	if !errors.Is(err, ErrRunnerAuthPostureUndeclared) {
		t.Fatalf("want ErrRunnerAuthPostureUndeclared, got %v", err)
	}
}
