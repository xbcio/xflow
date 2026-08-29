package xflow

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/apiserver"
)

// These tests cover the security posture an embedded SDK server must have on
// its own. cmd/server sets each of these on the apiserver.Config it builds by
// hand; a caller of NewServer never sees that config, so anything the SDK does
// not set is a downgrade the caller cannot detect from the outside. All three
// downgrades below are silent: the server starts, serves, and reports healthy.

func startTestServer(t *testing.T, opts ...ServerOption) *httptest.Server {
	t.Helper()
	opts = append(opts, WithServerInsecureNoRunnerAuth())
	srv, err := NewServer(ServerConfig{}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// The workflow API mutates the control plane: it registers definitions and
// seeds executions. Left unauthenticated it is an unauthenticated remote
// code-execution surface — a submitted workflow runs on every connected runner.
//
// The SDK had no way to guard it at all. serverConfig carried no workflow
// authenticator, so NewServer never set apiserver.Config.WorkflowAuth, and the
// control module falls back to DisabledWorkflowAuth (allow all) when it is nil.
// An embedded production server therefore accepted POST /v1/workflows from
// anyone who could reach the port, while cmd/server — same apiserver, same
// routes — required a bearer token.
func TestServerWorkflowAuthRejectsAnUnauthenticatedSubmit(t *testing.T) {
	ts := startTestServer(t, WithServerWorkflowAuth(apiserver.NewBearerTokenAuth("s3cret"), true))

	resp, err := http.Post(ts.URL+"/v1/workflows", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("POST /v1/workflows with no token = %d, want 401: the workflow API "+
			"is unauthenticated, so anyone who can reach the port can register a "+
			"workflow and have it execute on every connected runner", resp.StatusCode)
	}
}

// And the token must actually be honoured, so the guard is not a blanket deny
// that would be equally wrong.
func TestServerWorkflowAuthAcceptsTheConfiguredToken(t *testing.T) {
	ts := startTestServer(t, WithServerWorkflowAuth(apiserver.NewBearerTokenAuth("s3cret"), true))

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/workflows", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer s3cret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// The body is deliberately not a valid workflow; 400 proves the request got
	// past the authenticator and into the handler, which is all this asserts.
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("the configured token was rejected; the guard denies everything")
	}
}

// RequireWorkflowAuth is the fail-closed half: it turns a missing authenticator
// into a construction error instead of an open API. Without it a caller who
// means to require auth but wires the authenticator conditionally — from an
// env var, say — gets a silently open server when that value is empty.
func TestServerRequireWorkflowAuthFailsClosedWithNoAuthenticator(t *testing.T) {
	if _, err := NewServer(ServerConfig{}, WithServerWorkflowAuth(nil, true), WithServerInsecureNoRunnerAuth()); err == nil {
		t.Fatal("NewServer accepted RequireWorkflowAuth with a nil authenticator; " +
			"the workflow API would be open while the config reads as authenticated")
	}
}

// Wire-side supply encryption protects supply content between the server and
// the runner that fetches it. It is independent of at-rest encryption: at-rest
// needs a master key, the wire path does not, which is why cmd/server sets it
// unconditionally rather than gating it on the key being present.
//
// The SDK left it at false. A caller who correctly built a store with
// mysqlstore.WithSupplyEncryption got ciphertext in the database and plaintext
// on the fetch — credentials in the clear on exactly the hop that crosses a
// network boundary, with nothing in the config to suggest it.
func TestServerEnablesSupplyWireEncryption(t *testing.T) {
	cfg := buildServerAPIConfig(ServerConfig{}, &serverConfig{})
	if !cfg.EnableSupplyEncryption {
		t.Error("EnableSupplyEncryption = false: supply content — which is where " +
			"credentials live — crosses the server→runner hop in plaintext")
	}
}
