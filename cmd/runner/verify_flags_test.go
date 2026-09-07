package main

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

// This file covers the connection flags `verify` binds. It bound all of them
// long before it used any of them: the command built its own
// protocol.NewClient(serverURL, http.DefaultClient) and its own registration,
// so --tls-*, --token and --namespace were parsed, validated, and dropped.
//
// The failure mode is worse than "a flag does nothing", because verify's whole
// job is to answer a question about a runner that has not started yet. A
// preflight that connects differently from the runner answers about a
// different runner. Each test below fails against the pre-fix verify, and each
// for the reason an operator would have hit in production.

// writeCertPEM writes an httptest TLS server's own certificate out as a CA
// bundle, which is what --tls-server-ca takes.
func writeCertPEM(t *testing.T, server *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	encoded := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: server.Certificate().Raw,
	})
	if encoded == nil {
		t.Fatal("encode server certificate")
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// verifyHandler answers register+heartbeat, recording what it was sent and
// rejecting requests that fail check.
func verifyHandler(t *testing.T, recorded *protocol.RegisterRunnerRequest, check func(*http.Request) bool) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if check != nil && !check(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			var req protocol.RegisterRunnerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode register request: %v", err)
			}
			if recorded != nil {
				*recorded = req
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"runner_id":"` + req.RunnerID + `","session_id":"sess-1"}`))
		case protocol.HeartbeatPath:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"server_time":1}`))
		default:
			http.NotFound(w, r)
		}
	})
}

// TestVerifyUsesTLSMaterial is the one that matters most: an mTLS or
// private-CA control plane is the deployment where a preflight earns its
// keep, and it was the exact deployment where verify reported failure on a
// correct configuration. http.DefaultClient trusts only the system roots, so
// the pre-fix command fails here with an x509 "certificate signed by unknown
// authority".
func TestVerifyUsesTLSMaterial(t *testing.T) {
	var registered protocol.RegisterRunnerRequest
	server := httptest.NewTLSServer(verifyHandler(t, &registered, nil))
	defer server.Close()

	var out bytes.Buffer
	cmd := newRootCommand(commandOptions{out: &out, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "http",
		"--server", server.URL,
		"--tls-server-ca", writeCertPEM(t, server),
		"--id", "runner-tls",
		"--concurrency", "1",
		"--cap", "xflow.function",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("verify failed against a TLS server whose CA was supplied: %v\n"+
			"--tls-server-ca was parsed but never reached the HTTP client", err)
	}
	if registered.RunnerID != "runner-tls" {
		t.Fatalf("registered = %+v, want the TLS connection to have carried the registration", registered)
	}
}

// TestVerifyUsesToken pins that --token reaches the wire. Without it an
// authenticating control plane answers 401, so verify condemns a runner
// configuration that would have registered fine.
func TestVerifyUsesToken(t *testing.T) {
	var seenAuth string
	server := httptest.NewServer(verifyHandler(t, nil, func(r *http.Request) bool {
		seenAuth = r.Header.Get("Authorization")
		return seenAuth == "Bearer runner-secret"
	}))
	defer server.Close()

	var out bytes.Buffer
	cmd := newRootCommand(commandOptions{out: &out, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "http",
		"--server", server.URL,
		"--token", "runner-secret",
		"--id", "runner-token",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("verify failed against a server requiring the token it was given: %v\n"+
			"Authorization header seen by the server: %q", err, seenAuth)
	}
}

// TestVerifyRegistersDeclaredNamespaces pins the registration payload's
// namespaces. The server matches assignments on these, so a preflight that
// omitted them described a runner in the default namespace regardless of what
// --namespace said — accepted by the server, and routed nothing like the real
// runner.
func TestVerifyRegistersDeclaredNamespaces(t *testing.T) {
	var registered protocol.RegisterRunnerRequest
	server := httptest.NewServer(verifyHandler(t, &registered, nil))
	defer server.Close()

	var out bytes.Buffer
	cmd := newRootCommand(commandOptions{out: &out, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "http",
		"--server", server.URL,
		"--namespace", "team-a",
		"--namespace", "team-b",
		"--id", "runner-ns",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(registered.Namespaces, "team-a") ||
		!slices.Contains(registered.Namespaces, "team-b") {
		t.Fatalf("registered namespaces = %v, want both declared namespaces; "+
			"an empty list means the server places this runner in %q instead",
			registered.Namespaces, "default")
	}
}

// TestVerifyDefaultsNamespacesToDefault pins the other half of the
// empty-means-default rule the registration shares with a real session
// (runnersvc.NamespaceStrings). Sending an empty list rather than ["default"]
// is not equivalent: it leaves the namespace the server places the runner in
// up to the server's own default, which no longer has to agree with the
// runner's.
func TestVerifyDefaultsNamespacesToDefault(t *testing.T) {
	var registered protocol.RegisterRunnerRequest
	server := httptest.NewServer(verifyHandler(t, &registered, nil))
	defer server.Close()

	cmd := newRootCommand(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "http",
		"--server", server.URL,
		"--id", "runner-ns-default",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if len(registered.Namespaces) != 1 || registered.Namespaces[0] != "default" {
		t.Fatalf("registered namespaces = %v, want [default]", registered.Namespaces)
	}
}

// TestVerifyReportsSupportsEncryption pins the last field the hand-written
// registration omitted. The server decides whether to hand back a supply
// encryption key on it, so a runner that under-reports here is told nothing
// about supply encryption and a preflight cannot surface that.
func TestVerifyReportsSupportsEncryption(t *testing.T) {
	var registered protocol.RegisterRunnerRequest
	server := httptest.NewServer(verifyHandler(t, &registered, nil))
	defer server.Close()

	cmd := newRootCommand(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "http",
		"--server", server.URL,
		"--id", "runner-enc",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !registered.SupportsEncryption {
		t.Fatal("registered SupportsEncryption = false; a real session always reports true, " +
			"so the preflight describes a runner the server treats differently")
	}
}

// TestVerifyHeartbeatCarriesRegisteredSession pins that the heartbeat is bound
// to the session register just issued. The hand-written version sent no
// SessionID at all, so a server that ties heartbeats to a session rejects a
// preflight whose registration it had just accepted.
func TestVerifyHeartbeatCarriesRegisteredSession(t *testing.T) {
	sessions := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.RegisterRunnerPath:
			var req protocol.RegisterRunnerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode register request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"runner_id":"` + req.RunnerID + `","session_id":"sess-verify"}`))
		case protocol.HeartbeatPath:
			var hb protocol.HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
				t.Errorf("decode heartbeat request: %v", err)
			}
			sessions <- hb.SessionID
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"server_time":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cmd := newRootCommand(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "http",
		"--server", server.URL,
		"--id", "runner-sess",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	var got string
	select {
	case got = <-sessions:
	default:
		t.Fatal("no heartbeat reached the server")
	}
	if got != "sess-verify" {
		t.Fatalf("heartbeat SessionID = %q, want the id register returned", got)
	}
}

// TestVerifyRejectsGRPCTransportWithoutAServer is the transport half. It does
// not stand up a gRPC server; it only pins that --transport grpc makes verify
// dial the gRPC target rather than silently probing HTTP. That distinction is
// the false-positive case: against a control plane serving both, the pre-fix
// verify passed over HTTP while the runner would have used gRPC. The default
// transport is grpc, which is what made this the common case rather than the
// exotic one.
func TestVerifyRejectsGRPCTransportWithoutAServer(t *testing.T) {
	server := httptest.NewServer(verifyHandler(t, nil, nil))
	defer server.Close()

	cmd := newRootCommand(commandOptions{out: &bytes.Buffer{}, err: &bytes.Buffer{}})
	cmd.SetArgs([]string{
		"verify",
		"--transport", "grpc",
		"--server", server.URL,
		// A port nothing listens on: reaching it at all proves the transport
		// was honoured, because the HTTP server above would have answered.
		"--grpc-target", "127.0.0.1:1",
		"--id", "runner-grpc",
		"--concurrency", "1",
		"--cap", "xflow.function",
		"--allow-plaintext",
	})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("verify succeeded with --transport grpc pointed at a dead target; " +
			"it answered over HTTP instead, which is the false positive this pins")
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Fatalf("error = %v, want it to name the gRPC target that was dialled", err)
	}
}
