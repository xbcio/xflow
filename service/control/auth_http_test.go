package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func newAuthedServer(t *testing.T) (*httptest.Server, *MemoryRunnerDirectory) {
	t.Helper()
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:             "orders",
			IDPrefix:         "order-runner-",
			Token:            "secret-token",
			AllowedNodeTypes: []string{"xflow.function"},
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	srv := httptest.NewServer(NewServer(fake, dir, WithAuthenticator(store)).Handler())
	t.Cleanup(srv.Close)
	return srv, dir
}

func postAuthed(t *testing.T, url, token string, body any) *http.Response {
	t.Helper()
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHTTPRegisterRejectedWithoutToken(t *testing.T) {
	srv, _ := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "", protocol.RegisterRunnerRequest{
		RunnerID:    "order-runner-1",
		Concurrency: 1,
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestHTTPRegisterAcceptedWithValidBearerToken(t *testing.T) {
	srv, pool := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "secret-token", protocol.RegisterRunnerRequest{
		RunnerID:     "order-runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// Runner registered with the policy so its allowed types are enforced.
	snap, ok := pool.Runner(context.Background(), "order-runner-1")
	if !ok || snap.RunnerID != "order-runner-1" {
		t.Fatalf("runner not registered: %+v", snap)
	}
}

func TestHTTPRegisterRejectsForgedRunnerID(t *testing.T) {
	srv, _ := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "secret-token", protocol.RegisterRunnerRequest{
		RunnerID:    "hacker-runner",
		Concurrency: 1,
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("wrong id prefix should be rejected even with a valid token")
	}
}

func TestHTTPBodyTokenAcceptedWhenNoHeader(t *testing.T) {
	srv, _ := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "", protocol.RegisterRunnerRequest{
		RunnerID:    "order-runner-1",
		Concurrency: 1,
		AuthToken:   "secret-token",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body token fallback)", resp.StatusCode)
	}
}

func TestHTTPRegisterRejectsUnauthorizedCapability(t *testing.T) {
	srv, dir := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "secret-token", protocol.RegisterRunnerRequest{
		RunnerID:     "order-runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.script"}},
	})
	assertHTTPRegisterError(t, resp, http.StatusForbidden, ErrAuthCapabilityDenied.Error())

	if _, ok := dir.Runner(context.Background(), "order-runner-1"); ok {
		t.Fatal("unauthorized-capability runner was registered")
	}
}

func TestHTTPRegisterRejectsUnauthorizedNamespaceWithForbidden(t *testing.T) {
	srv, _ := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "secret-token", protocol.RegisterRunnerRequest{
		RunnerID:     "order-runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Namespaces:   []string{"other-team"},
	})
	assertHTTPRegisterError(t, resp, http.StatusForbidden, ErrAuthNamespaceDenied.Error())
}

func TestHTTPRegisterRejectsBlankCapability(t *testing.T) {
	srv, _ := newAuthedServer(t)
	resp := postAuthed(t, srv.URL+protocol.RegisterRunnerPath, "secret-token", protocol.RegisterRunnerRequest{
		RunnerID:     "order-runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: " \t "}},
	})
	assertHTTPRegisterError(t, resp, http.StatusBadRequest, ErrInvalidCapability.Error())
}

func assertHTTPRegisterError(t *testing.T, resp *http.Response, wantStatus int, wantMessage string) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Error != wantMessage {
		t.Fatalf("error message = %q, want fixed sentinel %q", body.Error, wantMessage)
	}
}
