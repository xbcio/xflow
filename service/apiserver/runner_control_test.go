package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
)

func newRunnerControlHTTPServer(t *testing.T, scopes []string) (*httptest.Server, *control.MemoryRunnerDirectory) {
	t.Helper()
	directory := control.NewMemoryRunnerDirectory()
	if _, err := directory.Register(context.Background(), control.RegisterRunnerRequest{
		RunnerID: "runner-a", Capacity: 1, Now: time.Now(),
	}); err != nil {
		t.Fatalf("register runner: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: local.New(), RunnerDirectory: directory})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "platform-op", Namespace: "tenant-a", Scopes: scopes,
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return httptest.NewServer(mux), directory
}

func runnerControlHTTP(t *testing.T, client *http.Client, baseURL, path, requestID, reason string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	return resp.StatusCode, out.String()
}

func TestRunnerControlHTTPRequiresGlobalScopeBeforeLookup(t *testing.T) {
	srv, _ := newRunnerControlHTTPServer(t, []string{scopeForOperation(OpManagementRunnerDrain)})
	defer srv.Close()
	code, body := runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/does-not-exist/drain", "request-a", "maintenance")
	if code != http.StatusForbidden || !strings.Contains(body, `"forbidden"`) {
		t.Fatalf("no global scope = %d %q, want 403 forbidden before lookup", code, body)
	}
}

func TestRunnerControlHTTPDrainResumeAndIdempotency(t *testing.T) {
	scopes := []string{
		scopeForOperation(OpManagementRunnerDrain),
		scopeForOperation(OpManagementRunnerResume),
		ScopeManagementRunnerControlGlobal,
	}
	srv, directory := newRunnerControlHTTPServer(t, scopes)
	defer srv.Close()

	code, body := runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/drain", "", "maintenance")
	if code != http.StatusBadRequest || !strings.Contains(body, `"request_id_required"`) {
		t.Fatalf("missing request ID = %d %q", code, body)
	}
	code, body = runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/drain", "request-a", "maintenance")
	if code != http.StatusOK || !strings.Contains(body, `"desired_state":"draining"`) {
		t.Fatalf("drain = %d %q", code, body)
	}
	code, body = runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/resume", "request-b", "maintenance done")
	if code != http.StatusOK || !strings.Contains(body, `"desired_state":"active"`) {
		t.Fatalf("resume = %d %q", code, body)
	}
	// A retry of the old drain is a receipt replay, not a second transition.
	code, body = runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/drain", "request-a", "maintenance")
	if code != http.StatusOK || !strings.Contains(body, `"desired_state":"draining"`) {
		t.Fatalf("old receipt = %d %q", code, body)
	}
	current, found, err := directory.RunnerControl(context.Background(), "runner-a")
	if err != nil || !found || current.DesiredState != control.RunnerDesiredStateActive || current.Generation != 2 {
		t.Fatalf("current after old receipt = %+v, found=%v, err=%v", current, found, err)
	}
	code, body = runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/drain", "request-a", "different reason")
	if code != http.StatusConflict || !strings.Contains(body, `"idempotency_key_reused"`) {
		t.Fatalf("conflicting receipt = %d %q", code, body)
	}
}

func TestRunnerControlHTTPValidatesReasonAndUnknownRunner(t *testing.T) {
	scopes := []string{scopeForOperation(OpManagementRunnerDrain), ScopeManagementRunnerControlGlobal}
	srv, _ := newRunnerControlHTTPServer(t, scopes)
	defer srv.Close()
	code, body := runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/drain", "request-a", "   ")
	if code != http.StatusBadRequest || !strings.Contains(body, `"bad_request"`) {
		t.Fatalf("empty reason = %d %q", code, body)
	}
	code, body = runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/unknown/drain", "request-b", "maintenance")
	if code != http.StatusNotFound || !strings.Contains(body, `"runner_not_found"`) {
		t.Fatalf("unknown runner = %d %q", code, body)
	}
}

type runnerDirectoryWithoutControl struct {
	control.RunnerDirectory
}

func TestRunnerControlHTTPReportsUnsupportedDirectory(t *testing.T) {
	base := control.NewMemoryRunnerDirectory()
	cp, err := control.NewControlPlane(control.Config{
		Backend:         local.New(),
		RunnerDirectory: runnerDirectoryWithoutControl{RunnerDirectory: base},
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "platform-op", Namespace: "tenant-a", Scopes: []string{
			scopeForOperation(OpManagementRunnerDrain), ScopeManagementRunnerControlGlobal,
		},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	code, body := runnerControlHTTP(t, srv.Client(), srv.URL, "/v1/management/runners/runner-a/drain", "request-a", "maintenance")
	if code != http.StatusNotImplemented || !strings.Contains(body, `"runner_control_unsupported"`) {
		t.Fatalf("unsupported directory = %d %q, want 501 runner_control_unsupported", code, body)
	}
}

func TestRunnerControlHTTPRejectsUnknownOrTrailingJSON(t *testing.T) {
	scopes := []string{scopeForOperation(OpManagementRunnerDrain), ScopeManagementRunnerControlGlobal}
	srv, _ := newRunnerControlHTTPServer(t, scopes)
	defer srv.Close()
	for index, body := range []string{
		`{"reason":"maintenance","unexpected":true}`,
		`{"reason":"maintenance"}{"reason":"second"}`,
	} {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/management/runners/runner-a/drain", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-Id", fmt.Sprintf("request-%d", index))
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want 400", body, resp.StatusCode)
		}
	}
}
