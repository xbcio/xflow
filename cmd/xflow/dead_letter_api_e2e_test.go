package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// TestDeadLetterCLIAPIPathListEndToEnd exercises the full CLI dispatch for the
// default (non-break-glass) API path: flag parsing -> deadLetterOptions ->
// newDeadLetterClient's success construction -> newDeadLetterListCommand's
// RunE -> apiDeadLetterClient.List -> writeJSONLines. Every other API-path
// test in this package (dead_letter_api_cli_test.go,
// dead_letter_api_env_test.go) constructs an *apiDeadLetterClient directly
// and never goes through executeRootWith/newDeadLetterClient, so a wiring bug
// in newDeadLetterClient's success branch (e.g. swapping opts.server and
// opts.token) would ship with every test in the package green. go tool cover
// confirms this: before this test, dead_letter.go:131 (the success
// construction inside newDeadLetterClient) and dead_letter.go:259-263 (the
// non-break-glass branch of newDeadLetterPrincipal) had zero executions
// across the whole suite.
func TestDeadLetterCLIAPIPathListEndToEnd(t *testing.T) {
	const wantToken = "e2e-token-xyz"
	var gotAuth, gotPath, gotNamespace string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotNamespace = r.Header.Get("X-XFlow-Namespace")
		dataBytes, err := json.Marshal(deadLetterListResponse{
			Entries: []engine.OutboxEntry{{ID: "e2e-entry-1"}},
		})
		if err != nil {
			t.Fatalf("marshal data: %v", err)
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Data: dataBytes})
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := executeRootWith(&out, "dead-letter",
		"--server", srv.URL, "--token", wantToken,
		"list", "--execution", "exec-e2e", "--namespace", "ns-e2e")
	if err != nil {
		t.Fatalf("dead-letter list (API path): %v", err)
	}

	// The wiring must have used wantToken as the bearer token, not the server
	// URL. A field swap in newDeadLetterClient's success construction sends
	// the server URL as the Authorization header instead.
	if gotAuth != "Bearer "+wantToken {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer "+wantToken)
	}
	if gotPath != "/v1/management/dead-letters/exec-e2e" {
		t.Fatalf("request path = %q, want /v1/management/dead-letters/exec-e2e", gotPath)
	}
	if gotNamespace != "ns-e2e" {
		t.Fatalf("X-XFlow-Namespace header = %q, want ns-e2e", gotNamespace)
	}

	var listed map[string]any
	line := strings.TrimSpace(out.String())
	if err := json.Unmarshal([]byte(line), &listed); err != nil {
		t.Fatalf("unmarshal CLI stdout line %q: %v", line, err)
	}
	if listed["id"] != "e2e-entry-1" {
		t.Fatalf("listed id = %v, want e2e-entry-1", listed["id"])
	}
}

// TestDeadLetterCLIAPIPathReplayEndToEnd is the replay-side counterpart: it
// drives the full CLI dispatch through executeRootWith over the default API
// path (never exercised end-to-end anywhere else in this package) and checks
// that the bearer token actually reaches the Authorization header and the
// replay result the server returns actually reaches CLI stdout.
func TestDeadLetterCLIAPIPathReplayEndToEnd(t *testing.T) {
	const wantToken = "e2e-token-replay"
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		dataBytes, err := json.Marshal(deadLetterReplayResponse{
			Outcome:     "replayed",
			AuditID:     "audit-e2e-1",
			ExecutionID: "exec-e2e-replay",
			NodeID:      "review",
		})
		if err != nil {
			t.Fatalf("marshal data: %v", err)
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Data: dataBytes})
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := executeRootWith(&out, "dead-letter",
		"--server", srv.URL, "--token", wantToken,
		"replay", "--execution", "exec-e2e-replay", "--entry", "e", "--reason", "e2e", "--namespace", "ns-e2e")
	if err != nil {
		t.Fatalf("dead-letter replay (API path): %v", err)
	}
	if gotAuth != "Bearer "+wantToken {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer "+wantToken)
	}
	if gotPath != "/v1/management/dead-letters/exec-e2e-replay/replay" {
		t.Fatalf("request path = %q, want /v1/management/dead-letters/exec-e2e-replay/replay", gotPath)
	}

	var res map[string]any
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal CLI stdout %q: %v", out.String(), err)
	}
	if res["audit_id"] != "audit-e2e-1" {
		t.Fatalf("audit_id = %v, want audit-e2e-1", res["audit_id"])
	}
	if res["outcome"] != "replayed" {
		t.Fatalf("outcome = %v, want replayed", res["outcome"])
	}
}
