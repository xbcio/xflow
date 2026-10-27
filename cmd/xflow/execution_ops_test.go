package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExecutionCancelPostsAndReportsCanceled(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{"accepted": true}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "execution",
		"--server", srv.URL, "--token", "tok-exec", "cancel", "exec-1"); err != nil {
		t.Fatalf("execution cancel: %v", err)
	}
	if rec.method != http.MethodPost {
		t.Fatalf("method = %q, want POST", rec.method)
	}
	if rec.path != "/v1/executions/exec-1/cancel" {
		t.Fatalf("path = %q, want /v1/executions/exec-1/cancel", rec.path)
	}
	if rec.auth != "Bearer tok-exec" {
		t.Fatalf("Authorization = %q, want Bearer tok-exec", rec.auth)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
	}
	if result["execution_id"] != "exec-1" {
		t.Fatalf("execution_id = %v, want exec-1", result["execution_id"])
	}
	if result["status"] != "canceled" {
		t.Fatalf("status = %v, want canceled", result["status"])
	}
	if result["accepted"] != true {
		t.Fatalf("accepted = %v, want true", result["accepted"])
	}
}

func TestExecutionCancelRejectsUnacceptedResponse(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{"accepted": false}, http.StatusOK, &rec)

	var out bytes.Buffer
	err := executeRootWith(&out, "execution",
		"--server", srv.URL, "--token", "tok", "cancel", "exec-1")
	if err == nil || !strings.Contains(err.Error(), "not accepted") {
		t.Fatalf("error = %v, want a not-accepted failure", err)
	}
}

func TestExecutionRerunPostsBodyAndIdempotencyKey(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{
		"execution_id":   "exec-new",
		"status":         "queued",
		"queue_position": 4,
		"duplicate":      true,
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "execution",
		"--server", srv.URL, "--token", "tok-rerun",
		"rerun", "exec-2", "--from-node", "review",
		"--include-success", "--idempotency-key", "idem-1"); err != nil {
		t.Fatalf("execution rerun: %v", err)
	}
	if rec.method != http.MethodPost || rec.path != "/v1/executions/exec-2/rerun" {
		t.Fatalf("request = %s %s, want POST /v1/executions/exec-2/rerun", rec.method, rec.path)
	}
	if rec.auth != "Bearer tok-rerun" {
		t.Fatalf("Authorization = %q, want Bearer tok-rerun", rec.auth)
	}
	if rec.idempotencyKey != "idem-1" {
		t.Fatalf("Idempotency-Key = %q, want idem-1", rec.idempotencyKey)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.body, &body); err != nil {
		t.Fatalf("request body is not JSON: %v (%q)", err, rec.body)
	}
	if body["from_node"] != "review" {
		t.Fatalf("from_node = %v, want review", body["from_node"])
	}
	if body["include_success"] != true {
		t.Fatalf("include_success = %v, want true", body["include_success"])
	}

	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
	}
	if result["execution_id"] != "exec-new" {
		t.Fatalf("execution_id = %v, want exec-new", result["execution_id"])
	}
	if result["status"] != "queued" {
		t.Fatalf("status = %v, want queued", result["status"])
	}
	if result["duplicate"] != true {
		t.Fatalf("duplicate = %v, want true", result["duplicate"])
	}
	if result["queue_position"] != float64(4) {
		t.Fatalf("queue_position = %v, want 4", result["queue_position"])
	}
}

func TestExecutionRerunOmitsIdempotencyKeyWhenUnset(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{"execution_id": "exec-new", "status": "running"}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "execution",
		"--server", srv.URL, "--token", "tok", "rerun", "exec-2", "--from-node", "start"); err != nil {
		t.Fatalf("execution rerun: %v", err)
	}
	if rec.idempotencyKey != "" {
		t.Fatalf("Idempotency-Key = %q, want absent", rec.idempotencyKey)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.body, &body); err != nil {
		t.Fatalf("request body is not JSON: %v (%q)", err, rec.body)
	}
	if body["include_success"] != false {
		t.Fatalf("include_success = %v, want false", body["include_success"])
	}
	if !strings.Contains(out.String(), `"duplicate":false`) {
		t.Fatalf("stdout should always carry duplicate: %q", out.String())
	}
}

func TestExecutionRerunRequiresFromNode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server was called despite the missing --from-node")
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := executeRootWith(&out, "execution",
		"--server", srv.URL, "--token", "tok", "rerun", "exec-2")
	if err == nil || !strings.Contains(err.Error(), "--from-node") {
		t.Fatalf("error = %v, want a --from-node requirement", err)
	}
}

func TestExecutionOpsCommandsRequireServerAndToken(t *testing.T) {
	t.Setenv("XFLOW_API_ADDR", "")
	t.Setenv("XFLOW_API_TOKEN", "")

	var out bytes.Buffer
	err := executeRootWith(&out, "execution", "cancel", "exec-1")
	if err == nil || !strings.Contains(err.Error(), "--server") {
		t.Fatalf("error = %v, want a --server requirement", err)
	}
	err = executeRootWith(&out, "execution", "--server", "http://127.0.0.1:1", "cancel", "exec-1")
	if err == nil || !strings.Contains(err.Error(), "--token") {
		t.Fatalf("error = %v, want a --token requirement", err)
	}
}

func TestExecutionOpsErrorMappingReachesTheCommand(t *testing.T) {
	cases := []struct {
		name   string
		status int
		code   string
		want   string
	}{
		{"not found", http.StatusNotFound, "execution_not_found", "status 404"},
		{"conflict", http.StatusConflict, "execution_not_rerunnable", "status 409"},
		{"forbidden", http.StatusForbidden, "forbidden", "scope"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeTestEnvelope(w, tc.status, testEnvelope{Success: false, Code: tc.code})
			}))
			defer srv.Close()
			var out bytes.Buffer
			err := executeRootWith(&out, "execution",
				"--server", srv.URL, "--token", "tok", "cancel", "exec-1")
			if err == nil {
				t.Fatal("cancel returned nil for a failure status")
			}
			var apiErr managementAPIError
			if !errors.As(err, &apiErr) || apiErr.status != tc.status {
				t.Fatalf("error = %v (%T), want managementAPIError status %d", err, err, tc.status)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestExecutionOpsCommandsAreRegistered(t *testing.T) {
	root := newRootCommand(io.Discard)
	for _, path := range [][]string{
		{"execution", "cancel"},
		{"execution", "rerun"},
	} {
		cmd, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("root.Find(%v): %v", path, err)
		}
		if cmd == nil || cmd.Name() != path[len(path)-1] {
			t.Fatalf("root.Find(%v) = %v, want command %q", path, cmd, path[len(path)-1])
		}
	}
}
