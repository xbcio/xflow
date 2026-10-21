package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func TestRunnerListSendsGETAndRendersTable(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, []map[string]any{
		{
			"runner_id":      "r1",
			"enrolled":       true,
			"state":          "online",
			"desired_state":  "draining",
			"last_heartbeat": "2026-10-21T10:00:00Z",
		},
		{"runner_id": "r2", "enrolled": false, "state": "never_connected"},
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok-runner", "list"); err != nil {
		t.Fatalf("runner list: %v", err)
	}
	if rec.method != http.MethodGet {
		t.Fatalf("method = %q, want GET", rec.method)
	}
	if rec.path != "/v1/management/runners" {
		t.Fatalf("path = %q, want /v1/management/runners", rec.path)
	}
	if rec.auth != "Bearer tok-runner" {
		t.Fatalf("Authorization = %q, want Bearer tok-runner", rec.auth)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("output has %d lines, want 3: %q", len(lines), out.String())
	}
	wantHeader := []string{"RUNNER_ID", "STATE", "DESIRED_STATE", "ENROLLED", "LAST_HEARTBEAT"}
	if got := strings.Fields(lines[0]); !equalStrings(got, wantHeader) {
		t.Fatalf("header = %v, want %v", got, wantHeader)
	}
	wantR1 := []string{"r1", "online", "draining", "true", "2026-10-21T10:00:00Z"}
	if got := strings.Fields(lines[1]); !equalStrings(got, wantR1) {
		t.Fatalf("r1 row = %v, want %v", got, wantR1)
	}
	wantR2 := []string{"r2", "never_connected", "-", "false", "-"}
	if got := strings.Fields(lines[2]); !equalStrings(got, wantR2) {
		t.Fatalf("r2 row = %v, want %v", got, wantR2)
	}
}

func TestRunnerListJSONOutputsServerArray(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, []map[string]any{
		{"runner_id": "r1", "enrolled": true, "state": "online"},
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok", "list", "-o", "json"); err != nil {
		t.Fatalf("runner list -o json: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal(out.Bytes(), &items); err != nil {
		t.Fatalf("stdout is not a JSON array: %v (%q)", err, out.String())
	}
	if len(items) != 1 || items[0]["runner_id"] != "r1" || items[0]["state"] != "online" {
		t.Fatalf("decoded roster = %v, want one online r1", items)
	}
}

func TestRunnerList501ExplainsBackendCannotEnumerate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestEnvelope(w, http.StatusNotImplemented, testEnvelope{
			Success: false,
			Code:    "runner_listing_unsupported",
		})
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := executeRootWith(&out, "runner", "--server", srv.URL, "--token", "tok", "list")
	if err == nil {
		t.Fatal("runner list returned nil for a 501")
	}
	msg := err.Error()
	if !strings.Contains(msg, "runner_listing_unsupported") || !strings.Contains(msg, "runner get") {
		t.Fatalf("error = %q, want the unsupported code and the runner get hint", msg)
	}
}

func TestRunnerGetTableShowsControlProjection(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{
		"runner_id":      "r1",
		"capacity":       8,
		"in_flight":      2,
		"last_heartbeat": "2026-10-21T10:00:00Z",
		"control": map[string]any{
			"desired_state": "draining",
			"generation":    3,
			"drain": map[string]any{
				"phase":            "quiescing",
				"deadline_at":      "2026-10-21T11:30:00Z",
				"server_quiescent": false,
				"runner_quiescent": true,
			},
		},
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok", "get", "r1"); err != nil {
		t.Fatalf("runner get: %v", err)
	}
	if rec.method != http.MethodGet || rec.path != "/v1/management/runners/r1" {
		t.Fatalf("request = %s %s, want GET /v1/management/runners/r1", rec.method, rec.path)
	}
	rendered := out.String()
	for _, want := range []string{
		"runner_id", "r1", "capacity", "8", "in_flight", "2",
		"desired_state", "draining", "generation", "3",
		"drain.phase", "quiescing", "drain.server_quiescent", "false",
		"drain.runner_quiescent", "true", "drain.deadline_at", "2026-10-21T11:30:00Z",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("runner get output missing %q:\n%s", want, rendered)
		}
	}
}

func TestRunnerGetWithoutControlRendersDashes(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{
		"runner_id":      "r1",
		"capacity":       1,
		"in_flight":      0,
		"last_heartbeat": "2026-10-21T10:00:00Z",
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok", "get", "r1"); err != nil {
		t.Fatalf("runner get: %v", err)
	}
	checked := map[string]bool{}
	for _, line := range strings.Split(out.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if fields[0] == "desired_state" || fields[0] == "generation" ||
			fields[0] == "drain.phase" || fields[0] == "drain.deadline_at" {
			checked[fields[0]] = true
			if fields[1] != "-" {
				t.Fatalf("row %q = %q, want - without a control snapshot", fields[0], fields[1])
			}
		}
	}
	for _, field := range []string{"desired_state", "generation", "drain.phase", "drain.deadline_at"} {
		if !checked[field] {
			t.Fatalf("missing %q row in output:\n%s", field, out.String())
		}
	}
}

func TestRunnerGetJSONOutputsServerSnapshot(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{
		"runner_id": "r1",
		"capacity":  4,
		"control":   map[string]any{"desired_state": "active", "generation": 1},
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok", "get", "r1", "-o", "json"); err != nil {
		t.Fatalf("runner get -o json: %v", err)
	}
	var snap map[string]any
	if err := json.Unmarshal(out.Bytes(), &snap); err != nil {
		t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
	}
	control, ok := snap["control"].(map[string]any)
	if !ok || control["desired_state"] != "active" {
		t.Fatalf("decoded snapshot = %v, want control.desired_state active", snap)
	}
}

func TestRunnerDrainSendsRequestIDReasonAndPrintsKey(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{
		"desired_state": "draining",
		"generation":    2,
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok-drain",
		"drain", "r1", "--reason", "maintenance window"); err != nil {
		t.Fatalf("runner drain: %v", err)
	}
	if rec.method != http.MethodPost || rec.path != "/v1/management/runners/r1/drain" {
		t.Fatalf("request = %s %s, want POST /v1/management/runners/r1/drain", rec.method, rec.path)
	}
	if rec.auth != "Bearer tok-drain" {
		t.Fatalf("Authorization = %q, want Bearer tok-drain", rec.auth)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`).MatchString(rec.requestID) {
		t.Fatalf("X-Request-Id = %q, want the server validation set", rec.requestID)
	}
	if !strings.HasPrefix(rec.requestID, "cli-") {
		t.Fatalf("generated X-Request-Id = %q, want a cli- prefix", rec.requestID)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.body, &body); err != nil {
		t.Fatalf("request body is not JSON: %v (%q)", err, rec.body)
	}
	if body["reason"] != "maintenance window" {
		t.Fatalf("reason = %q, want maintenance window", body["reason"])
	}

	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
	}
	if result["runner_id"] != "r1" {
		t.Fatalf("runner_id = %v, want r1", result["runner_id"])
	}
	if result["request_id"] != rec.requestID {
		t.Fatalf("printed request_id = %v, want the sent %q", result["request_id"], rec.requestID)
	}
	if result["desired_state"] != "draining" {
		t.Fatalf("desired_state = %v, want draining", result["desired_state"])
	}
	if result["generation"] != float64(2) {
		t.Fatalf("generation = %v, want 2", result["generation"])
	}
}

func TestRunnerDrainReusesExplicitRequestID(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{"desired_state": "draining", "generation": 1}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok",
		"drain", "r1", "--reason", "replay", "--request-id", "cli-replay-1"); err != nil {
		t.Fatalf("runner drain: %v", err)
	}
	if rec.requestID != "cli-replay-1" {
		t.Fatalf("X-Request-Id = %q, want cli-replay-1", rec.requestID)
	}
	if !strings.Contains(out.String(), `"request_id":"cli-replay-1"`) {
		t.Fatalf("stdout missing the replayed request id: %q", out.String())
	}
}

func TestRunnerDrainRequiresReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server was called despite the missing --reason")
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := executeRootWith(&out, "runner", "--server", srv.URL, "--token", "tok", "drain", "r1")
	if err == nil || !strings.Contains(err.Error(), "--reason") {
		t.Fatalf("error = %v, want a --reason requirement", err)
	}
}

// TestRunnerControlReportsGeneratedRequestIDOnFailure pins the recovery path
// for a lost response: the idempotency key travels in a header the error cannot
// echo, so a transport failure after a generated key must name that key. The
// operator can then retry with --request-id instead of applying a second
// transition. An operator-supplied key needs no such report.
func TestRunnerControlReportsGeneratedRequestIDOnFailure(t *testing.T) {
	const token = "tok-request-id-secret"
	// Stop the server before use: every call fails at the transport layer, which
	// is the shape a timeout or a dropped connection takes.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	t.Run("generated key is reported", func(t *testing.T) {
		var out bytes.Buffer
		err := executeRootWith(&out, "runner", "--server", srv.URL, "--token", token,
			"drain", "r1", "--reason", "lost response")
		if err == nil {
			t.Fatal("runner drain against a closed server returned nil")
		}
		msg := err.Error()
		match := regexp.MustCompile(`generated request id "(cli-[0-9a-f]+)"`).FindStringSubmatch(msg)
		if match == nil {
			t.Fatalf("error = %v, want it to carry the generated request id", err)
		}
		if !strings.Contains(msg, "--request-id "+match[1]) {
			t.Fatalf("error = %v, want a retry hint naming --request-id %s", err, match[1])
		}
		if strings.Contains(msg, token) {
			t.Fatalf("error leaked the bearer token: %v", err)
		}
	})

	t.Run("explicit key is not reported as generated", func(t *testing.T) {
		var out bytes.Buffer
		err := executeRootWith(&out, "runner", "--server", srv.URL, "--token", token,
			"resume", "r1", "--reason", "lost response", "--request-id", "cli-explicit-1")
		if err == nil {
			t.Fatal("runner resume against a closed server returned nil")
		}
		if msg := err.Error(); strings.Contains(msg, "generated request id") {
			t.Fatalf("error = %v, want no generated-id report for an explicit --request-id", err)
		}
	})
}

func TestRunnerResumePostsActive(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{"desired_state": "active", "generation": 4}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok",
		"resume", "r1", "--reason", "drain settled", "--request-id", "cli-resume-1"); err != nil {
		t.Fatalf("runner resume: %v", err)
	}
	if rec.method != http.MethodPost || rec.path != "/v1/management/runners/r1/resume" {
		t.Fatalf("request = %s %s, want POST /v1/management/runners/r1/resume", rec.method, rec.path)
	}
	if rec.requestID != "cli-resume-1" {
		t.Fatalf("X-Request-Id = %q, want cli-resume-1", rec.requestID)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
	}
	if result["desired_state"] != "active" {
		t.Fatalf("desired_state = %v, want active", result["desired_state"])
	}
}

func TestRunnerRevokeIdentityOutputsRunnerAndStatus(t *testing.T) {
	var rec recordedRequest
	srv := managementTestServer(t, map[string]any{
		"runner_id": "r1",
		"status":    "revoked",
	}, http.StatusOK, &rec)

	var out bytes.Buffer
	if err := executeRootWith(&out, "runner",
		"--server", srv.URL, "--token", "tok", "revoke-identity", "r1"); err != nil {
		t.Fatalf("runner revoke-identity: %v", err)
	}
	if rec.method != http.MethodPost || rec.path != "/v1/management/runners/r1/revoke-identity" {
		t.Fatalf("request = %s %s, want POST /v1/management/runners/r1/revoke-identity", rec.method, rec.path)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v (%q)", err, out.String())
	}
	if result["runner_id"] != "r1" || result["status"] != "revoked" {
		t.Fatalf("decoded result = %v, want r1/revoked", result)
	}
}

func TestRunnerCommandsRequireServerAndToken(t *testing.T) {
	t.Setenv("XFLOW_API_ADDR", "")
	t.Setenv("XFLOW_API_TOKEN", "")

	var out bytes.Buffer
	err := executeRootWith(&out, "runner", "list")
	if err == nil || !strings.Contains(err.Error(), "--server") {
		t.Fatalf("error = %v, want a --server requirement", err)
	}
	err = executeRootWith(&out, "runner", "--server", "http://127.0.0.1:1", "list")
	if err == nil || !strings.Contains(err.Error(), "--token") {
		t.Fatalf("error = %v, want a --token requirement", err)
	}
}

func TestRunnerCommandsAreRegistered(t *testing.T) {
	root := newRootCommand(io.Discard)
	for _, path := range [][]string{
		{"runner"},
		{"runner", "list"},
		{"runner", "get"},
		{"runner", "drain"},
		{"runner", "resume"},
		{"runner", "revoke-identity"},
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

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
