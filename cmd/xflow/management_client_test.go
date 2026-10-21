package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mustRawJSON marshals v into the envelope data field used by the CLI tests.
func mustRawJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal test data: %v", err)
	}
	return raw
}

// recordedRequest captures the request one command made against the fake
// management API. The handler completes before the CLI call returns, so the
// assertions after executeRootWith observe a finished request.
type recordedRequest struct {
	method         string
	path           string
	auth           string
	requestID      string
	idempotencyKey string
	contentType    string
	body           []byte
}

// managementTestServer records the last request and answers with a fixed
// success-envelope data payload.
func managementTestServer(t *testing.T, data any, status int, record *recordedRequest) *httptest.Server {
	t.Helper()
	raw := mustRawJSON(t, data)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record.method = r.Method
		record.path = r.URL.Path
		record.auth = r.Header.Get("Authorization")
		record.requestID = r.Header.Get("X-Request-Id")
		record.idempotencyKey = r.Header.Get("Idempotency-Key")
		record.contentType = r.Header.Get("Content-Type")
		if r.Body != nil {
			record.body, _ = io.ReadAll(r.Body)
		}
		writeTestEnvelope(w, status, testEnvelope{Success: true, Data: raw})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestManagementClientMapsStatusCodesToActionableErrors pins the operator-facing
// mapping for every status the runner and execution commands can hit. The
// message must name the status and the corrective action without echoing the
// response body or the bearer token.
func TestManagementClientMapsStatusCodesToActionableErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		code   string
		wants  []string
	}{
		{
			name:   "bad request",
			status: http.StatusBadRequest,
			code:   "bad_request",
			wants:  []string{"status 400", "check the command arguments", "bad_request"},
		},
		{
			name:   "forbidden names the missing scope",
			status: http.StatusForbidden,
			code:   "forbidden",
			wants:  []string{"status 403", "scope", "forbidden"},
		},
		{
			name:   "not found",
			status: http.StatusNotFound,
			code:   "runner_not_found",
			wants:  []string{"status 404", "not found", "runner_not_found"},
		},
		{
			name:   "conflict explains request id reuse",
			status: http.StatusConflict,
			code:   "idempotency_key_reused",
			wants:  []string{"status 409", "--request-id", "idempotency_key_reused"},
		},
		{
			name:   "not implemented",
			status: http.StatusNotImplemented,
			code:   "runner_control_unsupported",
			wants:  []string{"status 501", "does not support", "runner_control_unsupported"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeTestEnvelope(w, tc.status, testEnvelope{Success: false, Code: tc.code})
			}))
			defer srv.Close()
			client := &managementClient{server: srv.URL, token: "tok-secret", httpClient: srv.Client()}
			var out map[string]any
			err := client.do(context.Background(), http.MethodPost, "/v1/management/runners/r1/drain", nil, nil, &out)
			if err == nil {
				t.Fatal("do() returned nil for a failure status")
			}
			var apiErr managementAPIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("error %T is not managementAPIError: %v", err, err)
			}
			if apiErr.status != tc.status {
				t.Fatalf("managementAPIError.status = %d, want %d", apiErr.status, tc.status)
			}
			msg := err.Error()
			for _, want := range tc.wants {
				if !strings.Contains(msg, want) {
					t.Fatalf("error %q does not contain %q", msg, want)
				}
			}
			if strings.Contains(msg, "tok-secret") {
				t.Fatalf("error %q leaked the bearer token", msg)
			}
		})
	}
}

// TestManagementClient501ListingSuggestsGet pins the one 501 that has a
// recovery path: an enumeration-unsupported backend still serves point lookups.
func TestManagementClient501ListingSuggestsGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestEnvelope(w, http.StatusNotImplemented, testEnvelope{
			Success: false,
			Code:    "runner_listing_unsupported",
		})
	}))
	defer srv.Close()
	client := &managementClient{server: srv.URL, token: "tok", httpClient: srv.Client()}
	var out []runnerListItem
	err := client.do(context.Background(), http.MethodGet, "/v1/management/runners", nil, nil, &out)
	if err == nil {
		t.Fatal("do() returned nil for a 501")
	}
	if got := err.Error(); !strings.Contains(got, "runner get") || !strings.Contains(got, "runner_listing_unsupported") {
		t.Fatalf("501 listing error = %q, want it to name the code and suggest runner get", got)
	}
}

// TestManagementClientRejectsUnenvelopedSuccess guards against silent
// zero-value success: a 2xx body without the success key must be an error, not
// a decoded empty result.
func TestManagementClientRejectsUnenvelopedSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"runner_id":"r1"}`))
	}))
	defer srv.Close()
	client := &managementClient{server: srv.URL, token: "tok", httpClient: srv.Client()}
	var out map[string]any
	err := client.do(context.Background(), http.MethodGet, "/v1/management/runners/r1", nil, nil, &out)
	var notEnv notEnvelopeError
	if !errors.As(err, &notEnv) {
		t.Fatalf("error = %v (%T), want notEnvelopeError", err, err)
	}
}

// TestManagementClientRejectsSuccessWithoutData is the paired guard: a success
// envelope with no data must not decode as a zero-value result.
func TestManagementClientRejectsSuccessWithoutData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true})
	}))
	defer srv.Close()
	client := &managementClient{server: srv.URL, token: "tok", httpClient: srv.Client()}
	var out map[string]any
	err := client.do(context.Background(), http.MethodGet, "/v1/management/runners/r1", nil, nil, &out)
	var missing missingDataError
	if !errors.As(err, &missing) {
		t.Fatalf("error = %v (%T), want missingDataError", err, err)
	}
}

// TestNewManagementRequestIDMatchesServerValidation pins the generated key to
// the server's X-Request-Id validation set and its uniqueness.
func TestNewManagementRequestIDMatchesServerValidation(t *testing.T) {
	first, err := newManagementRequestID()
	if err != nil {
		t.Fatalf("newManagementRequestID: %v", err)
	}
	second, err := newManagementRequestID()
	if err != nil {
		t.Fatalf("newManagementRequestID: %v", err)
	}
	if first == second {
		t.Fatalf("two generated request ids are equal: %q", first)
	}
	if !strings.HasPrefix(first, "cli-") {
		t.Fatalf("request id %q does not start with cli-", first)
	}
	for _, r := range first {
		valid := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if !valid {
			t.Fatalf("request id %q contains an invalid character %q", first, r)
		}
	}
	if len(first) > 128 {
		t.Fatalf("request id length = %d, want <= 128", len(first))
	}
}
