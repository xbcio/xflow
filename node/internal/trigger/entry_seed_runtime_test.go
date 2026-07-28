package trigger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestHTTPEntrySeed_Accepted verifies an "accepted" response maps to
// EntrySeedResponse{Accepted:true} carrying the ExecutionID and Duplicate flag,
// and that the request is POSTed to /v1/executions with the bearer token and a
// well-formed SeedExecutionRequest body.
func TestHTTPEntrySeed_Accepted(t *testing.T) {
	var gotAuth, gotMethod, gotPath string
	var gotReq protocol.SeedExecutionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.SeedExecutionResponse{
			State:       "accepted",
			ExecutionID: "exec-x",
			Duplicate:   true,
		})
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client(), Token: "secret-token"}
	resp, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey:    "ak-1",
		WorkflowID:      "wf1",
		WorkflowVersion: "v1",
		EntryUnitID:     "g1",
		Outcome:         "success",
		Exits: []types.BoundaryExit{{
			NodeName: "trigger",
			Port:     "main",
			Data:     map[string]any{"value": "hello"},
		}},
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry returned err: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("Accepted = false, want true")
	}
	if !resp.Duplicate {
		t.Fatalf("Duplicate = false, want true")
	}
	if resp.Conflict {
		t.Fatalf("Conflict = true, want false")
	}
	if resp.ExecutionID != "exec-x" {
		t.Fatalf("ExecutionID = %q, want %q", resp.ExecutionID, "exec-x")
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v1/executions" {
		t.Fatalf("path = %q, want /v1/executions", gotPath)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("Authorization = %q, want %q", gotAuth, "Bearer secret-token")
	}
	if gotReq.AdmissionKey != "ak-1" || gotReq.WorkflowID != "wf1" || gotReq.EntryUnitID != "g1" || gotReq.Outcome != "success" {
		t.Fatalf("request body not mapped correctly: %+v", gotReq)
	}
	if gotReq.ProtocolVersion != protocol.EntrySeedProtocolVersion {
		t.Fatalf("ProtocolVersion = %d, want %d", gotReq.ProtocolVersion, protocol.EntrySeedProtocolVersion)
	}
	if len(gotReq.Exits) != 1 || gotReq.Exits[0].NodeName != "trigger" || gotReq.Exits[0].Port != "main" {
		t.Fatalf("exits not mapped correctly: %+v", gotReq.Exits)
	}
}

// TestHTTPEntrySeed_ConflictState maps a body State=="conflict" to Conflict:true.
func TestHTTPEntrySeed_ConflictState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(protocol.SeedExecutionResponse{
			State:       "conflict",
			ExecutionID: "exec-other",
		})
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client()}
	resp, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-c", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry returned err: %v", err)
	}
	if !resp.Conflict {
		t.Fatalf("Conflict = false, want true")
	}
	if resp.Accepted {
		t.Fatalf("Accepted = true, want false")
	}
	if resp.ExecutionID != "exec-other" {
		t.Fatalf("ExecutionID = %q, want %q", resp.ExecutionID, "exec-other")
	}
}

// TestHTTPEntrySeed_ServerError_ReturnsErr verifies a 5xx returns a non-nil
// error so the caller does NOT commit the Kafka offset.
func TestHTTPEntrySeed_ServerError_ReturnsErr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client()}
	resp, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-e", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	})
	if err == nil {
		t.Fatal("expected non-nil error on 5xx, got nil")
	}
	if resp.Accepted || resp.Conflict {
		t.Fatalf("expected zero-value response on error, got %+v", resp)
	}
}

// TestHTTPEntrySeed_NoToken_OmitsAuthHeader verifies that when Token is empty no
// Authorization header is sent.
func TestHTTPEntrySeed_NoToken_OmitsAuthHeader(t *testing.T) {
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadAuth = r.Header["Authorization"]
		_ = json.NewEncoder(w).Encode(protocol.SeedExecutionResponse{State: "accepted", ExecutionID: "e"})
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client()}
	if _, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hadAuth {
		t.Fatal("Authorization header should be absent when Token is empty")
	}
}
