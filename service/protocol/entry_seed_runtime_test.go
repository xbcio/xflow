package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestHTTPEntrySeed_Accepted verifies an "accepted" response maps to
// EntrySeedResponse{Accepted:true} carrying the ExecutionID and Duplicate flag,
// and that the request is POSTed to /v1/executions with the bearer token and a
// well-formed SeedExecutionRequest body.
func TestHTTPEntrySeed_Accepted(t *testing.T) {
	var gotAuth, gotMethod, gotPath string
	var gotReq SeedExecutionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(SeedExecutionResponse{
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
	if gotReq.ProtocolVersion != EntrySeedProtocolVersion {
		t.Fatalf("ProtocolVersion = %d, want %d", gotReq.ProtocolVersion, EntrySeedProtocolVersion)
	}
	if len(gotReq.Exits) != 1 || gotReq.Exits[0].NodeName != "trigger" || gotReq.Exits[0].Port != "main" {
		t.Fatalf("exits not mapped correctly: %+v", gotReq.Exits)
	}
}

// TestEntrySeedRuntimeGeneration verifies the runtime stamps its per-activation
// Generation onto every seed request it sends on the wire, so the control-plane
// fence admits exactly the current-generation seeds (IMPORTANT-1).
func TestEntrySeedRuntimeGeneration(t *testing.T) {
	var gotReq SeedExecutionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(SeedExecutionResponse{
			State:       "accepted",
			ExecutionID: "exec-g",
		})
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client(), Generation: 7}
	if _, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-g", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry returned err: %v", err)
	}
	if gotReq.Generation != 7 {
		t.Fatalf("Generation = %d, want 7", gotReq.Generation)
	}
}

// TestHTTPEntrySeed_ConflictState maps a body State=="conflict" to Conflict:true.
func TestHTTPEntrySeed_ConflictState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(SeedExecutionResponse{
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

// TestHTTPEntrySeed_StaleGeneration409_ReturnsErr verifies that a 409 carrying
// the fence-rejection body ({"error":"stale_generation"}, i.e. NO
// state=="conflict") is treated as a transient/retry failure: it must return a
// NON-NIL error and must NOT report Conflict:true. This is offset-safety
// critical — a stale-generation fence rejection must leave the Kafka offset
// UNCOMMITTED so the correctly-generationed new owner eventually processes the
// message (otherwise: silent message loss during a generation upgrade).
func TestHTTPEntrySeed_StaleGeneration409_ReturnsErr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mimic apiserver writeError(w, 409, "stale_generation").
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"stale_generation"}`))
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client()}
	resp, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-stale", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	})
	if err == nil {
		t.Fatal("expected non-nil error on stale-generation 409, got nil")
	}
	if resp.Conflict {
		t.Fatalf("Conflict = true, want false (stale-generation must not be treated as handled conflict)")
	}
	if resp.Accepted || resp.Duplicate {
		t.Fatalf("expected zero-value response on stale-generation 409, got %+v", resp)
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
		_ = json.NewEncoder(w).Encode(SeedExecutionResponse{State: "accepted", ExecutionID: "e"})
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
