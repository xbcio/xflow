package apiserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// TestHandleSeedExecution_StaleGeneration_BareErrorShapeNotEnveloped pins the
// load-bearing entry-seed 409 body shape. Per API-SPECIFICATION.md §0.1 + §8.2,
// POST /v1/executions is a RUNNER-PROTOCOL-FACE endpoint and must NOT be
// enveloped. Its 409 stale_generation body is byte-exactly
// {"error":"stale_generation"} — service/protocol/entry_seed_runtime.go
// distinguishes a genuine admission conflict (body has state=="conflict") from
// a generation-fence rejection (no state field) and commits or withholds a
// Kafka offset based on that. Getting this wrong means SILENT MESSAGE LOSS
// during a generation upgrade.
//
// Why an is-it-still-working assertion is insufficient here:
// the protocol runtime decodes the body into a struct carrying both `state` and
// `error` json fields. An ENVELOPED body
// {"success":false,"code":"conflict","message":"stale_generation","data":null,"trace_id":""}
// also decodes to State=="" (the `state` field is absent), so the fence branch
// STILL fires by accident — a behavioral test that only checks "did the runtime
// return an error" would pass even after the body was enveloped, while the real
// contract (a bare errorResponse carrying `error`) would be silently broken
// and the `code`/`trace_id` fields would be free to drift into the offset-safety
// discriminant later. Therefore this test asserts the BYTE shape directly: no
// `success`, no `code`, no `trace_id` key present, and the `error` value is
// exactly "stale_generation".
func TestHandleSeedExecution_StaleGeneration_BareErrorShapeNotEnveloped(t *testing.T) {
	f := &fakeControlFacade{seedErr: control.ErrStaleGeneration}
	mux := newControlMux(f)

	body := protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-in",
		AdmissionKey:    "default/wf-1/v1/kafka-in/topic/0/5-5",
		Outcome:         "success",
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/executions", &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}

	// json.Encoder.Encode appends a trailing newline; strip it for the exact
	// byte comparison.
	got := bytes.TrimRight(rec.Body.Bytes(), "\n")
	const want = `{"error":"stale_generation"}`
	if string(got) != want {
		t.Fatalf("409 body = %s, want exact %s (entry-seed must NOT be enveloped)", got, want)
	}

	// Belt-and-suspenders: decode and assert no envelope keys are present. A
	// future change that slips an envelope key in would be caught here even if
	// the exact-byte assertion above were loosened.
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, banned := range []string{"success", "code", "message", "data", "trace_id"} {
		if _, present := decoded[banned]; present {
			t.Errorf("envelope key %q is present in the 409 body — entry-seed must be bare", banned)
		}
	}
	if e, ok := decoded["error"]; !ok || string(e) != `"stale_generation"` {
		t.Errorf("error field = %v, want \"stale_generation\"", decoded["error"])
	}
}

// TestHandleSeedExecution_WorkflowUnknown_BareErrorShapeNotEnveloped pins the
// 404 branch of the same endpoint for the same reason: bare errorResponse, no
// envelope.
func TestHandleSeedExecution_WorkflowUnknown_BareErrorShapeNotEnveloped(t *testing.T) {
	f := &fakeControlFacade{seedErr: control.ErrEntrySeedWorkflowUnknown}
	mux := newControlMux(f)

	body := protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      "wf-missing",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-in",
		AdmissionKey:    "default/wf-missing/v1/kafka-in/topic/0/5-5",
		Outcome:         "success",
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/executions", &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	got := bytes.TrimRight(rec.Body.Bytes(), "\n")
	if string(got) != `{"error":"workflow_unknown"}` {
		t.Fatalf("404 body = %s, want exact {\"error\":\"workflow_unknown\"}", got)
	}
}

// TestHandleSeedExecution_InternalError_BareErrorShapeNotEnveloped pins the
// 500 branch: a generic message that leaks no internals, bare errorResponse.
func TestHandleSeedExecution_InternalError_BareErrorShapeNotEnveloped(t *testing.T) {
	f := &fakeControlFacade{seedErr: errBoomed}
	mux := newControlMux(f)

	body := protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-in",
		AdmissionKey:    "default/wf-1/v1/kafka-in/topic/0/5-5",
		Outcome:         "success",
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/executions", &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	got := bytes.TrimRight(rec.Body.Bytes(), "\n")
	if string(got) != `{"error":"internal server error"}` {
		t.Fatalf("500 body = %s, want exact {\"error\":\"internal server error\"}", got)
	}
}

// TestHandleSeedExecution_MissingFields_BareErrorShapeNotEnveloped pins the
// 400 branch: missing required fields still produce a bare errorResponse, not
// an envelope.
func TestHandleSeedExecution_MissingFields_BareErrorShapeNotEnveloped(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	// WorkflowID present but AdmissionKey/EntryUnitID/Outcome missing.
	body := protocol.SeedExecutionRequest{WorkflowID: "wf-1"}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/executions", &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	got := bytes.TrimRight(rec.Body.Bytes(), "\n")
	if string(got) != `{"error":"admission_key, workflow_id, entry_unit_id and outcome are required"}` {
		t.Fatalf("400 body = %s, want the bare missing-fields errorResponse", got)
	}
}

var errBoomed = errors.New("boom from facade")
