package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// TestAPIDeadLetterClientEnvelopeUnwrap pins Step 1 decision (A): the CLI's
// apiDeadLetterClient.do() must unwrap the spec §3.1 envelope and decode the
// data field into the typed target. Before this client existed, do() decoded
// the body directly into out; enveloping would have decoded zero values
// silently (no error) — the exact failure mode decision (A) requires this
// test to prevent. Every CLI test that came before this one uses --break-glass
// (Redis-direct) and so never exercises do(); the g1 e2e covers the server
// half, not the CLI half.
//
// The cases:
//  1. list 2xx success envelope → DeadLetterList.Entries / NextCursor populated
//  2. replay 2xx success envelope → ReplayDeadLetterResult populated
//  3. bare 2xx body (no envelope) → must ERROR, not silently decode to zero
//  4. 404 failure envelope → httpStatusError with status=404
//
// Case 3 is the load-bearing guard against silent degradation: a future
// server-side change that drops the envelope (or a field-name drift) must
// surface here as a hard failure, not a green test reading zero values.

// testEnvelope mirrors the spec §3.1 shape the real apiserver emits.
type testEnvelope struct {
	Success bool            `json:"success"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	TraceID string          `json:"trace_id"`
}

func writeTestEnvelope(w http.ResponseWriter, status int, env testEnvelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

// newEnvelopeTestServer builds an httptest server whose handler runs a switch
// on req.URL.Path so each subtest can mount a different response shape. It
// returns the server and a close func the caller defers.
func newEnvelopeTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *apiDeadLetterClient) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c := &apiDeadLetterClient{server: srv.URL, token: "tok-test"}
	return srv, c
}

func TestAPIDeadLetterClientListSuccessEnvelope(t *testing.T) {
	// The list success body rides inside envelope.data as {entries,next_cursor}.
	// entries is []engine.OutboxEntry; each entry's Task carries a SignalPayload
	// in production — the exact content redactBody leaked. Here we assert do()
	// never reads the body into an error string and decodes data correctly.
	now := time.Now().UTC().Truncate(time.Millisecond)
	wantEntry := engine.OutboxEntry{
		ID:          "execute/exec-env/echo/0",
		Task:        engine.Task{ExecutionID: "exec-env", NodeName: "echo", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		AvailableAt: now,
		CreatedAt:   now,
		Attempts:    3,
	}
	listBody := deadLetterListResponse{
		Entries:    []engine.OutboxEntry{wantEntry},
		NextCursor: "cursor-next",
	}
	dataBytes, err := json.Marshal(listBody)
	if err != nil {
		t.Fatalf("marshal list data: %v", err)
	}
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/management/dead-letters/exec-env" {
			http.NotFound(w, r)
			return
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{
			Success: true,
			Code:    "200",
			Data:    dataBytes,
			TraceID: "trace-list",
		})
	})

	list, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if list.NextCursor != "cursor-next" {
		t.Fatalf("NextCursor = %q, want cursor-next", list.NextCursor)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(list.Entries))
	}
	got := list.Entries[0]
	if got.ID != wantEntry.ID {
		t.Fatalf("Entry.ID = %q, want %q", got.ID, wantEntry.ID)
	}
	if got.Attempts != 3 {
		t.Fatalf("Entry.Attempts = %d, want 3 (zero-value silent degradation would yield 0)", got.Attempts)
	}
	if got.Task.NodeName != "echo" {
		t.Fatalf("Entry.Task.NodeName = %q, want echo", got.Task.NodeName)
	}
	if got.Task.ExecutionID != "exec-env" {
		t.Fatalf("Entry.Task.ExecutionID = %q, want exec-env", got.Task.ExecutionID)
	}
}

func TestAPIDeadLetterClientReplaySuccessEnvelope(t *testing.T) {
	// The replay result rides inside envelope.data as {outcome,audit_id,...}.
	wantOutcome := string(engine.ReplayReplayed)
	wantAudit := "audit-12345"
	dataBytes, err := json.Marshal(deadLetterReplayResponse{
		Outcome:      wantOutcome,
		AuditID:      wantAudit,
		ExecutionID:  "exec-env",
		NodeID:       "echo",
		ActivationID: "1",
	})
	if err != nil {
		t.Fatalf("marshal replay data: %v", err)
	}
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/management/dead-letters/exec-env/replay" {
			http.NotFound(w, r)
			return
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{
			Success: true,
			Code:    "200",
			Data:    dataBytes,
			TraceID: "trace-replay",
		})
	})

	res, err := c.Replay(context.Background(), control.DeadLetterReplayPrincipal{}, engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-env",
		EntryID:     "execute/exec-env/echo/0",
		RequestID:   "req-1",
		Reason:      "env test",
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("Outcome = %q, want %q (silent degradation would yield empty)", res.Outcome, engine.ReplayReplayed)
	}
	if res.AuditID != wantAudit {
		t.Fatalf("AuditID = %q, want %q", res.AuditID, wantAudit)
	}
	if res.ExecutionID != types.ExecutionID("exec-env") {
		t.Fatalf("ExecutionID = %q, want exec-env", res.ExecutionID)
	}
	if res.NodeID != "echo" {
		t.Fatalf("NodeID = %q, want echo", res.NodeID)
	}
	if res.ActivationID != "1" {
		t.Fatalf("ActivationID = %q, want 1", res.ActivationID)
	}
}

// TestAPIDeadLetterClientBareBodyMustError is the most valuable case: a 2xx
// response that is NOT an envelope (the pre-migration shape) must surface as
// an error, not silently decode into a zero-value typed target. If do() ever
// regresses to decoding the body directly, this test goes red on the assertion,
// not on a compile error — because the regression is precisely "decode
// succeeded with zero values".
//
// A bare {entries,next_cursor} body has no success key. do() must distinguish
// "success field absent" (response was never enveloped — notEnvelopeError)
// from "success:false present" (a §4.1 violation — httpStatusError). The two
// need different operator diagnostics: collapsing them would hide a missing
// envelope behind a phantom failure report. The success:false@2xx path is
// pinned by TestAPIDeadLetterClient2xxSuccessFalseIsError.
func TestAPIDeadLetterClientBareBodyMustError(t *testing.T) {
	// A bare {entries:[...],next_cursor:""} body with no success/code wrapper.
	bareList := deadLetterListResponse{
		Entries:    []engine.OutboxEntry{{ID: "bare-should-fail"}},
		NextCursor: "bare-next",
	}
	bareBytes, err := json.Marshal(bareList)
	if err != nil {
		t.Fatalf("marshal bare list: %v", err)
	}
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bareBytes)
	})

	_, err = c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100})
	if err == nil {
		t.Fatal("List against a bare (non-envelope) 2xx body: err = nil, want error — silent degradation is exactly what decision (A) forbids")
	}
	// The bare body has no success key, so do() must surface notEnvelopeError —
	// the "response was never enveloped" diagnosis — NOT httpStatusError,
	// which is reserved for a present success:false at 2xx (a §4.1 violation).
	// Collapsing the two would point the operator at a phantom failure report.
	var notEnvErr notEnvelopeError
	if !errors.As(err, &notEnvErr) {
		t.Fatalf("error = %v (%T), want notEnvelopeError (bare body must be reported as a missing envelope, not a success:false@2xx violation)", err, err)
	}
	var httpErr httpStatusError
	if errors.As(err, &httpErr) {
		t.Fatalf("error = %v (%T), must NOT be httpStatusError (bare body is a missing envelope, not a §4.1 success:false@2xx violation)", err, err)
	}
}

func TestAPIDeadLetterClient404FailureEnvelope(t *testing.T) {
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeTestEnvelope(w, http.StatusNotFound, testEnvelope{
			Success: false,
			Code:    "dead_letter_not_found",
			Message: "no such entry",
		})
	})

	_, err := c.Replay(context.Background(), control.DeadLetterReplayPrincipal{}, engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-env",
		EntryID:     "missing",
		Reason:      "env test",
	})
	if err == nil {
		t.Fatal("Replay against 404: err = nil, want httpStatusError")
	}
	var httpErr httpStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %v (%T), want httpStatusError", err, err)
	}
	if httpErr.status != http.StatusNotFound {
		t.Fatalf("httpStatusError.status = %d, want 404", httpErr.status)
	}
}

func TestAPIDeadLetterClient2xxSuccessFalseIsError(t *testing.T) {
	// A 2xx with success:false is a spec violation; do() surfaces it as an
	// httpStatusError rather than trying to decode data (which carries no
	// payload on a failure envelope). This guards the §4.1 rule that success
	// tracks 2xx strictly.
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeTestEnvelope(w, http.StatusOK, testEnvelope{
			Success: false,
			Code:    "dead_letter_not_found",
		})
	})
	_, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100})
	if err == nil {
		t.Fatal("List against 2xx success:false: err = nil, want httpStatusError")
	}
	var httpErr httpStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %v (%T), want httpStatusError", err, err)
	}
	if httpErr.status != http.StatusOK {
		t.Fatalf("httpStatusError.status = %d, want 200 (the actual 2xx)", httpErr.status)
	}
}

// TestAPIDeadLetterClientMissingDataMustError covers the data-missing branch:
// a success envelope whose data field is null OR absent must surface as an
// error, not silently decode into a zero-value typed target. Both do() call
// sites (List, Replay) pass a non-nil out and expect a payload — there is no
// legitimate payload-less 2xx — so a missing data field is a field-name drift
// or a server omission, exactly the silent-degradation form decision (A)
// requires this test to prevent.
//
// The previous form of this test (TestAPIDeadLetterClientNullDataIsNoPayload)
// asserted that data:null returned no error and left the target at zero — it
// pinned the silent degradation as legitimate behavior. It is now inverted.
func TestAPIDeadLetterClientMissingDataMustError(t *testing.T) {
	t.Run("data null", func(t *testing.T) {
		// data is explicitly null. A success envelope with no payload is a
		// server omission, not a valid empty success — the §3.1 envelope puts
		// the payload in data; success without data is malformed.
		_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeTestEnvelope(w, http.StatusOK, testEnvelope{
				Success: true,
				Code:    "200",
				Data:    json.RawMessage("null"),
			})
		})
		_, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100})
		if err == nil {
			t.Fatal("List against data:null: err = nil, want error — a success envelope with no payload is a field drift, not a valid empty success")
		}
		var missingErr missingDataError
		if !errors.As(err, &missingErr) {
			t.Fatalf("error = %v (%T), want missingDataError", err, err)
		}
	})

	t.Run("data absent", func(t *testing.T) {
		// The data key is entirely absent (field-name drift / server
		// omission), not null. This is the real silent-degradation shape: a
		// JSON decode leaves env.Data at zero length with no error, and a naive
		// do() would return nil and a zero-value typed target.
		_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true,"code":"200","message":"","trace_id":"trace-absent"}`))
		})
		_, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100})
		if err == nil {
			t.Fatal("List against absent data: err = nil, want error — a success envelope without a data key is a field drift, not a valid empty success")
		}
		var missingErr missingDataError
		if !errors.As(err, &missingErr) {
			t.Fatalf("error = %v (%T), want missingDataError", err, err)
		}
	})
}
