package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestHTTPEntrySeed_Accepted verifies an "accepted" response maps to
// EntrySeedResponse{Accepted:true} carrying the ExecutionID and Duplicate flag,
// and that the request is POSTed to /v1/executions with the bearer token and a
// well-formed SeedExecutionRequest body.
func TestHTTPEntrySeed_Accepted(t *testing.T) {
	var gotAuth, gotRunnerID, gotNamespace, gotMethod, gotPath string
	var gotReq SeedExecutionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotRunnerID = r.Header.Get(RunnerIDHeader)
		gotNamespace = r.Header.Get("X-Xflow-Namespace")
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

	rt := &HTTPEntrySeedRuntime{
		BaseURL: srv.URL, Client: srv.Client(), Token: "secret-token",
		RunnerID: "runner-issued", Namespace: "team-a",
	}
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
	if gotRunnerID != "runner-issued" {
		t.Fatalf("%s = %q, want runner-issued", RunnerIDHeader, gotRunnerID)
	}
	if gotNamespace != "team-a" {
		t.Fatalf("X-Xflow-Namespace = %q, want team-a", gotNamespace)
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

// recordingSeedObserver records every admission observation it receives.
type recordingSeedObserver struct {
	mu       sync.Mutex
	outcomes []string
	durs     []time.Duration
}

func (r *recordingSeedObserver) OnEntrySeedAdmission(_ context.Context, outcome string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, outcome)
	r.durs = append(r.durs, d)
}

func (r *recordingSeedObserver) snapshot() ([]string, []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.outcomes...), append([]time.Duration(nil), r.durs...)
}

// TestEntrySeedRequestTimeoutDefaultsTo15s pins the one property every existing
// embedder depends on: a runtime that configures nothing applies exactly the
// window it applied before the timeout became configurable. Turning the
// constant into an option is the whole change; a zero-value default smuggled in
// here would substitute an already-expired context deadline, turning every
// admission into an immediate deadline-exceeded and withholding every offset —
// an unbounded redelivery loop, not a slow one.
func TestEntrySeedRequestTimeoutDefaultsTo15s(t *testing.T) {
	rt := &HTTPEntrySeedRuntime{}
	if got := rt.requestTimeout(); got != 15*time.Second {
		t.Errorf("requestTimeout() = %v with nothing configured, want 15s", got)
	}
	if got := rt.requestTimeout(); got != DefaultEntrySeedRequestTimeout {
		t.Errorf("requestTimeout() = %v, want DefaultEntrySeedRequestTimeout (%v)",
			got, DefaultEntrySeedRequestTimeout)
	}
}

// TestEntrySeedRequestTimeoutRejectsNonPositive pins the other direction: a
// malformed (zero or negative) value must fall back to the default rather than
// being applied. This is asserted on the resolved value, not on the struct
// field, because the field is allowed to hold anything — what must never happen
// is a non-positive number reaching context.WithTimeout.
func TestEntrySeedRequestTimeoutRejectsNonPositive(t *testing.T) {
	for _, bad := range []time.Duration{0, -time.Second, -1} {
		rt := &HTTPEntrySeedRuntime{RequestTimeout: bad}
		if got := rt.requestTimeout(); got != DefaultEntrySeedRequestTimeout {
			t.Errorf("requestTimeout() = %v with RequestTimeout=%v, want the %v default",
				got, bad, DefaultEntrySeedRequestTimeout)
		}
	}
}

// TestEntrySeedRequestTimeoutConfiguredValueIsApplied proves the configured
// timeout reaches the code that APPLIES it, rather than only populating a
// struct field. The server deliberately answers far slower than the configured
// deadline, so the only way to get context.DeadlineExceeded back quickly is for
// the deadline to be the one that was configured: under the 15s default this
// test would block for the server's full delay and then succeed.
//
// This is the assertion shape that matters here. "The option set the field" is
// satisfied by a value that is then ignored at the request site, which is
// exactly the class of defect this whole change is about.
func TestEntrySeedRequestTimeoutConfiguredValueIsApplied(t *testing.T) {
	const configured = 60 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	rt := &HTTPEntrySeedRuntime{
		BaseURL: srv.URL, Client: srv.Client(), RequestTimeout: configured,
	}
	start := time.Now()
	_, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-timeout", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("SeedExecutionFromEntry succeeded against a server that never answered; want a deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	// Generous upper bound: this only has to fail if the 15s default were
	// applied instead of the configured deadline.
	if elapsed > 5*time.Second {
		t.Fatalf("admission took %v; the configured %v deadline was not applied "+
			"(the 15s default would look like this)", elapsed, configured)
	}
}

// TestEntrySeedObserverSeesTimeoutAndNotSuccess is the metric contract: an
// attempt that breaches the deadline is reported as "timeout", and an attempt
// that succeeds is reported as "accepted" and NOT as a timeout — the counter
// that an integrator alerts on must not fire on healthy admissions.
//
// Both directions are asserted on one observer because the defect that matters
// is not "the counter never increments" but "the counter cannot tell the two
// apart", and only a run that produces both outcomes can show that.
func TestEntrySeedObserverSeesTimeoutAndNotSuccess(t *testing.T) {
	obs := &recordingSeedObserver{}

	// 1. A server that never answers in time → timeout.
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	slowRT := &HTTPEntrySeedRuntime{
		BaseURL: slow.URL, Client: slow.Client(),
		RequestTimeout: 50 * time.Millisecond, Observer: obs,
	}
	if _, err := slowRT.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-slow", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	}); err == nil {
		t.Fatal("expected a deadline error from the non-answering server")
	}
	close(release)

	// 2. A healthy admission → accepted, and no timeout.
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(SeedExecutionResponse{State: "accepted", ExecutionID: "exec-ok"})
	}))
	defer fast.Close()
	fastRT := &HTTPEntrySeedRuntime{BaseURL: fast.URL, Client: fast.Client(), Observer: obs}
	if _, err := fastRT.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-fast", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	}); err != nil {
		t.Fatalf("healthy admission returned err: %v", err)
	}

	outcomes, durs := obs.snapshot()
	if len(outcomes) != 2 {
		t.Fatalf("observed %d admissions, want 2 (one timeout, one accepted): %v", len(outcomes), outcomes)
	}
	if outcomes[0] != "timeout" {
		t.Errorf("outcome[0] = %q, want %q: a breached deadline is the one outcome an "+
			"integrator must be able to alert on separately from a generic transport error",
			outcomes[0], "timeout")
	}
	if outcomes[1] != "accepted" {
		t.Errorf("outcome[1] = %q, want %q", outcomes[1], "accepted")
	}
	for i, d := range durs {
		if d <= 0 {
			t.Errorf("duration[%d] = %v, want a positive round-trip measurement", i, d)
		}
	}
	if durs[1] >= time.Second {
		t.Errorf("duration[1] = %v for an instantaneous local response, want a small value", durs[1])
	}
}

// TestEntrySeedObserverNilIsSafe pins that omitting the observer is not merely
// allowed but paid for with a single nil check: the runtime is constructed by
// every activation, so an observer field that was required would break every
// embedder that wires no metrics.
func TestEntrySeedObserverNilIsSafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(SeedExecutionResponse{State: "accepted", ExecutionID: "e"})
	}))
	defer srv.Close()

	rt := &HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: srv.Client()}
	if _, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
		AdmissionKey: "ak-nil", WorkflowID: "wf1", EntryUnitID: "g1", Outcome: "success",
	}); err != nil {
		t.Fatalf("admission with no observer returned err: %v", err)
	}
}

// TestAdmissionOutcomeClassification covers every outcome the counter can carry,
// including the ones the happy-path and timeout tests cannot reach. The label is
// a closed enum feeding a process-lifetime registry, so an unclassified shape
// must land in "error" rather than inventing a value.
func TestAdmissionOutcomeClassification(t *testing.T) {
	cases := []struct {
		name string
		resp types.EntrySeedResponse
		err  error
		want string
	}{
		{"accepted", types.EntrySeedResponse{Accepted: true}, nil, "accepted"},
		{"duplicate", types.EntrySeedResponse{Accepted: true, Duplicate: true}, nil, "duplicate"},
		{"conflict", types.EntrySeedResponse{Conflict: true}, nil, "conflict"},
		{"timeout", types.EntrySeedResponse{}, context.DeadlineExceeded, "timeout"},
		{"wrapped timeout", types.EntrySeedResponse{},
			fmt.Errorf("entry-seed: request failed: %w", context.DeadlineExceeded), "timeout"},
		{"transport error", types.EntrySeedResponse{}, errors.New("connection reset"), "error"},
		{"no flags and no error", types.EntrySeedResponse{}, nil, "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := admissionOutcome(tc.resp, tc.err); got != tc.want {
				t.Errorf("admissionOutcome = %q, want %q", got, tc.want)
			}
		})
	}
}
