package apiserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// registerBodyOfSize builds a syntactically valid POST /v1/workflows body of
// exactly n bytes by padding the workflow name. Using a real definition matters:
// a merely malformed body would also produce a 4xx and would not prove the size
// cap fired. The padding is plain ASCII, so each added character grows the
// marshaled body by exactly one byte; the first pass measures the fixed
// overhead (field names, commas) that the empty-name baseline cannot show when
// omitempty drops the field.
func registerBodyOfSize(t *testing.T, n int64) string {
	t.Helper()
	def := validWorkflow()
	def.Name = strings.Repeat("a", int(n))
	body, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal oversized body: %v", err)
	}
	excess := n - int64(len(body))
	padded := int64(len(def.Name)) + excess
	if padded <= 0 {
		t.Fatalf("body overhead exceeds requested size %d (body=%d bytes)", n, len(body))
	}
	def.Name = def.Name[:padded]
	body, err = json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal padded body: %v", err)
	}
	if int64(len(body)) != n {
		t.Fatalf("built body is %d bytes, want exactly %d", len(body), n)
	}
	return string(body)
}

// TestRegisterRejectsOversizedBodyWithoutPersisting pins the DoS bound on the
// registration route it exists for. On the authz path the pre-admission hook
// reads the body before the handler, so it must carry the cap itself; this
// asserts the observable contract: a registration body one byte over
// MaxRequestBodyBytes answers 413 request_too_large, and the handler never runs,
// so the registry gains no record. The body is valid JSON, so the rejection can
// only come from the size cap, not from a parse failure.
func TestRegisterRejectsOversizedBodyWithoutPersisting(t *testing.T) {
	srv, cp := newRegisterTestServer(t)
	ns := namespace.Namespace("namespaceA")
	before, err := cp.WorkflowRegistry().ListWorkflows(context.Background(), ns, backend.WorkflowListOptions{})
	if err != nil {
		t.Fatalf("ListWorkflows before: %v", err)
	}

	resp := postRawWorkflowBody(t, srv.URL, "tok-full", registerBodyOfSize(t, MaxRequestBodyBytes+1))
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body=%s)", resp.StatusCode, raw)
	}
	// The envelope must still carry trace_id even though this failure is written
	// before the handler (writeFail is the shared envelope path).
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode response envelope: %v (body=%s)", err, raw)
	}
	if _, ok := fields["trace_id"]; !ok {
		t.Fatalf("413 body must carry trace_id (body=%s)", raw)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode response envelope: %v (body=%s)", err, raw)
	}
	if env.Success {
		t.Fatalf("413 envelope must not be success: %+v", env)
	}
	if env.Code != "request_too_large" {
		t.Fatalf("code = %q, want request_too_large", env.Code)
	}
	if env.Message != "request body too large" {
		t.Fatalf("message = %q, want %q", env.Message, "request body too large")
	}
	// No internals in the rejection: not the cap value, not the reader's
	// stdlib error, not a server path or source location, not SQL.
	assertNo413Internals(t, raw)

	after, err := cp.WorkflowRegistry().ListWorkflows(context.Background(), ns, backend.WorkflowListOptions{})
	if err != nil {
		t.Fatalf("ListWorkflows after: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("registry records = %d after oversized request, want %d (handler must not run)", len(after), len(before))
	}
}

// TestRegisterAcceptsBodiesAtOrBelowLimit is the false-positive guard: an
// ordinary body still registers (201), and a valid body exactly at
// MaxRequestBodyBytes is not rejected by an off-by-one in the reader.
func TestRegisterAcceptsBodiesAtOrBelowLimit(t *testing.T) {
	srv, cp := newRegisterTestServer(t)
	ns := namespace.Namespace("namespaceA")

	normalResp := postRawWorkflowBody(t, srv.URL, "tok-full", registerBodyOfSize(t, 512))
	defer func() { _ = normalResp.Body.Close() }()
	if normalResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(normalResp.Body)
		t.Fatalf("normal body status = %d, want 201 (body=%s)", normalResp.StatusCode, body)
	}

	atLimit := registerBodyOfSize(t, MaxRequestBodyBytes)
	resp := postRawWorkflowBody(t, srv.URL, "tok-full", atLimit)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("exact-limit body status = %d, want 201 (body=%s)", resp.StatusCode, raw)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode response envelope: %v (body=%s)", err, raw)
	}
	if !env.Success {
		t.Fatalf("exact-limit envelope not success: %+v", env)
	}

	ids, err := cp.WorkflowRegistry().ListWorkflows(context.Background(), ns, backend.WorkflowListOptions{})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("registry records = %d, want 2 (normal + exact-limit registration)", len(ids))
	}
}

// assertNo413Internals pins the fixed 413 body: it may name only the stable
// code and the fixed message. It must not echo the configured cap, the stdlib
// reader error, any server path or source location, or SQL.
func assertNo413Internals(t *testing.T, raw []byte) {
	t.Helper()
	for _, leak := range []string{
		"8388608",
		"http: request body too large",
		"MaxRequestBodyBytes",
		"/Users/",
		".go:",
		"SELECT ",
	} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("413 body leaks %q: %s", leak, raw)
		}
	}
}

// countingReadCloser records how many bytes the caller actually consumed.
type countingReadCloser struct {
	r io.Reader
	n int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error { return nil }

// TestFAFPreAdmissionReadsBoundedBody proves the cap is enforced by the
// pre-admission hook itself, not merely by the decodeJSON call that follows it.
// Without it the hook would buffer the whole body before any decoder saw a
// limit, so an oversized request could exhaust memory while still being
// rejected later. The counting body makes the read cost observable: at most one
// probe byte past the cap may be consumed, and the body must not be restored
// for a handler that will never run.
func TestFAFPreAdmissionReadsBoundedBody(t *testing.T) {
	const total = MaxRequestBodyBytes + (1 << 20) // 1 MiB over the cap
	body := &countingReadCloser{r: strings.NewReader(registerBodyOfSize(t, total))}
	req := httptest.NewRequest(http.MethodPost, PathWorkflows, nil)
	req.Body = body
	rec := httptest.NewRecorder()

	if rejectFAFWorkflowRegistrationBeforeAdmission(rec, req) {
		t.Fatal("pre-admission accepted an oversized body")
	}
	// http.MaxBytesReader may consume one probe byte past the limit to detect
	// overflow; buffering the body would show up as a count near total here.
	if body.n > MaxRequestBodyBytes+1 {
		t.Fatalf("pre-admission read %d bytes, want at most %d", body.n, MaxRequestBodyBytes+1)
	}
	if body.n >= total {
		t.Fatalf("pre-admission buffered the entire %d-byte body", total)
	}
	if req.Body != body {
		t.Fatal("oversized body was restored for a handler that must not run")
	}

	raw := rec.Body.Bytes()
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body=%s)", rec.Code, raw)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode response envelope: %v (body=%s)", err, raw)
	}
	if env.Success || env.Code != "request_too_large" || env.Message != "request body too large" {
		t.Fatalf("envelope = %+v, want 413 request_too_large with the fixed message", env)
	}
	assertNo413Internals(t, raw)
}

// seedRequestBodyOfSize builds a syntactically valid POST /v1/executions body
// of exactly n bytes by padding the admission key. A valid body matters: a
// malformed one would also be refused, and the point is to show the size cap
// fired rather than the decoder tripping over garbage.
func seedRequestBodyOfSize(t *testing.T, n int64) string {
	t.Helper()
	const overhead = int64(len(`{"admission_key":""}`))
	if n <= overhead {
		t.Fatalf("requested seed body size %d does not fit the JSON overhead", n)
	}
	body := `{"admission_key":"` + strings.Repeat("a", int(n-overhead)) + `"}`
	if int64(len(body)) != n {
		t.Fatalf("built seed body is %d bytes, want exactly %d", len(body), n)
	}
	return body
}

// TestSeedExecutionRejectsOversizedBody pins the same size cap on the
// runner-protocol-face entry-seed route POST /v1/executions, whose decoder is
// deliberately bare. An ordinary seed is still accepted; an over-limit body
// answers 413 in that face's own shape — {"error":"request_too_large"}, the
// same stable code the enveloped routes use, with no envelope keys — and the
// counting body shows the read stopped at the cap instead of buffering the
// whole thing.
func TestSeedExecutionRejectsOversizedBody(t *testing.T) {
	srv := newSeedTestServer(t, false)

	// Regression: a normal seed is still accepted.
	resp := postSeed(t, srv.URL, "", protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-in",
		AdmissionKey:    "limit/default/wf-1/v1/kafka-in/topic/0/1-1",
		Outcome:         "success",
		Exits:           []protocol.BoundaryExit{{NodeName: "kafka-in", Port: "main", Data: map[string]any{"offset": float64(1)}}},
	})
	normalBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read normal seed response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("normal seed status = %d, want 200 (body=%s)", resp.StatusCode, normalBody)
	}

	const total = MaxRequestBodyBytes + (1 << 20) // 1 MiB over the cap
	body := &countingReadCloser{r: strings.NewReader(seedRequestBodyOfSize(t, total))}
	req := httptest.NewRequest(http.MethodPost, PathExecutions, nil)
	req.Body = body
	rec := httptest.NewRecorder()
	srv.Config.Handler.ServeHTTP(rec, req)

	// http.MaxBytesReader may consume one probe byte past the limit to detect
	// overflow; buffering the body would show up as a count near total here.
	if body.n > MaxRequestBodyBytes+1 {
		t.Fatalf("seed read %d bytes, want at most %d", body.n, MaxRequestBodyBytes+1)
	}
	if body.n >= total {
		t.Fatalf("seed buffered the entire %d-byte body", total)
	}
	t.Logf("POST /v1/executions read %d bytes of the %d-byte body", body.n, total)

	raw := rec.Body.Bytes()
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body=%s)", rec.Code, raw)
	}
	// This face is not enveloped: the 413 must be the bare errorResponse shape,
	// carrying the same stable code with no success/code/message/trace_id keys.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode 413 body: %v (body=%s)", err, raw)
	}
	for _, envelopeKey := range []string{"success", "code", "message", "trace_id"} {
		if _, ok := fields[envelopeKey]; ok {
			t.Fatalf("seed 413 body carries envelope key %q: %s", envelopeKey, raw)
		}
	}
	var bare errorResponse
	if err := json.Unmarshal(raw, &bare); err != nil {
		t.Fatalf("decode bare 413 body: %v (body=%s)", err, raw)
	}
	if bare.Error != "request_too_large" {
		t.Fatalf("error = %q, want request_too_large (body=%s)", bare.Error, raw)
	}
	assertNo413Internals(t, raw)
}

// TestRegisterFAFRejectionSurvivesPreAdmissionCap is the non-regression guard
// for the bounded pre-check: an under-limit FAF definition must still decode in
// the hook and answer the established 400 workflow_compile_failed before
// admission, and it must not reach the registry.
func TestRegisterFAFRejectionSurvivesPreAdmissionCap(t *testing.T) {
	srv, cp := newRegisterTestServer(t)
	def := validWorkflow()
	def.Name = "faf"
	def.Options = &types.WorkflowOptions{FAF: true}

	resp := postRegister(t, srv.URL, "tok-full", def)
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("status = %d, want 400 (body=%s)", resp.StatusCode, body)
	}
	env := decodeEnvelope(t, resp, nil)
	_ = resp.Body.Close()
	if env.Success || env.Code != "workflow_compile_failed" {
		t.Fatalf("envelope = %+v, want workflow_compile_failed", env)
	}

	records, err := cp.WorkflowRegistry().ListWorkflows(context.Background(), namespace.Namespace("namespaceA"), backend.WorkflowListOptions{})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("registry records = %d after FAF rejection, want 0", len(records))
	}
}
