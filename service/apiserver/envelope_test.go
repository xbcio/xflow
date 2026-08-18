package apiserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEnvelopeSuccessShape(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows/wf-1", nil)
	writeData(rec, req, http.StatusOK, map[string]any{"workflow_id": "wf-1"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["success"] != true {
		t.Errorf("success = %v, want true", got["success"])
	}
	if got["code"] != "200" {
		t.Errorf("code = %v, want \"200\"", got["code"])
	}
	if got["message"] != "" {
		t.Errorf("message = %v, want empty on success", got["message"])
	}
	if _, ok := got["trace_id"]; !ok {
		t.Error("trace_id key missing — every response must carry one")
	}
	data, ok := got["data"].(map[string]any)
	if !ok || data["workflow_id"] != "wf-1" {
		t.Errorf("data = %v, want the payload verbatim", got["data"])
	}
}

func TestEnvelopeFailureCarriesNoDataAndAStableCode(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows/nope", nil)
	writeFail(rec, req, http.StatusNotFound, "workflow_not_found", "workflow not found")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — a real status code, never 200 carrying an error", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["success"] != false {
		t.Errorf("success = %v, want false to match the non-2xx status", got["success"])
	}
	if got["code"] != "workflow_not_found" {
		t.Errorf("code = %v, want the stable business code", got["code"])
	}
	// The contract (spec §3 / xflow-v1.yaml Envelope) says data is OMITTED on
	// failure — not `data: null`. Asserting key absence (not "value == nil",
	// which passes for both null and absent) is what gives this teeth: a future
	// loss of `omitempty` on envelope.Data would re-emit `data: null` and turn
	// this red.
	if _, ok := got["data"]; ok {
		t.Errorf("data key present (= %v) on a failure envelope — want the key omitted entirely (spec §3)", got["data"])
	}
	// Belt-and-suspenders: also assert no `null` sneaks in via a different code
	// path that re-adds the key without a payload.
	if got["data"] != nil {
		t.Errorf("data = %v, want absent/null on failure", got["data"])
	}
}

func TestEnvelopeListShape(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	writeList(rec, req, []string{}, 0)

	var got struct {
		Data struct {
			List  json.RawMessage `json:"list"`
			Total int             `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// An empty list must serialize as [] — a null makes the frontend's .map throw.
	if string(got.Data.List) != "[]" {
		t.Errorf("data.list = %s, want [] for an empty page, never null", got.Data.List)
	}
	if got.Data.Total != 0 {
		t.Errorf("data.total = %d, want 0", got.Data.Total)
	}
}

// TestEnvelope_UntypedNilListSerializesAsEmptyArrayNotNull guards the
// frontend's .map() for the untyped-nil branch of emptySliceIfNil
// (`if v == nil { return []any{} }`). This is the easy case — a literal nil
// passed as `any`.
func TestEnvelope_UntypedNilListSerializesAsEmptyArrayNotNull(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	writeList(rec, req, nil, 0)

	var got struct {
		Data struct {
			List json.RawMessage `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(got.Data.List) != "[]" {
		t.Fatalf("data.list = %s, want [] (never null) for an untyped nil", got.Data.List)
	}
}

// TestEnvelope_TypedNilListSerializesAsEmptyArrayNotNull is the load-bearing
// assertion: a TYPED nil slice — `var list []string` (or `[]Workflow`, etc.)
// returned by a store that found nothing — must encode as [], never null.
// This is the reflect.MakeSlice branch of emptySliceIfNil, NOT the trivial
// `v == nil` branch: a typed nil is a non-nil `any` wrapping a nil slice, so
// `v == nil` is false and without the reflect branch it would pass straight
// through and encode as null (making the frontend's .map() throw).
//
// Teeth verified: deleting the reflect branch (leaving `if v == nil { ... };
// return v`) makes this test go RED with:
//   data.list = null, want [] (never null) for a typed nil []string
// while the untyped-nil test above stays green.
func TestEnvelope_TypedNilListSerializesAsEmptyArrayNotNull(t *testing.T) {
	var list []string // declared, not literal nil — typed nil
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	writeList(rec, req, list, 0)

	var got struct {
		Data struct {
			List json.RawMessage `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(got.Data.List) != "[]" {
		t.Fatalf("data.list = %s, want [] (never null) for a typed nil []string", got.Data.List)
	}
}

// --- X-Request-Id (spec §5.2) ---

// TestXRequestID_ValidValueRoundTrips verifies that a valid client-supplied
// X-Request-Id is echoed verbatim into the response header. This is the
// spec-mandated happy path; the value never enters the envelope body.
func TestXRequestID_ValidValueRoundTrips(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows/wf-1", nil)
	req.Header.Set("X-Request-Id", "req-abc.123_def-789")
	writeData(rec, req, http.StatusOK, map[string]any{"ok": true})

	if got := rec.Header().Get("X-Request-Id"); got != "req-abc.123_def-789" {
		t.Fatalf("X-Request-Id response header = %q, want the client value verbatim", got)
	}
	// And it must NOT appear in the envelope body — trace_id is the only
	// request-correlation field in the body, and it is server-authoritative.
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := body["request_id"]; ok {
		t.Error("request_id must not appear in the envelope body (trace_id is separate)")
	}
}

// TestXRequestID_OverlongValueDroppedSilently verifies that a value longer
// than 128 characters is treated as absent — the header is NOT echoed, and the
// request still succeeds (this is not a business failure).
func TestXRequestID_OverlongValueDroppedSilently(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows/wf-1", nil)
	long := strings.Repeat("a", 129)
	req.Header.Set("X-Request-Id", long)
	writeData(rec, req, http.StatusOK, map[string]any{"ok": true})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — an invalid X-Request-Id is never a business failure", rec.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "" {
		t.Fatalf("X-Request-Id response header = %q, want empty (over-long value must be dropped)", got)
	}
}

// TestXRequestID_NewlineOrSpaceDroppedSilently verifies the security-critical
// branch: a value containing a newline or space (the log-injection vector) is
// rejected — treated as absent, not echoed, and not a business failure.
func TestXRequestID_NewlineOrSpaceDroppedSilently(t *testing.T) {
	for _, bad := range []string{"req-with-newline\ninject", "req with space"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/workflows/wf-1", nil)
		req.Header.Set("X-Request-Id", bad)
		writeData(rec, req, http.StatusOK, map[string]any{"ok": true})

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d for %q, want 200 — invalid X-Request-Id is never a business failure", rec.Code, bad)
		}
		if got := rec.Header().Get("X-Request-Id"); got != "" {
			t.Fatalf("X-Request-Id response header = %q for input %q, want empty (must be dropped)", got, bad)
		}
	}
}

// TestXRequestID_AbsenceIsFine verifies that a request without the header is
// served normally and no X-Request-Id response header is set.
func TestXRequestID_AbsenceIsFine(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows/wf-1", nil)
	writeData(rec, req, http.StatusOK, map[string]any{"ok": true})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "" {
		t.Fatalf("X-Request-Id response header = %q, want empty when the client sent none", got)
	}
}
