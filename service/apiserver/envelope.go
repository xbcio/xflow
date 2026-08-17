package apiserver

import (
	"encoding/json"
	"net/http"
	"reflect"
	"regexp"
	"strconv"

	"github.com/xbcio/xflow/observability/tracing"
)

// envelope is the response shape every user-facing JSON endpoint returns.
// See docs/design/API-SPECIFICATION.md §3.
//
// Data is deliberately `any` rather than json.RawMessage: handlers pass their
// own typed payloads and the encoder handles the rest. It is omitted entirely
// on failure so a client cannot mistake a zero value for real data.
type envelope struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
	TraceID string `json:"trace_id"`
}

// listPage is the data payload of a collection response: {list, total}.
type listPage struct {
	List  any `json:"list"`
	Total int `json:"total"`
}

// requestIDPattern is the validation set for an inbound X-Request-Id header:
// alphanumerics plus dot, underscore, hyphen, length 1..128. A value failing
// this is treated as absent (spec §5.2) — it is never a business error. This is
// a security control: the value reaches logs and audit rows, and an unvalidated
// one lets an external caller inject newlines to forge log lines (org policy
// §7 direction-inverse).
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// writeData sends a successful response. Code is always "200" on success — the
// HTTP status carries the finer distinction (201 created, 202 accepted).
func writeData(w http.ResponseWriter, r *http.Request, status int, data any) {
	writeEnvelope(w, r, status, envelope{
		Success: true,
		Code:    "200",
		Data:    data,
	})
}

// writeFail sends a failure response. code must be a stable, machine-readable
// business identifier (snake_case); message is human-readable and may change.
//
// Neither may carry node output, credentials, SQL, stack traces or server
// paths — see the security constraints in API-SPECIFICATION.md §3.5.
func writeFail(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeEnvelope(w, r, status, envelope{
		Success: false,
		Code:    code,
		Message: message,
	})
}

// writeList sends a collection response. A nil or empty list serializes as []
// rather than null: a null would make a frontend's .map() throw.
func writeList(w http.ResponseWriter, r *http.Request, list any, total int) {
	writeData(w, r, http.StatusOK, listPage{List: emptySliceIfNil(list), Total: total})
}

// emptySliceIfNil turns a nil slice into an empty one of the same element type
// so it encodes as [] instead of null. Non-slice values pass through.
func emptySliceIfNil(v any) any {
	if v == nil {
		return []any{}
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice && rv.IsNil() {
		return reflect.MakeSlice(rv.Type(), 0, 0).Interface()
	}
	return v
}

// writeEnvelope is the single shared user-facing response writer. It stamps the
// server-authoritative trace_id (spec §5.1) and, when the caller supplied a
// valid X-Request-Id, echoes it verbatim into the response header (spec §5.2).
// Centralizing the echo here means no handler can forget it; centralizing the
// validation here means no handler can let a forged value reach the logs.
func writeEnvelope(w http.ResponseWriter, r *http.Request, status int, env envelope) {
	if r != nil {
		env.TraceID = tracing.TraceIDFromContext(r.Context())
	}
	w.Header().Set("Content-Type", "application/json")
	if r != nil {
		if rid := sanitizeRequestID(r.Header.Get("X-Request-Id")); rid != "" {
			w.Header().Set("X-Request-Id", rid)
		}
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

// sanitizeRequestID returns v when it passes the spec §5.2 validation (length
// ≤ 128, charset [A-Za-z0-9._-]) and "" otherwise. An empty input returns "".
// A failed value is indistinguishable from absence — this is NOT a business
// failure and must never surface to the caller.
func sanitizeRequestID(v string) string {
	if v == "" || len(v) > 128 {
		return ""
	}
	if !requestIDPattern.MatchString(v) {
		return ""
	}
	return v
}

// codeForStatus derives a business code from an HTTP status, for call sites
// that have not yet been given an explicit one. Transitional: every user-facing
// handler should eventually pass its own stable code to writeFail.
func codeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large"
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusServiceUnavailable:
		return "service_unavailable"
	default:
		if status >= 500 {
			return "internal_error"
		}
		return "error_" + strconv.Itoa(status)
	}
}
