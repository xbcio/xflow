package action

import (
	"errors"
	"net/url"
	"testing"
)

// safeMethodOp (http.go:209-214) extracts the operation name ("http", "Get",
// ...) from a *url.Error for transportErrorMessage's "op host: cause"
// message. Before this file nothing called it directly or indirectly checked
// its return value: http_test.go's TestHTTP_ConnectionErrorIsTransient and
// TestHTTP_ConnectionErrorDoesNotLeakQueryCredentials both feed a plain
// errors.New (not a *url.Error), so ue is always nil there and only the
// `ue == nil` branch of safeMethodOp ever ran. A mutation collapsing the
// function to `return "request"` unconditionally would drop ue.Op on every
// real *url.Error and still pass the whole suite.
func TestSafeMethodOp_NilErrorReturnsRequest(t *testing.T) {
	if got := safeMethodOp(nil); got != "request" {
		t.Fatalf("safeMethodOp(nil) = %q, want %q", got, "request")
	}
}

func TestSafeMethodOp_EmptyOpReturnsRequest(t *testing.T) {
	ue := &url.Error{Op: "", URL: "https://example.test", Err: errors.New("boom")}
	if got := safeMethodOp(ue); got != "request" {
		t.Fatalf("safeMethodOp(%+v) = %q, want %q", ue, got, "request")
	}
}

// TestSafeMethodOp_NonEmptyOpIsPreserved is the mutation's actual target: a
// *url.Error with a real Op set must come back unchanged, not replaced by the
// generic "request" fallback. net/http's client sets Op to strings like "Get"
// and "Post" (see net/http.(*Client).do), which is diagnostic information
// distinguishing "the GET failed" from "the redirect POST failed" -- losing
// it degrades every transport-error message the same way for every method.
func TestSafeMethodOp_NonEmptyOpIsPreserved(t *testing.T) {
	for _, op := range []string{"Get", "Post", "Head"} {
		ue := &url.Error{Op: op, URL: "https://example.test", Err: errors.New("boom")}
		if got := safeMethodOp(ue); got != op {
			t.Fatalf("safeMethodOp(%+v) = %q, want %q (the *url.Error's own Op, "+
				"not the generic fallback)", ue, got, op)
		}
	}
}
