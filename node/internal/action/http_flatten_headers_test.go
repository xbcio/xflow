package action

import (
	"net/http"
	"testing"
)

// flattenHeaders (http.go:413-423) is the only place HTTPNode.Execute builds
// out.Data["headers"]. Before this file, nothing in the package read that
// key: http_test.go's fixtures only check out.Data["status"] and
// out.Data["body"]. That leaves flattenHeaders free to be edited into
// `return map[string]any{}` -- dropping every response header from the node's
// output -- and the suite would stay green, silently breaking any workflow
// branch that reads a response header (Location, ETag, a custom
// rate-limit header, ...).
//
// These are white-box tests of flattenHeaders itself. The one line that puts
// its result into the node's output is pinned separately, in
// http_headers_output_test.go: deleting that line leaves every test in this
// file green.

// TestFlattenHeaders_SingleValueUnwrapsToScalar pins that a header with one
// value comes out as a bare scalar (http.go:416-417), not a one-element
// slice. Workflow authors read this as headers["content-type"], a plain
// string comparison; a slice there would break every such comparison even
// though the data is "present".
func TestFlattenHeaders_SingleValueUnwrapsToScalar(t *testing.T) {
	h := http.Header{"Content-Type": []string{"application/json"}}
	got := flattenHeaders(h)
	if len(got) != 1 {
		t.Fatalf("flattenHeaders(%v) = %v, want exactly 1 entry", h, got)
	}
	v, ok := got["Content-Type"]
	if !ok {
		t.Fatalf("flattenHeaders(%v) = %v, missing key %q", h, got, "Content-Type")
	}
	s, ok := v.(string)
	if !ok || s != "application/json" {
		t.Fatalf("flattenHeaders(%v)[%q] = %#v, want the bare string %q",
			h, "Content-Type", v, "application/json")
	}
}

// TestFlattenHeaders_MultiValueStaysASlice pins the other half of the
// len(v)==1 branch: a repeated header (e.g. multiple Set-Cookie) must survive
// as all of its values, not collapse to just the first one.
func TestFlattenHeaders_MultiValueStaysASlice(t *testing.T) {
	h := http.Header{"Set-Cookie": []string{"a=1", "b=2"}}
	got := flattenHeaders(h)
	v, ok := got["Set-Cookie"].([]string)
	if !ok {
		t.Fatalf("flattenHeaders(%v)[%q] = %#v (%T), want []string", h, "Set-Cookie", got["Set-Cookie"], got["Set-Cookie"])
	}
	if len(v) != 2 || v[0] != "a=1" || v[1] != "b=2" {
		t.Fatalf("flattenHeaders(%v)[%q] = %v, want [a=1 b=2]", h, "Set-Cookie", v)
	}
}

// TestFlattenHeaders_ReturnsEveryHeaderPresent is the direct guard against the
// candidate mutation: replacing the function body with an unconditional
// `return map[string]any{}`. A single-header input already exercises that,
// but this asserts on the full set and the exact count, so a mutation that
// drops even one header out of several -- not just all of them -- is also
// caught.
func TestFlattenHeaders_ReturnsEveryHeaderPresent(t *testing.T) {
	h := http.Header{
		"X-Request-Id": []string{"req-123"},
		"X-RateLimit":  []string{"100"},
		"Location":     []string{"https://example.test/next"},
	}
	got := flattenHeaders(h)
	if len(got) != 3 {
		t.Fatalf("flattenHeaders(%v) = %v (%d entries), want 3", h, got, len(got))
	}
	want := map[string]string{
		"X-Request-Id": "req-123",
		"X-RateLimit":  "100",
		"Location":     "https://example.test/next",
	}
	for k, wantV := range want {
		gotV, ok := got[k]
		if !ok {
			t.Fatalf("flattenHeaders(%v) is missing key %q entirely -- an empty-map "+
				"mutation would fail exactly this way", h, k)
		}
		if gotV != wantV {
			t.Fatalf("flattenHeaders(%v)[%q] = %v, want %v", h, k, gotV, wantV)
		}
	}
}

// TestFlattenHeaders_EmptyInputReturnsEmptyMap keeps the equivalence-class
// boundary honest: an actually-empty http.Header must still produce an empty
// (non-nil-shaped, len==0) map, so the assertions above are checking real
// content, not merely "non-empty".
func TestFlattenHeaders_EmptyInputReturnsEmptyMap(t *testing.T) {
	got := flattenHeaders(http.Header{})
	if len(got) != 0 {
		t.Fatalf("flattenHeaders(empty) = %v, want empty map", got)
	}
}
