package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPageSizeIsCappedServerSide(t *testing.T) {
	// A client asking for 100000 gets 200. The cap is the gate that keeps a
	// sensitive-data enumeration endpoint from full-table traversal.
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows?page=1&page_size=100000", nil)
	_, size := pageParams(req)
	if size != 200 {
		t.Fatalf("page_size = %d, want it capped at 200", size)
	}
}

func TestPageIsOneBased(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	page, size := pageParams(req)
	if page != 1 {
		t.Errorf("default page = %d, want 1 (antd Table/Pagination are 1-based)", page)
	}
	if size != 20 {
		t.Errorf("default page_size = %d, want 20", size)
	}
}

func TestPageZeroOrNegativeFallsBackToOne(t *testing.T) {
	for _, raw := range []string{"0", "-1", "abc", ""} {
		req := httptest.NewRequest(http.MethodGet, "/v1/workflows?page="+raw, nil)
		if page, _ := pageParams(req); page != 1 {
			t.Errorf("page=%q → %d, want 1", raw, page)
		}
	}
}

// TestPageSizeZeroNegativeFallsBackToDefault covers the page_size analogue of
// the page fallback above. page_size=0 is the antd Table's initial "I haven't
// picked a size yet" value; treating it as "return nothing" (LIMIT 0) would
// make a freshly-loaded page look permanently empty, and is also the exact
// shape that lets an "assert no rows" test pass spuriously. Negative / non-
// numeric must not panic either — they come straight off the query string.
func TestPageSizeZeroNegativeFallsBackToDefault(t *testing.T) {
	for _, raw := range []string{"0", "-1", "-99999", "abc", "", "1.5", "0x10"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/workflows?page_size="+raw, nil)
		if _, size := pageParams(req); size != 20 {
			t.Errorf("page_size=%q → %d, want 20 (default)", raw, size)
		}
	}
}

// TestPageBeyondIntRangeFallsBack guards against a client sending
// page=99999999999999999999. strconv.Atoi on such a value errors (out of int
// range), and the fallback is the respective default — NOT a 500. An
// over-large/unparseable value is never a server fault; page falls back to 1
// (the caller renders an empty first page) and page_size to 20. Note this is
// strictly safer than clamping page_size to 200: an unparseable value is
// "client sent garbage", not "client asked for a big page", so the default
// applies — exactly as for "abc" or "".
func TestPageBeyondIntRangeFallsBack(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows?page=99999999999999999999", nil)
	if page, _ := pageParams(req); page != 1 {
		t.Errorf("overflow page → %d, want 1 (fallback on Atoi error)", page)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/workflows?page_size=99999999999999999999", nil)
	if _, size := pageParams(req); size != 20 {
		t.Errorf("overflow page_size → %d, want 20 (default fallback, not the 200 cap)", size)
	}
}
