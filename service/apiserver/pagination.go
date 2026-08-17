package apiserver

import (
	"net/http"
	"strconv"
)

const (
	// defaultPageSize is applied when page_size is absent, empty, zero, or
	// unparseable. 20 matches antd Table's default and the spec §3.3 default.
	defaultPageSize = 20
	// maxPageSize is the hard server-side cap on page_size. This is a SECURITY
	// CONTROL, not a performance knob: org policy §2 mandates that sensitive
	// data enumeration endpoints must be authenticated AND paginated and may
	// not allow full-table traversal. A client asking for page_size=100000
	// without this gate would dump an entire namespace in one request. 200 is
	// the largest page any legitimate list UI renders at once.
	maxPageSize = 200
	// defaultPage is applied when page is absent, empty, zero, negative, or
	// unparseable. Pagination is 1-based: antd Table/Pagination default to
	// page 1, and a 0-based scheme forces every frontend to remember to add
	// 1 server-side or subtract 1 client-side — a class of off-by-one that
	// spreads virally (spec §3.3).
	defaultPage = 1
)

// pageParams parses the offset pagination parameters from a list request's
// query string. It is 1-based: page=1 is the first page. Defaults are page=1,
// page_size=20. page_size is clamped to maxPageSize (200) regardless of what
// the client sends — see the comment on maxPageSize for why this is a gate.
//
// Unparseable, empty, zero, or negative values all fall back to the defaults
// rather than producing an error. Rationale: these come straight off the query
// string from frontend table components, and a malformed value is never a
// business-level failure worth surfacing as a 4xx — it's a degraded request
// that still returns a usable (first, default-sized) page. This also means a
// test or probe that omits the params entirely gets deterministic behavior.
//
// This function only parses parameters; it makes no claims about whether the
// list endpoint it serves is registered. As of this writing no list endpoint
// is registered (see docs/design/API-SPECIFICATION.md §9).
func pageParams(r *http.Request) (page, pageSize int) {
	page = defaultPage
	pageSize = defaultPageSize

	q := r.URL.Query()

	if raw := q.Get("page"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			page = v
		}
	}
	if raw := q.Get("page_size"); raw != "" {
		// Only honor a strictly positive integer; zero/negative/non-numeric
		// all fall back to the default. Atoi rejects "1.5" and "0x10".
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			pageSize = v
		}
	}

	// Clamp the upper bound. pageSize could legitimately be larger than max
	// only via a client request; we never produce a larger value ourselves.
	// The check is `>` not `>=` so that exactly maxPageSize passes through.
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return page, pageSize
}
