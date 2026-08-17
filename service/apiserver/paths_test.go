package apiserver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// bodyFor returns the request body a real client would send for the given
// (method, path), so the positive cases reach the handler instead of failing
// at JSON decode. Negative cases never reach a handler, so they get no body.
func bodyFor(method, path string) io.Reader {
	if method == http.MethodPost && path == "/v1/executions/ex-1/signals" {
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(signalRequest{Name: "s1"})
		return &buf
	}
	return nil
}

// TestExecutionRouteShapesAfterMuxPattern proves the Go 1.22 mux pattern
// registration resolves every path shape the hand-rolled TrimPrefix parser
// resolved, and rejects the ones it rejected. A silently widened route is an
// authz hole: resolveExecutionRoute's default branch was what made an
// unrecognized shape 404 instead of reaching a handler, and the mux-pattern
// conversion must preserve that refusal.
//
// Per Decision B: the bare trailing-slash case "/v1/executions/" asserts
// "not 2xx" rather than a pin to 404 — under ServeMux a trailing-slash path
// may be answered with a 301 clean-path redirect, and what the case defends is
// "no handler ran". The other three negatives ("/wait/extra", "POST /wait",
// "GET /cancel") keep their exact 404 assertions: they are the authorization
// boundary, and the mux must still refuse them with 404 (not 405, which would
// leak that the route exists).
func TestExecutionRouteShapesAfterMuxPattern(t *testing.T) {
	cases := []struct {
		method, path string
		wantStatus   int
	}{
		{http.MethodGet, "/v1/executions/ex-1", http.StatusOK},
		{http.MethodGet, "/v1/executions/ex-1/wait", http.StatusOK},
		{http.MethodPost, "/v1/executions/ex-1/cancel", http.StatusOK},
		{http.MethodPost, "/v1/executions/ex-1/signals", http.StatusOK},
		// Shapes that must NOT resolve — each was rejected by the old parser's
		// default branch.
		{http.MethodGet, "/v1/executions/", http.StatusNotFound},
		{http.MethodGet, "/v1/executions/ex-1/wait/extra", http.StatusNotFound},
		{http.MethodPost, "/v1/executions/ex-1/wait", http.StatusNotFound},
		{http.MethodGet, "/v1/executions/ex-1/cancel", http.StatusNotFound},
	}

	// inspect returns a terminal status so handleWait returns immediately
	// instead of long-polling; DeliverSignal/Cancel succeed so the positive
	// cases reach 200.
	f := &fakeControlFacade{
		inspect: engine.ExecutionDetail{ExecutionID: "ex-1", Status: types.ExecutionStatusSuccess},
	}
	mux := newControlMux(f)

	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.path, bodyFor(c.method, c.path))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		// Decision B: the bare trailing-slash path may be answered with a 301
		// clean-path redirect rather than 404; assert "no handler ran" (not 2xx).
		if c.path == "/v1/executions/" {
			if rec.Code >= 200 && rec.Code < 300 {
				t.Errorf("%s %s: status = %d, want not 2xx (no handler should run)", c.method, c.path, rec.Code)
			}
			continue
		}
		if rec.Code != c.wantStatus {
			t.Errorf("%s %s: status = %d, want %d", c.method, c.path, rec.Code, c.wantStatus)
		}
	}
}
