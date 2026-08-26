package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// TestAPIDeadLetterClientListSendsCursor pins that apiDeadLetterClient.List
// forwards the caller's page cursor onto the request query string. The URL
// is built by string concatenation ("&cursor=" + page.Cursor); if that
// append were dropped, every page request would be byte-identical to the
// first page, so paginating through a large dead-letter list would silently
// loop on page one forever — no error, just a stuck "next page" that never
// advances.
func TestAPIDeadLetterClientListSendsCursor(t *testing.T) {
	var gotCursor string
	var gotHadCursorParam bool
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotHadCursorParam = r.URL.Query().Has("cursor")
		gotCursor = r.URL.Query().Get("cursor")
		dataBytes, err := json.Marshal(deadLetterListResponse{})
		if err != nil {
			t.Fatalf("marshal list data: %v", err)
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{
			Success: true,
			Code:    "200",
			Data:    dataBytes,
		})
	})

	const wantCursor = "page-2-cursor"
	if _, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100, Cursor: wantCursor}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if !gotHadCursorParam {
		t.Fatal("request URL carried no cursor query parameter; pagination would restart on page 1 every call")
	}
	if gotCursor != wantCursor {
		t.Fatalf("cursor query param = %q, want %q", gotCursor, wantCursor)
	}
}

// TestAPIDeadLetterClientListOmitsCursorOnFirstPage documents the paired
// behavior: an empty cursor (first page) must NOT add a "cursor=" parameter
// at all. Without this, the previous test could be satisfied by an
// unconditional append, which would send "cursor=" on the first page too.
func TestAPIDeadLetterClientListOmitsCursorOnFirstPage(t *testing.T) {
	var gotHadCursorParam bool
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotHadCursorParam = r.URL.Query().Has("cursor")
		dataBytes, err := json.Marshal(deadLetterListResponse{})
		if err != nil {
			t.Fatalf("marshal list data: %v", err)
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{
			Success: true,
			Code:    "200",
			Data:    dataBytes,
		})
	})

	if _, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if gotHadCursorParam {
		t.Fatal("first-page request carried a cursor query parameter; want none")
	}
}
