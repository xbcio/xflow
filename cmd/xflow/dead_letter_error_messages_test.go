package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store/memstore"
)

// TestHTTPStatusErrorMessagesMapStatusToOperatorText pins the status->message
// mapping in httpStatusError.Error(). Before this test, go tool cover showed
// Error() at 0% coverage: every existing test that produces an
// httpStatusError only checks errors.As(...) and the .status field, never the
// rendered message an operator actually sees on stderr. Swapping the 404 and
// 403 branches (or the 400 text, or the default fallback) would ship with the
// whole cmd/xflow suite green.
func TestHTTPStatusErrorMessagesMapStatusToOperatorText(t *testing.T) {
	cases := []struct {
		status int
		want   string
	}{
		{http.StatusNotFound, "execution or entry not found (status 404)"},
		{http.StatusForbidden, "replay unauthorized (status 403)"},
		{http.StatusBadRequest, "replay rejected: invalid request (status 400)"},
	}
	for _, tc := range cases {
		err := httpStatusError{status: tc.status, path: "/v1/management/dead-letters/x"}
		if got := err.Error(); got != tc.want {
			t.Fatalf("httpStatusError{status:%d}.Error() = %q, want %q", tc.status, got, tc.want)
		}
	}
}

// TestHTTPStatusErrorDefaultMessageCarriesStatusAndPath pins the fallback
// branch (any status not in {400,403,404}) which none of the envelope tests
// trigger — they only ever use 200 and 404. The default message is the only
// place an operator sees the actual numeric status for e.g. a 500 or 429, so
// a mutation that drops the status or the path from the format string would
// otherwise go unnoticed.
func TestHTTPStatusErrorDefaultMessageCarriesStatusAndPath(t *testing.T) {
	err := httpStatusError{status: http.StatusInternalServerError, path: "/v1/management/dead-letters/x/replay"}
	got := err.Error()
	if !strings.Contains(got, "500") {
		t.Fatalf("httpStatusError.Error() = %q, want it to contain the status code 500", got)
	}
	if !strings.Contains(got, "/v1/management/dead-letters/x/replay") {
		t.Fatalf("httpStatusError.Error() = %q, want it to contain the path", got)
	}
}

// TestRedactURLStripsQueryString pins redactURL's actual behavior: the query
// string must be stripped. go tool cover shows redactURL at 100% statement
// coverage (it is called from every error branch in do()), but no existing
// test ever inspects an error message for a query string — so the function
// being called is not the same as its redaction being verified. A regression
// that returns the URL unmodified (e.g. a future "helpful" change to include
// the cursor in error messages for debugging) would leak the cursor value
// into CLI stderr/error output undetected.
func TestRedactURLStripsQueryString(t *testing.T) {
	got := redactURL("http://example.com/v1/management/dead-letters/x?limit=100&cursor=super-secret-cursor")
	if strings.Contains(got, "super-secret-cursor") {
		t.Fatalf("redactURL(...) = %q, leaked the query string", got)
	}
	if strings.Contains(got, "?") {
		t.Fatalf("redactURL(...) = %q, want no query separator", got)
	}
	want := "http://example.com/v1/management/dead-letters/x"
	if got != want {
		t.Fatalf("redactURL(...) = %q, want %q", got, want)
	}
}

// TestRedactURLNoQueryIsUnchanged is the paired case: a URL with no query
// must be returned unchanged (not truncated or mangled by the query search).
func TestRedactURLNoQueryIsUnchanged(t *testing.T) {
	const u = "http://example.com/v1/management/dead-letters/x/replay"
	if got := redactURL(u); got != u {
		t.Fatalf("redactURL(%q) = %q, want unchanged", u, got)
	}
}

// TestAPIDeadLetterClientErrorMessagesRedactCursorFromRealFailure is the
// end-to-end version of the two unit tests above: a real List() call that
// fails with a non-2xx status must produce an error message that (a) is the
// mapped operator text and (b) never contains the caller-supplied cursor,
// even though the cursor rides in the request's query string. Without this,
// nothing in the suite ever calls List/Replay against a failing status code
// with a cursor set, so nothing proves do()'s error path actually redacts.
func TestAPIDeadLetterClientErrorMessagesRedactCursorFromRealFailure(t *testing.T) {
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeTestEnvelope(w, http.StatusInternalServerError, testEnvelope{Success: false, Code: "boom"})
	})
	_, err := c.List(context.Background(), "exec-env", "ns1", engine.DeadLetterPage{Limit: 100, Cursor: "super-secret-cursor"})
	if err == nil {
		t.Fatal("List against 500: err = nil, want error")
	}
	if strings.Contains(err.Error(), "super-secret-cursor") {
		t.Fatalf("error = %q, leaked the cursor query parameter", err.Error())
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error = %q, want it to surface the actual status code 500 (default httpStatusError branch)", err.Error())
	}
}

// TestReconcileReceiptReaderErrorExplainsTheLocalGap pins the one branch of the
// reconcile command that the shipped distributed backend can never reach: a
// StateStore without the engine.ReplayReceiptReader capability. The two CLI
// tests that drive reconcile end-to-end are MySQL-gated and skip wherever MySQL
// is unreachable, so without this test the message an operator actually sees is
// never executed in CI. The message must explain WHY the local backend cannot
// be reconciled by this command (its state is process-local, so a separate
// process cannot read the receipts) and name the in-process alternative
// (control.NewReceiptProjector) — a bare "does not implement" sends the
// operator hunting for a bug instead.
func TestReconcileReceiptReaderErrorExplainsTheLocalGap(t *testing.T) {
	_, err := receiptReaderFor(memstore.New())
	if err == nil {
		t.Fatal("receiptReaderFor(memstore): err = nil, want the missing-capability error")
	}
	got := err.Error()
	for _, want := range []string{"process-local", "control.NewReceiptProjector", "*memstore.Store"} {
		if !strings.Contains(got, want) {
			t.Fatalf("receiptReaderFor error = %q, want it to contain %q", got, want)
		}
	}
}

// TestReconcileReceiptReaderAcceptsAReader is the paired positive branch: a
// store that does implement the capability must be returned usable, with no
// error. The MySQL-gated CLI tests are the only other caller, so a regression
// that rejects every store (e.g. a future guard added to the wrong side of the
// type assertion) would otherwise surface only on a machine with MySQL.
func TestReconcileReceiptReaderAcceptsAReader(t *testing.T) {
	sentinel := errors.New("stub scan refuses")
	reader, err := receiptReaderFor(reconcileReaderStub{err: sentinel})
	if err != nil {
		t.Fatalf("receiptReaderFor(reconcileReaderStub): unexpected error: %v", err)
	}
	if reader == nil {
		t.Fatal("receiptReaderFor(reconcileReaderStub) = nil reader, want the store back")
	}
	if err := reader.ScanReplayReceipts(context.Background(), nil); err != sentinel {
		t.Fatalf("returned reader's scan error = %v, want %v (the exact store must come back)", err, sentinel)
	}
}

// reconcileReaderStub is a minimal engine.ReplayReceiptReader: the capability
// is a single-method interface, so no full StateStore double is needed.
type reconcileReaderStub struct{ err error }

func (s reconcileReaderStub) ScanReplayReceipts(context.Context, func(engine.ReplayReceipt) error) error {
	return s.err
}
