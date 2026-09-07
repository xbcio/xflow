package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// managementCeilingBytes mirrors maxManagementResponseBytes so the test states
// the number it is pinning rather than deriving it from the constant it guards.
// A production edit that widens the constant must show up here as a failure,
// not silently follow along.
const managementCeilingBytes = 4 << 20

// envelopeOfExactly renders a success envelope whose serialized length is
// exactly n bytes, padding the message field to make up the difference.
//
// The padding is a plain ASCII run, so json.Marshal neither escapes nor
// re-encodes it and the length grows one-for-one with the pad. Marker is
// embedded so the caller can assert the error never echoes the body.
func envelopeOfExactly(t *testing.T, n int, data json.RawMessage, marker string) []byte {
	t.Helper()
	env := testEnvelope{Success: true, Code: "200", Message: marker, Data: data}
	base, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	pad := n - len(base)
	if pad < 0 {
		t.Fatalf("envelope is already %d bytes, cannot shrink to %d", len(base), n)
	}
	env.Message = marker + strings.Repeat("p", pad)
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal padded envelope: %v", err)
	}
	if len(out) != n {
		t.Fatalf("padded envelope is %d bytes, want exactly %d", len(out), n)
	}
	return out
}

// serveRawBody stands up a server that answers every request with body verbatim.
func serveRawBody(t *testing.T, body []byte) *apiDeadLetterClient {
	t.Helper()
	_, c := newEnvelopeTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	return c
}

// TestAPIDeadLetterClientResponseByteCeiling pins do()'s 4 MiB response cap.
//
// The cap is the CLI's only defense against a management API that answers a
// list request with an unbounded body: io.ReadAll buffers the whole thing into
// the operator's process before anything is decoded. Deleting the length check
// (`if len(raw) > maxManagementResponseBytes` -> `if false`) leaves every test
// in cmd/xflow green — no existing test writes a body anywhere near 4 MiB.
//
// "List returns an error" is not the assertion, because it depends on size in
// a way that is easy to get wrong. Measured with the check removed:
//
//	cap+1 bytes: err = <nil>                       (io.LimitReader reads cap+1,
//	                                                so nothing is truncated and
//	                                                the body decodes cleanly)
//	2*cap bytes: err = "decode envelope from GET ...: unexpected end of JSON input"
//
// So the two over-cap arms fail differently and both are needed. At cap+1 the
// guard is the only thing that says no at all. Far over the cap something does
// fail, but it fails as malformed JSON — an error that points the operator at
// the server's encoder instead of at the size, after the process has already
// buffered the bytes. Asserting the message names the ceiling is what separates
// "we refused a body that was too large" from "we read it and tripped over the
// truncation".
//
// The at-cap arm is the other side: a body of exactly the cap must decode. The
// check is `>`, not `>=`, and the two are not interchangeable — objectstore's
// HTTPStore shipped with exactly that off-by-one (fixed in 9071f0e), where a
// 16 MiB artifact the server would happily store was one no runner could fetch
// back.
func TestAPIDeadLetterClientResponseByteCeiling(t *testing.T) {
	dataBytes, err := json.Marshal(deadLetterListResponse{
		Entries:    []engine.OutboxEntry{{ID: "execute/exec-cap/echo/0"}},
		NextCursor: "cursor-cap",
	})
	if err != nil {
		t.Fatalf("marshal list data: %v", err)
	}

	t.Run("exactly at the ceiling decodes", func(t *testing.T) {
		body := envelopeOfExactly(t, managementCeilingBytes, dataBytes, "at-cap")
		c := serveRawBody(t, body)

		list, err := c.List(context.Background(), "exec-cap", "ns1", engine.DeadLetterPage{Limit: 100})
		if err != nil {
			t.Fatalf("a response of exactly %d bytes was rejected: %v; the cap is `>`, "+
				"so the largest legal body must still decode", managementCeilingBytes, err)
		}
		if len(list.Entries) != 1 || list.Entries[0].ID != "execute/exec-cap/echo/0" {
			t.Fatalf("entries = %v, want the single seeded entry: the at-cap body must "+
				"decode in full, not merely avoid an error", list.Entries)
		}
		if list.NextCursor != "cursor-cap" {
			t.Fatalf("next_cursor = %q, want cursor-cap", list.NextCursor)
		}
	})

	overCap := []struct {
		name string
		size int
	}{
		// The exact boundary: without the guard this one is not merely
		// mis-reported, it is accepted outright.
		{"one byte over the ceiling is refused by size", managementCeilingBytes + 1},
		// Far over the cap, where truncation would otherwise supply a
		// misleading decode error in the guard's place.
		{"far over the ceiling is refused by size, not by truncation", 2 * managementCeilingBytes},
	}
	for _, tc := range overCap {
		t.Run(tc.name, func(t *testing.T) {
			const marker = "over-cap-body-marker"
			body := envelopeOfExactly(t, tc.size, dataBytes, marker)
			c := serveRawBody(t, body)

			_, err := c.List(context.Background(), "exec-cap", "ns1", engine.DeadLetterPage{Limit: 100})
			if err == nil {
				t.Fatalf("a %d-byte response was accepted; the CLI buffers the whole body "+
					"with io.ReadAll before decoding, so an unbounded server response is an "+
					"unbounded allocation in the operator's process", len(body))
			}
			if !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("error = %v, want one naming the size limit; an error that merely "+
					"reports malformed JSON means the body was read past the cap and the "+
					"failure came from truncation rather than from the guard", err)
			}
			// §3.5: the body may carry server internals, so the error must name
			// the request and the limit and nothing else.
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error echoed the response body: %v", err)
			}
		})
	}
}
