package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// TestDeadLetterCLIListPrintsNextCursor pins that `xflow dead-letter list`
// writes the server's next_cursor to stdout.
//
// TestAPIDeadLetterClientListSendsCursor and its paired omit test already cover
// the other end of the loop: given a cursor, the client puts it on the query
// string. Nothing covered where the operator is supposed to get that cursor
// from. The CLI is a one-shot process — it prints a page and exits — so the
// only channel between page N and page N+1 is this line on stdout. Deleting it
// (`if list.NextCursor != ""` -> `if false`) leaves the whole cmd/xflow package
// green, because every existing list test calls apiDeadLetterClient.List
// directly and reads the returned struct, never the command's output.
//
// What that costs is not a missing line. The dead-letter list is paginated
// precisely because an execution can accumulate more entries than one page
// holds; without the cursor an operator running `dead-letter list` sees the
// first page and has no way to reach the rest — and no error either. The
// truncation is indistinguishable from "that is all there is", which is the
// worst shape for a command an operator runs to find out what got stuck.
//
// The test drives executeRootWith rather than the client so the assertion
// covers the path the operator actually uses, and asserts the exact byte
// sequence: the cursor is copy-pasted into the next invocation's --cursor, so
// its serialization is part of the contract, not a rendering detail.
//
// There is deliberately no paired "the last page emits no cursor line" test.
// That direction is already covered, if incidentally: turning the guard into
// `if true` reds TestDeadLetterCLIAPIPathListEndToEnd, TestDeadLetterCLIListAndReplay
// and TestDeadLetterCLINamespaceListAndReplay, all of which parse the whole of
// stdout as a single JSON object and so cannot tolerate a second line. Adding a
// fourth test for it would restate existing coverage.
func TestDeadLetterCLIListPrintsNextCursor(t *testing.T) {
	// A cursor with characters that must survive JSON quoting. Real cursors are
	// opaque server-side encodings; if the CLI ever printed one unquoted, a
	// cursor containing a quote or a backslash would produce a line the operator
	// cannot feed back.
	const wantCursor = `page-2/"cursor"\x`

	entry := engine.OutboxEntry{ID: "execute/exec-page/echo/0", Attempts: 3}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dataBytes, err := json.Marshal(deadLetterListResponse{
			Entries:    []engine.OutboxEntry{entry},
			NextCursor: wantCursor,
		})
		if err != nil {
			t.Errorf("marshal list data: %v", err)
			return
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Code: "200", Data: dataBytes})
	}))
	defer srv.Close()

	var out bytes.Buffer
	if err := executeRootWith(&out, "dead-letter",
		"--server", srv.URL, "--token", "tok-page",
		"list", "--execution", "exec-page", "--namespace", "ns-page"); err != nil {
		t.Fatalf("dead-letter list: %v", err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	// One entry line plus the cursor line. Pinning the count rather than
	// scanning for the cursor keeps the entry from being dropped silently: an
	// operator reading a page needs both halves.
	if len(lines) != 2 {
		t.Fatalf("stdout has %d lines, want 2 (one entry, one next_cursor); got:\n%s",
			len(lines), out.String())
	}

	var gotEntry engine.OutboxEntry
	if err := json.Unmarshal([]byte(lines[0]), &gotEntry); err != nil {
		t.Fatalf("unmarshal entry line %q: %v", lines[0], err)
	}
	if gotEntry.ID != entry.ID {
		t.Fatalf("entry line ID = %q, want %q", gotEntry.ID, entry.ID)
	}

	wantLine := `{"next_cursor":` + string(mustJSONString(t, wantCursor)) + `}`
	if lines[1] != wantLine {
		t.Fatalf("cursor line = %s, want %s; the CLI exits after one page, so this "+
			"line is the operator's only route to page 2 — without it the list is "+
			"silently truncated at the first page", lines[1], wantLine)
	}

	// The cursor must round-trip back into the flag it was printed for: a line
	// the operator cannot parse is the same as no line at all.
	var round struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &round); err != nil {
		t.Fatalf("unmarshal cursor line %q: %v", lines[1], err)
	}
	if round.NextCursor != wantCursor {
		t.Fatalf("round-tripped cursor = %q, want %q", round.NextCursor, wantCursor)
	}
}

func mustJSONString(t *testing.T, s string) []byte {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal %q: %v", s, err)
	}
	return b
}
