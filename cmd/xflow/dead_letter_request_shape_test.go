package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDeadLetterReplaySendsEachFlagToItsOwnBodyField pins the outgoing replay
// request body (dead_letter.go:304-308).
//
// Nothing in this package has ever read a request body: grepping every
// *_test.go for r.Body or ReadAll(r. returns zero hits. Both end-to-end replay
// tests inspect only the Authorization header, the URL path, and the response.
// So swapping two fields of deadLetterReplayRequest — say
//
//	RequestID: req.Reason,
//	Reason:    req.RequestID,
//
// ships with the whole suite green.
//
// That swap is not cosmetic. request_id is the idempotency key: the command's
// own help text promises "retrying with the same --request-id returns
// already_replayed and the original audit_id, proving the operation happened
// exactly once". Under the swap the server dedupes on the human-readable
// reason text instead, so two unrelated replays that happened to share a
// reason collapse into one, while a genuine retry after a lost response looks
// like a brand new request and replays the entry a second time. Replay
// re-runs a node that already failed, so that is a duplicate side effect, and
// the audit row records the idempotency key as the operator's rationale.
//
// The three flag values below are pairwise distinct and none is a prefix of
// another, so any permutation of the three fields fails at least one
// assertion.
func TestDeadLetterReplaySendsEachFlagToItsOwnBodyField(t *testing.T) {
	const (
		wantEntry  = "entry-abc"
		wantReqID  = "idem-key-123"
		wantReason = "downstream recovered"
	)

	var got deadLetterReplayRequest
	var decodeErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			decodeErr = err
			return
		}
		decodeErr = json.Unmarshal(raw, &got)
		data, err := json.Marshal(deadLetterReplayResponse{Outcome: "replayed", AuditID: "audit-1"})
		if err != nil {
			t.Errorf("marshal data: %v", err)
			return
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Data: data})
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := executeRootWith(&out, "dead-letter",
		"--server", srv.URL, "--token", "t",
		"replay", "--execution", "exec-1",
		"--entry", wantEntry, "--request-id", wantReqID, "--reason", wantReason)
	if err != nil {
		t.Fatalf("dead-letter replay: %v", err)
	}
	if decodeErr != nil {
		t.Fatalf("the server could not decode the replay body: %v", decodeErr)
	}

	if got.EntryID != wantEntry {
		t.Fatalf("body entry_id = %q, want %q: --entry names which dead-lettered "+
			"entry is moved back to the ready set", got.EntryID, wantEntry)
	}
	if got.RequestID != wantReqID {
		t.Fatalf("body request_id = %q, want %q: this is the idempotency key the "+
			"server dedupes on, and the command's help text promises a retry with "+
			"the same value returns already_replayed rather than replaying twice",
			got.RequestID, wantReqID)
	}
	if got.Reason != wantReason {
		t.Fatalf("body reason = %q, want %q: the reason is what the durable audit "+
			"row records as why a failed node was re-run", got.Reason, wantReason)
	}
}

// TestDeadLetterListSendsTheLimitFlagAndTrimsTheServerSlash pins two things on
// the list request line that no test reads today.
//
// --limit. dead_letter.go:159 puts the flag into engine.DeadLetterPage and
// dead_letter.go:286 formats it into the query. Every direct-client test in
// this package hardcodes Limit: 100 when calling List itself, and the CLI
// dispatch tests read only the cursor parameter (dead_letter_cursor_test.go:23,
// 24, 55 are the package's only Query() calls). Hardcoding ?limit=100 in the
// format string therefore stays green while an operator's --limit is silently
// discarded — the failure mode is a page far larger than asked for, against
// the one endpoint whose response the CLI caps at 4 MiB.
//
// The trailing slash. dead_letter.go:131 trims it off opts.server. No test has
// ever passed a --server value ending in "/", so deleting the TrimRight is a
// no-op on every existing input. A trailing slash is the ordinary copy-paste
// shape of a base URL, and without the trim the CLI requests
// "//v1/management/..." — which this handler sees verbatim, and which a real
// server behind a router answers with a 404 that the CLI reports as
// "execution or entry not found".
func TestDeadLetterListSendsTheLimitFlagAndTrimsTheServerSlash(t *testing.T) {
	var gotLimit, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLimit = r.URL.Query().Get("limit")
		gotPath = r.URL.Path
		data, err := json.Marshal(deadLetterListResponse{})
		if err != nil {
			t.Errorf("marshal data: %v", err)
			return
		}
		writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Data: data})
	}))
	defer srv.Close()

	var out bytes.Buffer
	// 7 rather than the flag's own default of 100, so a client that ignores the
	// flag and hardcodes the default is distinguishable from one that honours it.
	err := executeRootWith(&out, "dead-letter",
		"--server", srv.URL+"/", "--token", "t",
		"list", "--execution", "exec-1", "--limit", "7")
	if err != nil {
		t.Fatalf("dead-letter list: %v", err)
	}

	if gotLimit != "7" {
		t.Fatalf("query limit = %q, want \"7\": --limit is how an operator bounds a "+
			"page against an execution with a large dead-letter backlog", gotLimit)
	}
	if gotPath != "/v1/management/dead-letters/exec-1" {
		t.Fatalf("request path = %q, want /v1/management/dead-letters/exec-1: a "+
			"--server value ending in \"/\" must be normalised, not concatenated into "+
			"a double-slash path", gotPath)
	}
}

// TestBreakGlassPrincipalNamesTheOperator pins newDeadLetterPrincipal
// (dead_letter.go:250-264).
//
// The break-glass path bypasses the management API and, with it, the server's
// authorization, metrics and durable SQL audit projection. The command's own
// help text says what is left in exchange: it "records a 'cli:breakglass:<user>'
// operator identity so the break-glass use is distinguishable in audit". That
// subject is the entire remaining accountability mechanism for the path.
//
// Nothing checks it. TestDeadLetterCLIBreakGlassWarning captures the same
// stderr stream the audit line is written to, but asserts only that the
// hardcoded warning sentence contains "--break-glass"; grepping the package
// for "cli:breakglass" outside comments returns nothing, and no test sets or
// clears $USER. Dropping the prefix, or always falling back to "unknown",
// leaves the suite green and makes an emergency Redis-direct replay
// indistinguishable in audit from a routine one.
//
// The far end of this chain is already pinned: service/control's
// TestReplayRecordsTheAuthenticatedPrincipalNotTheRequestedOperator asserts
// that DeadLetterManager.Replay overwrites req.Operator with principal.Subject
// and that the durable row's Principal is that value. This test closes the
// near end — where the subject is constructed.
//
// The API-path case is the weaker of the two and is here for the contract, not
// for a failure mode: the server injects the real principal from the bearer
// token and DeadLetterManager overwrites whatever the CLI sent, so a
// self-reported subject here would be discarded rather than believed.
func TestBreakGlassPrincipalNamesTheOperator(t *testing.T) {
	t.Run("named user", func(t *testing.T) {
		t.Setenv("USER", "alice")
		p := newDeadLetterPrincipal(&deadLetterOptions{breakGlass: true}, "ns-1")
		if p.Subject != "cli:breakglass:alice" {
			t.Fatalf("break-glass subject = %q, want \"cli:breakglass:alice\": this "+
				"string is the only record distinguishing an emergency Redis-direct "+
				"replay from a routine one, on a path with no server-side audit",
				p.Subject)
		}
		if p.Namespace != "ns-1" {
			t.Fatalf("namespace = %q, want ns-1", p.Namespace)
		}
	})

	t.Run("no user in the environment", func(t *testing.T) {
		t.Setenv("USER", "")
		p := newDeadLetterPrincipal(&deadLetterOptions{breakGlass: true}, "ns-1")
		if p.Subject != "cli:breakglass:unknown" {
			t.Fatalf("break-glass subject with an empty $USER = %q, want "+
				"\"cli:breakglass:unknown\": the identity must stay recognisably a "+
				"break-glass one even when the environment names nobody", p.Subject)
		}
	})

	t.Run("api path leaves the subject to the server", func(t *testing.T) {
		t.Setenv("USER", "alice")
		p := newDeadLetterPrincipal(&deadLetterOptions{}, "ns-1")
		if p.Subject != "" {
			t.Fatalf("api-path subject = %q, want empty: on this path the server "+
				"authenticates the bearer token and injects the principal itself, so "+
				"the CLI must not self-report a name", p.Subject)
		}
	})
}
