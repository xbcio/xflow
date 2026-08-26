package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/engine"
)

// TestDeadLetterCLIResolvesConnectionConfigFromTheEnvironment drives the
// envOr defaults at dead_letter.go:84-93, the documented way an operator
// configures this CLI without repeating four flags on every invocation.
//
// Nothing reached them before. Grepping the package for the four keys returns
// only dead_letter.go and dead_letter_reconcile.go; the only variable any test
// in the package ever sets is USER. The two existing tests that look like they
// would cover this do not: dead_letter_api_cli_test.go:21 proves --server is
// required when it is absent, which in that process is true only because the
// variable happens to be unset, and dead_letter_api_env_test.go builds an
// apiDeadLetterClient by hand and never touches the command tree. Every other
// API-path test passes --server and --token explicitly, and an explicit flag
// overrides a default unconditionally in cobra, so none of them can tell a
// working fallback from a deleted one.
//
// --redis-addr is the one that decides whether this is worth a test. Its
// fallback is not empty, it is "localhost:6379". If the variable stops being
// read, break-glass does not fail with a missing-config error the way the API
// path would — it quietly opens a connection to whatever is listening on the
// operator's own machine and reports on that instead of the cluster they meant.
// A break-glass session is by definition one where nobody is watching the
// normal outlets, so the wrong answer would be believed.
//
// Measured against the whole of cmd/xflow (nothing imports a main package, so
// that is the entire Arm B scope), each mutation also run with this file
// removed:
//
//   - XFLOW_API_ADDR's envOr dropped: the first sub-test goes red with
//     "--server (or XFLOW_API_ADDR) is required"; without this file, green.
//   - XFLOW_API_TOKEN's envOr dropped: red with "--token ... is required";
//     without this file, green.
//   - XFLOW_REDIS_ADDR's envOr dropped: red with
//     `connect redis "localhost:6379": connection refused` — which is the
//     failure mode only because nothing was listening there. That is the
//     whole point of the sub-test: on a machine that does run a local Redis,
//     the same mutation answers successfully from the wrong instance.
//     Without this file, green.
//
// One thing here is not backed by a mutation, and is a guard rather than a
// pin: the precedence sub-test. Flag-beats-environment is cobra's behaviour,
// not this package's, so there is no line here to break. It is worth keeping
// anyway — it fails if anyone reimplements the lookup after flag parsing —
// but it should not be read as covering code that this repo owns.
func TestDeadLetterCLIResolvesConnectionConfigFromTheEnvironment(t *testing.T) {
	t.Run("the api address and token come from the environment", func(t *testing.T) {
		const wantToken = "env-token-abc"
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			data, err := json.Marshal(deadLetterListResponse{
				Entries: []engine.OutboxEntry{{ID: "entry-from-env"}},
			})
			if err != nil {
				t.Errorf("marshal data: %v", err)
				return
			}
			writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Data: data})
		}))
		defer srv.Close()

		t.Setenv("XFLOW_API_ADDR", srv.URL)
		t.Setenv("XFLOW_API_TOKEN", wantToken)

		// No --server and no --token: the environment is the only source.
		var out bytes.Buffer
		if err := executeRootWith(&out, "dead-letter", "list", "--execution", "exec-env"); err != nil {
			t.Fatalf("dead-letter list with config from the environment: %v\n"+
				"this is the documented invocation for an operator who exported "+
				"XFLOW_API_ADDR/XFLOW_API_TOKEN once; if either stops being read "+
				"the command refuses to run at all", err)
		}
		if gotAuth != "Bearer "+wantToken {
			t.Fatalf("Authorization header = %q, want %q: XFLOW_API_TOKEN has to "+
				"reach the header, not just satisfy the required-flag check",
				gotAuth, "Bearer "+wantToken)
		}
		assertListedID(t, out.String(), "entry-from-env")
	})

	t.Run("an explicit flag still beats the environment", func(t *testing.T) {
		// A decoy that must never be contacted. If precedence ever inverted,
		// an operator's exported default would silently override the address
		// they typed on the command line, which is the more dangerous
		// direction of the two.
		decoy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			t.Errorf("the decoy server from XFLOW_API_ADDR was contacted despite an explicit --server")
			w.WriteHeader(http.StatusTeapot)
		}))
		defer decoy.Close()

		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			data, err := json.Marshal(deadLetterListResponse{
				Entries: []engine.OutboxEntry{{ID: "entry-from-flag"}},
			})
			if err != nil {
				t.Errorf("marshal data: %v", err)
				return
			}
			writeTestEnvelope(w, http.StatusOK, testEnvelope{Success: true, Data: data})
		}))
		defer srv.Close()

		t.Setenv("XFLOW_API_ADDR", decoy.URL)
		t.Setenv("XFLOW_API_TOKEN", "env-token-loses")

		var out bytes.Buffer
		if err := executeRootWith(&out, "dead-letter",
			"--server", srv.URL, "--token", "flag-token-wins",
			"list", "--execution", "exec-env"); err != nil {
			t.Fatalf("dead-letter list with explicit flags: %v", err)
		}
		if gotAuth != "Bearer flag-token-wins" {
			t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer flag-token-wins")
		}
		assertListedID(t, out.String(), "entry-from-flag")
	})

	t.Run("the break-glass redis address comes from the environment", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		defer mr.Close()
		const execID = "exec-env-bg"
		const entryID = "execute/exec-env-bg/review/1"
		seedDeadLetterRedis(t, mr.Addr(), "default", execID, entryID)

		t.Setenv("XFLOW_REDIS_ADDR", mr.Addr())

		// No --redis-addr. If the variable is ignored the fallback sends this
		// at localhost:6379, which does not fail loudly — it either refuses to
		// connect or reports on an unrelated Redis. Asserting the entry ID
		// rather than "no error" separates those two outcomes from success.
		var out bytes.Buffer
		if err := executeRootWith(&out, "dead-letter", "--break-glass",
			"list", "--execution", execID); err != nil {
			t.Fatalf("break-glass list with XFLOW_REDIS_ADDR set: %v\n"+
				"the fallback is localhost:6379, so a dropped lookup points an "+
				"emergency maintenance session at the operator's own machine", err)
		}
		assertListedID(t, out.String(), entryID)
	})
}

func assertListedID(t *testing.T, output, want string) {
	t.Helper()
	line := strings.TrimSpace(output)
	if line == "" {
		t.Fatalf("CLI printed nothing, want one entry with id %q", want)
	}
	var listed map[string]any
	if err := json.Unmarshal([]byte(line), &listed); err != nil {
		t.Fatalf("unmarshal CLI stdout line %q: %v", line, err)
	}
	if listed["id"] != want {
		t.Fatalf("listed id = %v, want %q: the command answered, but not from "+
			"the endpoint the environment named", listed["id"], want)
	}
}
