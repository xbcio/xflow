package protocol

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestEnrollJSONFieldNames pins the wire vocabulary for EnrollRequest and
// EnrollResponse. The runner side (a separate plan) and any hand-written
// curl must agree on these exact field names; renaming one is a breaking
// protocol change.
//
// EnrollPath, RunnerFacingPaths registration, and Client.Enroll are
// deliberately NOT part of this task. This package's dead-constant guard
// (TestRunnerPathsHaveMuxRegistration in paths_test.go) treats a declared
// "/v1/" path constant, its RunnerFacingPaths entry, and its registered route
// as one atomic unit — the same "interface and implementation land in the
// same task" rule the plan's Global Constraints already state. Task 4 does
// not land the route (that ships with the RunnerHTTPHandler method in Task
// 5), so it also does not declare EnrollPath: doing so here would either
// leave the constant off RunnerFacingPaths (failing the guard's Direction A)
// or list it there with no registered route (failing Direction B). Every
// combination was tried and captured in the task-4 report before this
// scope was fixed by controller ruling. All three — the constant, the list
// entry, and Client.Enroll — move to Task 5 together.
func TestEnrollJSONFieldNames(t *testing.T) {
	b, err := json.Marshal(EnrollRequest{
		RegistrationCode: "c",
		ProposedRunnerID: "p",
		Namespaces:       []string{"n"},
		NodeTypes:        []string{"t"},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{
		`"registration_code":"c"`,
		`"proposed_runner_id":"p"`,
		`"namespaces":["n"]`,
		`"node_types":["t"]`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("EnrollRequest JSON %s missing %s", b, want)
		}
	}

	b, err = json.Marshal(EnrollResponse{RunnerID: "r", Token: "t"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{`"runner_id":"r"`, `"token":"t"`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("EnrollResponse JSON %s missing %s", b, want)
		}
	}

	// omitempty only changes the output when the field IS the zero value, so
	// the population above (every field non-zero) can never observe it in
	// either direction. Marshal a request that leaves the three optional
	// fields unset and assert each of their keys individually — checking
	// only one would leave the other two exactly as unpinned as before.
	b, err = json.Marshal(EnrollRequest{RegistrationCode: "c"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, key := range []string{`"proposed_runner_id"`, `"namespaces"`, `"node_types"`} {
		if strings.Contains(string(b), key) {
			t.Fatalf("EnrollRequest JSON %s: zero-value field %s should have been omitted", b, key)
		}
	}
}

func TestEnrollPathIsInRunnerFacingPaths(t *testing.T) {
	for _, p := range RunnerFacingPaths {
		if p == EnrollPath {
			return
		}
	}
	t.Fatalf("EnrollPath %q missing from RunnerFacingPaths; the dead-constant guard exists to catch exactly this", EnrollPath)
}

func TestEnrollSendsCodeInBodyAndNoAuthorizationHeader(t *testing.T) {
	var (
		gotPath string
		gotAuth string
		gotBody []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EnrollResponse{RunnerID: "runner-abc", Token: "tok-xyz"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, srv.Client())
	resp, err := c.Enroll(context.Background(), EnrollRequest{
		RegistrationCode: "the-secret-code",
		ProposedRunnerID: "hint-only",
		Namespaces:       []string{"sas"},
		NodeTypes:        []string{"kafka.trigger"},
	})
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if resp.RunnerID != "runner-abc" || resp.Token != "tok-xyz" {
		t.Fatalf("response = %+v, want the server-issued identity", resp)
	}
	if gotPath != EnrollPath {
		t.Fatalf("path = %q, want %q", gotPath, EnrollPath)
	}
	// The runner has no credential at enroll time. Sending one would be a lie,
	// and putting the registration code in the Authorization header would leak
	// it into every proxy access log along the way (spec §2.3.1).
	if gotAuth != "" {
		t.Fatalf("Authorization header = %q, want empty on the unauthenticated enroll call", gotAuth)
	}
	if !strings.Contains(string(gotBody), "the-secret-code") {
		t.Fatalf("body %q does not carry the registration code", gotBody)
	}
	if strings.Contains(gotAuth, "the-secret-code") {
		t.Fatal("registration code leaked into the Authorization header")
	}
}
