package protocol

import (
	"encoding/json"
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
