package evidence

import (
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// requireErrContains asserts that the verification failed AND that it failed
// for the stated reason. Verification.Passed is the AND of every check, so any
// other still-failing check keeps it false; only the message pins the branch.
func requireErrContains(t *testing.T, v Verification, want string) {
	t.Helper()
	if v.Passed {
		t.Fatalf("expected verification to fail with %q, but it passed", want)
	}
	for _, e := range v.Errors {
		if strings.Contains(e, want) {
			return
		}
	}
	t.Fatalf("expected an error containing %q, got %v", want, v.Errors)
}

// TestHasSystemTaskDelivery_SurvivesTheJSONRoundTrip is about the gap between
// how the tests build Detail maps and how production reads them.
//
// Every unit test in this package constructs ProtocolObservation.Detail in Go,
// so `2` lands in the map as an `int` and intFromDetail's `case int` handles it.
// The real verifier never sees a Go-constructed map: the CLI calls
// MergeRawEnvelopes, which json.Unmarshals every fragment, and encoding/json
// decodes every number in a map[string]any as float64. So `case float64`
// (verifier.go:668) is the only arm that runs in production and the only arm no
// test reaches -- delete it and the OSKillSIGKILL phase-B exception silently
// stops applying to real runs while the whole suite stays green.
//
// This test therefore goes through the real reader rather than hand-writing a
// float64 literal: writing float64(2) would prove that the arm works without
// proving that JSON is what produces it.
func TestHasSystemTaskDelivery_SurvivesTheJSONRoundTrip(t *testing.T) {
	const execID = types.ExecutionID("exec-oskill")

	frag := &Envelope{
		SchemaVersion: SchemaVersion,
		RunID:         "run-1",
		Raw: RawLedger{
			ProtocolObservations: []ProtocolObservation{{
				RunID:       "run-1",
				ExecutionID: execID,
				Type:        "system_task_delivery",
				ObservedAt:  time.Unix(1700000000, 0).UTC(),
				// An int, exactly as a test or recorder would build it in Go.
				Detail: map[string]any{"system_task_deliveries": 2},
			}},
		},
	}

	// Before the round trip the value is an int and `case int` handles it.
	if !hasSystemTaskDelivery(frag, execID) {
		t.Fatal("hasSystemTaskDelivery() = false for a Go-constructed int detail")
	}

	dir := t.TempDir()
	writeFragment(t, dir, "a.json", frag)
	merged, err := MergeRawEnvelopes(dir)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v", err)
	}

	// After the round trip it is a float64. This is the shape the verifier
	// actually receives.
	got := merged.Raw.ProtocolObservations[0].Detail["system_task_deliveries"]
	if _, isFloat := got.(float64); !isFloat {
		t.Fatalf("after the JSON round trip the detail value is %T, want float64: "+
			"this test's premise no longer holds and it is no longer covering "+
			"verifier.go's float64 arm", got)
	}
	if !hasSystemTaskDelivery(merged, execID) {
		t.Fatal("hasSystemTaskDelivery() = false after a JSON round trip: the " +
			"OSKillSIGKILL phase-B exception does not apply to any real (merged) " +
			"run, only to in-memory unit tests")
	}

	// The "deliveries" spelling is the documented fallback (verifier.go:648)
	// and must survive the same round trip.
	frag.Raw.ProtocolObservations[0].Detail = map[string]any{"deliveries": 1}
	dir2 := t.TempDir()
	writeFragment(t, dir2, "a.json", frag)
	merged2, err := MergeRawEnvelopes(dir2)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v", err)
	}
	if !hasSystemTaskDelivery(merged2, execID) {
		t.Fatal("hasSystemTaskDelivery() = false for the \"deliveries\" spelling " +
			"after a JSON round trip")
	}
}

// TestHasSystemTaskDelivery_RequiresAtLeastOne pins the >= 1 threshold at
// verifier.go:645,648. An observation recording zero deliveries is evidence
// that the delivery did NOT happen; treating its presence as the exception
// would grant the phase-B exemption to exactly the runs it must not cover.
func TestHasSystemTaskDelivery_RequiresAtLeastOne(t *testing.T) {
	const execID = types.ExecutionID("exec-oskill")
	env := &Envelope{
		RunID: "run-1",
		Raw: RawLedger{
			ProtocolObservations: []ProtocolObservation{{
				RunID:       "run-1",
				ExecutionID: execID,
				Type:        "system_task_delivery",
				Detail:      map[string]any{"system_task_deliveries": 0},
			}},
		},
	}
	if hasSystemTaskDelivery(env, execID) {
		t.Fatal("hasSystemTaskDelivery() = true for a detail recording ZERO " +
			"deliveries: the observation's presence is not the evidence, its " +
			"count is")
	}

	// A different execution's delivery must not be borrowed.
	env.Raw.ProtocolObservations[0].Detail = map[string]any{"system_task_deliveries": 3}
	if hasSystemTaskDelivery(env, types.ExecutionID("some-other-exec")) {
		t.Fatal("hasSystemTaskDelivery() = true for an execution ID that has no " +
			"delivery observation of its own")
	}
}

// TestVerifyRejectsDivergentRunIdentityDigests covers the cross-record loop at
// verifier.go:530-543, which reported the lowest statement coverage of every
// check in the file.
//
// The loop is the ONLY place a manifest digest is ever compared to anything:
// the verifier never recomputes it (see canonical_test.go). If the loop body
// stopped running, two fragments produced against different manifests -- or by
// different test binaries -- would merge into one artifact that verifies clean,
// which is precisely the substitution the run identity exists to detect.
func TestVerifyRejectsDivergentRunIdentityDigests(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(ri *RunIdentity)
		want   string
	}{
		{
			name:   "manifest digest",
			mutate: func(ri *RunIdentity) { ri.ManifestDigest = sha256String("a different manifest") },
			want:   "run_identity: record 1 manifest_digest",
		},
		{
			name:   "test binary digest",
			mutate: func(ri *RunIdentity) { ri.TestBinaryDigest = sha256String("a different binary") },
			want:   "run_identity: record 1 test_binary_digest",
		},
		{
			name:   "run id",
			mutate: func(ri *RunIdentity) { ri.RunID = "some-other-run" },
			want:   "run_identity: record 1 run_id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			markAllRequired(env)
			second := env.Raw.RunIdentities[0]
			second.ProducerID = "second-producer"
			tc.mutate(&second)
			env.Raw.RunIdentities = append(env.Raw.RunIdentities, second)

			v := NewVerifier(defaultFakeProvenance())
			res := v.Verify(env, passEvents())
			requireErrContains(t, res, tc.want)
		})
	}
}

// TestVerifyAcceptsAgreeingRunIdentityRecords is the positive control for the
// test above. Every fragment of a real run stamps its own RunIdentity, so the
// merged ledger normally carries several agreeing records; a check that
// rejected those would fail every real run, and the negative tests alone would
// not notice.
func TestVerifyAcceptsAgreeingRunIdentityRecords(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	second := env.Raw.RunIdentities[0]
	second.ProducerID = "second-producer"
	env.Raw.RunIdentities = append(env.Raw.RunIdentities, second)

	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	if !res.Passed {
		t.Fatalf("verification failed for two agreeing run_identity records: %v", res.Errors)
	}
}

// TestVerifyRejectsEmptyRunIdentityDigests covers verifier.go:524-529. An empty
// digest is not a mismatch, so the cross-record loop above passes it happily:
// every record agreeing on "" is still agreement. Only the record-0 emptiness
// checks catch a recorder that never stamped the digests at all.
func TestVerifyRejectsEmptyRunIdentityDigests(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(ri *RunIdentity)
		want   string
	}{
		{
			name:   "test binary digest",
			mutate: func(ri *RunIdentity) { ri.TestBinaryDigest = "" },
			want:   "run_identity: record 0 has empty test_binary_digest",
		},
		{
			name:   "manifest digest",
			mutate: func(ri *RunIdentity) { ri.ManifestDigest = "" },
			want:   "run_identity: record 0 has empty manifest_digest",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			markAllRequired(env)
			// Blank the field on EVERY record so the cross-record loop stays
			// silent and only the emptiness check can produce the error.
			for i := range env.Raw.RunIdentities {
				tc.mutate(&env.Raw.RunIdentities[i])
			}
			v := NewVerifier(defaultFakeProvenance())
			res := v.Verify(env, passEvents())
			requireErrContains(t, res, tc.want)
		})
	}
}

// TestValuesAgree covers verifier.go:621-632 directly. checkEnvironmentIntegrity
// uses it to decide whether the observed Redis/MySQL versions are consistent
// across fragments; a version disagreement means the run spanned two different
// dependency versions, which invalidates the environment block the artifact
// claims. A valuesAgree that always returned true would erase that check while
// every environment test that supplies consistent versions stays green.
func TestValuesAgree(t *testing.T) {
	cases := []struct {
		name string
		vals []string
		want bool
	}{
		{"empty", nil, true},
		{"single", []string{"7.2"}, true},
		{"all identical", []string{"7.2", "7.2", "7.2"}, true},
		{"last differs", []string{"7.2", "7.2", "7.4"}, false},
		{"first differs", []string{"7.4", "7.2", "7.2"}, false},
		// An empty string is a distinct value, not a wildcard: a fragment that
		// failed to observe a version must not be treated as agreeing with one
		// that did.
		{"empty string among versions", []string{"7.2", ""}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := valuesAgree(tc.vals); got != tc.want {
				t.Fatalf("valuesAgree(%v) = %v, want %v", tc.vals, got, tc.want)
			}
		})
	}
}

// TestVerifyRejectsDisagreeingEnvironmentObservations is the same property one
// level up, through Verify, so the wiring between checkEnvironmentIntegrity and
// valuesAgree is exercised rather than assumed.
func TestVerifyRejectsDisagreeingEnvironmentObservations(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.Raw.EnvironmentObservations = append(env.Raw.EnvironmentObservations,
		EnvironmentObservation{RunID: env.RunID, Component: "redis", Query: "INFO server", Result: "7.4"})

	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireErrContains(t, res, "environment: redis observation results disagree")
}
