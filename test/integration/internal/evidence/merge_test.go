package evidence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// merge.go had no test file at all. Its only production caller is
// test/integration/cmd/evidence-verify/main.go, which also has no test files,
// so every branch below was reachable only by running the CLI by hand. The
// merge is what decides which raw records the verifier ever sees: a fragment
// silently dropped here becomes evidence that was never checked, and the
// verification still reports passed.

func writeFragment(t *testing.T, dir, name string, env *Envelope) {
	t.Helper()
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal fragment %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatalf("write fragment %s: %v", name, err)
	}
}

// fragmentWithObservation builds a minimal but non-empty fragment carrying one
// protocol observation, which is what the assertions below count.
func fragmentWithObservation(runID, obsType string) *Envelope {
	return &Envelope{
		SchemaVersion: SchemaVersion,
		RunID:         runID,
		StartedAt:     time.Unix(1700000000, 0).UTC(),
		Raw: RawLedger{
			ProtocolObservations: []ProtocolObservation{{
				RunID:      runID,
				Type:       obsType,
				ObservedAt: time.Unix(1700000001, 0).UTC(),
			}},
		},
	}
}

func observationTypes(env *Envelope) []string {
	types := make([]string, 0, len(env.Raw.ProtocolObservations))
	for _, po := range env.Raw.ProtocolObservations {
		types = append(types, po.Type)
	}
	return types
}

// TestMergeRawEnvelopes_SkipsDiagnosticFragments covers the filter at
// merge.go:42-44. Diagnostic fragments are written by the same recorder into
// the same directory; without the filter their records join the raw ledger and
// the verifier derives observations from data that was never meant to be
// evidence. Asserting only that the merge succeeded would not notice.
func TestMergeRawEnvelopes_SkipsDiagnosticFragments(t *testing.T) {
	dir := t.TempDir()
	writeFragment(t, dir, "a.json", fragmentWithObservation("run-1", "real"))
	writeFragment(t, dir, "a.diagnostic.json", fragmentWithObservation("run-1", "diagnostic"))

	merged, err := MergeRawEnvelopes(dir)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v", err)
	}
	got := observationTypes(merged)
	if len(got) != 1 || got[0] != "real" {
		t.Fatalf("protocol observation types = %v, want exactly [real]: a .diagnostic. "+
			"fragment was merged into the raw ledger", got)
	}
}

// TestMergeRawEnvelopes_IgnoresNonJSONFiles covers the suffix filter at
// merge.go:39-41. The raw directory also collects logs and other run artifacts;
// feeding one of those to json.Unmarshal would abort the whole merge with a
// parse error, so the filter is what keeps the CLI usable at all.
func TestMergeRawEnvelopes_IgnoresNonJSONFiles(t *testing.T) {
	dir := t.TempDir()
	writeFragment(t, dir, "a.json", fragmentWithObservation("run-1", "real"))
	if err := os.WriteFile(filepath.Join(dir, "run.log"), []byte("not json at all"), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	merged, err := MergeRawEnvelopes(dir)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v: a non-.json file in the raw dir "+
			"must be ignored, not parsed", err)
	}
	if got := observationTypes(merged); len(got) != 1 || got[0] != "real" {
		t.Fatalf("protocol observation types = %v, want exactly [real]", got)
	}
}

// TestMergeRawEnvelopes_RefusesCrossRunFragments covers merge.go:87-89. Merging
// two runs' records under one RunID would make the integrity check compare
// records that never belonged together, and the verifier has no way to
// reconstruct which run each record came from once they are concatenated.
func TestMergeRawEnvelopes_RefusesCrossRunFragments(t *testing.T) {
	dir := t.TempDir()
	writeFragment(t, dir, "a.json", fragmentWithObservation("run-1", "first"))
	writeFragment(t, dir, "b.json", fragmentWithObservation("run-2", "second"))

	_, err := MergeRawEnvelopes(dir)
	if err == nil {
		t.Fatal("MergeRawEnvelopes() error = nil, want a refusal to merge two run_ids")
	}
	const want = "cross-run merge refused"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("MergeRawEnvelopes() error = %q, want substring %q: several other "+
			"branches also return an error here, so asserting only err != nil would "+
			"survive deleting the cross-run guard", err.Error(), want)
	}
}

// TestMergeRawEnvelopes_RejectsNonEmptyFragmentWithoutRunID covers merge.go:71.
// A fragment that carries records but no run_id is a misconfigured recorder;
// skipping it would drop real evidence and still report a clean verification.
func TestMergeRawEnvelopes_RejectsNonEmptyFragmentWithoutRunID(t *testing.T) {
	dir := t.TempDir()
	writeFragment(t, dir, "a.json", fragmentWithObservation("run-1", "real"))
	writeFragment(t, dir, "b.json", fragmentWithObservation("", "orphan"))

	_, err := MergeRawEnvelopes(dir)
	if err == nil {
		t.Fatal("MergeRawEnvelopes() error = nil, want a rejection of a record-carrying " +
			"fragment with an empty run_id")
	}
	const want = "has empty run_id"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("MergeRawEnvelopes() error = %q, want substring %q", err.Error(), want)
	}
}

// TestMergeRawEnvelopes_SkipsEmptyFragmentWithoutRunID covers the other side of
// the same branch (merge.go:68-70). A genuinely empty fragment carries nothing
// to lose, so it must be skipped rather than failing the whole merge -- the
// recorder writes one per topology whether or not that topology produced
// records. Only asserting the error case would let the skip turn into an error.
func TestMergeRawEnvelopes_SkipsEmptyFragmentWithoutRunID(t *testing.T) {
	dir := t.TempDir()
	writeFragment(t, dir, "a.json", fragmentWithObservation("run-1", "real"))
	writeFragment(t, dir, "b.json", &Envelope{SchemaVersion: SchemaVersion})

	merged, err := MergeRawEnvelopes(dir)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v: an empty fragment without a run_id "+
			"must be skipped, not rejected", err)
	}
	if merged.RunID != "run-1" {
		t.Fatalf("merged.RunID = %q, want %q", merged.RunID, "run-1")
	}
	if got := observationTypes(merged); len(got) != 1 || got[0] != "real" {
		t.Fatalf("protocol observation types = %v, want exactly [real]", got)
	}
}

// TestMergeRawEnvelopes_RejectsDirWithoutFragments covers merge.go:48-50. An
// empty raw directory must fail loudly: an empty ledger verifies clean, so
// returning a zero envelope here would turn "the tests never ran" into
// "verification passed".
func TestMergeRawEnvelopes_RejectsDirWithoutFragments(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "run.log"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	_, err := MergeRawEnvelopes(dir)
	if err == nil {
		t.Fatal("MergeRawEnvelopes() error = nil, want an error for a dir with no " +
			"usable fragment -- an empty ledger verifies clean")
	}
	// Pin the no-fragment message specifically. merge.go has a second, later
	// guard whose message ("no raw envelopes with run_id in dir") contains this
	// one as a prefix, so a bare "no raw envelopes" check would stay green if
	// the len(names)==0 guard were deleted and the merge fell through to it.
	const want = "no raw envelopes in dir"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("MergeRawEnvelopes() error = %q, want substring %q", err.Error(), want)
	}
}

// TestMergeRawEnvelopes_KeepsFirstSourceAndDropsSelfReportedVerification covers
// merge.go:73-86 and 97-99. Source is deliberately the one self-reported block
// the merge keeps, because the verifier compares it against its own independent
// recomputation -- taking it from a later fragment instead would silently swap
// which fragment's claim gets checked. Suite/Environment/DerivedObservations/
// Verification must NOT survive the merge: the verifier recomputes them, and a
// fragment's own "passed: true" reaching the merged envelope is exactly the
// self-certification the verifier exists to prevent.
func TestMergeRawEnvelopes_KeepsFirstSourceAndDropsSelfReportedVerification(t *testing.T) {
	dir := t.TempDir()

	first := fragmentWithObservation("run-1", "first")
	first.Source = SourceProvenance{CommitSHA: "sha-from-first", GoVersion: "go-from-first"}
	writeFragment(t, dir, "a.json", first)

	second := fragmentWithObservation("run-1", "second")
	second.Source = SourceProvenance{CommitSHA: "sha-from-second", GoVersion: "go-from-second"}
	second.Suite = SuiteSummary{ExitCode: 7, SkipCount: 3}
	second.Environment = Environment{RedisVersion: "9.9.9", MySQLVersion: "9.9.9"}
	second.DerivedObservations = []DerivedObservation{{Kind: "fabricated"}}
	second.Verification = Verification{Passed: true, SourceRecomputed: true, SuiteRecomputed: true}
	writeFragment(t, dir, "b.json", second)

	merged, err := MergeRawEnvelopes(dir)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v", err)
	}

	if merged.Source.CommitSHA != "sha-from-first" || merged.Source.GoVersion != "go-from-first" {
		t.Errorf("merged.Source = %#v, want the FIRST fragment's provenance: a later "+
			"fragment overwrote the Source the verifier recomputes against", merged.Source)
	}
	if merged.Suite != (SuiteSummary{}) {
		t.Errorf("merged.Suite = %#v, want zero: the suite summary is recomputed from "+
			"go test -json, never taken from a fragment's self-report", merged.Suite)
	}
	if merged.Environment != (Environment{}) {
		t.Errorf("merged.Environment = %#v, want zero", merged.Environment)
	}
	if len(merged.DerivedObservations) != 0 {
		t.Errorf("merged.DerivedObservations = %#v, want none: the verifier derives "+
			"these from the raw ledger", merged.DerivedObservations)
	}
	if merged.Verification.Passed {
		t.Error("merged.Verification.Passed is true: a fragment's own verification " +
			"result reached the merged envelope, which lets the artifact certify itself")
	}
	// Both fragments' raw records must still be present -- dropping the second
	// fragment entirely would satisfy every assertion above.
	if got := observationTypes(merged); len(got) != 2 {
		t.Fatalf("protocol observation types = %v, want both fragments' records", got)
	}
}
