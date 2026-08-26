package evidence

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ManifestDigest and AtomicWriteDiagnostic both reported 0.0% statement
// coverage. Neither is inert: the digest is what binds a run's identity to the
// required manifest, and the diagnostic writer's filename is what keeps
// diagnostics out of the merged raw ledger.

// TestManifestDigest_DistinguishesManifests is the assertion that a constant
// cannot satisfy. The verifier never recomputes the manifest digest -- it only
// checks that every RunIdentity record carries the SAME value
// (verifier.go:540). So a ManifestDigest that returned a fixed string would
// verify perfectly while silently un-binding every artifact from the manifest
// it was supposed to have been produced against: two runs requiring different
// evidence would claim the same manifest.
//
// Stability alone is therefore not enough to test; discrimination is the point.
func TestManifestDigest_DistinguishesManifests(t *testing.T) {
	base := DefaultManifest()
	d1, err := ManifestDigest(base)
	if err != nil {
		t.Fatalf("ManifestDigest() error = %v", err)
	}
	if d1 == "" {
		t.Fatal("ManifestDigest() returned an empty digest")
	}
	if len(d1) != 64 {
		t.Fatalf("ManifestDigest() = %q (%d chars), want a 64-char hex SHA-256", d1, len(d1))
	}

	d1again, err := ManifestDigest(DefaultManifest())
	if err != nil {
		t.Fatalf("ManifestDigest() error = %v", err)
	}
	if d1again != d1 {
		t.Fatalf("ManifestDigest() is not stable: %q then %q", d1, d1again)
	}

	// Drop one required A0 scenario. The manifest now demands strictly less
	// evidence; if that does not move the digest, the digest is not a function
	// of the manifest.
	fewerScenarios := DefaultManifest()
	fewerScenarios.A0Scenarios = fewerScenarios.A0Scenarios[:len(fewerScenarios.A0Scenarios)-1]
	d2, err := ManifestDigest(fewerScenarios)
	if err != nil {
		t.Fatalf("ManifestDigest() error = %v", err)
	}
	if d2 == d1 {
		t.Fatalf("dropping a required A0 scenario did not change the manifest digest (%q): "+
			"the digest does not actually bind the artifact to its manifest", d1)
	}

	// Flip one matrix row's real-pair flag. Same row count, same fixtures, same
	// topologies -- only the enforcement contract changed. This is the edit the
	// digest most needs to catch, because nothing about the manifest's shape
	// reveals it.
	flipped := DefaultManifest()
	if len(flipped.A3Rows) == 0 {
		t.Fatal("DefaultManifest() has no A3 rows")
	}
	flipped.A3Rows[0].DatabaseRealPair = !flipped.A3Rows[0].DatabaseRealPair
	d3, err := ManifestDigest(flipped)
	if err != nil {
		t.Fatalf("ManifestDigest() error = %v", err)
	}
	if d3 == d1 {
		t.Fatalf("flipping A3Rows[0].DatabaseRealPair did not change the manifest "+
			"digest (%q): a relaxed enforcement contract would be indistinguishable "+
			"from the canonical one", d1)
	}
}

func TestManifestDigest_RejectsNilManifest(t *testing.T) {
	got, err := ManifestDigest(nil)
	if err == nil {
		t.Fatalf("ManifestDigest(nil) = %q, nil; want an error rather than a digest "+
			"of the empty manifest", got)
	}
	if got != "" {
		t.Errorf("ManifestDigest(nil) returned digest %q alongside the error", got)
	}
}

// TestAtomicWriteDiagnostic_WritesWithoutPassingVerification pins the one
// behaviour that separates AtomicWriteDiagnostic from AtomicFinalize: it must
// write even when verification failed. A failing run is precisely the run whose
// raw ledger someone needs to read, so gating this on Verification.Passed --
// the way AtomicFinalize is gated (canonical.go:54) -- would discard the
// evidence exactly when it matters.
func TestAtomicWriteDiagnostic_WritesWithoutPassingVerification(t *testing.T) {
	dir := t.TempDir()
	env := &Envelope{
		SchemaVersion: SchemaVersion,
		RunID:         "run-diag",
		Verification:  Verification{Passed: false, Errors: []string{"something failed"}},
	}

	path, err := AtomicWriteDiagnostic(env, dir)
	if err != nil {
		t.Fatalf("AtomicWriteDiagnostic() error = %v: a diagnostic must be written "+
			"for a run whose verification did NOT pass", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("diagnostic not on disk at %s: %v", path, err)
	}

	// No temp file may survive: the raw directory is scanned by name, so a
	// leftover .tmp is either merged as a fragment or breaks the scan.
	leftovers, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(leftovers) != 0 {
		t.Errorf("temp files left behind: %v", leftovers)
	}
}

// TestAtomicWriteDiagnostic_NameIsSkippedByTheMerge is a cross-file contract:
// the diagnostic writer picks the filename (canonical.go:107) and
// MergeRawEnvelopes decides what to skip by matching ".diagnostic." in that
// name (merge.go:42). Nothing links the two -- change either side's spelling
// and diagnostics start flowing into the merged raw ledger as if they were
// evidence, with no error anywhere. Asserting the literal suffix in isolation
// would not catch a drift on the merge side, so this test runs the real merge
// over the real producer's output.
func TestAtomicWriteDiagnostic_NameIsSkippedByTheMerge(t *testing.T) {
	dir := t.TempDir()

	diag := fragmentWithObservation("run-1", "diagnostic-record")
	diag.Verification = Verification{Passed: false}
	diagPath, err := AtomicWriteDiagnostic(diag, dir)
	if err != nil {
		t.Fatalf("AtomicWriteDiagnostic() error = %v", err)
	}
	if !strings.HasSuffix(filepath.Base(diagPath), ".diagnostic.json") {
		t.Fatalf("diagnostic filename = %q, want a .diagnostic.json suffix",
			filepath.Base(diagPath))
	}

	writeFragment(t, dir, "aa-real.json", fragmentWithObservation("run-1", "real-record"))

	merged, err := MergeRawEnvelopes(dir)
	if err != nil {
		t.Fatalf("MergeRawEnvelopes() error = %v", err)
	}
	got := observationTypes(merged)
	if len(got) != 1 || got[0] != "real-record" {
		t.Fatalf("merged protocol observation types = %v, want exactly [real-record]: "+
			"the file AtomicWriteDiagnostic produced was ingested as evidence", got)
	}
}

// TestAtomicWriteDiagnostic_RefusesToOverwrite covers canonical.go:111-113.
// The CLI writes a diagnostic on the failure path; a second invocation against
// the same output directory must not silently replace the first run's ledger
// with the second's.
//
// Note the doc comment on AtomicWriteDiagnostic says "must not overwrite an
// existing final artifact", but the path it stats is the diagnostic's own
// (.diagnostic.json), never the final artifact (.json). What the guard actually
// protects is a previous diagnostic. The comment is inaccurate; the behaviour
// asserted here is the behaviour the code implements.
func TestAtomicWriteDiagnostic_RefusesToOverwrite(t *testing.T) {
	dir := t.TempDir()
	first := fragmentWithObservation("run-1", "first-write")
	firstPath, err := AtomicWriteDiagnostic(first, dir)
	if err != nil {
		t.Fatalf("first AtomicWriteDiagnostic() error = %v", err)
	}
	before, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatalf("read first diagnostic: %v", err)
	}

	second := fragmentWithObservation("run-1", "second-write")
	if _, err := AtomicWriteDiagnostic(second, dir); err == nil {
		t.Fatal("second AtomicWriteDiagnostic() error = nil, want a refusal to " +
			"overwrite the existing diagnostic for the same run")
	}

	after, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatalf("read diagnostic after refused second write: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("the refused second write still modified the existing diagnostic " +
			"on disk: returning an error is not enough if the bytes changed")
	}
}

func TestAtomicWriteDiagnostic_RejectsEmptyRunID(t *testing.T) {
	dir := t.TempDir()
	_, err := AtomicWriteDiagnostic(&Envelope{SchemaVersion: SchemaVersion}, dir)
	if err == nil {
		t.Fatal("AtomicWriteDiagnostic() error = nil for an envelope with no run_id")
	}
	// Without a run ID the filename would collapse to "evidence-.diagnostic.json"
	// for every run, so each run would collide with the last.
	entries, globErr := filepath.Glob(filepath.Join(dir, "*"))
	if globErr != nil {
		t.Fatalf("glob: %v", globErr)
	}
	if len(entries) != 0 {
		t.Errorf("files written despite the rejection: %v", entries)
	}
}
