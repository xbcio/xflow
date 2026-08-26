package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// evidence-verify had no test file at all. It is the entry point of the whole
// evidence pipeline: it reads `go test -json`, merges the raw ledger, verifies,
// and decides whether a final artifact is written. main() calls os.Exit so it
// is not directly testable, but readGoTestJSON is -- and it is the one place
// where suite evidence can silently shrink.
//
// Why shrinking matters: the verifier does not trust the envelope's exit code,
// it recomputes it from these events (verifier.go:310 recomputeSuite). A
// dropped `"Action":"fail"` line therefore does not produce a smaller report,
// it produces a *passing* one -- "the tests failed" becomes "verification
// passed", with a final artifact written to prove it.

func writeLines(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.json")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func eventLine(t *testing.T, action, pkg, test string, elapsed float64) string {
	t.Helper()
	m := map[string]any{
		"Time":    "2026-08-26T10:00:00.000000000Z",
		"Action":  action,
		"Package": pkg,
	}
	if test != "" {
		m["Test"] = test
	}
	if elapsed != 0 {
		m["Elapsed"] = elapsed
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return string(b)
}

// TestReadGoTestJSONKeepsEveryEventAndBindsEveryField is the load-bearing test.
// It asserts two things a "parsed without error" check would not:
//
//  1. No line is dropped. A reader that skipped what it could not parse would
//     satisfy every err==nil assertion while quietly deleting the failure.
//  2. The struct tags actually bind. If GoTestEvent's json tags drifted (or a
//     field were renamed), every line would still unmarshal cleanly into a
//     zero-valued struct -- json.Unmarshal does not treat unknown keys as an
//     error. Every event would then carry Action "" and Test "", the verifier
//     would see no failures, and the suite would recompute as passing.
func TestReadGoTestJSONKeepsEveryEventAndBindsEveryField(t *testing.T) {
	const pkg = "github.com/xbcio/xflow/example"
	path := writeLines(t,
		eventLine(t, "run", pkg, "TestAlpha", 0),
		eventLine(t, "pass", pkg, "TestAlpha", 0.25),
		eventLine(t, "run", pkg, "TestBeta", 0),
		eventLine(t, "fail", pkg, "TestBeta", 1.5),
		eventLine(t, "fail", pkg, "", 1.75), // package-level failure
	)

	events, err := readGoTestJSON(path)
	if err != nil {
		t.Fatalf("readGoTestJSON() error = %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("readGoTestJSON() returned %d events, want 5: a line was dropped, "+
			"and a dropped fail line turns a failing suite into a passing one", len(events))
	}

	// The failure must have survived with its identity intact.
	var sawTestFail, sawPackageFail bool
	for _, ev := range events {
		if ev.Action == "fail" && ev.Test == "TestBeta" {
			sawTestFail = true
			if ev.Elapsed != 1.5 {
				t.Errorf("TestBeta fail event Elapsed = %v, want 1.5", ev.Elapsed)
			}
			if ev.Package != pkg {
				t.Errorf("TestBeta fail event Package = %q, want %q", ev.Package, pkg)
			}
		}
		if ev.Action == "fail" && ev.Test == "" {
			sawPackageFail = true
		}
	}
	if !sawTestFail {
		t.Fatal("no event with Action=fail Test=TestBeta: either the line was dropped " +
			"or the Action/Test json tags no longer bind, and the recomputed suite " +
			"would report the run as passing")
	}
	if !sawPackageFail {
		t.Fatal("no package-level Action=fail event (Test empty): recomputeSuite reads " +
			"package failure from exactly this shape")
	}
	if events[0].Time.IsZero() {
		t.Error("event Time is the zero value: the Time json tag no longer binds")
	}
}

// TestReadGoTestJSONRejectsMalformedLine pins the error return. Turning it into
// a `continue` is the single most dangerous edit in this file: it is the change
// anyone would make to "tolerate noise in the log", and it silently converts a
// failing run into a passing one whenever the dropped line is the failure.
func TestReadGoTestJSONRejectsMalformedLine(t *testing.T) {
	const pkg = "github.com/xbcio/xflow/example"
	path := writeLines(t,
		eventLine(t, "run", pkg, "TestAlpha", 0),
		`{"Action":"fail","Test":"TestBeta"`, // truncated JSON
		eventLine(t, "pass", pkg, "TestAlpha", 0.25),
	)

	events, err := readGoTestJSON(path)
	if err == nil {
		t.Fatalf("readGoTestJSON() error = nil for a malformed line (returned %d events): "+
			"unparseable output must abort the verification, never be skipped", len(events))
	}
	if events != nil {
		t.Errorf("readGoTestJSON() returned %d events alongside the error; callers that "+
			"check the slice before the error would verify against partial evidence", len(events))
	}
	// The message must locate the offending line -- an operator cannot fix a
	// parse failure that does not say what failed to parse.
	if !strings.Contains(err.Error(), "TestBeta") {
		t.Errorf("error = %q, want it to quote the offending line", err.Error())
	}
}

// TestReadGoTestJSONSkipsBlankLines is the paired positive case: blank lines
// must NOT be treated as malformed. Without the skip, json.Unmarshal("")
// returns "unexpected end of JSON input" and one stray blank line aborts a
// verification whose evidence is entirely intact.
func TestReadGoTestJSONSkipsBlankLines(t *testing.T) {
	const pkg = "github.com/xbcio/xflow/example"
	path := writeLines(t,
		eventLine(t, "run", pkg, "TestAlpha", 0),
		"",
		"   ",
		eventLine(t, "pass", pkg, "TestAlpha", 0.25),
	)

	events, err := readGoTestJSON(path)
	if err != nil {
		t.Fatalf("readGoTestJSON() error = %v: a blank line must be skipped, not rejected", err)
	}
	if len(events) != 2 {
		t.Fatalf("readGoTestJSON() returned %d events, want 2 (blank lines skipped, "+
			"real lines kept)", len(events))
	}
}

// TestReadGoTestJSONFailsLoudlyOnOverlongLine covers bufio.Scanner's 64 KiB
// default token limit against a shape `go test -json` really produces: an
// Output event carrying a test's stdout. A test that prints more than 64 KiB on
// one line yields a JSON line the default Scanner cannot hold.
//
// The behaviour that matters is not that it errors -- it is that it must never
// return the events it managed to read BEFORE the long line. Scan() stops at
// the overlong token, so a reader that ignored scanner.Err() would return a
// truncated event list, silently discarding every later event including the
// terminal package-level fail. That is the same "tests never ran ->
// verification passed" failure the raw-ledger merge guards against, arriving
// through the suite side instead.
func TestReadGoTestJSONFailsLoudlyOnOverlongLine(t *testing.T) {
	const pkg = "github.com/xbcio/xflow/example"
	huge, err := json.Marshal(map[string]any{
		"Action":  "output",
		"Package": pkg,
		"Test":    "TestNoisy",
		"Output":  strings.Repeat("x", 128*1024),
	})
	if err != nil {
		t.Fatalf("marshal huge event: %v", err)
	}
	path := writeLines(t,
		eventLine(t, "run", pkg, "TestNoisy", 0),
		string(huge),
		eventLine(t, "fail", pkg, "TestNoisy", 2),
		eventLine(t, "fail", pkg, "", 2.5),
	)

	events, err := readGoTestJSON(path)
	if err == nil {
		t.Fatalf("readGoTestJSON() error = nil for a %d-byte line, returned %d events: "+
			"the scanner stopped at the overlong token, so the two fail events after it "+
			"were silently dropped and the suite would recompute as passing",
			len(huge), len(events))
	}
	if len(events) != 0 {
		t.Errorf("readGoTestJSON() returned %d truncated events alongside the error", len(events))
	}
	if !strings.Contains(err.Error(), "too long") {
		t.Logf("note: error is %q rather than a token-too-long error; that is fine as "+
			"long as it is an error, but the diagnosis is worth keeping readable", err.Error())
	}
}

// TestReadGoTestJSONMissingFileErrors pins the open failure. The CLI's -in flag
// is operator-supplied; a typo must not be reported as an empty (and therefore
// exit-code-1-recomputing) event stream.
func TestReadGoTestJSONMissingFileErrors(t *testing.T) {
	_, err := readGoTestJSON(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err == nil {
		t.Fatal("readGoTestJSON() error = nil for a nonexistent path")
	}
	if !os.IsNotExist(err) {
		t.Errorf("error = %v, want an os.IsNotExist error so the caller can tell a "+
			"missing file from a corrupt one", err)
	}
}
