package architecture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	fakeEvidenceCandidateSHA = "1111111111111111111111111111111111111111"
	fakeEvidenceDriftedSHA   = "2222222222222222222222222222222222222222"
	fakePreviousReport       = "previous-report\n"
	fakePreviousEvents       = "previous-events\n"
	fakePreviousManifest     = "previous-manifest\n"
	fakeG1Kind               = "xflow.g1-evidence"
	fakeG1BindingVersion     = "xflow-g1-evidence-v2"
	fakeG1Package            = "github.com/xbcio/xflow/test/integration"
	fakeG1TestName           = "TestG1ProductionE2E"
)

var (
	fakeG1UUIDv4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	fakeG1SHA40Pattern  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	fakeG1SHA64Pattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type fakeG1ArtifactDescriptor struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type fakeG1TestIdentity struct {
	Package  string `json:"package"`
	Name     string `json:"name"`
	ExitCode int    `json:"exit_code"`
}

type fakeG1Manifest struct {
	Kind          string                   `json:"kind"`
	SchemaVersion int                      `json:"schema_version"`
	RunID         string                   `json:"run_id"`
	CandidateSHA  string                   `json:"candidate_sha"`
	Test          fakeG1TestIdentity       `json:"test"`
	Report        fakeG1ArtifactDescriptor `json:"report"`
	Events        fakeG1ArtifactDescriptor `json:"events"`
	Binding       struct {
		Version string `json:"version"`
		SHA256  string `json:"sha256"`
	} `json:"binding"`
}

type fakeG1Bundle struct {
	Manifest    fakeG1Manifest
	ReportPath  string
	EventsPath  string
	ReportBytes []byte
	EventsBytes []byte
}

func TestG1EvidenceTargetPublishesValidatedBoundArtifacts(t *testing.T) {
	run := runFakeG1EvidenceTarget(t, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
	if run.err != nil {
		t.Fatalf("G1 evidence target failed: %v\n%s", run.err, run.output)
	}

	bundle := validateG1ManifestArtifacts(t, run.manifestPath)
	manifest := bundle.Manifest
	if manifest.CandidateSHA != fakeEvidenceCandidateSHA {
		t.Fatalf("manifest candidate SHA = %q, want %q", manifest.CandidateSHA, fakeEvidenceCandidateSHA)
	}
	if !fakeG1UUIDv4Pattern.MatchString(manifest.RunID) {
		t.Fatalf("manifest run ID = %q, want canonical UUIDv4", manifest.RunID)
	}
	if bundle.ReportPath == run.reportPath || bundle.EventsPath == run.eventsPath {
		t.Fatalf("manifest references compatibility aliases instead of immutable generation files: report=%q events=%q", bundle.ReportPath, bundle.EventsPath)
	}
	assertFileMatches(t, run.reportPath, bundle.ReportBytes)
	assertFileMatches(t, run.eventsPath, bundle.EventsBytes)

	var report struct {
		SchemaVersion     int    `json:"schema_version"`
		RunID             string `json:"run_id"`
		CommitSHA         string `json:"commit_sha"`
		FullWorktreeClean bool   `json:"full_worktree_clean"`
	}
	if err := json.Unmarshal(bundle.ReportBytes, &report); err != nil {
		t.Fatalf("decode generated report: %v", err)
	}
	if report.SchemaVersion != 1 || report.RunID != manifest.RunID || report.CommitSHA != fakeEvidenceCandidateSHA || !report.FullWorktreeClean {
		t.Fatalf("published report provenance = %+v, want schema=1, manifest run ID, candidate SHA, and clean=true", report)
	}
	if err := verifyG1EventStream(bundle.EventsBytes, manifest.RunID); err != nil {
		t.Fatalf("verify immutable event stream: %v", err)
	}

	announcements := []string{
		"G1 immutable report published: " + bundle.ReportPath,
		"G1 immutable events published: " + bundle.EventsPath,
		"G1 report alias published: " + run.reportPath,
		"G1 event alias published: " + run.eventsPath,
		"G1 manifest published: " + run.manifestPath,
		"G1 evidence bundle committed for run " + manifest.RunID,
	}
	previous := -1
	for _, announcement := range announcements {
		position := strings.Index(run.output, announcement)
		if position < 0 {
			t.Fatalf("target did not announce %q:\n%s", announcement, run.output)
		}
		if position <= previous {
			t.Fatalf("publication announcements are out of manifest-last order at %q:\n%s", announcement, run.output)
		}
		previous = position
	}

	gitLog, err := os.ReadFile(run.gitLogPath)
	if err != nil {
		t.Fatalf("read fake Git log: %v", err)
	}
	if got := strings.Count(string(gitLog), "rev-parse --verify HEAD"); got != 2 {
		t.Fatalf("HEAD checks = %d, want pre/post checks; log:\n%s", got, gitLog)
	}
	if got := strings.Count(string(gitLog), "status --porcelain=v1 --untracked-files=all"); got != 2 {
		t.Fatalf("full-worktree checks = %d, want pre/post checks; log:\n%s", got, gitLog)
	}

	goLog, err := os.ReadFile(run.goLogPath)
	if err != nil {
		t.Fatalf("read fake Go log: %v", err)
	}
	if strings.Contains(string(goLog), "report_path="+run.reportPath+"\n") {
		t.Fatalf("test process wrote directly to the compatibility alias instead of a temporary path:\n%s", goLog)
	}
	for _, want := range []string{
		"run_id=" + manifest.RunID + "\n",
		"candidate_sha=" + fakeEvidenceCandidateSHA + "\n",
	} {
		if !strings.Contains(string(goLog), want) {
			t.Fatalf("fake producer log lacks %q, so Make did not propagate the run identity:\n%s", want, goLog)
		}
	}
}

func TestG1EvidenceTargetRejectsMixedAndTamperedGenerationEvents(t *testing.T) {
	t.Run("mixed generations", func(t *testing.T) {
		harness := newFakeG1TargetHarness(t)
		firstRun := harness.run(t, harness.paths, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
		if firstRun.err != nil {
			t.Fatalf("first G1 evidence target failed: %v\n%s", firstRun.err, firstRun.output)
		}
		first := validateG1ManifestArtifacts(t, firstRun.manifestPath)

		secondRun := harness.run(t, harness.paths, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
		if secondRun.err != nil {
			t.Fatalf("second G1 evidence target failed: %v\n%s", secondRun.err, secondRun.output)
		}
		second := validateG1ManifestArtifacts(t, secondRun.manifestPath)
		if first.Manifest.RunID == second.Manifest.RunID {
			t.Fatalf("consecutive runs reused UUID %q", first.Manifest.RunID)
		}
		if err := os.WriteFile(second.EventsPath, first.EventsBytes, 0o644); err != nil {
			t.Fatalf("mix first-generation events into second generation: %v", err)
		}
		if _, err := verifyG1ManifestArtifacts(secondRun.manifestPath); err == nil {
			t.Fatal("manifest unexpectedly accepted events from a different generation")
		} else if !strings.Contains(err.Error(), "events digest mismatch") {
			t.Fatalf("mixed-generation error = %v, want events digest mismatch", err)
		}
	})

	t.Run("tampered generation", func(t *testing.T) {
		run := runFakeG1EvidenceTarget(t, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
		if run.err != nil {
			t.Fatalf("G1 evidence target failed: %v\n%s", run.err, run.output)
		}
		bundle := validateG1ManifestArtifacts(t, run.manifestPath)
		if err := os.WriteFile(bundle.EventsPath, append(bundle.EventsBytes, []byte("tampered\n")...), 0o644); err != nil {
			t.Fatalf("tamper with generation events: %v", err)
		}
		if _, err := verifyG1ManifestArtifacts(run.manifestPath); err == nil {
			t.Fatal("manifest unexpectedly accepted tampered generation events")
		} else if !strings.Contains(err.Error(), "events size mismatch") {
			t.Fatalf("tamper error = %v, want events size mismatch", err)
		}
	})
}

func TestG1EvidenceTargetPreservesPriorGenerationAcrossManifestSwitch(t *testing.T) {
	harness := newFakeG1TargetHarness(t)
	firstRun := harness.run(t, harness.paths, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
	if firstRun.err != nil {
		t.Fatalf("first G1 evidence target failed: %v\n%s", firstRun.err, firstRun.output)
	}
	firstManifestBytes, err := os.ReadFile(firstRun.manifestPath)
	if err != nil {
		t.Fatalf("read first manifest: %v", err)
	}
	first, err := verifyG1ManifestBytes(firstManifestBytes, filepath.Dir(firstRun.manifestPath))
	if err != nil {
		t.Fatalf("verify first bundle: %v", err)
	}

	secondRun := harness.run(t, harness.paths, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
	if secondRun.err != nil {
		t.Fatalf("second G1 evidence target failed: %v\n%s", secondRun.err, secondRun.output)
	}
	secondManifestBytes, err := os.ReadFile(secondRun.manifestPath)
	if err != nil {
		t.Fatalf("read second manifest: %v", err)
	}
	second := validateG1ManifestArtifacts(t, secondRun.manifestPath)
	if first.Manifest.RunID == second.Manifest.RunID {
		t.Fatalf("manifest did not switch generations: both run IDs are %q", first.Manifest.RunID)
	}
	if bytes.Equal(firstManifestBytes, secondManifestBytes) {
		t.Fatal("second successful publication did not atomically switch the manifest")
	}
	if first.ReportPath == second.ReportPath || first.EventsPath == second.EventsPath {
		t.Fatalf("second publication reused immutable generation paths: first=%q/%q second=%q/%q", first.ReportPath, first.EventsPath, second.ReportPath, second.EventsPath)
	}

	assertFileMatches(t, first.ReportPath, first.ReportBytes)
	assertFileMatches(t, first.EventsPath, first.EventsBytes)
	if _, err := verifyG1ManifestBytes(firstManifestBytes, filepath.Dir(firstRun.manifestPath)); err != nil {
		t.Fatalf("first bundle no longer verifies after manifest switch: %v", err)
	}
	assertFileMatches(t, secondRun.reportPath, second.ReportBytes)
	assertFileMatches(t, secondRun.eventsPath, second.EventsBytes)

	for pattern, want := range map[string]int{
		"g1-evidence-*.report.json": 2,
		"g1-evidence-*.events.json": 2,
	} {
		matches, globErr := filepath.Glob(filepath.Join(filepath.Dir(secondRun.manifestPath), pattern))
		if globErr != nil {
			t.Fatalf("glob %s: %v", pattern, globErr)
		}
		if len(matches) != want {
			t.Fatalf("immutable files matching %s = %v, want %d generations", pattern, matches, want)
		}
	}
}

func TestG1EvidenceTargetRejectsGenerationOccupiedAfterPreflight(t *testing.T) {
	for _, generation := range []string{"report", "events"} {
		t.Run(generation, func(t *testing.T) {
			harness := newFakeG1TargetHarness(t)
			firstRun := harness.run(t, harness.paths, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceCandidateSHA})
			if firstRun.err != nil {
				t.Fatalf("initial G1 evidence target failed: %v\n%s", firstRun.err, firstRun.output)
			}
			firstManifestBytes, err := os.ReadFile(firstRun.manifestPath)
			if err != nil {
				t.Fatalf("read initial manifest: %v", err)
			}
			first := validateG1ManifestArtifacts(t, firstRun.manifestPath)

			const occupiedContents = "preoccupied-generation-must-not-be-overwritten\n"
			secondRun := harness.run(t, harness.paths, map[string]string{
				"EVIDENCE_CANDIDATE_SHA":       fakeEvidenceCandidateSHA,
				"FAKE_G1_PREOCCUPY_GENERATION": generation,
				"FAKE_G1_PREOCCUPIED_CONTENTS": occupiedContents,
			})
			if secondRun.err == nil {
				t.Fatalf("G1 evidence target overwrote a preoccupied %s generation:\n%s", generation, secondRun.output)
			}
			if !strings.Contains(secondRun.output, "File exists") {
				t.Fatalf("collision output lacks create-only failure:\n%s", secondRun.output)
			}
			if strings.Contains(secondRun.output, "G1 manifest published:") || strings.Contains(secondRun.output, "G1 evidence bundle committed") {
				t.Fatalf("target announced publication after a generation collision:\n%s", secondRun.output)
			}

			secondRunID := g1RunIDFromOutput(t, secondRun.output)
			if secondRunID == first.Manifest.RunID {
				t.Fatalf("collision run reused initial run ID %q", secondRunID)
			}
			occupiedPath := filepath.Join(
				filepath.Dir(secondRun.manifestPath),
				fmt.Sprintf("g1-evidence-%s.%s.json", secondRunID, generation),
			)
			assertFileContents(t, occupiedPath, occupiedContents)

			assertFileMatches(t, secondRun.reportPath, first.ReportBytes)
			assertFileMatches(t, secondRun.eventsPath, first.EventsBytes)
			assertFileMatches(t, secondRun.manifestPath, firstManifestBytes)
			assertFileMatches(t, first.ReportPath, first.ReportBytes)
			assertFileMatches(t, first.EventsPath, first.EventsBytes)
			if _, err := verifyG1ManifestBytes(firstManifestBytes, filepath.Dir(secondRun.manifestPath)); err != nil {
				t.Fatalf("initial generation no longer verifies after %s collision: %v", generation, err)
			}
		})
	}
}

func TestG1EvidenceTargetRequiresDistinctColocatedOutputPaths(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*fakeG1TargetPaths, string)
		wantOutput string
	}{
		{
			name: "report and events are the same path",
			mutate: func(paths *fakeG1TargetPaths, _ string) {
				paths.events = paths.report
			},
			wantOutput: "G1_REPORT, G1_EVENTS, and G1_MANIFEST must be different paths",
		},
		{
			name: "manifest is in another directory",
			mutate: func(paths *fakeG1TargetPaths, tmp string) {
				paths.manifest = filepath.Join(tmp, "other", "g1-manifest.json")
			},
			wantOutput: "G1_REPORT, G1_EVENTS, and G1_MANIFEST must share one canonical directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newFakeG1TargetHarness(t)
			paths := harness.paths
			tt.mutate(&paths, harness.tmp)
			run := harness.run(t, paths, nil)
			if run.err == nil {
				t.Fatalf("G1 evidence target unexpectedly accepted invalid output paths:\n%s", run.output)
			}
			if !strings.Contains(run.output, tt.wantOutput) {
				t.Fatalf("target output lacks %q:\n%s", tt.wantOutput, run.output)
			}
			assertFileContents(t, harness.paths.report, fakePreviousReport)
			assertFileContents(t, harness.paths.events, fakePreviousEvents)
			assertFileContents(t, harness.paths.manifest, fakePreviousManifest)
		})
	}
}

func TestG1EvidenceTargetRejectsUnpublishableRunsWithoutOverwritingFinals(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		wantOutput string
	}{
		{
			name:       "outer candidate SHA mismatch",
			env:        map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeEvidenceDriftedSHA},
			wantOutput: "does not match EVIDENCE_CANDIDATE_SHA",
		},
		{
			name:       "dirty before run",
			env:        map[string]string{"FAKE_GIT_PRE_STATUS": " M service/control/controlplane.go"},
			wantOutput: "worktree is dirty before G1 evidence",
		},
		{
			name:       "HEAD drift after run",
			env:        map[string]string{"FAKE_GIT_POST_SHA": fakeEvidenceDriftedSHA},
			wantOutput: "HEAD changed during G1 evidence",
		},
		{
			name:       "dirty after run",
			env:        map[string]string{"FAKE_GIT_POST_STATUS": "?? unexpected.go"},
			wantOutput: "worktree became dirty during G1 evidence",
		},
		{
			name:       "go test exits nonzero",
			env:        map[string]string{"FAKE_GO_TEST_EXIT": "47"},
			wantOutput: "G1 required evidence test failed (exit 47)",
		},
		{
			name:       "skipped test event",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "skip"},
			wantOutput: "contains forbidden Action=skip",
		},
		{
			name:       "failed test event",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "fail"},
			wantOutput: "contains forbidden Action=fail",
		},
		{
			name:       "duplicate top-level run",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "duplicate-run"},
			wantOutput: "must have exactly one run and one pass event",
		},
		{
			name:       "missing package pass",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "missing-package-pass"},
			wantOutput: "package must have exactly one package-level pass event",
		},
		{
			name:       "missing run marker",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "missing-marker"},
			wantOutput: "run ID marker must occur exactly once",
		},
		{
			name:       "duplicate run marker",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "duplicate-marker"},
			wantOutput: "run ID marker must occur exactly once",
		},
		{
			name:       "malformed event JSON",
			env:        map[string]string{"FAKE_G1_EVENT_MODE": "malformed"},
			wantOutput: "is not JSON",
		},
		{
			name:       "report SHA mismatch",
			env:        map[string]string{"FAKE_G1_REPORT_SHA": fakeEvidenceDriftedSHA},
			wantOutput: "commit_sha does not match candidate SHA",
		},
		{
			name:       "report records dirty worktree",
			env:        map[string]string{"FAKE_G1_REPORT_CLEAN": "false"},
			wantOutput: "full_worktree_clean must be true",
		},
		{
			name:       "report schema mismatch",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-schema"},
			wantOutput: "schema_version must equal 1",
		},
		{
			name:       "report run ID mismatch",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-run-id"},
			wantOutput: "run_id does not match the Make-issued run ID",
		},
		{
			name:       "authz metadata mismatch",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-authz-status"},
			wantOutput: "metadata does not match the required contract",
		},
		{
			name:       "authz row missing",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "missing-cancel-allow"},
			wantOutput: "authz_matrix must contain exactly 12 required rows",
		},
		{
			name:       "runtime alias mismatch",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-runtime"},
			wantOutput: "redis_addr must exactly match runtime.redis_endpoint",
		},
		{
			name:       "trace boolean false",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-trace"},
			wantOutput: "trace_graph.one_trace_id must be true",
		},
		{
			name:       "approval not pass",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-approval"},
			wantOutput: "approval_dag.timer_fired must equal pass",
		},
		{
			name:       "audit counts invalid",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-audit"},
			wantOutput: "audit_reconcile row counts must be equal and non-zero",
		},
		{
			name:       "dead-letter projection absent",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-dead-letter"},
			wantOutput: "dead_letter.durable_projection_rows must equal 1",
		},
		{
			name:       "required metrics incomplete",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-metrics"},
			wantOutput: "metrics_scrape.counters_observed must contain exactly",
		},
		{
			name:       "idempotency fence invalid",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "bad-idempotency"},
			wantOutput: "host fence outcomes must match an allowed terminal fence",
		},
		{
			name:       "empty report",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "empty"},
			wantOutput: "temporary structured report missing or empty",
		},
		{
			name:       "malformed report",
			env:        map[string]string{"FAKE_G1_REPORT_MODE": "malformed"},
			wantOutput: "cannot parse structured report",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := runFakeG1EvidenceTarget(t, tt.env)
			if run.err == nil {
				t.Fatalf("G1 evidence target unexpectedly succeeded:\n%s", run.output)
			}
			if !strings.Contains(run.output, tt.wantOutput) {
				t.Fatalf("target output lacks %q:\n%s", tt.wantOutput, run.output)
			}
			if strings.Contains(run.output, "G1 evidence bundle committed") || strings.Contains(run.output, "G1 manifest published:") {
				t.Fatalf("target announced a committed bundle after a failed gate:\n%s", run.output)
			}
			assertFileContents(t, run.reportPath, fakePreviousReport)
			assertFileContents(t, run.eventsPath, fakePreviousEvents)
			assertFileContents(t, run.manifestPath, fakePreviousManifest)
			assertNoG1GenerationFiles(t, filepath.Dir(run.manifestPath))
		})
	}
}

func TestG0EvidenceTargetRequiresStableCleanWorktree(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		wantOutput string
	}{
		{
			name:       "dirty before run",
			env:        map[string]string{"FAKE_GIT_PRE_STATUS": " M engine/core.go"},
			wantOutput: "worktree is dirty before G0 evidence",
		},
		{
			name:       "HEAD drift after verifier",
			env:        map[string]string{"FAKE_GIT_POST_SHA": fakeEvidenceDriftedSHA},
			wantOutput: "HEAD changed during G0 evidence",
		},
		{
			name:       "dirty after verifier",
			env:        map[string]string{"FAKE_GIT_POST_STATUS": "?? generated.txt"},
			wantOutput: "worktree became dirty during G0 evidence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := runFakeG0EvidenceTarget(t, tt.env)
			if run.err == nil {
				t.Fatalf("G0 evidence target unexpectedly succeeded:\n%s", run.output)
			}
			if !strings.Contains(run.output, tt.wantOutput) {
				t.Fatalf("target output lacks %q:\n%s", tt.wantOutput, run.output)
			}
			if strings.Contains(run.output, "G0 evidence artifact published") {
				t.Fatalf("target announced publication after a failed git gate:\n%s", run.output)
			}
			assertFileContents(t, run.artifactPath, fakePreviousG0Artifact)
			assertFileContents(t, run.digestPath, fakePreviousG0Digest)
		})
	}
}

type fakeG1TargetPaths struct {
	report   string
	events   string
	manifest string
}

type fakeG1TargetHarness struct {
	tmp        string
	fakeGo     string
	fakeGit    string
	fakeMktemp string
	realMktemp string
	paths      fakeG1TargetPaths
	goLogPath  string
	gitLogPath string
	runCount   int
}

type fakeG1TargetRun struct {
	output       string
	err          error
	reportPath   string
	eventsPath   string
	manifestPath string
	goLogPath    string
	gitLogPath   string
}

func newFakeG1TargetHarness(t *testing.T) *fakeG1TargetHarness {
	t.Helper()
	tmp := t.TempDir()
	realMktemp, err := exec.LookPath("mktemp")
	if err != nil {
		t.Fatalf("find real mktemp: %v", err)
	}
	publishedDir := filepath.Join(tmp, "published evidence with spaces")
	if err := os.MkdirAll(publishedDir, 0o755); err != nil {
		t.Fatalf("create fake publication directory: %v", err)
	}
	harness := &fakeG1TargetHarness{
		tmp:        tmp,
		fakeGo:     filepath.Join(tmp, "go"),
		fakeGit:    filepath.Join(tmp, "git"),
		fakeMktemp: filepath.Join(tmp, "mktemp"),
		realMktemp: realMktemp,
		paths: fakeG1TargetPaths{
			report:   filepath.Join(publishedDir, "g1-report.json"),
			events:   filepath.Join(publishedDir, "g1-events.json"),
			manifest: filepath.Join(publishedDir, "g1-manifest.json"),
		},
		goLogPath:  filepath.Join(tmp, "go-invocations.log"),
		gitLogPath: filepath.Join(tmp, "git-invocations.log"),
	}
	writeFakeG1Go(t, harness.fakeGo)
	writeFakeEvidenceGit(t, harness.fakeGit)
	writeFakeG1Mktemp(t, harness.fakeMktemp)
	for path, contents := range map[string]string{
		harness.paths.report:   fakePreviousReport,
		harness.paths.events:   fakePreviousEvents,
		harness.paths.manifest: fakePreviousManifest,
	} {
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatalf("seed prior artifact %s: %v", path, err)
		}
	}
	return harness
}

func runFakeG1EvidenceTarget(t *testing.T, extraEnv map[string]string) fakeG1TargetRun {
	t.Helper()
	harness := newFakeG1TargetHarness(t)
	return harness.run(t, harness.paths, extraEnv)
}

func (h *fakeG1TargetHarness) run(t *testing.T, paths fakeG1TargetPaths, extraEnv map[string]string) fakeG1TargetRun {
	t.Helper()
	h.runCount++
	gitStateDir := filepath.Join(h.tmp, fmt.Sprintf("git-state-%d", h.runCount))
	if err := os.MkdirAll(gitStateDir, 0o755); err != nil {
		t.Fatalf("create fake Git state directory: %v", err)
	}
	cmd := exec.Command(
		"make",
		"test-g1-evidence-required",
		"GO="+h.fakeGo,
		"GIT="+h.fakeGit,
		"G1_REPORT="+paths.report,
		"G1_EVENTS="+paths.events,
		"G1_MANIFEST="+paths.manifest,
	)
	cmd.Dir = findEvidenceRepositoryRoot(t)
	cmd.Env = append(os.Environ(),
		"PATH="+h.tmp+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_GO_LOG="+h.goLogPath,
		"FAKE_GIT_LOG="+h.gitLogPath,
		"FAKE_GIT_STATE_DIR="+gitStateDir,
		"FAKE_G1_FINAL_REPORT="+paths.report,
		"FAKE_REAL_MKTEMP="+h.realMktemp,
	)
	for key, value := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	return fakeG1TargetRun{
		output:       string(output),
		err:          err,
		reportPath:   paths.report,
		eventsPath:   paths.events,
		manifestPath: paths.manifest,
		goLogPath:    h.goLogPath,
		gitLogPath:   h.gitLogPath,
	}
}

func g1RunIDFromOutput(t *testing.T, output string) string {
	t.Helper()
	const prefix = "==> G1 evidence run ID: "
	for _, line := range strings.Split(output, "\n") {
		if runID, ok := strings.CutPrefix(line, prefix); ok {
			if !fakeG1UUIDv4Pattern.MatchString(runID) {
				t.Fatalf("published run ID = %q, want canonical UUIDv4", runID)
			}
			return runID
		}
	}
	t.Fatalf("target output lacks G1 run ID:\n%s", output)
	return ""
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	assertFileMatches(t, path, []byte(want))
}

func assertFileMatches(t *testing.T, path string, want []byte) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("contents of %s = %q, want %q", path, raw, want)
	}
}

func assertNoG1GenerationFiles(t *testing.T, directory string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(directory, "g1-evidence-*.json"))
	if err != nil {
		t.Fatalf("glob G1 generation files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("failed publication left generation files behind: %v", matches)
	}
}

func validateG1ManifestArtifacts(t *testing.T, manifestPath string) fakeG1Bundle {
	t.Helper()
	bundle, err := verifyG1ManifestArtifacts(manifestPath)
	if err != nil {
		t.Fatalf("verify G1 manifest: %v", err)
	}
	return bundle
}

func verifyG1ManifestArtifacts(manifestPath string) (fakeG1Bundle, error) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return fakeG1Bundle{}, fmt.Errorf("read manifest: %w", err)
	}
	return verifyG1ManifestBytes(raw, filepath.Dir(manifestPath))
}

func verifyG1ManifestBytes(raw []byte, manifestDir string) (fakeG1Bundle, error) {
	manifest, err := decodeG1Manifest(raw)
	if err != nil {
		return fakeG1Bundle{}, err
	}
	if manifest.Kind != fakeG1Kind || manifest.SchemaVersion != 1 {
		return fakeG1Bundle{}, fmt.Errorf("invalid manifest kind/schema: kind=%q schema=%d", manifest.Kind, manifest.SchemaVersion)
	}
	if !fakeG1UUIDv4Pattern.MatchString(manifest.RunID) {
		return fakeG1Bundle{}, fmt.Errorf("manifest run_id %q is not a canonical UUIDv4", manifest.RunID)
	}
	if !fakeG1SHA40Pattern.MatchString(manifest.CandidateSHA) {
		return fakeG1Bundle{}, fmt.Errorf("manifest candidate_sha %q is invalid", manifest.CandidateSHA)
	}
	if manifest.Test != (fakeG1TestIdentity{Package: fakeG1Package, Name: fakeG1TestName, ExitCode: 0}) {
		return fakeG1Bundle{}, fmt.Errorf("manifest test identity = %+v, want package/name/exit 0", manifest.Test)
	}
	if manifest.Report.Path != fmt.Sprintf("g1-evidence-%s.report.json", manifest.RunID) {
		return fakeG1Bundle{}, fmt.Errorf("report generation basename = %q, does not bind run ID", manifest.Report.Path)
	}
	if manifest.Events.Path != fmt.Sprintf("g1-evidence-%s.events.json", manifest.RunID) {
		return fakeG1Bundle{}, fmt.Errorf("events generation basename = %q, does not bind run ID", manifest.Events.Path)
	}
	if manifest.Report.Path == manifest.Events.Path {
		return fakeG1Bundle{}, fmt.Errorf("report and events generation paths are identical")
	}

	root, err := filepath.Abs(manifestDir)
	if err != nil {
		return fakeG1Bundle{}, fmt.Errorf("resolve manifest directory: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fakeG1Bundle{}, fmt.Errorf("resolve manifest directory symlinks: %w", err)
	}
	reportPath, reportBytes, err := verifyG1ArtifactDescriptor(root, "report", manifest.Report)
	if err != nil {
		return fakeG1Bundle{}, err
	}
	eventsPath, eventsBytes, err := verifyG1ArtifactDescriptor(root, "events", manifest.Events)
	if err != nil {
		return fakeG1Bundle{}, err
	}

	if manifest.Binding.Version != fakeG1BindingVersion {
		return fakeG1Bundle{}, fmt.Errorf("binding version = %q, want %q", manifest.Binding.Version, fakeG1BindingVersion)
	}
	if !fakeG1SHA64Pattern.MatchString(manifest.Binding.SHA256) {
		return fakeG1Bundle{}, fmt.Errorf("binding SHA-256 = %q, want lowercase 64-hex", manifest.Binding.SHA256)
	}
	bindingMaterial := map[string]any{
		"binding_version": fakeG1BindingVersion,
		"candidate_sha":   manifest.CandidateSHA,
		"events": map[string]any{
			"path": manifest.Events.Path, "size": manifest.Events.Size, "sha256": manifest.Events.SHA256,
		},
		"kind": fakeG1Kind,
		"report": map[string]any{
			"path": manifest.Report.Path, "size": manifest.Report.Size, "sha256": manifest.Report.SHA256,
		},
		"run_id":         manifest.RunID,
		"schema_version": manifest.SchemaVersion,
		"test": map[string]any{
			"exit_code": manifest.Test.ExitCode, "name": manifest.Test.Name, "package": manifest.Test.Package,
		},
	}
	canonical, err := json.Marshal(bindingMaterial)
	if err != nil {
		return fakeG1Bundle{}, fmt.Errorf("marshal binding material: %w", err)
	}
	bindingSum := sha256.Sum256(canonical)
	wantBinding := hex.EncodeToString(bindingSum[:])
	if manifest.Binding.SHA256 != wantBinding {
		return fakeG1Bundle{}, fmt.Errorf("binding digest mismatch: got %s want %s", manifest.Binding.SHA256, wantBinding)
	}

	var reportProvenance struct {
		SchemaVersion     int    `json:"schema_version"`
		RunID             string `json:"run_id"`
		CommitSHA         string `json:"commit_sha"`
		FullWorktreeClean bool   `json:"full_worktree_clean"`
	}
	if err := json.Unmarshal(reportBytes, &reportProvenance); err != nil {
		return fakeG1Bundle{}, fmt.Errorf("decode generation report: %w", err)
	}
	if reportProvenance.SchemaVersion != 1 || reportProvenance.RunID != manifest.RunID || reportProvenance.CommitSHA != manifest.CandidateSHA || !reportProvenance.FullWorktreeClean {
		return fakeG1Bundle{}, fmt.Errorf("generation report provenance does not match manifest: %+v", reportProvenance)
	}
	if err := verifyG1EventStream(eventsBytes, manifest.RunID); err != nil {
		return fakeG1Bundle{}, fmt.Errorf("generation events: %w", err)
	}

	return fakeG1Bundle{
		Manifest:    manifest,
		ReportPath:  reportPath,
		EventsPath:  eventsPath,
		ReportBytes: reportBytes,
		EventsBytes: eventsBytes,
	}, nil
}

func decodeG1Manifest(raw []byte) (fakeG1Manifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest fakeG1Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return fakeG1Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fakeG1Manifest{}, fmt.Errorf("decode manifest: trailing JSON value")
		}
		return fakeG1Manifest{}, fmt.Errorf("decode manifest trailing data: %w", err)
	}
	return manifest, nil
}

func verifyG1ArtifactDescriptor(root, field string, descriptor fakeG1ArtifactDescriptor) (string, []byte, error) {
	if descriptor.Path == "" || descriptor.Path == "." || descriptor.Path == ".." || filepath.IsAbs(descriptor.Path) || filepath.Base(descriptor.Path) != descriptor.Path || strings.ContainsAny(descriptor.Path, `/\\`) {
		return "", nil, fmt.Errorf("%s path %q is not a safe relative basename", field, descriptor.Path)
	}
	if descriptor.Size < 0 {
		return "", nil, fmt.Errorf("%s size %d is negative", field, descriptor.Size)
	}
	if !fakeG1SHA64Pattern.MatchString(descriptor.SHA256) {
		return "", nil, fmt.Errorf("%s SHA-256 %q is invalid", field, descriptor.SHA256)
	}
	path := filepath.Join(root, descriptor.Path)
	info, err := os.Lstat(path)
	if err != nil {
		return "", nil, fmt.Errorf("stat %s generation: %w", field, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, fmt.Errorf("%s generation is not a regular non-symlink file", field)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", nil, fmt.Errorf("resolve %s generation: %w", field, err)
	}
	if filepath.Dir(resolved) != root {
		return "", nil, fmt.Errorf("%s generation escapes manifest directory", field)
	}
	raw, err := os.ReadFile(resolved)
	if err != nil {
		return "", nil, fmt.Errorf("read %s generation: %w", field, err)
	}
	if int64(len(raw)) != descriptor.Size {
		return "", nil, fmt.Errorf("%s size mismatch: got %d want %d", field, len(raw), descriptor.Size)
	}
	digest := sha256.Sum256(raw)
	gotDigest := hex.EncodeToString(digest[:])
	if gotDigest != descriptor.SHA256 {
		return "", nil, fmt.Errorf("%s digest mismatch: got %s want %s", field, gotDigest, descriptor.SHA256)
	}
	return resolved, raw, nil
}

func verifyG1EventStream(raw []byte, runID string) error {
	marker := "xflow-g1-evidence-run-id=" + runID
	lines := bytes.Split(raw, []byte("\n"))
	var testRuns, testPasses, packagePasses, markerPositions []int
	for index, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event struct {
			Action  string `json:"Action"`
			Package string `json:"Package"`
			Test    string `json:"Test"`
			Output  string `json:"Output"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("line %d is not a JSON object: %w", index+1, err)
		}
		if event.Package != fakeG1Package {
			return fmt.Errorf("line %d package = %q, want %q", index+1, event.Package, fakeG1Package)
		}
		if event.Action == "" {
			return fmt.Errorf("line %d action is empty", index+1)
		}
		if event.Action == "skip" || event.Action == "fail" {
			return fmt.Errorf("line %d contains forbidden action %q", index+1, event.Action)
		}
		if event.Test == fakeG1TestName && event.Action == "run" {
			testRuns = append(testRuns, index)
		}
		if event.Test == fakeG1TestName && event.Action == "pass" {
			testPasses = append(testPasses, index)
		}
		if event.Test == "" && event.Action == "pass" {
			packagePasses = append(packagePasses, index)
		}
		count := strings.Count(event.Output, marker)
		if count > 0 {
			if event.Test != fakeG1TestName || event.Action != "output" {
				return fmt.Errorf("run ID marker is not a test output event")
			}
			for range count {
				markerPositions = append(markerPositions, index)
			}
		}
	}
	if len(testRuns) != 1 || len(testPasses) != 1 {
		return fmt.Errorf("test run/pass counts = %d/%d, want 1/1", len(testRuns), len(testPasses))
	}
	if len(packagePasses) != 1 {
		return fmt.Errorf("package pass count = %d, want 1", len(packagePasses))
	}
	if len(markerPositions) != 1 {
		return fmt.Errorf("run ID marker count = %d, want 1", len(markerPositions))
	}
	if !(testRuns[0] < markerPositions[0] && markerPositions[0] < testPasses[0] && testPasses[0] < packagePasses[0]) {
		return fmt.Errorf("run, marker, test pass, and package pass events are out of order")
	}
	return nil
}

func writeFakeG1Mktemp(t *testing.T, path string) {
	t.Helper()
	const script = `#!/bin/sh
set -eu
created="$($FAKE_REAL_MKTEMP "$@")"
case "$created" in
  *.xflow-g1-generation-${FAKE_G1_PREOCCUPY_GENERATION:-}.??????)
    if [ -n "${FAKE_G1_PREOCCUPY_GENERATION:-}" ]; then
      run_id="$(awk -F= '/^run_id=/{value=$2} END{print value}' "$FAKE_GO_LOG")"
      if [ -z "$run_id" ]; then
        printf '%s\n' 'fake mktemp could not resolve the G1 run ID' >&2
        exit 93
      fi
      occupied="$(dirname "$created")/g1-evidence-$run_id.${FAKE_G1_PREOCCUPY_GENERATION}.json"
      printf '%s' "${FAKE_G1_PREOCCUPIED_CONTENTS:-preoccupied-generation}" > "$occupied"
    fi
    ;;
esac
printf '%s\n' "$created"
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake G1 mktemp command: %v", err)
	}
}

func writeFakeG1Go(t *testing.T, path string) {
	t.Helper()
	const script = `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_GO_LOG"
case "$*" in
  "list -m -f {{.GoVersion}}")
    printf '%s\n' '1.25.0'
    ;;
  "env GOVERSION")
    printf '%s\n' 'go1.25.0'
    ;;
  "env GOTOOLCHAIN")
    printf '%s\n' 'go1.25.0'
    ;;
  "list -tags=integration ./test/integration")
    printf '%s\n' 'github.com/xbcio/xflow/test/integration'
    ;;
  test\ -json\ *)
    report_path="${XFLOW_G1_REPORT_PATH:-}"
    candidate_sha="${XFLOW_G1_CANDIDATE_SHA:-}"
    run_id="${XFLOW_G1_RUN_ID:-}"
    if [ -z "$report_path" ] || [ -z "$candidate_sha" ] || [ -z "$run_id" ]; then
      printf '%s\n' 'missing G1 report path, candidate SHA, or run ID environment' >&2
      exit 91
    fi
    if [ -n "${FAKE_G1_FINAL_REPORT:-}" ] && [ "$report_path" = "$FAKE_G1_FINAL_REPORT" ]; then
      printf '%s\n' 'test process was given the final report alias path' >&2
      exit 92
    fi
    printf 'report_path=%s\nrun_id=%s\ncandidate_sha=%s\n' "$report_path" "$run_id" "$candidate_sha" >> "$FAKE_GO_LOG"
    case "${FAKE_G1_REPORT_MODE:-valid}" in
      missing)
        ;;
      empty)
        : > "$report_path"
        ;;
      malformed)
        printf '%s\n' '{' > "$report_path"
        ;;
      *)
        python3 - "$report_path" "${FAKE_G1_REPORT_SHA:-$candidate_sha}" "${FAKE_G1_REPORT_CLEAN:-true}" "$run_id" "${FAKE_G1_REPORT_MODE:-valid}" <<'PY'
import json
import pathlib
import sys

report_path, candidate_sha, clean_text, run_id, mode = sys.argv[1:]
authz = [
    {"scenario": "workflow_submit_allow", "operation": "workflow.create", "route": "POST /v1/workflows/execute", "token": "full-A", "scope": "workflow", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "workflow_submit_no_execution_scope_allow", "operation": "workflow.create", "route": "POST /v1/workflows/execute", "token": "noexec-A", "scope": "workflow", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "workflow_entry_invoke_allow", "operation": "workflow.create", "route": "POST /v1/workflows/execute", "token": "full-A", "scope": "workflow", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_inspect_allow", "operation": "execution.read", "route": "GET /v1/executions/{id}", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_inspect_scope_deny", "operation": "execution.read", "route": "GET /v1/executions/{id}", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
    {"scenario": "execution_inspect_cross_namespace_idor", "operation": "execution.read", "route": "GET /v1/executions/{id}", "token": "full-B (cross-namespace)", "scope": "execution", "expected": 404, "got": 404, "decision": "deny"},
    {"scenario": "execution_signal_allow", "operation": "execution.signal", "route": "POST /v1/executions/{id}/signals", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_signal_scope_deny", "operation": "execution.signal", "route": "POST /v1/executions/{id}/signals", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
    {"scenario": "execution_revoke_allow", "operation": "execution.revoke", "route": "DELETE /v1/executions/{id}/signals/{name}", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_revoke_scope_deny", "operation": "execution.revoke", "route": "DELETE /v1/executions/{id}/signals/{name}", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
    {"scenario": "execution_cancel_allow", "operation": "execution.cancel", "route": "POST /v1/executions/{id}/cancel", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_cancel_scope_deny", "operation": "execution.cancel", "route": "POST /v1/executions/{id}/cancel", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
]
required_metrics = [
    "xflow_lease_acquire_duration_seconds",
    "xflow_audit_reconcile_scan_total",
    "xflow_audit_reconcile_settled_total",
]
document = {
    "schema_version": 1,
    "run_id": run_id,
    "generated_at": "2026-08-30T00:00:00Z",
    "go_version": "go1.25.0",
    "os": "darwin/arm64",
    "commit_sha": candidate_sha,
    "full_worktree_clean": clean_text == "true",
    "redis_addr": "redis.example:6379",
    "mysql_dsn_host": "mysql.example:3306",
    "runtime": {
        "redis_endpoint": "redis.example:6379",
        "redis_version": "8.2.1",
        "mysql_network": "tcp",
        "mysql_endpoint": "mysql.example:3306",
        "mysql_database": "xflow",
        "mysql_server_version": "8.4.0",
    },
    "authz_matrix": authz,
    "trace_graph": {
        "spans_present": [
            "xflow.workflow.execute",
            "xflow.task.dispatch",
            "xflow.task.execute",
            "xflow.task.report",
            "xflow.task.commit",
        ],
        "one_trace_id": True,
        "dispatch_parented_to_submit": True,
        "commit_parented_to_report": True,
        "namespace_a_one_trace_id": True,
        "namespace_a_dispatch_parented_to_submit": True,
        "namespace_a_commit_parented_to_report": True,
        "cross_namespace_carrier_isolated": True,
    },
    "approval_dag": {
        "multi_signal_quorum": "pass",
        "timer_fired": "pass",
        "cancel": "pass",
        "cyclic_reset": "pass",
        "repeat_signal_409": "pass",
    },
    "audit_reconcile": {
        "admission_rows": 3,
        "outcome_rows": 3,
        "reconciled_by_worker": 3,
        "idempotent_outcome_appends": True,
        "sweeps_to_settle": 1,
        "fault_matrix_pass": True,
    },
    "dead_letter": {
        "seeded": True,
        "replay_outcome": "replayed",
        "receipt_audit_id_set": True,
        "durable_projection_rows": 1,
    },
    "metrics_scrape": {
        "scraped": True,
        "counters_observed": required_metrics,
        "required_families": required_metrics,
        "observed_families": required_metrics,
        "missing_families": [],
    },
    "idempotency_report": {
        "repeat_signal_outcome": "409",
        "duplicate_report_outcome": "duplicate_terminal",
        "handler_side_effects_assertion": "handler_invocations=2, business_rows=1 (idempotent receiver keyed by execution_id+node_name; host fence=duplicate_terminal)",
        "independent_executions_for_same_def": True,
        "invocation_level_idempotency_key": "not implemented (out of G1 scope)",
        "handler_invocations": 2,
        "business_rows": 1,
        "idempotency_key": "execution_id+node_name (UNIQUE constraint)",
        "host_fence_outcome": "duplicate_terminal",
    },
}
if mode == "bad-schema":
    document["schema_version"] = 2
elif mode == "bad-run-id":
    document["run_id"] = "550e8400-e29b-41d4-a716-446655440000"
elif mode == "bad-authz-status":
    document["authz_matrix"][0]["got"] = 500
elif mode == "missing-cancel-allow":
    document["authz_matrix"] = [row for row in document["authz_matrix"] if row["scenario"] != "execution_cancel_allow"]
elif mode == "bad-runtime":
    document["redis_addr"] = "another-redis.example:6379"
elif mode == "bad-trace":
    document["trace_graph"]["one_trace_id"] = False
elif mode == "bad-approval":
    document["approval_dag"]["timer_fired"] = "fail"
elif mode == "bad-audit":
    document["audit_reconcile"]["outcome_rows"] = 2
elif mode == "bad-dead-letter":
    document["dead_letter"]["durable_projection_rows"] = 0
elif mode == "bad-metrics":
    document["metrics_scrape"]["counters_observed"] = required_metrics[:1]
elif mode == "bad-idempotency":
    document["idempotency_report"]["host_fence_outcome"] = "accepted"
pathlib.Path(report_path).write_text(json.dumps(document, separators=(",", ":")) + "\n", encoding="utf-8")
PY
        ;;
    esac
    package='github.com/xbcio/xflow/test/integration'
    emit_run() {
      printf '{"Test":"TestG1ProductionE2E","Action":"run","Package":"%s"}\n' "$package"
    }
    emit_marker() {
      printf '{"Output":"xflow-g1-evidence-run-id=%s\\n","Package":"%s","Test":"TestG1ProductionE2E","Action":"output"}\n' "$run_id" "$package"
    }
    emit_test_pass() {
      printf '{"Package":"%s","Elapsed":0.01,"Action":"pass","Test":"TestG1ProductionE2E"}\n' "$package"
    }
    emit_package_pass() {
      printf '{"Elapsed":0.02,"Package":"%s","Action":"pass"}\n' "$package"
    }
    emit_run
    case "${FAKE_G1_EVENT_MODE:-pass}" in
      skip)
        emit_marker
        printf '{"Package":"%s","Action":"skip","Test":"TestG1ProductionE2E"}\n' "$package"
        emit_package_pass
        ;;
      fail)
        emit_marker
        printf '{"Test":"TestG1ProductionE2E","Package":"%s","Action":"fail"}\n' "$package"
        emit_package_pass
        ;;
      duplicate-run)
        emit_run
        emit_marker
        emit_test_pass
        emit_package_pass
        ;;
      missing-package-pass)
        emit_marker
        emit_test_pass
        ;;
      missing-marker)
        emit_test_pass
        emit_package_pass
        ;;
      duplicate-marker)
        emit_marker
        emit_marker
        emit_test_pass
        emit_package_pass
        ;;
      malformed)
        printf '%s\n' 'not-json'
        emit_marker
        emit_test_pass
        emit_package_pass
        ;;
      *)
        emit_marker
        emit_test_pass
        emit_package_pass
        ;;
    esac
    exit "${FAKE_GO_TEST_EXIT:-0}"
    ;;
  *)
    printf '%s\n' "unexpected fake Go invocation: $*" >&2
    exit 97
    ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake G1 Go command: %v", err)
	}
}

func writeFakeEvidenceGit(t *testing.T, path string) {
	t.Helper()
	const script = `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_GIT_LOG"
next_count() {
  counter="$FAKE_GIT_STATE_DIR/$1"
  count=0
  if [ -f "$counter" ]; then
    count="$(cat "$counter")"
  fi
  count=$((count + 1))
  printf '%s\n' "$count" > "$counter"
  printf '%s\n' "$count"
}
case "$*" in
  "rev-parse --verify HEAD")
    count="$(next_count rev-count)"
    if [ "$count" -gt 1 ] && [ -n "${FAKE_GIT_POST_SHA:-}" ]; then
      printf '%s\n' "$FAKE_GIT_POST_SHA"
    else
      printf '%s\n' "${FAKE_GIT_PRE_SHA:-1111111111111111111111111111111111111111}"
    fi
    ;;
  "status --porcelain=v1 --untracked-files=all")
    count="$(next_count status-count)"
    if [ "$count" -gt 1 ]; then
      status="${FAKE_GIT_POST_STATUS:-}"
    else
      status="${FAKE_GIT_PRE_STATUS:-}"
    fi
    if [ -n "$status" ]; then
      printf '%s\n' "$status"
    fi
    ;;
  *)
    printf '%s\n' "unexpected fake Git invocation: $*" >&2
    exit 98
    ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake Git command: %v", err)
	}
}
