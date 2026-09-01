package architecture

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	fakeG0EvidenceCandidateSHA = "1111111111111111111111111111111111111111"
	fakeG0EvidenceDriftedSHA   = "2222222222222222222222222222222222222222"
	fakeG0EvidenceRunID        = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	fakePreviousG0Artifact     = "previous-g0-artifact\n"
	fakePreviousG0Digest       = "previous-g0-digest\n"
)

func TestG0EvidenceTargetPropagatesCriticalFailures(t *testing.T) {
	tests := []struct {
		name                string
		buildExit           string
		testExit            string
		verifyExit          string
		wantVerifierInvoked bool
	}{
		{
			name:      "test binary build",
			buildExit: "41",
		},
		{
			name:     "test suite",
			testExit: "42",
		},
		{
			name:                "independent verifier",
			verifyExit:          "43",
			wantVerifierInvoked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := runFakeG0EvidenceTarget(t, map[string]string{
				"FAKE_GO_BUILD_EXIT":  tt.buildExit,
				"FAKE_GO_TEST_EXIT":   tt.testExit,
				"FAKE_GO_VERIFY_EXIT": tt.verifyExit,
			})
			if run.err == nil {
				t.Fatalf("target succeeded after %s failure; output:\n%s", tt.name, run.output)
			}
			if strings.Contains(run.output, "G0 evidence artifact published") {
				t.Fatalf("target announced a published artifact after %s failure; output:\n%s", tt.name, run.output)
			}

			invocations, err := os.ReadFile(run.goLogPath)
			if err != nil {
				t.Fatalf("read fake Go invocation log: %v", err)
			}
			verifierInvoked := strings.Contains(string(invocations), "run ./test/integration/cmd/evidence-verify")
			if verifierInvoked != tt.wantVerifierInvoked {
				t.Fatalf("verifier invoked = %v, want %v; invocations:\n%s", verifierInvoked, tt.wantVerifierInvoked, invocations)
			}
			assertG0FileContents(t, run.artifactPath, fakePreviousG0Artifact)
			assertG0FileContents(t, run.digestPath, fakePreviousG0Digest)
		})
	}
}

func TestG0EvidenceTargetPublishesOnlyAfterStrictValidation(t *testing.T) {
	run := runFakeG0EvidenceTarget(t, map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeG0EvidenceCandidateSHA})
	if run.err != nil {
		t.Fatalf("G0 evidence target failed: %v\n%s", run.err, run.output)
	}
	if !strings.Contains(run.output, "G0 evidence artifact published") {
		t.Fatalf("target did not announce publication:\n%s", run.output)
	}

	artifact, err := os.ReadFile(run.artifactPath)
	if err != nil {
		t.Fatalf("read published artifact: %v", err)
	}
	digest, err := os.ReadFile(run.digestPath)
	if err != nil {
		t.Fatalf("read published digest: %v", err)
	}
	sum := sha256.Sum256(artifact)
	wantDigest := hex.EncodeToString(sum[:]) + "\n"
	if string(digest) != wantDigest {
		t.Fatalf("published digest = %q, want SHA-256(artifact) = %q", digest, wantDigest)
	}
	for _, path := range []string{run.testBinPath, run.eventsPath} {
		if info, statErr := os.Stat(path); statErr != nil || info.Size() == 0 {
			t.Fatalf("published file %s missing or empty: info=%v err=%v", path, info, statErr)
		}
	}
}

func TestG0EvidenceTargetRejectsInvalidFinalsWithoutOverwriting(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		wantOutput string
	}{
		{
			name:       "outer candidate SHA mismatch",
			env:        map[string]string{"EVIDENCE_CANDIDATE_SHA": fakeG0EvidenceDriftedSHA},
			wantOutput: "does not match EVIDENCE_CANDIDATE_SHA",
		},
		{
			name:       "wrong schema version",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "wrong-schema-version"},
			wantOutput: "schema_version must be integer 2",
		},
		{
			name:       "invalid run id",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "invalid-run-id"},
			wantOutput: "run_id must be a canonical RFC 4122 UUIDv4",
		},
		{
			name:       "non-v4 run id",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "non-v4-run-id"},
			wantOutput: "run_id must be a canonical RFC 4122 UUIDv4",
		},
		{
			name:       "noncanonical run id",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "noncanonical-run-id"},
			wantOutput: "run_id must be a canonical RFC 4122 UUIDv4",
		},
		{
			name:       "verification did not pass",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "verification-failed"},
			wantOutput: "verification.passed must be true",
		},
		{
			name:       "source was not recomputed",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "source-not-recomputed"},
			wantOutput: "verification.source_recomputed must be true",
		},
		{
			name:       "digest mismatch",
			env:        map[string]string{"FAKE_G0_ARTIFACT_MODE": "bad-digest"},
			wantOutput: "SHA-256 sidecar does not exactly match",
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
				t.Fatalf("target announced publication after failed validation:\n%s", run.output)
			}
			assertG0FileContents(t, run.artifactPath, fakePreviousG0Artifact)
			assertG0FileContents(t, run.digestPath, fakePreviousG0Digest)
		})
	}
}

func TestG0EvidenceTargetRejectsInvalidDerivedObservationsWithoutOverwriting(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		wantOutput string
	}{
		{
			name:       "duplicate required row",
			mode:       "duplicate-derived-row",
			wantOutput: "duplicate a0_scenario row CommitThenFlushBeforeDelivery",
		},
		{
			name:       "missing required row",
			mode:       "missing-derived-row",
			wantOutput: "derived_observations must contain exactly 20 rows",
		},
		{
			name:       "extra row",
			mode:       "extra-derived-row",
			wantOutput: "derived_observations must contain exactly 20 rows",
		},
		{
			name:       "wrong kind",
			mode:       "wrong-derived-kind",
			wantOutput: "kind is not a required evidence kind",
		},
		{
			name:       "wrong A0 scenario",
			mode:       "wrong-a0-scenario",
			wantOutput: "a0_scenario set mismatch",
		},
		{
			name:       "wrong A3 combination",
			mode:       "wrong-a3-combination",
			wantOutput: "a3_matrix_row set mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := runFakeG0EvidenceTarget(t, map[string]string{"FAKE_G0_ARTIFACT_MODE": tt.mode})
			if run.err == nil {
				t.Fatalf("G0 evidence target accepted invalid derived observations (%s):\n%s", tt.mode, run.output)
			}
			if !strings.Contains(run.output, tt.wantOutput) {
				t.Fatalf("target output lacks %q for %s:\n%s", tt.wantOutput, tt.mode, run.output)
			}
			if strings.Contains(run.output, "G0 evidence artifact published") {
				t.Fatalf("target announced publication for invalid derived observations (%s):\n%s", tt.mode, run.output)
			}
			assertG0FileContents(t, run.artifactPath, fakePreviousG0Artifact)
			assertG0FileContents(t, run.digestPath, fakePreviousG0Digest)
		})
	}
}

func TestG0EvidenceTargetRejectsUnsafeRawDirectory(t *testing.T) {
	repoRoot := findEvidenceRepositoryRoot(t)
	tests := []struct {
		name       string
		rawDir     string
		wantOutput string
	}{
		{name: "empty", rawDir: "", wantOutput: "G0_RAW_DIR must not be empty"},
		{name: "filesystem root", rawDir: "/", wantOutput: "G0_RAW_DIR must not resolve to /"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			fakeGo := filepath.Join(tmp, "go")
			fakeGit := filepath.Join(tmp, "git")
			writeFakeGo(t, fakeGo)
			writeFakeG0EvidenceGit(t, fakeGit)
			cmd := exec.Command(
				"make",
				"test-g0-evidence-required",
				"GO="+fakeGo,
				"GIT="+fakeGit,
				"G0_TEST_BIN="+filepath.Join(tmp, "xflow-g0.test"),
				"G0_RAW_DIR="+tt.rawDir,
				"G0_JSON="+filepath.Join(tmp, "g0-events.json"),
			)
			cmd.Dir = repoRoot
			cmd.Env = append(os.Environ(),
				"PATH="+tmp+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FAKE_GO_LOG="+filepath.Join(tmp, "go-invocations.log"),
				"FAKE_GIT_LOG="+filepath.Join(tmp, "git-invocations.log"),
				"FAKE_GIT_STATE_DIR="+tmp,
			)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("G0 target accepted unsafe raw directory %q:\n%s", tt.rawDir, output)
			}
			if !strings.Contains(string(output), tt.wantOutput) {
				t.Fatalf("target output lacks %q:\n%s", tt.wantOutput, output)
			}
		})
	}
}

type fakeG0TargetRun struct {
	output       string
	err          error
	artifactPath string
	digestPath   string
	testBinPath  string
	eventsPath   string
	goLogPath    string
}

func runFakeG0EvidenceTarget(t *testing.T, extraEnv map[string]string) fakeG0TargetRun {
	t.Helper()
	repoRoot := findEvidenceRepositoryRoot(t)
	tmp := t.TempDir()
	outputRoot := filepath.Join(tmp, "published evidence with spaces")
	fakeGo := filepath.Join(tmp, "go")
	fakeGit := filepath.Join(tmp, "git")
	rawDir := filepath.Join(outputRoot, "raw evidence")
	testBinPath := filepath.Join(outputRoot, "xflow-g0.test")
	eventsPath := filepath.Join(outputRoot, "g0-events.json")
	artifactPath := filepath.Join(rawDir, "evidence-"+fakeG0EvidenceRunID+".json")
	digestPath := filepath.Join(rawDir, "evidence-"+fakeG0EvidenceRunID+".sha256")
	goLogPath := filepath.Join(tmp, "go-invocations.log")
	writeFakeGo(t, fakeGo)
	writeFakeG0EvidenceGit(t, fakeGit)
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		t.Fatalf("create seeded G0 output directory: %v", err)
	}
	if err := os.WriteFile(artifactPath, []byte(fakePreviousG0Artifact), 0o644); err != nil {
		t.Fatalf("seed prior G0 artifact: %v", err)
	}
	if err := os.WriteFile(digestPath, []byte(fakePreviousG0Digest), 0o644); err != nil {
		t.Fatalf("seed prior G0 digest: %v", err)
	}

	cmd := exec.Command(
		"make",
		"test-g0-evidence-required",
		"GO="+fakeGo,
		"GIT="+fakeGit,
		"G0_TEST_BIN="+testBinPath,
		"G0_RAW_DIR="+rawDir,
		"G0_JSON="+eventsPath,
	)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"PATH="+tmp+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_GO_LOG="+goLogPath,
		"FAKE_GIT_LOG="+filepath.Join(tmp, "git-invocations.log"),
		"FAKE_GIT_STATE_DIR="+tmp,
	)
	for key, value := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	return fakeG0TargetRun{
		output:       string(output),
		err:          err,
		artifactPath: artifactPath,
		digestPath:   digestPath,
		testBinPath:  testBinPath,
		eventsPath:   eventsPath,
		goLogPath:    goLogPath,
	}
}

func writeFakeGo(t *testing.T, path string) {
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
  test\ -c\ *)
    rc="${FAKE_GO_BUILD_EXIT:-0}"
    if [ "$rc" -ne 0 ]; then
      exit "$rc"
    fi
    output=''
    previous=''
    for arg in "$@"; do
      if [ "$previous" = '-o' ]; then
        output="$arg"
      fi
      previous="$arg"
    done
    if [ -z "$output" ]; then
      printf '%s\n' 'fake build did not receive -o' >&2
      exit 95
    fi
    printf '%s\n' 'fake integration test binary' > "$output"
    chmod 0755 "$output"
    ;;
  tool\ test2json\ *)
    printf '%s\n' '{"Action":"pass","Package":"github.com/xbcio/xflow/test/integration"}'
    exit "${FAKE_GO_TEST_EXIT:-0}"
    ;;
  run\ ./test/integration/cmd/evidence-verify\ *)
    rc="${FAKE_GO_VERIFY_EXIT:-0}"
    if [ "$rc" -ne 0 ]; then
      exit "$rc"
    fi
    out_dir=''
    previous=''
    for arg in "$@"; do
      if [ "$previous" = '-out' ]; then
        out_dir="$arg"
      fi
      previous="$arg"
    done
    if [ -z "$out_dir" ]; then
      printf '%s\n' 'fake verifier did not receive -out' >&2
      exit 96
    fi
    mkdir -p "$out_dir"
    candidate_sha="${FAKE_GIT_PRE_SHA:-1111111111111111111111111111111111111111}"
    python3 - "$out_dir/evidence-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json" "$out_dir/evidence-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.sha256" "$candidate_sha" "${FAKE_G0_ARTIFACT_MODE:-valid}" <<'PY'
import hashlib
import json
import pathlib
import sys

artifact_path, digest_path, candidate_sha, mode = sys.argv[1:]
a0_scenarios = [
    "CommitThenFlushBeforeDelivery",
    "ReportAckLoss",
    "ReportRequestLoss",
    "QueueHandoff",
    "OSKillSIGKILL",
]
a3_fixtures = [
    "transient_then_success",
    "transient_retry_exhausted",
    "permanent_no_retry",
    "business_error_no_retry",
    "error_port_retry_exhausted",
]
a3_topologies = ["local", "server-runner", "cluster-durable"]
derived_observations = [
    {"kind": "a0_scenario", "scenario": scenario}
    for scenario in a0_scenarios
] + [
    {"kind": "a3_matrix_row", "fixture": fixture, "topology": topology}
    for fixture in a3_fixtures
    for topology in a3_topologies
]
document = {
    "schema_version": 2,
    "run_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "source": {
        "commit_sha": candidate_sha,
        "relevant_tree_clean": True,
    },
    "environment": {
        "redis_version": "7.2.0",
        "mysql_version": "8.4.0",
    },
    "suite": {
        "exit_code": 0,
        "skip_count": 0,
        "dropped_runtime_events": 0,
        "required_rows": 20,
        "observed_rows": 20,
    },
    "derived_observations": derived_observations,
    "verification": {
        "passed": True,
        "errors": [],
        "source_recomputed": True,
        "suite_recomputed": True,
    },
}
if mode == "wrong-schema-version":
    document["schema_version"] = 1
elif mode == "invalid-run-id":
    document["run_id"] = "fake-run"
elif mode == "non-v4-run-id":
    document["run_id"] = "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa"
elif mode == "noncanonical-run-id":
    document["run_id"] = document["run_id"].upper()
elif mode == "verification-failed":
    document["verification"]["passed"] = False
    document["verification"]["errors"] = ["forced failure"]
elif mode == "source-not-recomputed":
    document["verification"]["source_recomputed"] = False
elif mode == "duplicate-derived-row":
    document["derived_observations"][1] = dict(document["derived_observations"][0])
elif mode == "missing-derived-row":
    document["derived_observations"].pop()
elif mode == "extra-derived-row":
    document["derived_observations"].append(
        {"kind": "a0_scenario", "scenario": "UnexpectedScenario"}
    )
elif mode == "wrong-derived-kind":
    document["derived_observations"][0]["kind"] = "unexpected_kind"
elif mode == "wrong-a0-scenario":
    document["derived_observations"][0]["scenario"] = "UnexpectedScenario"
elif mode == "wrong-a3-combination":
    document["derived_observations"][5]["fixture"] = "unexpected_fixture"
raw = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
pathlib.Path(artifact_path).write_bytes(raw)
digest = hashlib.sha256(raw).hexdigest() + "\n"
if mode == "bad-digest":
    digest = "0" * 64 + "\n"
pathlib.Path(digest_path).write_text(digest, encoding="ascii")
PY
    ;;
  *)
    printf '%s\n' "unexpected fake Go invocation: $*" >&2
    exit 97
    ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake Go command: %v", err)
	}
}

func assertG0FileContents(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if got := string(raw); got != want {
		t.Fatalf("contents of %s = %q, want preserved %q", path, got, want)
	}
}

func writeFakeG0EvidenceGit(t *testing.T, path string) {
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
		t.Fatalf("write fake G0 Git command: %v", err)
	}
}

func findEvidenceRepositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repository root not found from %s", dir)
		}
		dir = parent
	}
}
