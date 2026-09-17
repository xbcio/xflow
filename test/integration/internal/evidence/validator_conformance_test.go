package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The G0 artifact validator lives in the Makefile (G0_EVIDENCE_VALIDATE_PY) and
// the schema lives in this package. That split is a real defect risk in both
// directions: a field the schema gained but the validator ignores is
// unenforced, and a check the validator gained but the Go verifier does not
// implement is dead weight the gate pays for.
//
// These tests close the loop by running the SAME program the gate runs —
// extracted through `make -s print-g0-evidence-validator`, so Make's variable
// expansion (REQUIRED_GO_VERSION) applies — against artifacts marshalled from
// this package's real types. GO is not involved: the fixture is whatever the Go
// schema produces.

// g0ValidatorSource returns the gate's validator program.
func g0ValidatorSource(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make is not available; cannot extract the G0 artifact validator")
	}
	cmd := exec.Command("make", "-s", "print-g0-evidence-validator")
	cmd.Dir = repositoryRootForTest(t)
	// Strip the parent make's jobserver/flag state: this is a plain extraction,
	// not a recursive build, and inheriting MAKEFLAGS can make the child fail on
	// an unavailable jobserver.
	env := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		switch {
		case strings.HasPrefix(entry, "MAKEFLAGS="), strings.HasPrefix(entry, "MFLAGS="), strings.HasPrefix(entry, "MAKELEVEL="):
		default:
			env = append(env, entry)
		}
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -s print-g0-evidence-validator: %v\n%s", err, out)
	}
	source := string(out)
	if !strings.Contains(source, "G0 artifact validation failed") {
		t.Fatalf("print-g0-evidence-validator did not print the validator program:\n%s", source)
	}
	return source
}

func repositoryRootForTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "Makefile")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repository root not found from %s", dir)
		}
		dir = parent
	}
}

// requiredGoVersionFromValidator reads the toolchain the validator enforces, so
// the fixture satisfies it even when the test binary runs under a different
// toolchain than `make` pins. Reading it instead of hard-coding "go1.25.0"
// keeps this test from becoming a second, silently divergent pin.
func requiredGoVersionFromValidator(t *testing.T, source string) string {
	t.Helper()
	const marker = `REQUIRED_GO_VERSION = "`
	index := strings.Index(source, marker)
	if index < 0 {
		t.Fatalf("the G0 validator no longer declares REQUIRED_GO_VERSION; this conformance test cannot pin the toolchain")
	}
	rest := source[index+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("REQUIRED_GO_VERSION is not a quoted literal in the G0 validator")
	}
	return rest[:end]
}

// goProducedG0Artifact finalizes a real, passing envelope through the real
// schema and returns the artifact path, its sidecar digest path, and the
// candidate SHA it is bound to.
func goProducedG0Artifact(t *testing.T, goVersion string) (artifactPath, digestPath, candidateSHA string) {
	t.Helper()
	prov := defaultFakeProvenance()
	prov.goVersion = goVersion
	prov.release = fakeReleaseProvenance(goVersion)
	// The candidate SHA the artifact claims must be the one the validator is
	// told to expect; keep them the same value by construction.
	candidateSHA = prov.commitSHA

	env := validEnvelope()
	env.Source.GoVersion = goVersion
	markAllRequired(env)

	res := NewVerifier(prov).Verify(env, passEvents())
	requirePassed(t, res)

	if env.SchemaVersion != SchemaVersion {
		t.Fatalf("verified envelope schema_version = %d, want %d", env.SchemaVersion, SchemaVersion)
	}
	dir := t.TempDir()
	artifactPath, digestPath, err := AtomicFinalize(env, dir)
	if err != nil {
		t.Fatalf("AtomicFinalize: %v", err)
	}
	return artifactPath, digestPath, candidateSHA
}

// runG0Validator executes the gate's validator on an artifact and returns its
// exit code and combined output.
func runG0Validator(t *testing.T, validatorPath, artifactPath, digestPath, candidateSHA string) (int, string) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not available; cannot run the G0 artifact validator")
	}
	cmd := exec.Command("python3", validatorPath, artifactPath, digestPath, candidateSHA)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out)
	}
	t.Fatalf("run G0 validator: %v\n%s", err, out)
	return 0, ""
}

// TestG0ValidatorAcceptsAGoProducedV3Artifact is the positive half of the
// conformance contract: what this package finalizes, the gate accepts.
func TestG0ValidatorAcceptsAGoProducedV3Artifact(t *testing.T) {
	source := g0ValidatorSource(t)
	validatorPath := filepath.Join(t.TempDir(), "g0_validator.py")
	if err := os.WriteFile(validatorPath, []byte(source), 0o644); err != nil {
		t.Fatalf("write validator: %v", err)
	}
	goVersion := requiredGoVersionFromValidator(t, source)
	if goVersion != runtime.Version() {
		t.Logf("validating a fixture pinned to %s while this binary runs %s; the fixture, not the runtime, is pinned",
			goVersion, runtime.Version())
	}

	artifact, digest, sha := goProducedG0Artifact(t, goVersion)
	code, out := runG0Validator(t, validatorPath, artifact, digest, sha)
	if code != 0 {
		t.Fatalf("the gate's validator rejected a v%d artifact this package finalized (exit %d):\n%s",
			SchemaVersion, code, out)
	}
}

// TestG0ValidatorRejectsEveryReleaseMutation is the negative half: every
// schema-v3 release field the Go verifier enforces must also be enforced by the
// gate's validator. A mutation writes a fresh sidecar so the digest check still
// passes and the release check is demonstrably what fails.
func TestG0ValidatorRejectsEveryReleaseMutation(t *testing.T) {
	source := g0ValidatorSource(t)
	validatorPath := filepath.Join(t.TempDir(), "g0_validator.py")
	if err := os.WriteFile(validatorPath, []byte(source), 0o644); err != nil {
		t.Fatalf("write validator: %v", err)
	}
	goVersion := requiredGoVersionFromValidator(t, source)
	artifact, _, candidateSHA := goProducedG0Artifact(t, goVersion)

	raw, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatalf("read finalized artifact: %v", err)
	}

	mutations := []struct {
		name    string
		mutate  func(document map[string]any)
		wantErr string
	}{
		{"release block removed", func(d map[string]any) { delete(d, "release") }, "release must be an object"},
		{"unknown schema version", func(d map[string]any) { d["schema_version"] = 4 }, "schema_version must be integer 2 or 3"},
		{"gate name emptied", func(d map[string]any) { releaseOf(d)["gate"].(map[string]any)["name"] = "" }, "release.gate.name must be a non-empty string"},
		{"gate command emptied", func(d map[string]any) { releaseOf(d)["gate"].(map[string]any)["command"] = "  " }, "release.gate.command must be a non-empty string"},
		{"gate start removed", func(d map[string]any) { releaseOf(d)["gate"].(map[string]any)["started_at"] = "" }, "release.gate.started_at"},
		{"gate zero start", func(d map[string]any) {
			releaseOf(d)["gate"].(map[string]any)["started_at"] = "0001-01-01T00:00:00Z"
		}, "is not a real timestamp"},
		{"gate duration zero", func(d map[string]any) { releaseOf(d)["gate"].(map[string]any)["duration_seconds"] = 0 }, "duration_seconds must be a positive number"},
		{"gate exit code non-zero", func(d map[string]any) { releaseOf(d)["gate"].(map[string]any)["exit_code"] = 1 }, "release.gate.exit_code must be integer zero"},
		{"tag kind unknown", func(d map[string]any) { releaseOf(d)["tag_kind"] = "signed" }, "release.tag_kind must be one of"},
		{"tag with kind none", func(d map[string]any) { releaseOf(d)["tag_kind"] = TagKindNone }, "must be empty when release.tag_kind is none"},
		{"tag kind without a tag", func(d map[string]any) { releaseOf(d)["tag"] = "" }, "must be non-empty when release.tag_kind is"},
		{"go version wrong", func(d map[string]any) { releaseOf(d)["go_version"] = "go1.0.0" }, "release.go_version must equal"},
		{"node version not exact", func(d map[string]any) { releaseOf(d)["node_version"] = "22" }, "release.node_version must be an exact x.y.z version"},
		{"pnpm version not exact", func(d map[string]any) { releaseOf(d)["pnpm_version"] = "^10.10.0" }, "release.pnpm_version must be an exact x.y.z version"},
		{"os emptied", func(d map[string]any) { releaseOf(d)["os"] = "" }, "release.os must be a platform identifier"},
		{"arch removed", func(d map[string]any) { delete(releaseOf(d), "arch") }, "release fields mismatch"},
		{"no container images", func(d map[string]any) { releaseOf(d)["container_images"] = []any{} }, "container_images must be a non-empty array"},
		{"resolved image with a short digest", func(d map[string]any) {
			imagesOf(d)[0]["digest"] = "sha256:abc"
		}, "must be sha256:<64 lowercase hex> when resolved is true"},
		{"unresolved image carrying a digest", func(d map[string]any) {
			imagesOf(d)[0]["resolved"] = false
		}, "must be empty when resolved is false"},
		{"duplicate image component", func(d map[string]any) {
			images := imagesOf(d)
			images[1]["component"] = images[0]["component"]
		}, "is declared more than once"},
		{"signed off without names", func(d map[string]any) {
			releaseOf(d)["attestation"] = map[string]any{"reviewer": "", "re_runner": "", "signed_off": true}
		}, "signed_off must be true exactly when"},
		{"names without signed off", func(d map[string]any) {
			releaseOf(d)["attestation"] = map[string]any{"reviewer": "a", "re_runner": "b", "signed_off": false}
		}, "signed_off must be true exactly when"},
		{"empty unverified scope", func(d map[string]any) { releaseOf(d)["unverified_scope"] = []any{} }, "unverified_scope must be a non-empty array"},
		{"duplicate unverified scope", func(d map[string]any) {
			releaseOf(d)["unverified_scope"] = []any{"same", "same"}
		}, "more than once"},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			var document map[string]any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatalf("unmarshal artifact: %v", err)
			}
			mutation.mutate(document)
			dir := t.TempDir()
			artifactPath := filepath.Join(dir, "evidence-mutated.json")
			digestPath := filepath.Join(dir, "evidence-mutated.sha256")
			writeArtifactWithSidecar(t, artifactPath, digestPath, document)

			code, out := runG0Validator(t, validatorPath, artifactPath, digestPath, candidateSHA)
			if code == 0 {
				t.Fatalf("the gate's validator accepted an artifact with %s; the Go verifier rejects it, so the two disagree:\n%s",
					mutation.name, out)
			}
			if !strings.Contains(out, mutation.wantErr) {
				t.Fatalf("validator rejected %s for an unexpected reason; want %q in:\n%s", mutation.name, mutation.wantErr, out)
			}
		})
	}
}

// TestG0ValidatorStillAcceptsALegacyV2ArtifactWithoutRelease pins the
// backwards-compatible branch. Historical artifacts predate the release block;
// they must stay readable, and their checks must be exactly the ones they
// always were — not silently fewer.
func TestG0ValidatorStillAcceptsALegacyV2ArtifactWithoutRelease(t *testing.T) {
	source := g0ValidatorSource(t)
	validatorPath := filepath.Join(t.TempDir(), "g0_validator.py")
	if err := os.WriteFile(validatorPath, []byte(source), 0o644); err != nil {
		t.Fatalf("write validator: %v", err)
	}
	goVersion := requiredGoVersionFromValidator(t, source)
	artifact, _, candidateSHA := goProducedG0Artifact(t, goVersion)

	raw, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatalf("read finalized artifact: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("unmarshal artifact: %v", err)
	}
	document["schema_version"] = 2
	delete(document, "release")

	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "evidence-legacy.json")
	digestPath := filepath.Join(dir, "evidence-legacy.sha256")
	writeArtifactWithSidecar(t, artifactPath, digestPath, document)

	code, out := runG0Validator(t, validatorPath, artifactPath, digestPath, candidateSHA)
	if code != 0 {
		t.Fatalf("the gate's validator rejected a legacy v2 artifact (exit %d):\n%s", code, out)
	}

	// A v2 artifact that carries a release block is still fully checked: the
	// version-dispatch must not become a way to smuggle a partial block past
	// the gate.
	var tampered map[string]any
	if err := json.Unmarshal(raw, &tampered); err != nil {
		t.Fatalf("unmarshal artifact: %v", err)
	}
	tampered["schema_version"] = 2
	imagesOf(tampered)[0]["digest"] = "sha256:abc"
	artifactPath2 := filepath.Join(dir, "evidence-legacy-tampered.json")
	digestPath2 := filepath.Join(dir, "evidence-legacy-tampered.sha256")
	writeArtifactWithSidecar(t, artifactPath2, digestPath2, tampered)
	code, out = runG0Validator(t, validatorPath, artifactPath2, digestPath2, candidateSHA)
	if code == 0 {
		t.Fatalf("the gate's validator accepted a v2 artifact carrying an invalid release block:\n%s", out)
	}
}

func writeArtifactWithSidecar(t *testing.T, artifactPath, digestPath string, document map[string]any) {
	t.Helper()
	raw, err := MarshalCanonical(document)
	if err != nil {
		t.Fatalf("marshal mutated artifact: %v", err)
	}
	if err := os.WriteFile(artifactPath, raw, 0o644); err != nil {
		t.Fatalf("write mutated artifact: %v", err)
	}
	sum := sha256.Sum256(raw)
	if err := os.WriteFile(digestPath, []byte(hex.EncodeToString(sum[:])+"\n"), 0o644); err != nil {
		t.Fatalf("write mutated sidecar: %v", err)
	}
}

func releaseOf(document map[string]any) map[string]any {
	return document["release"].(map[string]any)
}

// imagesOf returns the release block's image entries as mutable maps. The
// entries are the maps the document already holds, so a caller's mutation is
// visible in the document.
func imagesOf(document map[string]any) []map[string]any {
	entries := releaseOf(document)["container_images"].([]any)
	images := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		images = append(images, entry.(map[string]any))
	}
	return images
}
