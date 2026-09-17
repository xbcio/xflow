package evidence

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const testDigestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const testDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestParseContainerImages(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		want    []ContainerImage
		wantErr string
	}{
		{
			name: "resolved and unresolved entries sort by component",
			spec: "redis=docker.io/library/redis:7.2@" + testDigestA + ";mysql=docker.io/library/mysql:8.0",
			want: []ContainerImage{
				{Component: "mysql", Reference: "docker.io/library/mysql:8.0"},
				{Component: "redis", Reference: "docker.io/library/redis:7.2", Digest: testDigestA, Resolved: true},
			},
		},
		{
			name: "newlines separate entries too",
			spec: "kafka=docker.io/apache/kafka:3.7.2@" + testDigestB + "\nredis=redis:7.2@" + testDigestA,
			want: []ContainerImage{
				{Component: "kafka", Reference: "docker.io/apache/kafka:3.7.2", Digest: testDigestB, Resolved: true},
				{Component: "redis", Reference: "redis:7.2", Digest: testDigestA, Resolved: true},
			},
		},
		{
			name:    "empty spec is refused",
			spec:    "   \n ; ",
			wantErr: "container image spec is empty",
		},
		{
			name:    "entry without a component is refused",
			spec:    "redis:7.2",
			wantErr: "must be <component>=<reference>",
		},
		{
			name:    "entry with an empty reference is refused",
			spec:    "redis=",
			wantErr: "must be <component>=<reference>",
		},
		{
			name:    "duplicate component is refused",
			spec:    "redis=redis:7.2;redis=redis:7.4",
			wantErr: "more than once",
		},
		{
			name:    "malformed digest is refused",
			spec:    "redis=redis:7.2@sha256:deadbeef",
			wantErr: "must be sha256:<64 lowercase hex>",
		},
		{
			name:    "uppercase digest is refused",
			spec:    "redis=redis:7.2@SHA256:" + strings.Repeat("A", 64),
			wantErr: "must be sha256:<64 lowercase hex>",
		},
		{
			name:    "digest without a reference is refused",
			spec:    "redis=@" + testDigestA,
			wantErr: "has an empty reference",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseContainerImages(tt.spec)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseContainerImages(%q) succeeded, want error containing %q", tt.spec, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseContainerImages(%q) error = %v, want it to contain %q", tt.spec, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseContainerImages(%q) error = %v", tt.spec, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseContainerImages(%q) = %+v, want %+v", tt.spec, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("parseContainerImages(%q)[%d] = %+v, want %+v", tt.spec, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseUnverifiedScopeRequiresAnExplicitDeclaration(t *testing.T) {
	if _, err := parseUnverifiedScope("  \n ;\n"); err == nil {
		t.Fatal("parseUnverifiedScope(\"\") succeeded: an empty unverified scope must be refused so " +
			"\"this evidence covers everything\" can never be the silent default")
	}

	scope, err := parseUnverifiedScope("G2 HA is not exercised; CI artifact checkout is outstanding\nG2 HA is not exercised")
	if err != nil {
		t.Fatalf("parseUnverifiedScope() error = %v", err)
	}
	want := []string{"G2 HA is not exercised", "CI artifact checkout is outstanding"}
	if len(scope) != len(want) {
		t.Fatalf("parseUnverifiedScope() = %q, want %q (duplicates removed)", scope, want)
	}
	for i := range want {
		if scope[i] != want[i] {
			t.Fatalf("parseUnverifiedScope() = %q, want %q", scope, want)
		}
	}
}

func TestParseGateStartedAt(t *testing.T) {
	if _, err := parseGateStartedAt(""); err == nil {
		t.Fatal("parseGateStartedAt(\"\") succeeded, want an error: the gate start time is what makes the duration derivable")
	}
	if _, err := parseGateStartedAt("2026-09-17 10:00:00"); err == nil {
		t.Fatal("parseGateStartedAt() accepted a non-RFC3339 timestamp")
	}
	got, err := parseGateStartedAt("2026-09-17T10:00:00+08:00")
	if err != nil {
		t.Fatalf("parseGateStartedAt() error = %v", err)
	}
	if want := time.Date(2026, 9, 17, 2, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("parseGateStartedAt() = %s, want %s (normalized to UTC)", got, want)
	}
}

func TestToolchainPins(t *testing.T) {
	tests := []struct {
		name         string
		nvmrc        string
		packageJSON  string
		wantNode     string
		wantPnpm     string
		wantErr      string
		omitPackageJ bool
	}{
		{
			name:        "the repository pins are accepted",
			nvmrc:       "22.15.0\n",
			packageJSON: `{"packageManager":"pnpm@10.10.0","engines":{"node":"22.15.0","pnpm":">=10.10.0"}}`,
			wantNode:    "22.15.0",
			wantPnpm:    "10.10.0",
		},
		{
			name:        "nvmrc and engines.node must agree",
			nvmrc:       "22.15.0",
			packageJSON: `{"packageManager":"pnpm@10.10.0","engines":{"node":"22.14.0"}}`,
			wantErr:     "Node pin conflict",
		},
		{
			name:        "a range is not an exact Node pin",
			nvmrc:       "22.15.0",
			packageJSON: `{"packageManager":"pnpm@10.10.0","engines":{"node":"^22.15.0"}}`,
			wantErr:     "engines.node must pin an exact x.y.z",
		},
		{
			name:        "packageManager is required",
			nvmrc:       "22.15.0",
			packageJSON: `{"engines":{"node":"22.15.0"}}`,
			wantErr:     "packageManager must pin pnpm@",
		},
		{
			name:        "npm is not a pnpm pin",
			nvmrc:       "22.15.0",
			packageJSON: `{"packageManager":"npm@10.10.0","engines":{"node":"22.15.0"}}`,
			wantErr:     "packageManager must pin pnpm@",
		},
		{
			name:        "the pnpm pin must satisfy engines.pnpm",
			nvmrc:       "22.15.0",
			packageJSON: `{"packageManager":"pnpm@9.0.0","engines":{"node":"22.15.0","pnpm":">=10.10.0"}}`,
			wantErr:     "violates web/package.json engines.pnpm",
		},
		{
			name:         "a missing package.json is an error, not an empty pin",
			nvmrc:        "22.15.0",
			omitPackageJ: true,
			wantErr:      "cannot read web workspace manifest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			webDir := filepath.Join(root, "web")
			if err := os.MkdirAll(webDir, 0o755); err != nil {
				t.Fatalf("mkdir web: %v", err)
			}
			if err := os.WriteFile(filepath.Join(webDir, ".nvmrc"), []byte(tt.nvmrc), 0o644); err != nil {
				t.Fatalf("write .nvmrc: %v", err)
			}
			if !tt.omitPackageJ {
				if err := os.WriteFile(filepath.Join(webDir, "package.json"), []byte(tt.packageJSON), 0o644); err != nil {
					t.Fatalf("write package.json: %v", err)
				}
			}

			node, pnpm, err := toolchainPins(root)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("toolchainPins() succeeded with node=%q pnpm=%q, want error containing %q", node, pnpm, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("toolchainPins() error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("toolchainPins() error = %v", err)
			}
			if node != tt.wantNode || pnpm != tt.wantPnpm {
				t.Fatalf("toolchainPins() = (%q, %q), want (%q, %q)", node, pnpm, tt.wantNode, tt.wantPnpm)
			}
		})
	}
}

// TestTagAtCommit is the only test of the tag axis that runs real git: an
// annotated tag preferred over a lightweight one, a lightweight-only commit,
// and an untagged commit, which must be distinguishable from "not recorded".
func TestTagAtCommit(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		full := append([]string{"-C", repo, "-c", "user.name=xflow-test", "-c", "user.email=xflow-test@example.invalid"}, args...)
		out, err := exec.Command("git", full...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("one\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	git("add", "file.txt")
	git("commit", "-q", "-m", "initial")
	sha := git("rev-parse", "HEAD")

	// tagAtCommit, like the rest of RealProvenance, reads git from the process
	// working directory; the CLI's cwd is the repository root. Run the probe
	// from inside the fixture repo so the test exercises that exact contract.
	t.Chdir(repo)

	tag, kind, err := tagAtCommit(sha)
	if err != nil {
		t.Fatalf("tagAtCommit(untagged) error = %v", err)
	}
	if tag != "" || kind != TagKindNone {
		t.Fatalf("tagAtCommit(untagged) = (%q, %q), want (\"\", %q)", tag, kind, TagKindNone)
	}

	git("tag", "v0.0.1")
	tag, kind, err = tagAtCommit(sha)
	if err != nil {
		t.Fatalf("tagAtCommit(lightweight) error = %v", err)
	}
	if tag != "v0.0.1" || kind != TagKindLightweight {
		t.Fatalf("tagAtCommit(lightweight) = (%q, %q), want (\"v0.0.1\", %q)", tag, kind, TagKindLightweight)
	}

	git("tag", "-a", "v0.0.2", "-m", "annotated")
	tag, kind, err = tagAtCommit(sha)
	if err != nil {
		t.Fatalf("tagAtCommit(annotated) error = %v", err)
	}
	if tag != "v0.0.2" || kind != TagKindAnnotated {
		t.Fatalf("tagAtCommit(annotated) = (%q, %q), want (\"v0.0.2\", %q) — the annotated tag is the release record", tag, kind, TagKindAnnotated)
	}

	if _, _, err := tagAtCommit(""); err == nil {
		t.Fatal("tagAtCommit(\"\") succeeded, want an error")
	}
}

// TestRealProvenanceReleaseOnThisRepository exercises the production code path
// the G0 target depends on, against the repository it actually runs in: the
// pinned toolchain files, the real tag set, and the real platform. It is the
// only coverage the release block gets short of a full G0 gate run (which needs
// real Redis + MySQL), so a rename of web/.nvmrc or a changed packageManager
// pin fails here rather than at release time.
func TestRealProvenanceReleaseOnThisRepository(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	root := repositoryRootForTest(t)
	t.Chdir(root)

	prov := RealProvenance{ReleaseInput: ReleaseInput{
		GateName:        "g0",
		GateCommand:     "make test-g0-evidence-required",
		GateStartedAt:   "2026-09-17T08:00:00Z",
		ContainerImages: "redis=docker.io/library/redis:7.2;mysql=docker.io/library/mysql:8.0",
		Reviewer:        "reviewer-a",
		ReRunner:        "rerunner-b",
		UnverifiedScope: "G2 HA / multi-namespace is not established by this artifact",
	}}
	sha, err := prov.CommitSHA()
	if err != nil {
		t.Fatalf("CommitSHA: %v", err)
	}

	release, err := prov.Release(sha)
	if err != nil {
		t.Fatalf("Release(%s) on this repository: %v", sha, err)
	}

	if release.TagKind != TagKindAnnotated && release.TagKind != TagKindLightweight && release.TagKind != TagKindNone {
		t.Fatalf("release.TagKind = %q, want one of the declared kinds", release.TagKind)
	}
	if (release.Tag == "") != (release.TagKind == TagKindNone) {
		t.Fatalf("release tag/tag_kind disagree: tag=%q tag_kind=%q", release.Tag, release.TagKind)
	}
	if release.OS != runtime.GOOS || release.Arch != runtime.GOARCH {
		t.Fatalf("release platform = %s/%s, want %s/%s", release.OS, release.Arch, runtime.GOOS, runtime.GOARCH)
	}
	if !semanticVersion.MatchString(release.NodeVersion) {
		t.Fatalf("release.NodeVersion = %q, want the exact pin from web/.nvmrc", release.NodeVersion)
	}
	if !semanticVersion.MatchString(release.PnpmVersion) {
		t.Fatalf("release.PnpmVersion = %q, want the exact pin from web/package.json", release.PnpmVersion)
	}
	if release.GoVersion != runtime.Version() {
		t.Fatalf("release.GoVersion = %q, want the runtime version %q", release.GoVersion, runtime.Version())
	}
	if len(release.ContainerImages) != 2 || release.ContainerImages[0].Component != "mysql" || release.ContainerImages[1].Component != "redis" {
		t.Fatalf("release.ContainerImages = %+v, want the two harness-declared images, sorted by component", release.ContainerImages)
	}
	for _, image := range release.ContainerImages {
		// The harness supplied references without digests, so resolved must be
		// false: a reference is not a digest and must never read as one.
		if image.Resolved || image.Digest != "" {
			t.Fatalf("release.ContainerImages entry %+v claims a resolved digest the harness never supplied", image)
		}
	}
	if release.TagKind != TagKindNone {
		// The evidence is bound to a SHA, so an annotated tag describing a
		// different commit must never be picked up.
		out, err := exec.Command("git", "rev-list", "-n", "1", release.Tag).Output()
		if err != nil {
			t.Fatalf("git rev-list -n 1 %s: %v", release.Tag, err)
		}
		if strings.TrimSpace(string(out)) != sha {
			t.Fatalf("release.Tag %q does not resolve to %s", release.Tag, sha)
		}
	}
	if errs := checkReleaseIntegrity(&Envelope{Release: release}); len(errs) == 0 {
		t.Fatal("checkReleaseIntegrity accepted a block with no gate finish time / suite exit code: " +
			"these are the fields the verifier fills in, so a bare provider block must not validate")
	}
}

// TestRealProvenanceReleaseRefusesMissingHarnessInput pins that the production
// provider fails (rather than zero-filling) when the harness did not supply the
// inputs it alone can know.
func TestRealProvenanceReleaseRefusesMissingHarnessInput(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	t.Chdir(repositoryRootForTest(t))
	sha := "abcdef1234567890abcdef1234567890abcdef12"

	tests := []struct {
		name    string
		input   ReleaseInput
		wantErr string
	}{
		{
			name:    "no container images",
			input:   ReleaseInput{GateStartedAt: "2026-09-17T08:00:00Z", UnverifiedScope: "gap"},
			wantErr: "container image spec is empty",
		},
		{
			name:    "no unverified scope",
			input:   ReleaseInput{GateStartedAt: "2026-09-17T08:00:00Z", ContainerImages: "redis=redis:7.2"},
			wantErr: "unverified scope is empty",
		},
		{
			name:    "no gate start time",
			input:   ReleaseInput{ContainerImages: "redis=redis:7.2", UnverifiedScope: "gap"},
			wantErr: "gate start time is empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RealProvenance{ReleaseInput: tt.input}.Release(sha)
			if err == nil {
				t.Fatalf("Release() succeeded without %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Release() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestCheckReleaseIntegrityRejectsEveryField walks the schema-v3 release block
// one field at a time. Each mutation is applied to a fully valid block so a
// passing case proves the mutation — not a previously broken field — is what
// the check caught.
func TestCheckReleaseIntegrityRejectsEveryField(t *testing.T) {
	base := func() *Envelope {
		env := validEnvelope()
		env.SchemaVersion = SchemaVersion
		env.FinishedAt = time.Date(2020, 1, 2, 3, 5, 5, 0, time.UTC)
		env.Release = fakeReleaseProvenance(env.Source.GoVersion)
		env.Release.Gate.FinishedAt = env.FinishedAt
		env.Release.Gate.DurationSeconds = 60
		return env
	}

	if errs := checkReleaseIntegrity(base()); len(errs) != 0 {
		t.Fatalf("checkReleaseIntegrity(valid) = %v, want no errors", errs)
	}

	tests := []struct {
		name    string
		mutate  func(env *Envelope)
		wantErr string
	}{
		{"gate name", func(env *Envelope) { env.Release.Gate.Name = "" }, "gate.name must be non-empty"},
		{"gate command", func(env *Envelope) { env.Release.Gate.Command = "" }, "gate.command must be non-empty"},
		{"gate start", func(env *Envelope) { env.Release.Gate.StartedAt = time.Time{} }, "gate.started_at must be set"},
		{"gate finish", func(env *Envelope) { env.Release.Gate.FinishedAt = time.Time{} }, "gate.finished_at must be set"},
		{"gate order", func(env *Envelope) {
			env.Release.Gate.FinishedAt = env.Release.Gate.StartedAt.Add(-time.Second)
		}, "gate.finished_at must be after gate.started_at"},
		{"gate exit code", func(env *Envelope) { env.Release.Gate.ExitCode = 1 }, "!= recomputed suite exit code"},
		{"tag with kind none", func(env *Envelope) { env.Release.TagKind = TagKindNone }, "must be empty when tag_kind"},
		{"tag kind with empty tag", func(env *Envelope) { env.Release.Tag = "" }, "tag must be non-empty when tag_kind"},
		{"unknown tag kind", func(env *Envelope) { env.Release.TagKind = "signed" }, "tag_kind"},
		{"go version", func(env *Envelope) { env.Release.GoVersion = "" }, "go_version must be non-empty"},
		{"go version disagrees with source", func(env *Envelope) { env.Release.GoVersion = "go1.24.0" }, "!= source.go_version"},
		{"node version", func(env *Envelope) { env.Release.NodeVersion = "" }, "node_version must be non-empty"},
		{"pnpm version", func(env *Envelope) { env.Release.PnpmVersion = "" }, "pnpm_version must be non-empty"},
		{"os", func(env *Envelope) { env.Release.OS = "" }, "os must be non-empty"},
		{"arch", func(env *Envelope) { env.Release.Arch = "" }, "arch must be non-empty"},
		{"no images", func(env *Envelope) { env.Release.ContainerImages = nil }, "container_images must be non-empty"},
		{"duplicate image component", func(env *Envelope) {
			env.Release.ContainerImages = append(env.Release.ContainerImages, env.Release.ContainerImages[0])
		}, "is declared more than once"},
		{"empty image component", func(env *Envelope) { env.Release.ContainerImages[0].Component = "" }, "component must be non-empty"},
		{"empty image reference", func(env *Envelope) { env.Release.ContainerImages[0].Reference = "" }, "reference must be non-empty"},
		{"resolved with a bad digest", func(env *Envelope) { env.Release.ContainerImages[0].Digest = "sha256:short" }, "not sha256:<64 lowercase hex>"},
		{"unresolved with a digest", func(env *Envelope) {
			env.Release.ContainerImages[0].Resolved = false
		}, "is not resolved but carries digest"},
		{"signed off without names", func(env *Envelope) {
			env.Release.Attestation = Attestation{SignedOff: true}
		}, "attestation.signed_off true disagrees"},
		{"names without signed off", func(env *Envelope) {
			env.Release.Attestation = Attestation{Reviewer: "a", ReRunner: "b"}
		}, "attestation.signed_off false disagrees"},
		{"empty unverified scope", func(env *Envelope) { env.Release.UnverifiedScope = nil }, "unverified_scope must declare at least one"},
		{"blank unverified scope entry", func(env *Envelope) { env.Release.UnverifiedScope = []string{"  "} }, "unverified_scope[0] must be non-empty"},
		{"duplicate unverified scope entry", func(env *Envelope) {
			env.Release.UnverifiedScope = []string{"same", "same"}
		}, "is listed more than once"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := base()
			tt.mutate(env)
			errs := checkReleaseIntegrity(env)
			if len(errs) == 0 {
				t.Fatalf("checkReleaseIntegrity() accepted a broken release block (%s)", tt.name)
			}
			for _, e := range errs {
				if strings.Contains(e, tt.wantErr) {
					return
				}
			}
			t.Fatalf("checkReleaseIntegrity() = %v, want an error containing %q", errs, tt.wantErr)
		})
	}
}

// TestVerifyFailsWhenReleaseProvenanceIsUnavailable pins the failure mode that
// matters: a verifier that cannot establish the release block must fail the
// artifact, not publish one with an empty block that reads as "nothing to
// check". source_recomputed going false is what the G0 validator enforces.
func TestVerifyFailsWhenReleaseProvenanceIsUnavailable(t *testing.T) {
	prov := defaultFakeProvenance()
	prov.releaseErr = errors.New("container image spec is empty; the harness must record the images the gate ran against")

	env := validEnvelope()
	markAllRequired(env)

	v := NewVerifier(prov)
	res := v.Verify(env, passEvents())

	requireNotPassed(t, res, "release provenance could not be recomputed")
	requireErrContains(t, res, "cannot recompute release provenance")
	if res.SourceRecomputed {
		t.Fatal("source_recomputed = true when release provenance could not be recomputed: the G0 validator " +
			"requires source_recomputed==true, so this must fail closed")
	}
}

// TestVerifyDerivesGateIdentityFromTheRun pins that the gate's exit code and
// duration are derived by the verifier: a provider cannot assert them, and a
// mismatch between the recomputed suite and the block is caught.
func TestVerifyDerivesGateIdentityFromTheRun(t *testing.T) {
	prov := defaultFakeProvenance()
	prov.release.Gate.ExitCode = 7 // must be overwritten from the suite
	prov.release.Gate.FinishedAt = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)

	env := validEnvelope()
	markAllRequired(env)

	v := NewVerifier(prov)
	res := v.Verify(env, passEvents())
	requirePassed(t, res)

	if env.SchemaVersion != SchemaVersion {
		t.Fatalf("SchemaVersion = %d, want %d: the verifier is the finalizer and owns the artifact's version",
			env.SchemaVersion, SchemaVersion)
	}
	if env.FinishedAt.IsZero() {
		t.Fatal("FinishedAt is zero after verification: the gate duration would be underivable")
	}
	if env.Release.Gate.ExitCode != env.Suite.ExitCode {
		t.Fatalf("release.gate.exit_code = %d, want the recomputed suite exit code %d",
			env.Release.Gate.ExitCode, env.Suite.ExitCode)
	}
	if !env.Release.Gate.FinishedAt.Equal(env.FinishedAt) {
		t.Fatalf("release.gate.finished_at = %s, want the artifact's finalization time %s",
			env.Release.Gate.FinishedAt, env.FinishedAt)
	}
	if env.Release.Gate.DurationSeconds <= 0 {
		t.Fatalf("release.gate.duration_seconds = %v, want a positive derived duration", env.Release.Gate.DurationSeconds)
	}
	if env.Release.OS != runtime.GOOS || env.Release.Arch != runtime.GOARCH {
		t.Fatalf("release platform = %s/%s, want the runtime platform %s/%s",
			env.Release.OS, env.Release.Arch, runtime.GOOS, runtime.GOARCH)
	}
}

// TestVerifyRejectsGateStartedAfterFinalization keeps the duration honest: a
// harness timestamp in the future would otherwise yield a negative duration
// that the field itself cannot express.
func TestVerifyRejectsGateStartedAfterFinalization(t *testing.T) {
	prov := defaultFakeProvenance()
	prov.release.Gate.StartedAt = time.Now().UTC().Add(24 * time.Hour)

	env := validEnvelope()
	markAllRequired(env)

	res := NewVerifier(prov).Verify(env, passEvents())
	requireNotPassed(t, res, "gate start is later than artifact finalization")
	requireErrContains(t, res, "is not before artifact finalization")
}
