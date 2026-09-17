package evidence

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

// ReleaseInput is the release-harness input that no repository read can
// recover: the container images the gate ran against, the human attestation,
// the declared unverified scope, and the gate's own identity. The Makefile
// supplies it (see docs/references/release-summary-format.md); `make
// test-g0-evidence-required` derives the image list from
// test/env/docker-compose.yml through scripts/evidence-images.sh.
//
// It is a plain string surface on purpose: the harness is a shell, and a
// compact spec that is parsed strictly with a named error beats a struct that
// silently zero-values a missing flag.
type ReleaseInput struct {
	GateName        string
	GateCommand     string
	GateStartedAt   string
	ContainerImages string
	Reviewer        string
	ReRunner        string
	UnverifiedScope string
}

var (
	// sha256Digest matches a registry digest reference.
	sha256Digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	// semanticVersion matches the exact toolchain pins this repo uses.
	semanticVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
)

// Release recomputes the schema-v3 release provenance block for commitSHA.
//
// Every field is either recomputed from an authoritative source (git for tag,
// .nvmrc + package.json for the toolchain pins, runtime for GOOS/GOARCH, the
// Go runtime for the compiler version) or copied verbatim from the harness
// ReleaseInput, which is the only authority for the images the gate ran
// against and for who signed off. Nothing is taken from the envelope.
//
// An error means the block could not be established; the verifier turns that
// into a failed verification rather than an empty (and therefore silently
// unverifiable) block.
func (p RealProvenance) Release(commitSHA string) (ReleaseProvenance, error) {
	// The tag must describe the SHA the artifact is bound to. Resolving it
	// against HEAD instead would let a moved HEAD silently relabel the artifact.
	tag, tagKind, err := tagAtCommit(commitSHA)
	if err != nil {
		return ReleaseProvenance{}, err
	}

	root, err := repositoryRoot()
	if err != nil {
		return ReleaseProvenance{}, err
	}
	nodeVersion, pnpmVersion, err := toolchainPins(root)
	if err != nil {
		return ReleaseProvenance{}, err
	}

	images, err := parseContainerImages(p.ReleaseInput.ContainerImages)
	if err != nil {
		return ReleaseProvenance{}, err
	}
	scope, err := parseUnverifiedScope(p.ReleaseInput.UnverifiedScope)
	if err != nil {
		return ReleaseProvenance{}, err
	}
	startedAt, err := parseGateStartedAt(p.ReleaseInput.GateStartedAt)
	if err != nil {
		return ReleaseProvenance{}, err
	}

	return ReleaseProvenance{
		Gate: GateIdentity{
			Name:      strings.TrimSpace(p.ReleaseInput.GateName),
			Command:   strings.TrimSpace(p.ReleaseInput.GateCommand),
			StartedAt: startedAt,
		},
		Tag:             tag,
		TagKind:         tagKind,
		GoVersion:       p.GoVersion(),
		NodeVersion:     nodeVersion,
		PnpmVersion:     pnpmVersion,
		OS:              runtime.GOOS,
		Arch:            runtime.GOARCH,
		ContainerImages: images,
		Attestation: Attestation{
			Reviewer:  strings.TrimSpace(p.ReleaseInput.Reviewer),
			ReRunner:  strings.TrimSpace(p.ReleaseInput.ReRunner),
			SignedOff: strings.TrimSpace(p.ReleaseInput.Reviewer) != "" && strings.TrimSpace(p.ReleaseInput.ReRunner) != "",
		},
		UnverifiedScope: scope,
	}, nil
}

// repositoryRoot returns the top of the worktree. `git rev-parse
// --show-toplevel` is used rather than the process working directory because
// the evidence-verify CLI is also run from test binaries whose cwd is a
// subdirectory.
func repositoryRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("cannot locate repository root (git rev-parse --show-toplevel): %w", err)
	}
	root := strings.TrimSpace(string(out))
	if root == "" {
		return "", fmt.Errorf("cannot locate repository root: git returned an empty path")
	}
	return root, nil
}

// tagAtCommit reports the tag that points at sha and how it is stored. Annotated
// tags are preferred when several point at the same commit, then the first in
// lexical order, so the result is deterministic.
func tagAtCommit(sha string) (string, string, error) {
	if sha == "" {
		return "", "", fmt.Errorf("cannot resolve tag: empty commit SHA")
	}
	out, err := exec.Command("git", "tag", "--points-at", sha).Output()
	if err != nil {
		return "", "", fmt.Errorf("cannot resolve tag at %s (git tag --points-at): %w", sha, err)
	}
	var tags []string
	for _, line := range strings.Split(string(out), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			tags = append(tags, trimmed)
		}
	}
	if len(tags) == 0 {
		return "", TagKindNone, nil
	}
	sort.Strings(tags)
	for _, tag := range tags {
		// cat-file on the ref distinguishes an annotated tag object ("tag")
		// from a lightweight tag pointing straight at a commit ("commit").
		kind, err := exec.Command("git", "cat-file", "-t", "refs/tags/"+tag).Output()
		if err != nil {
			return "", "", fmt.Errorf("cannot resolve tag %s (git cat-file -t): %w", tag, err)
		}
		if strings.TrimSpace(string(kind)) == "tag" {
			return tag, TagKindAnnotated, nil
		}
	}
	return tags[0], TagKindLightweight, nil
}

// toolchainPins reads the pinned Node and pnpm versions from the tracked web
// workspace. The pins are read from the repository (not from the host) because
// a release record must state what the candidate declares, and the gate runs
// with whatever the host happens to have installed.
//
// Cross-checks that make this a verification rather than a copy:
//   - web/.nvmrc and package.json engines.node must be exact and equal;
//   - packageManager must pin pnpm@<x.y.z>;
//   - when engines.pnpm is a lower bound (">=x.y.z"), the pin must satisfy it.
func toolchainPins(root string) (string, string, error) {
	nvmrcPath := filepath.Join(root, "web", ".nvmrc")
	nvmrcRaw, err := os.ReadFile(nvmrcPath)
	if err != nil {
		return "", "", fmt.Errorf("cannot read pinned Node version (%s): %w", nvmrcPath, err)
	}
	nvmrc := strings.TrimSpace(string(nvmrcRaw))
	if !semanticVersion.MatchString(nvmrc) {
		return "", "", fmt.Errorf("web/.nvmrc must pin an exact x.y.z Node version, got %q", nvmrc)
	}

	packageJSONPath := filepath.Join(root, "web", "package.json")
	packageJSONRaw, err := os.ReadFile(packageJSONPath)
	if err != nil {
		return "", "", fmt.Errorf("cannot read web workspace manifest (%s): %w", packageJSONPath, err)
	}
	var manifest struct {
		PackageManager string `json:"packageManager"`
		Engines        struct {
			Node string `json:"node"`
			Pnpm string `json:"pnpm"`
		} `json:"engines"`
	}
	if err := json.Unmarshal(packageJSONRaw, &manifest); err != nil {
		return "", "", fmt.Errorf("cannot parse %s: %w", packageJSONPath, err)
	}

	engineNode := strings.TrimSpace(manifest.Engines.Node)
	if !semanticVersion.MatchString(engineNode) {
		return "", "", fmt.Errorf("web/package.json engines.node must pin an exact x.y.z Node version, got %q", engineNode)
	}
	if engineNode != nvmrc {
		return "", "", fmt.Errorf("Node pin conflict: web/.nvmrc=%s but web/package.json engines.node=%s", nvmrc, engineNode)
	}

	const pnpmPrefix = "pnpm@"
	packageManager := strings.TrimSpace(manifest.PackageManager)
	if !strings.HasPrefix(packageManager, pnpmPrefix) {
		return "", "", fmt.Errorf("web/package.json packageManager must pin %s<x.y.z>, got %q", pnpmPrefix, packageManager)
	}
	pnpmVersion := strings.TrimSpace(strings.TrimPrefix(packageManager, pnpmPrefix))
	if !semanticVersion.MatchString(pnpmVersion) {
		return "", "", fmt.Errorf("web/package.json packageManager must pin an exact pnpm version, got %q", packageManager)
	}
	if enginePnpm := strings.TrimSpace(manifest.Engines.Pnpm); strings.HasPrefix(enginePnpm, ">=") {
		lower := strings.TrimSpace(strings.TrimPrefix(enginePnpm, ">="))
		if !semanticVersion.MatchString(lower) {
			return "", "", fmt.Errorf("web/package.json engines.pnpm lower bound must be an exact x.y.z version, got %q", enginePnpm)
		}
		if compareSemanticVersions(pnpmVersion, lower) < 0 {
			return "", "", fmt.Errorf("pnpm pin %s violates web/package.json engines.pnpm %s", pnpmVersion, enginePnpm)
		}
	}

	return nvmrc, pnpmVersion, nil
}

// compareSemanticVersions compares two exact x.y.z versions numerically.
func compareSemanticVersions(left, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	for i := 0; i < 3; i++ {
		var l, r int
		_, _ = fmt.Sscanf(leftParts[i], "%d", &l)
		_, _ = fmt.Sscanf(rightParts[i], "%d", &r)
		if l != r {
			if l < r {
				return -1
			}
			return 1
		}
	}
	return 0
}

// parseContainerImages parses the harness image spec:
//
//	redis=redis:7.2@sha256:<64 hex>;mysql=mysql:8.0
//
// The digest suffix is optional; its absence is recorded as resolved=false
// rather than as an empty-looking digest, so a reader can never mistake an
// unresolved image for a pinned one. Entries are returned sorted by component
// so the artifact is byte-stable across harness orderings.
func parseContainerImages(spec string) ([]ContainerImage, error) {
	items := splitSpecList(spec)
	if len(items) == 0 {
		return nil, fmt.Errorf("container image spec is empty; the harness must record the images the gate ran against")
	}
	seen := make(map[string]struct{}, len(items))
	images := make([]ContainerImage, 0, len(items))
	for _, item := range items {
		component, rest, ok := strings.Cut(item, "=")
		component = strings.TrimSpace(component)
		rest = strings.TrimSpace(rest)
		if !ok || component == "" || rest == "" {
			return nil, fmt.Errorf("container image entry %q must be <component>=<reference>[@sha256:<64 hex>]", item)
		}
		if _, duplicate := seen[component]; duplicate {
			return nil, fmt.Errorf("container image spec declares component %q more than once", component)
		}
		seen[component] = struct{}{}

		image := ContainerImage{Component: component, Reference: rest}
		if reference, digest, found := strings.Cut(rest, "@"); found {
			image.Reference = strings.TrimSpace(reference)
			image.Digest = strings.TrimSpace(digest)
			if image.Reference == "" {
				return nil, fmt.Errorf("container image entry %q has an empty reference", item)
			}
			if !sha256Digest.MatchString(image.Digest) {
				return nil, fmt.Errorf("container image %q digest %q must be sha256:<64 lowercase hex>", component, image.Digest)
			}
			image.Resolved = true
		}
		images = append(images, image)
	}
	sort.Slice(images, func(i, j int) bool { return images[i].Component < images[j].Component })
	return images, nil
}

// parseUnverifiedScope parses the harness declaration of what this artifact
// does NOT establish. An empty declaration is an error: "this evidence covers
// everything" must never be the silent default of a release record.
func parseUnverifiedScope(spec string) ([]string, error) {
	entries := splitSpecList(spec)
	if len(entries) == 0 {
		return nil, fmt.Errorf("unverified scope is empty; the harness must declare what this artifact does not establish")
	}
	seen := make(map[string]struct{}, len(entries))
	scope := make([]string, 0, len(entries))
	for _, entry := range entries {
		if _, duplicate := seen[entry]; duplicate {
			continue
		}
		seen[entry] = struct{}{}
		scope = append(scope, entry)
	}
	return scope, nil
}

// splitSpecList splits a ";"- or newline-separated harness list, trimming
// whitespace and dropping empty entries.
func splitSpecList(spec string) []string {
	var items []string
	for _, part := range strings.FieldsFunc(spec, func(r rune) bool { return r == ';' || r == '\n' }) {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

// parseGateStartedAt parses the harness gate start timestamp. RFC3339 in UTC is
// required so the recorded duration is comparable across gates and hosts.
func parseGateStartedAt(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("gate start time is empty; the harness must record when the gate started")
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("gate start time %q must be RFC3339: %w", trimmed, err)
	}
	return parsed.UTC(), nil
}

// populateRelease recomputes the release block from the provenance provider and
// records the artifacts' own end time, so gate duration is derived rather than
// self-reported. A provider error is returned to the caller's error list; the
// block is left empty in that case so checkReleaseIntegrity fails loudly rather
// than certifying a partially populated record.
func (v *Verifier) populateRelease(env *Envelope, commitSHA string) []string {
	// The finalizer owns the artifact's schema version and end time: a merged
	// envelope carries the first fragment's StartAt, and nothing else in the
	// pipeline knows when the artifact was finalized.
	env.SchemaVersion = SchemaVersion
	env.FinishedAt = time.Now().UTC()

	release, err := v.Provenance.Release(commitSHA)
	if err != nil {
		return []string{fmt.Sprintf("release: cannot recompute release provenance: %v", err)}
	}
	release.Gate.ExitCode = env.Suite.ExitCode
	release.Gate.FinishedAt = env.FinishedAt
	if !release.Gate.StartedAt.IsZero() {
		if elapsed := env.FinishedAt.Sub(release.Gate.StartedAt); elapsed > 0 {
			release.Gate.DurationSeconds = elapsed.Seconds()
		} else {
			return []string{fmt.Sprintf("release: gate start %s is not before artifact finalization %s",
				release.Gate.StartedAt.Format(time.RFC3339), env.FinishedAt.Format(time.RFC3339))}
		}
	}
	env.Release = release
	return nil
}

// checkReleaseIntegrity validates the recomputed release block. It mirrors the
// G0 artifact validator in the Makefile field for field; when the two disagree
// the gate and the artifact would disagree about what a valid record is, so a
// conformance test executes both against the same fixtures
// (validator_conformance_test.go).
func checkReleaseIntegrity(env *Envelope) []string {
	var errs []string
	release := env.Release

	if release.Gate.Name == "" {
		errs = append(errs, "release: gate.name must be non-empty")
	}
	if release.Gate.Command == "" {
		errs = append(errs, "release: gate.command must be non-empty")
	}
	if release.Gate.StartedAt.IsZero() {
		errs = append(errs, "release: gate.started_at must be set")
	}
	if release.Gate.FinishedAt.IsZero() {
		errs = append(errs, "release: gate.finished_at must be set")
	}
	if !release.Gate.StartedAt.IsZero() && !release.Gate.FinishedAt.IsZero() && !release.Gate.FinishedAt.After(release.Gate.StartedAt) {
		errs = append(errs, "release: gate.finished_at must be after gate.started_at")
	}
	if release.Gate.ExitCode != env.Suite.ExitCode {
		errs = append(errs, fmt.Sprintf("release: gate.exit_code %d != recomputed suite exit code %d", release.Gate.ExitCode, env.Suite.ExitCode))
	}

	switch release.TagKind {
	case TagKindNone:
		if release.Tag != "" {
			errs = append(errs, fmt.Sprintf("release: tag %q must be empty when tag_kind is %q", release.Tag, TagKindNone))
		}
	case TagKindAnnotated, TagKindLightweight:
		if release.Tag == "" {
			errs = append(errs, fmt.Sprintf("release: tag must be non-empty when tag_kind is %q", release.TagKind))
		}
	default:
		errs = append(errs, fmt.Sprintf("release: tag_kind %q must be one of %q, %q, %q",
			release.TagKind, TagKindNone, TagKindLightweight, TagKindAnnotated))
	}

	for field, value := range map[string]string{
		"go_version":   release.GoVersion,
		"node_version": release.NodeVersion,
		"pnpm_version": release.PnpmVersion,
		"os":           release.OS,
		"arch":         release.Arch,
	} {
		if strings.TrimSpace(value) == "" {
			errs = append(errs, "release: "+field+" must be non-empty")
		}
	}
	if release.GoVersion != "" && release.GoVersion != env.Source.GoVersion {
		errs = append(errs, fmt.Sprintf("release: go_version %q != source.go_version %q", release.GoVersion, env.Source.GoVersion))
	}

	if len(release.ContainerImages) == 0 {
		errs = append(errs, "release: container_images must be non-empty")
	}
	seenComponents := make(map[string]struct{}, len(release.ContainerImages))
	for i, image := range release.ContainerImages {
		if image.Component == "" {
			errs = append(errs, fmt.Sprintf("release: container_images[%d].component must be non-empty", i))
		} else if _, duplicate := seenComponents[image.Component]; duplicate {
			errs = append(errs, fmt.Sprintf("release: container_images component %q is declared more than once", image.Component))
		} else {
			seenComponents[image.Component] = struct{}{}
		}
		if image.Reference == "" {
			errs = append(errs, fmt.Sprintf("release: container_images[%d].reference must be non-empty", i))
		}
		switch {
		case image.Resolved && !sha256Digest.MatchString(image.Digest):
			errs = append(errs, fmt.Sprintf("release: container_images[%d] (%s) is marked resolved but digest %q is not sha256:<64 lowercase hex>",
				i, image.Component, image.Digest))
		case !image.Resolved && image.Digest != "":
			errs = append(errs, fmt.Sprintf("release: container_images[%d] (%s) is not resolved but carries digest %q",
				i, image.Component, image.Digest))
		}
	}

	attestation := release.Attestation
	signedOff := attestation.Reviewer != "" && attestation.ReRunner != ""
	if attestation.SignedOff != signedOff {
		errs = append(errs, fmt.Sprintf("release: attestation.signed_off %v disagrees with reviewer=%q re_runner=%q",
			attestation.SignedOff, attestation.Reviewer, attestation.ReRunner))
	}

	if len(release.UnverifiedScope) == 0 {
		errs = append(errs, "release: unverified_scope must declare at least one uncovered claim")
	}
	seenScope := make(map[string]struct{}, len(release.UnverifiedScope))
	for i, entry := range release.UnverifiedScope {
		if strings.TrimSpace(entry) == "" {
			errs = append(errs, fmt.Sprintf("release: unverified_scope[%d] must be non-empty", i))
			continue
		}
		if _, duplicate := seenScope[entry]; duplicate {
			errs = append(errs, fmt.Sprintf("release: unverified_scope entry %q is listed more than once", entry))
		}
		seenScope[entry] = struct{}{}
	}
	return errs
}
