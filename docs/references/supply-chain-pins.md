# Supply-chain pin inventory and how it is checked

**Owner of the check:** `scripts/pin-audit.sh` (+ `make pin-audit`,
`make pin-audit-strict`, `make pin-audit-selftest`).
**Debt register:** `scripts/pins-allowlist.txt`.
**Status:** the check passes on `main` today and **fails under `--strict` on 10
declared references**. This document is the human-readable companion to that
register; the register is the machine-readable truth, and if the two disagree,
trust the register and fix this file.

## 1. What is checked, and what is deliberately NOT

This is a **source-text audit**. It proves a human wrote an immutable reference
into a tracked file. It deliberately does **not** claim:

| Not claimed | Why not |
|---|---|
| Provenance / signature verification | needs signing keys and a verification identity; see [release-signing-prerequisites.md](release-signing-prerequisites.md) |
| That a commit SHA belongs to the tag written beside it | needs a network fetch of the upstream repository |
| That a `@sha256:` digest names the image someone believes it names | needs a registry query |
| That a pinned version is free of vulnerabilities | that is a different check; see [vulnerability-disposition.md](vulnerability-disposition.md) |
| Signature or attestation of the SBOM itself | see [sbom.md](sbom.md) and the signing prerequisites |

A check that overstates its own coverage is worse than no check, because it
retires the question. The script prints this scope limitation in its header and
this document repeats it.

## 2. Reference classes and the rules applied

| Class | Pinned form | Rule |
|---|---|---|
| GitHub Actions `uses:` | `owner/repo@<40-hex>` | 40-hex commit SHA **plus** a `# vN.N.N` comment. A bare SHA is a pin nobody can review: nothing records what it is a pin *of*, so a substitution pointed at a fork is indistinguishable from an upgrade. |
| Container images | `image:repo@sha256:<64 hex>` | Any `image:` in a workflow service block or a compose services block. A tag is a moving pointer, however specific it looks. |
| Go tool installs | `pkg@vX.Y.Z` | `go install` with no `@version`, or with a range/dist-tag, is unpinned. |
| Node tool invocations | `pkg@X.Y.Z` | Same. `npx foo` with no version resolves `latest` at run time. |
| Downloaded release archives | checksum or signature | A `curl` of a `releases/download/...` archive with no checksum in the surrounding lines is unpinned. |

Two structural rules beyond per-reference pinning:

- **One SHA per action per tree.** The same action pinned to two different SHAs
  in one tree is a partially applied upgrade, and it is exactly the state a
  reviewer cannot see by reading one file. Reported as a violation.
- **Allowlist entries must be live.** An entry that matches nothing is either a
  fixed reference (delete the entry) or a drifted key that has silently stopped
  protecting anything. Under `--strict` it is a failure.

## 3. Known gaps — the declared register

`make pin-audit` reports these and exits 0. `make pin-audit-strict` fails on
every one of them. **This is the current, unclosed state of F's part 1.**

| Key | Issue | Blocked on |
|---|---|---|
| `images:docker.io/library/redis:7.2` | CI service + compose Redis float on a tag | registry query + a named owner for the digest refresh path |
| `images:docker.io/library/mysql:8.0` | CI service + compose MySQL float on a tag | same |
| `images:docker.io/apache/kafka:3.7.2` | CI service + compose Kafka float on a tag | same |
| `images:docker.io/library/redis:7` | **`perf-sample.yml` uses the bare minor tag `redis:7` while `ci.yml` and compose use `7.2`** — the perf gate runs a Redis minor that reading the other files would not predict | owner decision on which minor is intended |
| `tools:secret-scan.yml:curl-download` | gitleaks tarball is fetched and untarred to `/usr/local/bin` with **no checksum or signature**. `GITLEAKS_VERSION` is pinned to `8.30.1`, so the version is fixed — but the artifact is trusted on TLS alone | a checksum recorded from a trusted source (human step) |
| `tools:Makefile:npx:@stoplight/spectral-cli` | `make validate-openapi` runs `npx @stoplight/spectral-cli` with **no `@version`**; npm resolves `latest` at run time, so the rules engine changes without a commit. The sibling redocly call *is* pinned (`@1.34.2`), so the asymmetry looks like an oversight rather than a decision | one-line fix + reviewer; owner unassigned |

Write-gating detail: resolving an image digest needs a registry query, and
pinning is only *meaningful* once somebody also owns the refresh path — a digest
nobody re-resolves is a frozen image with silently unpatched CVEs. That owner
decision is a release-process decision, which is why these are declared with
`owner=release-process (UNASSIGNED)` rather than silently fixed.

## 4. What a sibling workstream already did (not redone here)

All 21 `uses:` references across the three workflow files are already 40-hex
SHA-pinned with `# vN.N.N` comments, and `govulncheck` is pinned to `@v1.8.0` in
`.github/workflows/ci.yml`. That work is **verified** by this check — the check
fails if any of it regresses — but it was not repeated.

Note the interaction with the SHA-without-comment rule: every existing pin
carries its tag comment, so the rule adds no violations on the current tree. It
exists to stop the *next* pin from being unreviewable.

## 5. Teeth: proving the check can fail

`make pin-audit-selftest` builds throwaway fixtures in `$TMPDIR` and asserts that
the auditor reports each defect class and passes on a correctly pinned tree.
**26 of 31 assertions** would fail if the auditor went blind. The cases:

| # | Fixture defect | What it defends |
|---|---|---|
| 1 | `uses: actions/checkout@v4` | mutable action tag |
| 2 | SHA-pinned action with no `# vN` comment | the pin must stay readable |
| 3 | `actions/cache` at two SHAs in one tree | half-applied upgrade |
| 4 | `uses: some/action` with no `@ref` | resolves the default branch |
| 5 | `image: redis:7.2` with no digest | tag-only image |
| 6 | `@sha256:`-pinned image | the check does **not** fire on a pinned tree (no always-fail check) |
| 7 | `npx @stoplight/spectral-cli` | unpinned npx tool |
| 8 | `go install .../govulncheck` (no version) | unpinned go install |
| 9 | `curl .../releases/download/...tar.gz`, no checksum | artifact trusted on TLS alone |
| 10 | declared-but-unpinned image | default mode reports, `--strict` fails |
| 11 | allowlist entry with `expires=2000-01-01` | hard failure in **both** modes |
| 12 | the self-test sentinel key in the allowlist | refuses to run on a fabricated inventory |
| 13 | a missing allowlist file | refuses to guess instead of downgrading every gap to a violation |

Failure modes the tool refuses rather than guesses at:

- **A missing allowlist is exit 2, not "everything is a violation"** — and not
  "everything is declared". Either reading would be a silent lie about the tree.
- **An allowlist entry without `owner=` or `expires=` is exit 2.** The fields are
  what make the register a debt list rather than a permanent excuse.
- **A tree with no scan targets is exit 2, not clean.** A gate that passes
  because it found nothing to look at is the exact failure
  `.github/workflows/secret-scan.yml:45-49` already documents for gitleaks.
- **`expires` in the past is a hard failure in both modes.** Expiry is the
  mechanism that stops the register from becoming permanent; someone has to
  re-sign each entry.

## 6. Wiring

```sh
make pin-audit           # report; exit 0 on the current tree
make pin-audit-strict    # release gate; FAILS today (10 declared references)
make pin-audit-selftest  # prove the check has teeth; exit 0
```

Observed exit codes — note the `make` wrapper: the script itself exits **1**
under `--strict`, and `make` translates a failed recipe into its own exit
**2**. A release job must test for non-zero, not for `1`:

```
$ ./scripts/pin-audit.sh --strict --quiet ; echo $?
1
$ make pin-audit-strict ; echo $?
2
```

`pin-audit` and `pin-audit-selftest` are standalone and are **not** added to
`test`, `vet`, or any aggregate: neither needs the Go toolchain, both run in
under a second, but a release gate that fails must not be able to break the
default local loop for everyone. `pin-audit-strict` is what a release job runs.

## 7. Remaining owner work

| # | Action | Owner |
|---|---|---|
| 1 | Resolve and commit `@sha256:` digests for the 3 CI images + 3 compose images, and define who re-resolves them | release-process — **UNASSIGNED** |
| 2 | Decide whether `perf-sample.yml`'s `redis:7` or `ci.yml`'s `7.2` is intended, and make them agree | release-process — **UNASSIGNED** |
| 3 | Pin `@stoplight/spectral-cli` to a version in `Makefile`'s `validate-openapi` target (the `npx @stoplight/spectral-cli` line; `grep -n spectral-cli Makefile`) | api-contract — **UNASSIGNED**; one-line change, no blocker |
| 4 | Record a gitleaks release checksum in `secret-scan.yml` | security — **UNASSIGNED** |
| 5 | Assign owners to 1–4 and re-date the `expires` fields | release-process |

Until then the honest status of plan §9 F.1 items 1–3 is: **actions pinned and
machine-checked; tool versions mostly pinned and machine-checked; container
images unpinned, declared, and dated.**
