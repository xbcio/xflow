# Release evidence summary — format contract

> **Status:** tracked contract. The file it describes
> (`release/evidence-summary.json`) is a release record; this document is what a
> second person needs to reproduce and review it.
> **Producer:** `make release-summary` → `scripts/release-summary.sh`
> **Summary format version:** `1`

## 1. Why this file exists

Every artifact the release gates produce is **gitignored** — the raw ledger
fragments, the G0 envelope and its `.sha256` sidecar
(`test/integration/testdata/evidence/`), the merged `go test -json` stream, and
the G1 report/events/manifest. That is deliberate: they are per-run machine
output, they are large, and they must never be committed.

The consequence is that a fresh clone cannot tell *current* evidence from
*historical* evidence, and a release claim rests entirely on files the reader
does not have. The summary closes exactly that gap and nothing else: it is a
small, reviewable, non-sensitive **derivative** of a full ignored artifact, kept
under version control so the claim and the artifact stay checkable against each
other.

It is derived, never asserted. The generator recomputes the G0 sidecar digest,
recomputes the G1 canonical binding digest, requires both gates to name the same
candidate SHA, and refuses to exist at all for an **unsigned** gate run.

## 2. Destination

```
release/evidence-summary.json
```

`release/` at the repository root is the release record, deliberately separate
from `docs/design/RELEASE-GATES.md`, which is the *policy* a record is judged
against. No rule in `.gitignore` matches this path — verified with
`git check-ignore -v release/evidence-summary.json` (no output means tracked).
The directory ships with only this README until a signed gate run produces a
record: a placeholder summary would be indistinguishable from a real one.

The file is not present in a checkout that has never run
`make release-summary`. Absence means "no signed record", never "nothing to
review".

## 3. Producing and reviewing a record

```sh
# 1. The gate records who vouches for the evidence. Without both names the
#    artifact is still published, but it is UNSIGNED and no record can be
#    derived from it.
make test-g0-evidence-required REVIEWER="<reviewer>" RE_RUNNER="<re-runner>"
make test-g1-evidence-required

# 2. Derive the tracked record (G1 is optional; its absence is recorded).
make release-summary

# 3. Review it like any other tracked change.
git diff release/evidence-summary.json
```

To review someone else's record, check the three things the generator already
checked, because they are the only reasons to believe the numbers:

1. `evidence_inputs[].sha256` matches the artifact still on disk (the G0
   sidecar and the G1 manifest/report/events digests are recomputed, not copied);
2. `candidate_sha` equals the commit the record's claiming commit was built
   from, and `candidate_is_head` says whether it was HEAD when generated;
3. `attestation.signed_off` is `true` and names two people.

## 4. Fields

Everything is derived from the artifacts listed in `evidence_inputs`. No field
is hand-written.

| Field | Type | Source | Notes |
|---|---|---|---|
| `summary_format_version` | int | constant `1` | bump when the shape changes |
| `kind` | string | constant `xflow.release-evidence-summary` | |
| `generated_at` | string | generator clock | UTC RFC3339, `Z` |
| `generated_by` | string | constant | `scripts/release-summary.sh` |
| `candidate_sha` | string | G0 `source.commit_sha` | 40-hex; the SHA both gates must agree on |
| `candidate_is_head` | bool | `git rev-parse HEAD` at generation time | |
| `tag` / `tag_kind` | string | G0 `release.tag` / `release.tag_kind` | `tag_kind: none` means no tag points at the candidate, which is stated rather than left empty |
| `toolchain.go` | string | G0 `release.go_version` | pinned by `check-go` |
| `toolchain.node` | string | G0 `release.node_version` | from `web/.nvmrc`, cross-checked against `web/package.json` `engines.node` |
| `toolchain.pnpm` | string | G0 `release.pnpm_version` | from `web/package.json` `packageManager` |
| `platform.os` / `platform.arch` | string | G0 `release.os` / `arch` | the `GOOS`/`GOARCH` that produced the evidence |
| `container_images[]` | array | G0 `release.container_images` | see §5 |
| `gates[]` | array | one row per gate covered | see §6 |
| `attestation` | object | G0 `release.attestation` | `reviewer`, `re_runner`, `signed_off` |
| `unverified_scope[]` | array of string | G0 `release.unverified_scope` plus generator-added gaps | what this record does **not** establish |
| `evidence_inputs[]` | array | generator | `role`, `path`, `sha256` for every artifact read |
| `selection` | object | generator | how the G0 artifact was chosen when several were present (`newest-by-mtime`) and how many were present |

### 5. `container_images[]`

```json
{ "component": "redis", "reference": "docker.io/library/redis:7.2",
  "digest": "sha256:…64 hex…", "resolved": true }
```

`resolved` is the load-bearing field. When `true`, `digest` is a registry digest
and is the pinned identity of the image. When `false`, `digest` is the empty
string and the record says so explicitly — an unresolved digest is never
rendered as a pinned one, and a reader may not treat `reference` as a pin.

`make test-g0-evidence-required` resolves digests best-effort from a local
docker/podman daemon via `scripts/evidence-images.sh`, which reads the image
references from `test/env/docker-compose.yml`. When no daemon can answer (CI, a
remote host, a native Redis/MySQL install), the entry is recorded unresolved.
Pin them explicitly by passing the spec yourself:

```sh
make evidence-image-digests          # print a ready-to-paste value
make test-g0-evidence-required EVIDENCE_CONTAINER_IMAGES='redis=redis:7.2@sha256:…'
```

### 6. `gates[]`

| Field | G0 | G1 |
|---|---|---|
| `gate` | `g0` | `g1` |
| `command` | recorded by the harness (`EVIDENCE_GATE_COMMAND`) | `make test-g1-evidence-required` (the G1 manifest format carries no command field) |
| `exit_code` | `release.gate.exit_code` (the independently recomputed suite's code; a mismatch fails the artifact) | `manifest.test.exit_code` |
| `duration_seconds` | `release.gate.duration_seconds` (harness start → verifier finalization) | the `Elapsed` of `TestG1ProductionE2E` in the captured event stream |
| `go_version`, `os` | G0 release block | G1 report |
| artifacts | `artifact`, `artifact_sha256` | `report`, `report_sha256`, `events_sha256`, `binding_sha256` |
| `run_id` | envelope `run_id` | manifest `run_id` |

G0's duration is wall clock from the harness-recorded gate start to the moment
the verifier finalized the artifact, so it **includes** the test binary build and
the test run and excludes everything after publication. G1's duration is the
gate test's own elapsed time. The two are not the same measurement and are not
meant to be compared as if they were.

## 7. What is deliberately absent

The summary is a tracked file in a public repository. It copies:

- **no logs** — no `go test -json` lines, no raw ledger records, no stdout;
- **no connection details** — the G1 report's `redis_addr`, `mysql_dsn_host`,
  `runtime.*` endpoints and database names are read for nothing and copied
  nowhere;
- **no credentials** — the generator refuses to write a value matching a
  credential shape (assignment of `password`/`secret`/`token`/`api_key`,
  `Authorization:`, a `scheme://user:pass@host` URL, a JWT, a PEM private-key
  header). This is a guard rail, not a proof: it catches the shape a careless
  copy produces, and it is not a substitute for reading the diff.

The generator's `unverified_scope` for a G0-only record names the missing G1 gate
in full, so "one gate covered" cannot be mistaken for "both gates passed".

## 8. Envelope schema v3 (what the record's G0 half comes from)

`test/integration/internal/evidence/schema.go` moved from `schema_version` 2 to
3. The `release` block is **required at v3** and is populated by the verifier,
never by the test recorder — `MergeRawEnvelopes` does not carry a fragment's
release block, so nothing self-reported can reach a finalized artifact.

| Field | Required at v3 | Recomputed by the verifier from | Enforced by |
|---|---|---|---|
| `release.gate.name`, `.command`, `.started_at` | yes (non-empty / RFC3339) | harness flags (`-gate-name`, `-gate-command`, `-gate-started-at`) | Go `checkReleaseIntegrity` + `G0_EVIDENCE_VALIDATE_PY` |
| `release.gate.exit_code` | yes, must be `0` and equal `suite.exit_code` | the independently recomputed suite (a mismatch fails verification) | both |
| `release.gate.finished_at`, `.duration_seconds` | yes, positive | verifier finalization time | both |
| `release.tag`, `.tag_kind` | yes; non-empty unless kind is `none` | `git tag --points-at <sha>` + `git cat-file -t` | both |
| `release.go_version` | yes, must equal the pinned toolchain | `runtime.Version()` | both |
| `release.node_version`, `.pnpm_version` | yes, exact `x.y.z` | `web/.nvmrc` + `web/package.json` (conflicts are errors) | both |
| `release.os`, `.arch` | yes | `runtime.GOOS` / `runtime.GOARCH` | both |
| `release.container_images` | yes, non-empty | harness spec; `resolved` gates the digest | both |
| `release.attestation` | yes; `signed_off` iff both names are set | harness flags (`-reviewer`, `-re-runner`) | both |
| `release.unverified_scope` | yes, non-empty, unique | harness flag (`-unverified-scope`) | both |

v2 artifacts — which predate the `release` block — stay readable, but their
contract is unchanged and a v2 artifact carrying a partial release block is
validated in full. The two validators (Go and the Makefile's Python) enforce the
identical contract, and
`test/integration/internal/evidence/validator_conformance_test.go` proves it by
running the gate's own program (extracted with
`make -s print-g0-evidence-validator`) against artifacts this package finalized.

## 9. Known gaps

- The **G1 report schema** (`schema_version` 1) still has no tag, Node/pnpm pins,
  image digests or attestation fields, so those columns in a G1 row come from
  the G0 half. Extending it means changing
  `test/integration/g1_production_e2e_test.go` and the G1 validators together.
- The G1 gate command is a constant in the generator because the G1 manifest
  format has no command field, and the manifest's canonical binding digest is
  pinned by `test/architecture/g1_evidence_make_target_test.go`.
- `resolved: false` on an image is an honest statement, not a failure: the gate
  does not fail when a digest cannot be resolved, because a digest is a registry
  fact and a native (non-container) dependency has none.
