# SBOM: scope, format choice, and what it does not cover

**Owner of the artifacts:** `scripts/sbom.sh` (generate),
`scripts/sbom-validate.sh` (validate).
**Makefile targets:** `make sbom`, `make sbom-validate`.
**Default output:** `release/xflow-sbom.cdx.json` — deliberately **not**
committed (see §6). Note that `release/` is **not** gitignored, so generating
the artifact dirties `git status`; see §6 for why that ordering matters.

## 1. Format choice: CycloneDX 1.5, not SPDX — and why

SPDX is the better choice when the deliverable is a licence-compliance artifact:
its licence-expression model is richer, and `PackageVerificationCode` is a
stronger tamper story. CycloneDX is chosen here for three concrete reasons, not
a preference:

1. **The dependency graph is the thing this repository actually knows.**
   CycloneDX expresses it in a single `dependencies` array; SPDX models each edge
   as a separate `Relationship` element. For a Go module graph the former is the
   natural shape.
2. **`bom-ref` + `purl` give every node a stable join key.** CycloneDX-native
   tooling (Dependency-Track, Grype, Trivy) consumes `pkg:golang/...` purls
   directly, so a downstream scanner needs no translation layer.
3. **A future container SBOM merges rather than competes.** CycloneDX is what
   container scanners emit, so the Go-module SBOM and a future image SBOM become
   one document instead of two that must be reconciled.

If a release process later needs SPDX for licence policy, that is a **second**
artifact, not a replacement. Stating that now prevents the choice from being
re-litigated at the point where it is expensive.

**Spec version 1.5** specifically: 1.5 is the version whose `dependencies` array
carries the acyclicity requirement this generator has to satisfy (§4), and it is
the version current scanners accept.

## 2. What this SBOM IS

A complete inventory of the **Go module build list and resolved module graph**,
produced entirely **offline** from local state:

- `go list -m -json all` — the 145-module build list with versions, `Indirect`
  flags, per-module `GoVersion`, and `Replace` entries;
- `go mod graph` — the requirement graph, **restricted to build-list modules** so
  the document describes the resolved graph rather than MVS-pruned candidates;
- `go.sum` — Go's own `h1` dirhashes, recorded as *properties*;
- the module cache — where present, the SHA-256 of the actual module zip;
- `git rev-parse HEAD` + `git status --porcelain` — the subject commit and
  whether the tree was clean;
- `bin/server`, `bin/runner`, `bin/xflow` when they happen to exist — each as a
  real artifact SHA-256.

Current output on `main`, observed:

```
sbom: 144 component(s), 145 dependency entr(ies), 442 edge(s)
sbom: SHA-256 present on 141/144 components; the remainder carry only the go.sum h1 dirhash
sbom: removed 11 cycle back edge(s) to keep the CycloneDX graph acyclic (listed in annotations)
```

`--deterministic` pins `metadata.timestamp` to `SOURCE_DATE_EPOCH` (or the HEAD
commit time) so two runs on the same tree are byte-identical. **Verified:**
two `--deterministic` runs produced the same SHA-256,
`9bc3750b74d99bdd9a533ff26f00984523395e7022f4fc7a6fcee3ec1771555b`.
It is *off* by default on purpose: a generation timestamp that is not the
generation time is a small lie, and this artifact is meant to be quotable.

## 3. What this SBOM is NOT — read before calling it "the SBOM"

| Not covered | Consequence |
|---|---|
| **Container / OS packages** | No base image, no `dpkg`/`rpm`/`apk`, no layers. `redis:7.2`, `mysql:8.0` and the Kafka image **are invisible to this document**, as are glibc, openssl and every transitive OS component. A Go-module SBOM is **not** a container SBOM. Closing this needs an image scanner against a built image, plus registry and daemon access. |
| **The npm/pnpm tree** | `web/pnpm-lock.yaml` is a second, independent dependency universe and is not here. |
| **Native build toolchains** | protoc and the protoc-gen-go plugins (`Makefile:24-33`) are build-time tools, not module dependencies. |
| **Vendored or embedded C** | The WASM guests (wazero/qjs) are Go modules and appear as such; a C toolchain they link against would not. |
| **Signatures and provenance** | Nothing here is signed. There is no attestation, and the document does not claim one. See [release-signing-prerequisites.md](release-signing-prerequisites.md). |
| **Full SHA-256 coverage** | Only 3/144 components lack a SHA-256 today, and only because their module zips were not in the local cache. `--fetch-hashes` closes that when module-proxy access exists. |

The scope limits are also written **into the generated document** as
`xflow:sbom-scope` and `xflow:not-covered` properties plus two `annotations`
texts, so a consumer reading only the JSON still learns them.

## 4. Honesty properties built into the generator

These are the decisions that make the artifact quotable rather than merely
well-formed:

1. **The `go.sum` `h1` dirhash is never presented as a SHA-256.** It is a
   dirhash over the extracted tree, not a digest of the archive bytes. It is
   tagged `xflow:gosum-h1`. Where a real artifact digest exists it goes in
   `hashes[]`, and `xflow:hash-subject` states what was hashed (*"module zip as
   served by the configured Go module proxy"*). Where no zip was available,
   `xflow:sha256-unavailable` says so explicitly. Substituting one for the other
   is the quiet mislabelling that makes an SBOM worthless.
2. **The dependency graph is a declared approximation.** Go's module graph is
   legitimately cyclic — `golang.org/x/crypto -> golang.org/x/net ->
   golang.org/x/crypto` and 10 more on this tree. CycloneDX since 1.4 requires an
   **acyclic** graph. Rather than emitting a non-conforming document or silently
   dropping edges, the generator removes back edges by DFS and records all 11
   removed edges in an `annotations` entry plus a
   `xflow:cycle-back-edges-removed` count.
3. **Dropped graph edges are counted.** Edges whose endpoints are not in the
   build list are MVS-pruned candidates; the count is carried as
   `xflow:graph-edges-dropped-not-in-build-list` so the restriction is visible
   rather than invisible.
4. **A missing `git` is stated, not fabricated.** With no commit readable, the
   version is `unversioned` and `xflow:git-commit` reads *"unavailable: no git
   commit could be read; this tree's provenance is the empty string"*.
5. **A missing `go.sum` entry is reported as unavailable**, not omitted.

## 5. Validation — and the honest limit of it

`scripts/sbom-validate.sh` runs **structural** checks (2,184 of them on the
current document) encoding the CycloneDX 1.5 requirements that consumers actually
trip over: `bomFormat` literal, `specVersion` shape, RFC 4122 `serialNumber`,
`metadata.timestamp` RFC 3339, component `type` enumeration, `scope`
enumeration, non-empty `name`, well-formed `purl`, unique `bom-ref`, hash `alg`
enumeration with digest-length matching, every `dependsOn` target resolving to a
`bom-ref` in the document, and **graph acyclicity**.

It also runs a **drift check** (`--against-repo`) requiring the document's module
set to equal `go list -m all` right now. A stale SBOM is worse than no SBOM,
because it is read as current.

### 5.0 Proving the structural checks can fail

`scripts/sbom-validate.sh --selftest` (or `make SBOM_SELFTEST=1 sbom-validate`)
mutates a **copy** of the document, one defect at a time, and re-invokes the
validator through its ordinary CLI path on each mutant. Any mutant that comes
back VALID is a hole in the checks. The unmutated copy is the **negative
control** — if it were rejected, every "rejected" result would be meaningless.

Observed: **17 passed, 0 failed**, against 16 defect classes plus the control.

| Mutation | Defends |
|---|---|
| `bomFormat` → `SPDX` | format literal |
| `specVersion` → `9.9` | spec version shape |
| `serialNumber` → `not-a-uuid` | urn:uuid requirement |
| `metadata.timestamp` → `yesterday` | RFC 3339 |
| component `type` → `gadget` | component-type enumeration |
| component `name` → `""` | required non-empty name |
| `purl` → `not a purl` | package-URL shape |
| hash `alg` → `CRC32` | hash-algorithm enumeration |
| SHA-256 with an 8-hex digest | digest length matches the algorithm |
| a `dependencies[].ref` that matches nothing | every ref resolves to a `bom-ref` |
| a `dependsOn` target that matches nothing | every edge target resolves |
| a duplicated `bom-ref` | bom-ref uniqueness |
| `components: []` | non-empty component array |
| an introduced `A→B→A` cycle | **graph acyclicity** — the check the Go module graph makes non-trivial |
| `metadata` removed | required metadata object |
| `version` removed | required BOM revision |

**The distinction the script refuses to blur:** a structural pass is **not** a
full JSON-Schema pass. Full schema validation is attempted only when a validator
is installed, and the run always prints which one was looked for and what
installing it would enable. The observed output on this host (no validator
installed) says so in the same breath as the pass:

```
VALID (structural conformance to the enumerated CycloneDX 1.5 requirements)
       NOT full JSON-Schema validated — see the schema check line above; do not
       read this pass as stronger than it is.
```

### 5.1 Full JSON-Schema validation: performed, and it found a real bug

A validator was **not** installed, so it was installed into a throwaway
directory (`$PWD/.tmp/wsf/`) with the CycloneDX 1.5 schema and its two external
`$refs` (`spdx.schema.json`, `jsf-0.82.schema.json`) fetched from
`raw.githubusercontent.com/CycloneDX/specification/1.5/schema/`. The schema
SHA-256 was recorded.

`ajv-cli` alone **fails on this schema** — it rejects `format: date-time` and
cannot resolve the two `$refs` — which is exactly the "validator said no" that
turns into a false finding. The working configuration registers `ajv-formats`
and pre-loads both referenced schemas.

**Result on the first run: FAIL.** One real schema violation, which the
structural checks had passed:

```
/components/26/evidence/identity must be object {"type":"object"}
```

CycloneDX 1.5 types `evidence.identity` as a **single object** (its `methods` is
the array), and the generator was emitting an array of identities. Fixed in
`scripts/sbom.sh`; after the fix:

```
schema validation: PASS (ajv draft-07 + ajv-formats, schema bom-1.5.schema.json)
```

**This is the strongest evidence in this document that the validation is real
rather than decorative.** A structural checker written alongside a generator
reproduces the generator's own misunderstandings; only an independent schema
catches them.

### 5.2 The two layers catch different things — demonstrated

The layering is not decorative either. Setting `metadata.lifecycles[].phase` to
`"not-a-real-phase"` — an enum the schema constrains and the structural checks
deliberately do not enumerate — produces exactly the split the script warns
about:

```
$ ./scripts/sbom-validate.sh --sbom .tmp/wsf/schemaonly.json
VALID (structural conformance to the enumerated CycloneDX 1.5 requirements)
       NOT full JSON-Schema validated — ...

$ node ajv-validate.js .tmp/wsf .tmp/wsf/schemaonly.json
schema validation: FAIL against bom-1.5.schema.json
exit 1

$ node ajv-validate.js .tmp/wsf .tmp/wsf-sbom.cdx.json
schema validation: PASS (ajv draft-07 + ajv-formats, schema bom-1.5.schema.json)
exit 0
```

So: the structural layer passes a document the schema rejects, the schema layer
rejects it, and the schema layer passes the real artifact. Both layers are
load-bearing and neither substitutes for the other. A structural checker written alongside a generator
reproduces the generator's own misunderstandings; only an independent schema
catches them. `sbom.sh` carries the finding as a comment at the fixed site so it
cannot silently regress.

## 6. Why the artifact is not committed — and a real ordering hazard

`release/` holds the *release record* (`release/README.md`), and its stated
principle is that `evidence-summary.json` is generated and "deliberately **not**
committed as a placeholder — its presence means a signed gate run happened for
the `candidate_sha` it names."

The same logic applies more strongly here: an SBOM's value is that its
`metadata.component.version` and `git-commit` identify a real candidate. A
committed SBOM would be stale within one commit and read as current. It is
generated per release candidate and shipped alongside the artifact.

**Ordering hazard, verified and worth stating because it is easy to get wrong.**
`release/` is **not** in `.gitignore`:

```
$ git check-ignore -v release/xflow-sbom.cdx.json
$ echo $?
1
$ git status --porcelain release/
?? release/
```

So writing an SBOM into `release/` makes `git status --porcelain` non-empty —
and the evidence gates require exactly the opposite, before *and* after:

- `Makefile:841` / `:921` — G0 captures pre- and post-run
  `git status --porcelain=v1 --untracked-files=all` and requires both empty;
- `Makefile:1502` / `:1546` — G1 does the same;
- `Makefile:1092-1093` — the G0 validator asserts
  `full_worktree_clean is True`.

`make release-summary` has the same property and is already documented as a
**post-gate** step. `make sbom` therefore belongs in the same place in the
sequence: **after** `make test-g0-evidence-required` /
`make test-g1-evidence-required`, never before. `SBOM_OUT` overrides the path if
a caller needs it elsewhere.

This is recorded rather than "fixed" because the alternative — a different
default directory — would put the SBOM somewhere less discoverable than the other
release artifacts, and the ordering constraint is real regardless of where the
file lands.

## 7. Wiring

```sh
make sbom                                     # -> release/xflow-sbom.cdx.json
make sbom-validate                            # structural + drift vs `go list -m all`
make SBOM_STRICT=1 sbom-validate              # also fail on a stale document
make SBOM_SCHEMA=<path> sbom-validate         # full JSON-Schema pass when a validator exists
make SBOM_SELFTEST=1 sbom-validate            # prove the validator can fail
scripts/sbom.sh --fetch-hashes                # populate module zips for 100% SHA-256
scripts/sbom.sh --deterministic               # byte-reproducible output
```

Neither target is added to `test`, `vet`, or any aggregate: `make sbom` needs the
Go toolchain and a module cache, and a release artifact generator must not be
able to break the default local loop.

## 8. Remaining owner work

| # | Action | Owner |
|---|---|---|
| 1 | Container/OS-package SBOM for the release images — a Go-module SBOM does not cover them | release-process — **UNASSIGNED** |
| 2 | npm/pnpm SBOM for `web/`, or a decision that it is out of scope | web / release-process — **UNASSIGNED** |
| 3 | Decide whether `--fetch-hashes` runs in the release job (needs module-proxy access) | CI owner |
| 4 | Install a JSON-Schema validator in the release environment so §5.1's full pass runs in CI, not only here | CI owner |
| 5 | Sign the SBOM (or decide not to) | see [release-signing-prerequisites.md](release-signing-prerequisites.md) |

**Plan §9 F.1 item 4 status:** the *achievable offline part* — "SBOM" — is
**delivered** for the Go module graph, with a generator, a validator, and a
schema-verified artifact. The same item's "镜像签名和 provenance" is **not
delivered** and is not locally possible; see the signing document.
