# Release signing and provenance — prerequisites, and why nothing is signed yet

**Status: NOT IMPLEMENTED. Deliberately so.**
**Owner: UNASSIGNED — this document exists to make that assignment possible.**
**Blocked on: key material, a signing identity, and a trust root. None of these
exist in this repository, and none of them can be created here.**

## 0. What this document is, and what it is NOT

This is a **prerequisites document**, not a signature, not a placeholder
attestation, and not a stub that a reader could mistake for evidence.

There is **no** cosign signature, **no** sigstore attestation, **no** key, **no**
certificate, **no** Rekor entry, and **no** in-toto statement anywhere in this
repository or in its release outputs. Nothing in `release/`, `scripts/`, or the
workflows produces or verifies one, and no file in this repository should be read
as asserting otherwise.

Generating a key pair here and self-signing an artifact would be actively
harmful: it would produce something that *looks* like provenance in a release
directory while proving nothing, because the verification path would terminate at
a key this repository itself generated. That is the failure mode this document is
written to prevent.

**Verified absences** (reproduce at the bottom, §6):

- no signing tooling, no workflow step, and no verification command: the only
  `attestation` matches in the tree are the **human reviewer sign-off** field in
  the G0 evidence schema — see §0.1, which matters because it is exactly the
  thing a reader could mistake for provenance;
- the three workflow files contain no `id-token: write` permission, which any
  keyless sigstore signing requires;
- no `.sig`, `.sigstore`, `.pem`, `.key`, `.crt`, or `.intoto.jsonl` artifact is
  produced or referenced anywhere.

### 0.1 The existing "attestation" is a human's name, not a signature

This distinction is the single most important thing in this document, because
the repository already contains a field literally named `attestation` and it is
**not** cryptographic provenance.

`Makefile:670-678` validates a `release.attestation` block in the G0 evidence
artifact:

```python
attestation = exact_keys(release.get("attestation"), {"reviewer", "re_runner", "signed_off"}, "release.attestation")
...
if attestation["signed_off"] != (reviewer != "" and re_runner != ""):
    fail("release.attestation.signed_off must be true exactly when reviewer and re_runner are both set")
```

So `signed_off` means: two named humans were typed into a Make variable
(`REVIEWER=`, `RE_RUNNER=`), and the schema enforces internal consistency — that
the boolean agrees with whether the names are present. That is a genuine
accountability control and it is worth having. What it is **not**:

- it does not bind the artifact to a key;
- it does not bind it to an identity a third party can verify;
- it cannot be checked by anyone who does not already trust whoever wrote the
  JSON — the "signature" is a string in a file the same process produced;
- it proves nothing about whether those two people exist, reviewed anything, or
  agreed.

Anyone reading `release.attestation.signed_off = true` as "this artifact is
signed" would be wrong, and no field in the schema says so. Making that explicit
is the reason this section exists: the gap is not "nothing was thought about
accountability", it is "what exists is human sign-off, and it is not provenance."

## 1. Why this cannot be done locally

Signing is not a code change. Every input it needs is external to the source
tree:

| Input | Why it cannot come from here |
|---|---|
| A private signing key, or an OIDC identity for keyless signing | producing either locally makes the signature self-asserted and worthless. The value of a signature is that the verifier's trust root is *independent* of the signer. |
| A signing **identity** that a verifier will accept | this is an organizational claim ("this artifact was built by the xflow release process"), not a technical one. It requires a decision about who is accountable. |
| A trust root / verification policy | "what does a valid signature mean for xflow, and what does a verifier do when it is missing?" is a policy question with no code answer. |
| A transparency-log or attestation-store destination | requires network access and an account. |

A local sandbox has the first two only in the degenerate form (self-generated),
which is exactly the form that must not ship.

## 2. Prerequisites, concretely

Each row is a prerequisite with an explicit decision, an owner class, and a
concrete artifact it produces. Nothing here is optional; a partial implementation
produces a signature that cannot be verified.

### 2.1 Key material / signing identity

**Decision required:** keyless (sigstore OIDC / Fulcio) **or** long-lived key
(KMS/HSM-backed).

| | Keyless (Fulcio + OIDC) | Long-lived key (KMS/HSM) |
|---|---|---|
| Needs | GitHub Actions `id-token: write`, an OIDC issuer, network to Fulcio/Rekor | a KMS/HSM key, a way for CI to authenticate to it |
| Cost | no key custody problem; the identity is the CI workflow identity | key custody, rotation, and an access policy become ongoing work |
| Verification | `cosign verify-blob --certificate-identity <workflow> --certificate-oidc-issuer https://token.actions.githubusercontent.com` | `cosign verify-blob --key <public.pem>` |
| Blocked on | `id-token: write` is **not** currently granted by any workflow | key provisioning, which is an organizational act |

**Recommended: keyless**, because it removes the key-custody problem entirely and
binds the signature to a CI identity that is already auditable — and because the
alternative makes "who holds the key" a permanent operational question this
project has no process for.

### 2.2 What gets signed

The artifacts that constitute a release, and nothing else. From
`docs/design/RELEASE-GATES.md` and `release/README.md` the candidate set is:

| Artifact | Producer | Signed? |
|---|---|---|
| `bin/server`, `bin/runner`, `bin/xflow` | `make build` (`Makefile:59-76`) | **yes** — the shipped binaries |
| `release/evidence-summary.json` | `make release-summary` (`Makefile:1613-1617`) | **yes** — the release record |
| G0 artifact + `sha256` sidecar | `make test-g0-evidence-required` | **yes** — verified today by `evidence-verify`, but not *signed* |
| G1 report / events / manifest | `make test-g1-evidence-required` | **yes** |
| `release/xflow-sbom.cdx.json` | `make sbom` | **yes** — see §2.4 |
| Container images | **not built by this repository** | n/a — see §3 |

An SBOM that is not covered by the signature is a suggestion; it must be inside
the signed set or its digest must be bound into a signed statement.

### 2.3 Where signing runs, and in what order

Signing must run **after** every gate that establishes the artifact's identity,
and it must not be able to sign a dirty tree. The ordering constraint is already
encoded in the repository:

- `test-g0-evidence-required` refuses to run unless
  `git status --porcelain=v1 --untracked-files=all` is empty before and after
  (`Makefile:841`, `:921`), and the validator asserts
  `full_worktree_clean is True` (`Makefile:1092-1093`);
- `test-g1-evidence-required` does the same (`Makefile:1502`, `:1546`);
- `make sbom` and `make release-summary` are therefore **post-gate** steps —
  `release/` is not gitignored, so generating them dirties `git status` (see
  [sbom.md](sbom.md) §6).

So the release sequence becomes:

```
make vet && make test-coverage && make validate-openapi
make test-integration-required
make test-g0-evidence-required REVIEWER=... RE_RUNNER=...
make test-g1-evidence-required
make sbom && make sbom-validate        # post-gate: dirties git status
make release-summary
# ↓ the new, unimplemented step
# sign: binaries + evidence-summary.json + G0/G1 artifacts + SBOM
# verify: re-verify every signature from the PUBLIC trust root only
```

**The verify step is not optional and must run in a job that has no signing
credential.** A pipeline that only signs has demonstrated nothing; the property
worth having is that a *different* job, holding only public material, can reject
a bad artifact.

### 2.4 What the SBOM's relationship to the signature is

`make sbom` produces a CycloneDX document whose hashes are the SHA-256 of module
zips, not of release artifacts. Two things must therefore be true before the SBOM
can be called signed evidence:

1. the SBOM **file** is itself in the signed set (or its digest is bound into a
   signed in-toto statement), and
2. the release **binaries'** SHA-256 values — which `scripts/sbom.sh` already
   embeds as `xflow:binary-sha256:bin/...` properties when `bin/` is populated —
   must match the digests in the signed artifacts.

Point 2 is a real join and it is currently unverified. It is the cheapest useful
thing a signing implementation can add: cross-check the SBOM's binary digests
against the signed artifact digests, and fail when they disagree.

### 2.5 The verification command (to be written into the runbook)

Not runnable yet — it needs §2.1 to exist. This is the shape a verifier must
have, written out so the requirement is concrete rather than aspirational:

```sh
# keyless
cosign verify-blob \
  --certificate-identity-regexp '^https://github\.com/xbcio/xflow/\.github/workflows/release\.yml@refs/tags/v.*$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --signature bin/server.sig --certificate bin/server.pem bin/server
```

A verification policy also needs three answers that do not exist yet, and each
one is a decision somebody must make:

- **What is the accepted signer identity?** (the regexp above is a guess)
- **What happens on verification failure — block the release, or warn?** (today:
  neither, because nothing verifies)
- **Is a transparency-log inclusion proof required, or is a signature enough?**

## 3. The container-image half of the item, which is a separate gap

Plan §9 F.1 item 4 asks for "SBOM、镜像签名和 provenance" — **image** signatures.
This repository **builds no container images**: `find . -name 'Dockerfile*'`
returns nothing outside the Go module cache, and there is no image build step in
any workflow. The only images involved are *consumed* third-party CI services
(`redis:7.2`, `mysql:8.0`, `apache/kafka:3.7.2`), which cannot be signed by
anyone here and are currently not even digest-pinned (see
[supply-chain-pins.md](supply-chain-pins.md) §3).

So "image signature" has **no subject** in this repository today. The honest
formulation of the requirement is:

1. close the digest-pinning gap first — signing a mutable tag is meaningless;
2. if xflow later ships its own image, sign its digest, not its tag;
3. for consumed third-party images, the available control is **verification**
   (check the upstream signature/digest), not signing.

Item 1 is a prerequisite for item 3, which is why the pin register must close
before the signing work is worth starting.

## 4. Gap register

| # | Gap | Blocking decision | Owner |
|---|---|---|---|
| S1 | No signing tooling, no key, no identity | keyless vs KMS/HSM | **UNASSIGNED** |
| S2 | No `id-token: write` in any workflow (needed for keyless) | CI permissions policy | **UNASSIGNED** |
| S3 | No verification step, on any artifact, from any trust root | verification policy (§2.5) | **UNASSIGNED** |
| S4 | Signed-artifact set not enumerated in a tracked policy | release-process decision | **UNASSIGNED** |
| S5 | SBOM not in any signed set; binary-digest join to signed artifacts unverified | follows S1/S4 | **UNASSIGNED** |
| S6 | Container images are third-party and not digest-pinned, so "image signature" has no subject | closes with the pin register | release-process — **UNASSIGNED** |
| S7 | No documented answer to "what happens when verification fails" | release-process decision | **UNASSIGNED** |

**Plan §9 F.1 item 4's "签名/provenance" half is UNCLOSED and cannot be closed
from this repository.** The exit predicate it feeds (§11's 供应链 gate,
"pin 清单、SBOM、签名/provenance、漏洞状态 … 发布流程的自动检查通过") therefore
**cannot pass** on the current tree regardless of what else is finished.

## 5. What a human must supply, in order

1. Decide **keyless vs KMS/HSM** (S1). Recommended: keyless.
2. Grant `id-token: write` to the release workflow, scoped to that job only (S2).
3. Enumerate the **signed artifact set** in a tracked policy file (S4).
4. Write and run the **verification** job with no signing credential (S3).
5. Answer the **failure policy** question (S7) — block or warn — and write it
   down where a verifier can read it.
6. Add the SBOM to the signed set and verify the binary-digest join (S5).
7. Assign an owner. **This is the only step that unblocks all the others**, and
   it is the reason this document exists.

## 6. Reproduce the absences

```sh
# No cryptographic signing tooling anywhere. NB the only `attestation` matches
# are the human sign-off field described in §0.1 — read them, do not grep -c them.
git grep -n -i 'cosign\|sigstore\|in-toto\|slsa' -- .              # no match
git grep -n 'attestation' -- Makefile                              # human sign-off only, §0.1

# No key-like files, and no key material produced
git ls-files | grep -Ei '\.(sig|sigstore|pem|key|crt|intoto\.jsonl)$'   # empty

# No OIDC permission granted anywhere (keyless signing needs it)
grep -n 'id-token' .github/workflows/*.yml    # no output

# The repository builds no images, so image signing has no subject
find . -name 'Dockerfile*' -not -path './.tmp/*' -not -path './.git/*'  # empty
```

If any of those commands ever starts returning a match, this document's status
line is stale and must be updated in the same change.
