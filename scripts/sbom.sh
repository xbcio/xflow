#!/usr/bin/env bash
#
# sbom.sh emits a CycloneDX 1.5 SBOM for the Go module dependency graph of this
# repository — the part of the supply chain that is fully knowable OFFLINE.
#
# WHY CycloneDX and not SPDX
#
# SPDX is the better choice when the deliverable is a licence-compliance
# artifact: its licence-expression model is richer and its `PackageVerificationCode`
# is a stronger tamper story. CycloneDX is the better choice here for three
# concrete reasons, not a preference:
#   1. its `dependencies` array can express the module graph directly, where
#      SPDX models relationships as separate `Relationship` elements — the graph
#      is the thing this repository actually knows and wants to publish;
#   2. `bom-ref` + `purl` give every node a stable identifier that a scanner can
#      join on, and CycloneDX tooling (Dependency-Track, Grype, Trivy) consumes
#      it natively in CI;
#   3. CycloneDX is the format `govulncheck`-adjacent tooling and container
#      scanners converge on, so the module SBOM and a future image SBOM can be
#      merged into one document instead of two.
# If the release process later needs SPDX for licence policy, that is a second
# artifact, not a replacement — say so rather than re-litigating this choice.
#
# WHAT THIS DOES **NOT** COVER — read this before quoting the file as "the SBOM"
#
#   * No container/OS packages. There is no OS-package inventory here: no
#     dpkg/rpm/apk, no base-image layers. A Go-module SBOM does not see
#     `redis:7.2`, `mysql:8.0` or the Kafka image, and it does not see glibc,
#     openssl or any other transitive OS component. Those need a scanner run
#     against the built image, which needs a registry and a daemon.
#   * No npm/pnpm tree. web/pnpm-lock.yaml is a second, independent dependency
#     universe and is NOT in this document.
#   * No vendored or embedded C. The WASM guests (wazero/qjs) are Go module
#     dependencies and appear as such; build-time native toolchains do not.
#   * No signature and no provenance claim. Nothing here is signed, and the
#     doc is careful to say so. See docs/references/release-signing-prerequisites.md.
#   * `hashes` coverage is partial by construction: a SHA-256 is emitted only
#     for module zips actually present in the local module cache. The rest carry
#     Go's own `go.sum` h1 dirhash as a *property*, never mislabelled as a
#     SHA-256 of an artifact, because it is not one.
#
# Output: CycloneDX 1.5 JSON (--format cdx-json, the only format today).
#
# Usage:
#   scripts/sbom.sh [--out FILE] [--root DIR] [--deterministic]
#
#   --out FILE        output path (default: release/xflow-sbom.cdx.json)
#   --root DIR        repository root (default: this script's parent directory)
#   --deterministic   pin metadata.timestamp to SOURCE_DATE_EPOCH (or the HEAD
#                     commit time) so two runs on the same tree produce
#                     byte-identical output. Off by default: a generation
#                     timestamp that is not the generation time is a lie.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="release/xflow-sbom.cdx.json"
format="cdx-json"
deterministic=0
fetch_hashes=0

usage() {
	cat <<'USAGE'
usage: scripts/sbom.sh [--out FILE] [--root DIR] [--format cdx-json]
                       [--deterministic] [--fetch-hashes]

  --out FILE        output path (default: release/xflow-sbom.cdx.json)
  --root DIR        repository root (default: this script's parent directory)
  --format          cdx-json (CycloneDX 1.5 JSON); the only supported format
  --deterministic   pin metadata.timestamp for byte-reproducible output
  --fetch-hashes    run `go mod download all` first so every module zip lands in
                    the local cache and every component gets a real SHA-256.
                    Requires module-proxy access; without it a component keeps
                    only the go.sum h1 dirhash and the document says so.
USAGE
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--out)
		[ "$#" -ge 2 ] || { echo "ERROR: --out needs a path" >&2; exit 2; }
		out="$2"
		shift 2
		;;
	--root)
		[ "$#" -ge 2 ] || { echo "ERROR: --root needs a path" >&2; exit 2; }
		root="$2"
		shift 2
		;;
	--format)
		[ "$#" -ge 2 ] || { echo "ERROR: --format needs a value" >&2; exit 2; }
		format="$2"
		shift 2
		;;
	--deterministic)
		deterministic=1
		shift
		;;
	--fetch-hashes)
		fetch_hashes=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "ERROR: unknown argument: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

case "$format" in
cdx-json) ;;
*)
	echo "ERROR: --format must be cdx-json, got $format" >&2
	exit 2
	;;
esac

if [ ! -f "$root/go.mod" ]; then
	echo "ERROR: no go.mod under $root; this SBOM is a Go module inventory" >&2
	exit 2
fi
root="$(cd "$root" && pwd)"

case "$out" in
/*) out_abs="$out" ;;
*) out_abs="$root/$out" ;;
esac
mkdir -p "$(dirname "$out_abs")"

SBOM_ROOT="$root" SBOM_OUT="$out_abs" SBOM_DETERMINISTIC="$deterministic" \
	SBOM_FETCH_HASHES="$fetch_hashes" python3 - <<'PY'
import base64
import datetime
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import uuid

root = pathlib.Path(os.environ["SBOM_ROOT"])
out_path = pathlib.Path(os.environ["SBOM_OUT"])
deterministic = os.environ["SBOM_DETERMINISTIC"] == "1"
fetch_hashes = os.environ["SBOM_FETCH_HASHES"] == "1"

# The serialNumber namespace. Fixed and arbitrary: it only has to be constant so
# the same inputs derive the same UUID.
UUID_NAMESPACE = uuid.UUID("6f1d1b3a-7c2e-4f6a-9d31-6f3f0f2c9a11")
GENERATOR_NAME = "xflow-go-module-sbom"
GENERATOR_VERSION = "1"

# CycloneDX 1.5 component types this generator can legitimately produce.
COMPONENT_TYPES = {
    "application", "framework", "library", "container", "platform",
    "operating-system", "device", "device-driver", "firmware", "file",
    "machine-learning-model", "data", "cryptographic-asset",
}

env = dict(os.environ)
go = shutil.which("go", path=env.get("PATH", ""))
if not go:
    print("ERROR: `go` is not on PATH; the module inventory cannot be computed", file=sys.stderr)
    raise SystemExit(1)


def run(args):
    completed = subprocess.run(
        args, cwd=str(root), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if completed.returncode != 0:
        print(
            "ERROR: " + " ".join(args) + " failed (exit " + str(completed.returncode) + "):\n"
            + completed.stderr.decode("utf-8", "replace"),
            file=sys.stderr,
        )
        raise SystemExit(1)
    return completed.stdout.decode("utf-8", "replace")


def run_optional(args):
    """Return stdout, or None when the command is unavailable or fails.

    Used for `git` metadata: a source checkout without git is still a valid
    input, it just cannot claim a commit SHA — and the document must say so
    rather than invent one.
    """
    try:
        completed = subprocess.run(
            args, cwd=str(root), env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.decode("utf-8", "replace").strip()


# ── inputs ───────────────────────────────────────────────────────────────────

def decode_stream(text):
    decoder = json.JSONDecoder()
    i = 0
    while i < len(text):
        while i < len(text) and text[i].isspace():
            i += 1
        if i >= len(text):
            break
        obj, i = decoder.raw_decode(text, i)
        yield obj


# Opt-in: populate the module cache so the SHA-256 of each module zip can be
# computed. This is the one step that may need the module proxy; everything else
# in this script reads only local state.
if fetch_hashes:
    print("sbom: --fetch-hashes: running `go mod download all` (needs module-proxy access)", file=sys.stderr)
    run([go, "mod", "download", "all"])

mods = list(decode_stream(run([go, "list", "-m", "-json", "all"])))
if not mods:
    print("ERROR: `go list -m -json all` returned no modules", file=sys.stderr)
    raise SystemExit(1)

main_mod = next((m for m in mods if m.get("Main")), None)
if main_mod is None:
    print("ERROR: no main module in `go list -m all`; is this a module root?", file=sys.stderr)
    raise SystemExit(1)

# The dependency relation. `go mod graph` prints the *requirement* graph, which
# includes versions that MVS pruned out of the build list. Restricting both ends
# to build-list modules yields the resolved module graph, which is what an SBOM
# consumer can act on. The restriction is recorded in the document.
raw_edges = []
for line in run([go, "mod", "graph"]).splitlines():
    parts = line.split()
    if len(parts) == 2:
        raw_edges.append((parts[0], parts[1]))

module_paths = {m["Path"] for m in mods}
build_version = {m["Path"]: (m.get("Version") or "") for m in mods}


def path_of(spec):
    return spec.rsplit("@", 1)[0] if "@" in spec else spec


edges = set()
dropped_edges = 0
for src, dst in raw_edges:
    s, d = path_of(src), path_of(dst)
    if s in module_paths and d in module_paths and s != d:
        edges.add((s, d))
    else:
        dropped_edges += 1

# go.sum: the h1 dirhashes Go itself records. Read as data, not as a signature —
# go.sum is an integrity record against a trusted checksum database, not a
# cryptographic signature by the module author.
gosum_h1 = {}
gosum_path = root / "go.sum"
gosum_sha256 = None
if gosum_path.is_file():
    gosum_bytes = gosum_path.read_bytes()
    gosum_sha256 = hashlib.sha256(gosum_bytes).hexdigest()
    for line in gosum_bytes.decode("utf-8", "replace").splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        mod, verspec, h = parts
        kind = "gomod" if verspec.endswith("/go.mod") else "zip"
        version = verspec[: -len("/go.mod")] if kind == "gomod" else verspec
        gosum_h1.setdefault((mod, version), {})[kind] = h

gomodcache = run([go, "env", "GOMODCACHE"]).strip()
cache_download = pathlib.Path(gomodcache) / "cache" / "download" if gomodcache else None


def zip_sha256(module_path, version):
    """SHA-256 of the module zip in the local cache, or None.

    Deliberately the digest of the actual archive bytes. Go's `go.sum` h1 value
    is a dirhash over the extracted tree, not this; substituting one for the
    other is the kind of quiet mislabelling that makes an SBOM worthless.
    """
    if cache_download is None or not version:
        return None
    candidate = cache_download / module_path / "@v" / (version + ".zip")
    if not candidate.is_file():
        return None
    digest = hashlib.sha256()
    try:
        with candidate.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def purl(module_path, version):
    # Purl for Go: pkg:golang/<module path>@<version>. The module path is the
    # namespace+name; its slashes stay literal (they are path separators in the
    # Go module identity), which is what scanners expect for the golang type.
    if not version:
        return "pkg:golang/" + module_path
    return "pkg:golang/" + module_path + "@" + version


# ── component assembly ───────────────────────────────────────────────────────

components = []
refs = {}
hash_coverage = {"with_sha256": 0, "without_sha256": 0}

for mod in sorted(mods, key=lambda m: m["Path"]):
    if mod.get("Main"):
        continue
    module_path = mod["Path"]
    version = mod.get("Version") or ""
    replaced = mod.get("Replace")
    properties = []

    if replaced:
        # Report what actually builds (the replacement) and keep the original
        # identity visible, because a reader auditing "did we ship log4j" needs
        # to see the module that was asked for, not only what answered.
        properties.append({
            "name": "xflow:replaces",
            "value": (replaced.get("Path") or "") + "@" + (replaced.get("Version") or "(local)"),
        })
        module_path = replaced.get("Path") or module_path
        version = replaced.get("Version") or version

    ref = purl(module_path, version)
    if ref in refs:
        print(
            "ERROR: duplicate purl " + ref + "; two modules would share one bom-ref",
            file=sys.stderr,
        )
        raise SystemExit(1)
    refs[ref] = mod["Path"]

    sums = gosum_h1.get((mod["Path"], mod.get("Version") or ""), {})
    if sums.get("zip"):
        properties.append({"name": "xflow:gosum-h1", "value": sums["zip"]})
    if sums.get("gomod"):
        properties.append({"name": "xflow:gosum-gomod-h1", "value": sums["gomod"]})
    if not sums:
        # Honest gap: the module is in the build list but has no recorded
        # checksum in go.sum (usually a module whose packages are never built,
        # so nothing ever verified it).
        properties.append({"name": "xflow:gosum-h1", "value": "unavailable: no go.sum entry"})
    if mod.get("Indirect"):
        properties.append({"name": "xflow:indirect", "value": "true"})
    if mod.get("GoVersion"):
        properties.append({"name": "xflow:go-version", "value": mod["GoVersion"]})
    if mod.get("Time"):
        properties.append({"name": "xflow:module-time", "value": mod["Time"]})

    component = {
        "type": "library",
        "bom-ref": ref,
        "name": module_path,
        "scope": "required",
        "purl": ref,
        "properties": properties,
    }
    if version:
        component["version"] = version
    if mod.get("Dir"):
        # Not a hash — where the module's bytes were read from on this machine,
        # which is what makes a local `replace` directory visible. CycloneDX
        # 1.5 types `evidence.identity` as a single object (methods are the
        # array), not as an array of identities; emitting an array here is a
        # schema violation that a structural check alone does not catch, and
        # `make sbom-validate` with a schema installed is what caught it.
        component["evidence"] = {
            "identity": {
                "field": "purl",
                "confidence": 1.0,
                "methods": [
                    {
                        "technique": "manifest-analysis",
                        "confidence": 1.0,
                        "value": str(mod["Dir"]),
                    }
                ],
            }
        }

    digest = zip_sha256(module_path, version)
    if digest:
        component["hashes"] = [{"alg": "SHA-256", "content": digest}]
        properties.append({"name": "xflow:hash-subject", "value": "module zip as served by the configured Go module proxy"})
        hash_coverage["with_sha256"] += 1
    else:
        properties.append({
            "name": "xflow:sha256-unavailable",
            "value": "module zip not present in the local module cache; only the go.sum h1 dirhash is recorded",
        })
        hash_coverage["without_sha256"] += 1

    components.append(component)

# ── the main module ──────────────────────────────────────────────────────────

commit = run_optional(["git", "rev-parse", "HEAD"])
dirty = run_optional(["git", "status", "--porcelain"])
commit_time = run_optional(["git", "log", "-1", "--format=%cI"]) if commit else None
main_version = commit[:12] if commit else "unversioned"

main_ref = purl(main_mod["Path"], main_version)
main_properties = [
    {"name": "xflow:go-version", "value": main_mod.get("GoVersion", "")},
]
if commit:
    main_properties.append({"name": "xflow:git-commit", "value": commit})
    main_properties.append({"name": "xflow:worktree-clean", "value": "false" if dirty else "true"})
else:
    main_properties.append({
        "name": "xflow:git-commit",
        "value": "unavailable: no git commit could be read; this tree's provenance is the empty string",
    })
if commit_time:
    main_properties.append({"name": "xflow:source-commit-time", "value": commit_time})

main_component = {
    "type": "application",
    "bom-ref": main_ref,
    "name": main_mod["Path"],
    "version": main_version,
    "purl": main_ref,
    "properties": main_properties,
}

# Release binaries, when they happen to be built. Their SHA-256 is a real
# artifact digest and is the closest thing in this document to release evidence.
binary_hashes = []
for name in ("bin/server", "bin/runner", "bin/xflow"):
    binary = root / name
    if binary.is_file():
        digest = hashlib.sha256()
        with binary.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                digest.update(chunk)
        binary_hashes.append({"name": name, "sha256": digest.hexdigest(), "size": binary.stat().st_size})
        main_properties.append({"name": "xflow:binary-sha256:" + name, "value": digest.hexdigest()})

# ── dependency graph, with Go's cycles broken ────────────────────────────────
#
# Go's module graph is legitimately cyclic (golang.org/x/* depend on each
# other). CycloneDX's `dependencies` array must NOT be: since 1.4 the spec
# requires an acyclic graph. So the two are not the same object, and pretending
# otherwise would emit a document that a conforming validator rejects. Back
# edges are dropped by DFS and every dropped edge is listed, counted, and
# carried in an annotation — the graph is an approximation and the document
# says which edges the approximation removed.

adjacency = {p: set() for p in module_paths}
for src, dst in edges:
    adjacency[src].add(dst)

WHITE, GRAY, BLACK = 0, 1, 2
color = {p: WHITE for p in module_paths}
kept = {p: [] for p in module_paths}
removed_edges = []

# Iterative DFS: a recursive walk over ~500 nodes is fine, but the module graph
# of a large repo is not bounded by anything this script controls.
for start in sorted(module_paths):
    if color[start] != WHITE:
        continue
    stack = [(start, iter(sorted(adjacency[start])))]
    color[start] = GRAY
    while stack:
        node, children = stack[-1]
        advanced = False
        for child in children:
            if color[child] == WHITE:
                kept[node].append(child)
                color[child] = GRAY
                stack.append((child, iter(sorted(adjacency[child]))))
                advanced = True
                break
            if color[child] == GRAY:
                removed_edges.append((node, child))
            else:
                kept[node].append(child)
        if not advanced:
            color[node] = BLACK
            stack.pop()

# Adjacency is stored on module paths; the emitted refs are purls.
def ref_for(module_path):
    if module_path == main_mod["Path"]:
        return main_ref
    return purl(module_path, build_version.get(module_path, ""))


dependencies = []
for module_path in sorted(module_paths):
    depends_on = sorted({ref_for(p) for p in kept[module_path] if p != module_path})
    entry = {"ref": ref_for(module_path)}
    if depends_on:
        entry["dependsOn"] = depends_on
    dependencies.append(entry)
dependencies.sort(key=lambda d: d["ref"])

# ── metadata and document ────────────────────────────────────────────────────

now = datetime.datetime.now(datetime.timezone.utc)
if deterministic:
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    if epoch:
        stamp = datetime.datetime.fromtimestamp(int(epoch), datetime.timezone.utc)
    elif commit_time:
        stamp = datetime.datetime.fromisoformat(commit_time).astimezone(datetime.timezone.utc)
    else:
        print("ERROR: --deterministic needs SOURCE_DATE_EPOCH or a readable git commit time", file=sys.stderr)
        raise SystemExit(2)
else:
    stamp = now
timestamp = stamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")

go_version = run([go, "version"]).strip()
serial_inputs = json.dumps(
    {
        "main": main_mod["Path"],
        "commit": commit or "",
        "gosum": gosum_sha256 or "",
        "modules": sorted(f"{m['Path']}@{m.get('Version', '')}" for m in mods),
    },
    sort_keys=True,
)
serial = "urn:uuid:" + str(uuid.uuid5(UUID_NAMESPACE, serial_inputs))

coverage_properties = [
    {"name": "xflow:sbom-scope", "value": "Go module build list (go list -m all) + resolved module graph (go mod graph restricted to build-list modules)"},
    {"name": "xflow:module-count", "value": str(len(mods))},
    {"name": "xflow:dependency-edge-count", "value": str(sum(len(v) for v in kept.values()))},
    {"name": "xflow:cycle-back-edges-removed", "value": str(len(removed_edges))},
    {"name": "xflow:graph-edges-dropped-not-in-build-list", "value": str(dropped_edges)},
    {"name": "xflow:hashes-with-sha256", "value": str(hash_coverage["with_sha256"])},
    {"name": "xflow:hashes-without-sha256", "value": str(hash_coverage["without_sha256"])},
    {"name": "xflow:go-toolchain", "value": go_version},
    {"name": "xflow:goflags", "value": env.get("GOFLAGS", "")},
    {"name": "xflow:gotoolchain", "value": env.get("GOTOOLCHAIN", "")},
    {"name": "xflow:generator", "value": "scripts/sbom.sh (" + GENERATOR_NAME + " v" + GENERATOR_VERSION + ")"},
    {"name": "xflow:not-covered", "value": "container/OS packages, npm/pnpm tree, signatures, provenance attestations"},
]
if gosum_sha256:
    coverage_properties.append({"name": "xflow:go-sum-sha256", "value": gosum_sha256})
if binary_hashes:
    coverage_properties.append({
        "name": "xflow:release-binaries",
        "value": "; ".join(f"{b['name']}={b['sha256']}" for b in binary_hashes),
    })

annotations = [
    {
        "subjects": [main_ref],
        "annotator": {"component": {"type": "application", "name": GENERATOR_NAME, "version": GENERATOR_VERSION}},
        "timestamp": timestamp,
        "text": (
            "Scope: this document inventories the Go module build list and the resolved module graph. "
            "It is NOT a container or OS-package SBOM: no base image, no dpkg/rpm/apk, no npm/pnpm tree. "
            "It carries no signature and asserts no provenance."
        ),
    },
    {
        "subjects": [main_ref],
        "annotator": {"component": {"type": "application", "name": GENERATOR_NAME, "version": GENERATOR_VERSION}},
        "timestamp": timestamp,
        "text": (
            "Hash semantics: a `hashes` entry is the SHA-256 of the module zip as served by the configured "
            "Go module proxy, present only when that zip is in the local module cache "
            f"({hash_coverage['with_sha256']} of {len(components)} components). Every other component records Go's "
            "own go.sum h1 dirhash in the `xflow:gosum-h1` property. The two are different objects and are never "
            "presented as the same thing."
        ),
    },
]
if removed_edges:
    annotations.append({
        "subjects": [main_ref],
        "annotator": {"component": {"type": "application", "name": GENERATOR_NAME, "version": GENERATOR_VERSION}},
        "timestamp": timestamp,
        "text": (
            "Go's module graph is cyclic; a CycloneDX dependency graph must be acyclic. "
            f"{len(removed_edges)} back edge(s) were removed so this document conforms: "
            + "; ".join(f"{a} -> {b}" for a, b in sorted(removed_edges))
        ),
    })

document = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "serialNumber": serial,
    "version": 1,
    "metadata": {
        "timestamp": timestamp,
        "lifecycles": [{"phase": "build"}],
        "tools": [{"vendor": "xflow", "name": "scripts/sbom.sh", "version": GENERATOR_VERSION}],
        "component": main_component,
        "properties": coverage_properties,
    },
    "components": components,
    "dependencies": dependencies,
    "annotations": annotations,
}

out_path.write_text(json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8")

print(f"sbom: wrote {out_path}")
print(
    f"sbom: {len(components)} component(s), {len(dependencies)} dependency entr(ies), "
    f"{sum(len(v) for v in kept.values())} edge(s)"
)
print(
    f"sbom: SHA-256 present on {hash_coverage['with_sha256']}/{len(components)} components; "
    f"the remainder carry only the go.sum h1 dirhash (recorded as a property, not as a hash)"
)
if removed_edges:
    print(f"sbom: removed {len(removed_edges)} cycle back edge(s) to keep the CycloneDX graph acyclic (listed in annotations)")
print("sbom: NOT covered by this document: container/OS packages, npm/pnpm tree, signatures, provenance")
PY
