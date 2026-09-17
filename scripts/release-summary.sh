#!/usr/bin/env bash
#
# release-summary.sh derives the ONE tracked release record from the ignored
# per-run evidence artifacts.
#
# Why this exists: every evidence artifact this repository produces is
# gitignored (raw fragments, the G0 envelope, the G1 report/events/manifest —
# see test/integration/testdata/ in .gitignore). A fresh clone therefore cannot
# tell current evidence from historical evidence, and a release claim rests on
# files the reader does not have. This script reduces those artifacts to a
# small, reviewable, non-sensitive summary that IS tracked, so the record and
# the artifact are checkable against each other.
#
# It derives, it does not assert: the G0 artifact's SHA-256 sidecar is
# recomputed, the G1 manifest's canonical binding digest is recomputed, both
# gates must name the same candidate SHA, and an unsigned gate run is refused.
#
# It copies no logs, no raw ledger, no host addresses, and no connection
# strings, and it refuses to write a summary containing a credential-shaped
# value. The output format is a documented contract:
# docs/references/release-summary-format.md
#
# Usage:
#   scripts/release-summary.sh --out release/evidence-summary.json \
#       [--g0-dir test/integration/testdata/evidence] [--g0-artifact PATH] \
#       [--g1-manifest test/integration/testdata/g1_e2e_manifest.json] \
#       [--allow-different-head]

set -euo pipefail

out="release/evidence-summary.json"
g0_dir="test/integration/testdata/evidence"
g0_artifact=""
g1_manifest="test/integration/testdata/g1_e2e_manifest.json"
allow_different_head=0

usage() {
	cat <<'USAGE'
usage: scripts/release-summary.sh [options]

  --out PATH               where to write the summary (default release/evidence-summary.json)
  --g0-dir PATH            directory holding the published G0 artifact + .sha256 sidecar
  --g0-artifact PATH       use this G0 artifact instead of the newest one in --g0-dir
  --g1-manifest PATH       G1 manifest; when absent the summary records the gap
  --allow-different-head   permit a G0 artifact whose commit_sha is not HEAD
USAGE
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--out)
		[ "$#" -ge 2 ] || { echo "ERROR: --out needs a path" >&2; exit 2; }
		out="$2"
		shift 2
		;;
	--g0-dir)
		[ "$#" -ge 2 ] || { echo "ERROR: --g0-dir needs a path" >&2; exit 2; }
		g0_dir="$2"
		shift 2
		;;
	--g0-artifact)
		[ "$#" -ge 2 ] || { echo "ERROR: --g0-artifact needs a path" >&2; exit 2; }
		g0_artifact="$2"
		shift 2
		;;
	--g1-manifest)
		[ "$#" -ge 2 ] || { echo "ERROR: --g1-manifest needs a path" >&2; exit 2; }
		g1_manifest="$2"
		shift 2
		;;
	--allow-different-head)
		allow_different_head=1
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

G0_DIR="$g0_dir" G0_ARTIFACT="$g0_artifact" G1_MANIFEST="$g1_manifest" \
	OUT="$out" ALLOW_DIFFERENT_HEAD="$allow_different_head" python3 - <<'PY'
import datetime
import hashlib
import json
import os
import pathlib
import re
import sys
import tempfile

SUMMARY_FORMAT_VERSION = 1
KIND = "xflow.release-evidence-summary"
G1_BINDING_VERSION = "xflow-g1-evidence-v2"
G1_TEST_NAME = "TestG1ProductionE2E"

# Credential-shaped values. This is a guard rail around a tracked file, not a
# proof: it catches the shapes a careless copy would produce (an assignment, an
# Authorization header, a userinfo URL, a JWT, a PEM block).
CREDENTIAL_PATTERNS = (
    re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key)\s*[:=]"),
    re.compile(r"(?i)\bauthorization\s*:"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@"),
    re.compile(r"^ey[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}$"),
)


def fail(message):
    print("ERROR: release summary: " + message, file=sys.stderr)
    raise SystemExit(1)


def sha256_file(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def load_json(path, field):
    try:
        return json.loads(pathlib.Path(path).read_bytes())
    except Exception as exc:
        fail("cannot parse " + field + " (" + str(path) + "): " + str(exc))


def gate_row_from_g0(artifact_path, envelope):
    sidecar = pathlib.Path(str(artifact_path)[: -len(".json")] + ".sha256")
    if not sidecar.is_file():
        fail("G0 SHA-256 sidecar is missing: " + str(sidecar) +
             " (an artifact without its sidecar cannot be checked against its digest)")
    recorded = sidecar.read_text(encoding="ascii").strip()
    if re.fullmatch(r"[0-9a-f]{64}", recorded) is None:
        fail("G0 SHA-256 sidecar does not contain a bare lowercase hex digest: " + str(sidecar))
    recomputed = sha256_file(artifact_path)
    if recomputed != recorded:
        fail("G0 artifact digest mismatch: sidecar=" + recorded + " recomputed=" + recomputed)

    if envelope.get("verification", {}).get("passed") is not True:
        fail("G0 artifact is not a passing artifact (verification.passed != true)")
    if envelope.get("schema_version", 0) < 3:
        fail("G0 artifact schema_version " + str(envelope.get("schema_version")) +
             " predates the release block; regenerate evidence with the current gate")

    release = envelope.get("release")
    if not isinstance(release, dict):
        fail("G0 artifact has no release block")
    attestation = release.get("attestation") or {}
    if attestation.get("signed_off") is not True:
        fail("G0 artifact is UNSIGNED (release.attestation.signed_off != true). "
             "Re-run the gate with REVIEWER=\"<name>\" RE_RUNNER=\"<name>\" so the "
             "evidence names who vouches for it: " +
             "make test-g0-evidence-required REVIEWER=... RE_RUNNER=...")
    for field in ("tag", "tag_kind", "go_version", "node_version", "pnpm_version", "os", "arch",
                  "container_images", "unverified_scope"):
        if field not in release:
            fail("G0 release block is missing " + field)

    gate = release.get("gate") or {}
    suite = envelope.get("suite") or {}
    return {
        "gate": gate.get("name") or "g0",
        "command": gate.get("command") or "",
        "exit_code": gate.get("exit_code", suite.get("exit_code")),
        "duration_seconds": gate.get("duration_seconds"),
        "started_at": gate.get("started_at"),
        "finished_at": gate.get("finished_at"),
        "go_version": release.get("go_version"),
        "os": release.get("os"),
        "run_id": envelope.get("run_id"),
        "candidate_sha": (envelope.get("source") or {}).get("commit_sha"),
        "artifact": pathlib.Path(artifact_path).name,
        "artifact_sha256": recomputed,
        "envelope_schema_version": envelope.get("schema_version"),
        "required_rows": suite.get("required_rows"),
        "observed_rows": suite.get("observed_rows"),
        "skipped_tests": suite.get("skip_count"),
    }, release


def gate_row_from_g1(manifest_path, expected_sha):
    manifest = load_json(manifest_path, "G1 manifest")
    if manifest.get("kind") != "xflow.g1-evidence":
        fail("G1 manifest kind is not xflow.g1-evidence")
    binding = manifest.get("binding") or {}
    bound = {key: manifest[key] for key in ("kind", "schema_version", "run_id", "candidate_sha", "test", "report", "events")}
    bound["binding_version"] = G1_BINDING_VERSION
    canonical = json.dumps(bound, sort_keys=True, separators=(",", ":")).encode("utf-8")
    recomputed = hashlib.sha256(canonical).hexdigest()
    if binding.get("version") != G1_BINDING_VERSION or binding.get("sha256") != recomputed:
        fail("G1 manifest binding digest does not recompute: recorded=" + str(binding.get("sha256")) +
             " recomputed=" + recomputed)
    candidate = manifest.get("candidate_sha")
    if expected_sha and candidate != expected_sha:
        fail("G1 manifest candidate_sha " + str(candidate) + " != G0 candidate SHA " + expected_sha +
             ": the two gates did not run on the same candidate")

    root = pathlib.Path(manifest_path).parent
    generation = {}
    for role in ("report", "events"):
        descriptor = manifest.get(role) or {}
        name = descriptor.get("path")
        if not isinstance(name, str) or "/" in name or "\\" in name or name in ("", ".", ".."):
            fail("G1 manifest " + role + ".path is not a safe basename")
        path = root / name
        if not path.is_file():
            fail("G1 " + role + " generation file is missing: " + str(path))
        recomputed_digest = sha256_file(path)
        if recomputed_digest != descriptor.get("sha256") or path.stat().st_size != descriptor.get("size"):
            fail("G1 " + role + " generation file does not match its recorded size/digest: " + str(path))
        generation[role] = {"path": name, "sha256": recomputed_digest}

    report = load_json(root / generation["report"]["path"], "G1 report")
    if report.get("commit_sha") != candidate:
        fail("G1 report commit_sha " + str(report.get("commit_sha")) + " != manifest candidate_sha " + str(candidate))

    # Duration comes from the go-test event stream the gate captured: the
    # elapsed time of the gate's own test, which is the only duration the G1
    # artifacts record.
    duration = None
    try:
        for line in (root / generation["events"]["path"]).read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("Test") == G1_TEST_NAME and isinstance(event.get("Elapsed"), (int, float)) and event["Elapsed"] > 0:
                duration = float(event["Elapsed"])
    except Exception as exc:
        fail("cannot read the G1 event stream for the gate duration: " + str(exc))
    if duration is None:
        fail("the G1 event stream records no elapsed time for " + G1_TEST_NAME)

    return {
        "gate": "g1",
        "command": "make test-g1-evidence-required",
        "exit_code": (manifest.get("test") or {}).get("exit_code"),
        "duration_seconds": duration,
        "go_version": report.get("go_version"),
        "os": report.get("os"),
        "run_id": manifest.get("run_id"),
        "candidate_sha": candidate,
        "report": generation["report"]["path"],
        "report_sha256": generation["report"]["sha256"],
        "events_sha256": generation["events"]["sha256"],
        "binding_sha256": binding.get("sha256"),
        "test": (manifest.get("test") or {}).get("name"),
    }, {
        "g1_report_sha256": generation["report"]["sha256"],
        "g1_events_sha256": generation["events"]["sha256"],
    }


g0_dir = os.environ["G0_DIR"]
g0_artifact = os.environ["G0_ARTIFACT"]
g1_manifest = os.environ["G1_MANIFEST"]
out_path = pathlib.Path(os.environ["OUT"])
allow_different_head = os.environ["ALLOW_DIFFERENT_HEAD"] == "1"

selection = {}
if not g0_artifact:
    candidates = sorted(
        (p for p in pathlib.Path(g0_dir).glob("evidence-*.json") if p.is_file() and ".diagnostic." not in p.name),
        key=lambda p: p.stat().st_mtime,
    ) if pathlib.Path(g0_dir).is_dir() else []
    if not candidates:
        fail("no G0 artifact found under " + g0_dir +
             "; run make test-g0-evidence-required first (or pass --g0-artifact)")
    g0_artifact = str(candidates[-1])
    selection["g0"] = "newest-by-mtime"
    selection["g0_artifacts_present"] = len(candidates)

g0_rows, release = gate_row_from_g0(g0_artifact, load_json(g0_artifact, "G0 artifact"))
candidate_sha = g0_rows["candidate_sha"]
if not isinstance(candidate_sha, str) or not candidate_sha:
    fail("G0 artifact has no source.commit_sha")

head = None
try:
    import subprocess
    head = subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, check=True).stdout.decode().strip()
except Exception:
    head = None
if head and head != candidate_sha and not allow_different_head:
    fail("G0 artifact is bound to " + candidate_sha + " but HEAD is " + head +
         "; the tracked summary must describe the current candidate. Re-run the gate on HEAD, "
         "or pass --allow-different-head when deliberately recording a historical run.")

gates = [g0_rows]
unverified_scope = list(release.get("unverified_scope") or [])
evidence_inputs = [{
    "role": "g0-artifact",
    "path": g0_artifact,
    "sha256": g0_rows["artifact_sha256"],
}]

g1_manifest_path = pathlib.Path(g1_manifest)
if g1_manifest_path.is_file():
    g1_row, g1_inputs = gate_row_from_g1(g1_manifest_path, candidate_sha)
    gates.append(g1_row)
    evidence_inputs.append({"role": "g1-manifest", "path": g1_manifest, "sha256": sha256_file(g1_manifest_path)})
    evidence_inputs.append({"role": "g1-report", "path": str(g1_manifest_path.parent / g1_row["report"]), "sha256": g1_row["report_sha256"]})
    evidence_inputs.append({"role": "g1-events", "path": str(g1_manifest_path.parent / (json.loads(g1_manifest_path.read_bytes()).get("events") or {}).get("path", "")), "sha256": g1_row["events_sha256"]})
else:
    unverified_scope.append(
        "g1: no manifest at " + g1_manifest + "; this record does not cover the G1 gate"
    )

summary = {
    "summary_format_version": SUMMARY_FORMAT_VERSION,
    "kind": KIND,
    "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "generated_by": "scripts/release-summary.sh",
    "candidate_sha": candidate_sha,
    "candidate_is_head": bool(head) and head == candidate_sha,
    "tag": release.get("tag"),
    "tag_kind": release.get("tag_kind"),
    "toolchain": {
        "go": release.get("go_version"),
        "node": release.get("node_version"),
        "pnpm": release.get("pnpm_version"),
    },
    "platform": {"os": release.get("os"), "arch": release.get("arch")},
    "container_images": release.get("container_images"),
    "gates": gates,
    "attestation": release.get("attestation"),
    "unverified_scope": unverified_scope,
    "evidence_inputs": evidence_inputs,
    "selection": selection,
}

serialized = json.dumps(summary, indent=2, sort_keys=True) + "\n"

# Refuse to write a tracked file that carries anything credential-shaped.
def scan(value, path):
    if isinstance(value, dict):
        for key, item in value.items():
            scan(item, path + "." + str(key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            scan(item, path + "[" + str(index) + "]")
    elif isinstance(value, str):
        for pattern in CREDENTIAL_PATTERNS:
            if pattern.search(value):
                fail("refusing to write " + path + ": value looks like a credential (pattern " + pattern.pattern + ")")


scan(summary, "summary")

# Atomic publish: a half-written tracked record is worse than none.
out_path.parent.mkdir(parents=True, exist_ok=True)
handle, temporary = tempfile.mkstemp(dir=str(out_path.parent), prefix=".release-summary.", suffix=".tmp")
try:
    with os.fdopen(handle, "w", encoding="utf-8") as stream:
        stream.write(serialized)
    os.replace(temporary, out_path)
except BaseException:
    pathlib.Path(temporary).unlink(missing_ok=True)
    raise

print("release summary written: " + str(out_path))
for row in gates:
    print("  gate " + str(row["gate"]) + ": " + str(row["command"]) + " exit=" + str(row["exit_code"]) +
          " duration=" + str(row["duration_seconds"]) + "s sha=" + candidate_sha)
if any(not (image or {}).get("resolved") for image in (release.get("container_images") or [])):
    print("  WARNING: at least one container image digest is unresolved (resolved=false); "
          "the record states the reference without a digest", file=sys.stderr)
if release.get("tag_kind") == "none":
    print("  WARNING: no tag points at " + candidate_sha + "; the record states tag_kind=none", file=sys.stderr)
if len(gates) == 1:
    print("  WARNING: only the G0 gate is covered; G1 has no manifest yet", file=sys.stderr)
PY
