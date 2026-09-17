#!/usr/bin/env bash
#
# sbom-validate.sh checks that a generated CycloneDX SBOM is actually usable:
# that it parses, that the fields the schema requires are present and legal,
# that every `bom-ref` resolves, that the dependency graph is acyclic (CycloneDX
# forbids cycles and Go's module graph contains them, so this is a real check on
# scripts/sbom.sh rather than a formality), and — the check that matters most
# over time — that the document still describes THIS tree.
#
# It deliberately separates two things that are easy to conflate:
#
#   * the built-in structural checks below, which run offline with no
#     dependencies and encode the CycloneDX 1.5 requirements this repository
#     relies on; and
#   * OPTIONAL full JSON-Schema validation against the published CycloneDX
#     schema, which is attempted only when a validator is already installed.
#
# The distinction is printed, never glossed. A structural pass is not a schema
# pass, and this script will not let a reader think it is. If no validator is
# available the run prints exactly which one was looked for and what installing
# it would enable — it does not silently downgrade to "valid".
#
# Usage:
#   scripts/sbom-validate.sh [--sbom FILE] [--root DIR] [--schema FILE]
#                            [--against-repo] [--format text|json]
#
#   --sbom FILE      document to validate (default: release/xflow-sbom.cdx.json)
#   --root DIR       repository root, for --against-repo (default: script parent)
#   --schema FILE    JSON Schema to validate against, when a validator exists
#   --against-repo   also require the SBOM's module set to match `go list -m all`
#                    right now. This is the drift check: a stale SBOM is worse
#                    than no SBOM, because it is read as current.
#
# Exit: 0 valid (with any unavailable-validator notice stated), 1 invalid, 2 usage.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sbom="release/xflow-sbom.cdx.json"
selftest=0
schema=""
against_repo=0
format="text"

usage() {
	cat <<'USAGE'
usage: scripts/sbom-validate.sh [--sbom FILE] [--root DIR] [--schema FILE]
                                [--against-repo] [--format text|json]

  --sbom FILE      document to validate (default: release/xflow-sbom.cdx.json)
  --root DIR       repository root for --against-repo
  --schema FILE    JSON Schema file for full validation, if a validator is present
  --against-repo   require the SBOM module set to match `go list -m all` now
  --format         text (default) or json
  --selftest       prove the structural checks can FAIL: mutate the given document
                   in memory, one defect at a time, and require each mutation to
                   be rejected. A validator that has never rejected anything is
                   indistinguishable from one that checks nothing.
USAGE
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--sbom)
		[ "$#" -ge 2 ] || { echo "ERROR: --sbom needs a path" >&2; exit 2; }
		sbom="$2"
		shift 2
		;;
	--root)
		[ "$#" -ge 2 ] || { echo "ERROR: --root needs a path" >&2; exit 2; }
		root="$2"
		shift 2
		;;
	--schema)
		[ "$#" -ge 2 ] || { echo "ERROR: --schema needs a path" >&2; exit 2; }
		schema="$2"
		shift 2
		;;
	--against-repo)
		against_repo=1
		shift
		;;
	--selftest)
		selftest=1
		shift
		;;
	--format)
		[ "$#" -ge 2 ] || { echo "ERROR: --format needs a value" >&2; exit 2; }
		format="$2"
		shift 2
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
text | json) ;;
*)
	echo "ERROR: --format must be text or json, got $format" >&2
	exit 2
	;;
esac

case "$sbom" in
/*) sbom_abs="$sbom" ;;
*) sbom_abs="$root/$sbom" ;;
esac
if [ ! -f "$sbom_abs" ]; then
	echo "ERROR: SBOM not found: $sbom_abs" >&2
	echo "       Generate one with: scripts/sbom.sh" >&2
	exit 2
fi

# ── self-test: prove the structural checks can fail ──────────────────────────
#
# A validator that has never rejected anything is indistinguishable from one that
# checks nothing. This mode mutates a COPY of the document, one defect at a time,
# and re-invokes THIS script on each mutant through the ordinary CLI path — so it
# tests the real entry point, not an internal function. Any mutant that comes
# back VALID is a hole in the checks.
#
# The unmutated copy is included as the negative control: if it were rejected,
# every "rejected" result below would be meaningless.
if [ "$selftest" = "1" ]; then
	work="$(mktemp -d "${TMPDIR:-/tmp}/sbom-validate-selftest.XXXXXX")"
	trap 'rm -rf "$work"' EXIT

	python3 - "$sbom_abs" "$work" <<'MUTATE'
import copy
import json
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])
base = json.loads(src.read_text(encoding="utf-8"))


def mutate(name, fn):
    doc = copy.deepcopy(base)
    try:
        fn(doc)
    except (KeyError, IndexError, TypeError) as exc:
        print("MUTATION-FAILED " + name + ": " + str(exc), file=sys.stderr)
        raise SystemExit(2)
    (out / (name + ".json")).write_text(json.dumps(doc, indent=2), encoding="utf-8")


def first_component(doc):
    return doc["components"][0]


mutate("00-unmutated", lambda d: None)
mutate("01-bomformat", lambda d: d.__setitem__("bomFormat", "SPDX"))
mutate("02-specversion", lambda d: d.__setitem__("specVersion", "9.9"))
mutate("03-serial", lambda d: d.__setitem__("serialNumber", "not-a-uuid"))
mutate("04-timestamp", lambda d: d["metadata"].__setitem__("timestamp", "yesterday"))
mutate("05-component-type", lambda d: first_component(d).__setitem__("type", "gadget"))
mutate("06-component-name", lambda d: first_component(d).__setitem__("name", ""))
mutate("07-purl", lambda d: first_component(d).__setitem__("purl", "not a purl"))
mutate("08-hash-alg", lambda d: first_component(d).__setitem__("hashes", [{"alg": "CRC32", "content": "abcd"}]))
mutate("09-hash-length", lambda d: first_component(d).__setitem__("hashes", [{"alg": "SHA-256", "content": "deadbeef"}]))
mutate("10-dangling-ref", lambda d: d["dependencies"][0].__setitem__("ref", "pkg:golang/does-not-exist@v1.0.0"))
mutate("11-dangling-depends-on", lambda d: d["dependencies"][0].__setitem__("dependsOn", ["pkg:golang/also-missing@v1.0.0"]))
mutate("12-duplicate-bomref", lambda d: d["components"].append(copy.deepcopy(d["components"][0])))
mutate("13-empty-components", lambda d: d.__setitem__("components", []))


def make_cycle(d):
    deps = d["dependencies"]
    # Two real refs that do not already point at each other, forced into A->B->A.
    a, b = deps[0]["ref"], deps[1]["ref"]
    deps[0]["dependsOn"] = [b]
    deps[1]["dependsOn"] = [a]


mutate("14-cycle", make_cycle)
mutate("15-no-metadata", lambda d: d.pop("metadata"))
mutate("16-missing-version", lambda d: d.pop("version"))
MUTATE

	pass_n=0
	fail_n=0
	echo "sbom-validate --selftest: mutants in $work"
	echo
	for mutant in "$work"/*.json; do
		name="$(basename "$mutant" .json)"
		status=0
		"$0" --sbom "$mutant" --format json >"$work/$name.out" 2>&1 || status=$?
		if [ "$name" = "00-unmutated" ]; then
			if [ "$status" = "0" ]; then
				pass_n=$((pass_n + 1))
				echo "  ok   negative control: the unmutated document is VALID"
			else
				fail_n=$((fail_n + 1))
				echo "  FAIL negative control: the unmutated document was REJECTED; every result below is meaningless" >&2
				sed 's/^/       | /' "$work/$name.out" >&2
			fi
		elif [ "$status" != "0" ]; then
			pass_n=$((pass_n + 1))
			echo "  ok   rejected as required: ${name#*-}"
		else
			fail_n=$((fail_n + 1))
			echo "  FAIL NOT rejected: ${name#*-}" >&2
		fi
	done

	echo
	echo "sbom-validate --selftest: $pass_n passed, $fail_n failed"
	[ "$fail_n" -eq 0 ] || exit 1
	exit 0
fi

# Optional full schema validation. Detection is explicit so the report can name
# what was looked for instead of implying the check ran.
schema_validator=""
if [ -n "$schema" ]; then
	if python3 -c "import jsonschema" >/dev/null 2>&1; then
		schema_validator="python3-jsonschema"
	elif command -v check-jsonschema >/dev/null 2>&1; then
		schema_validator="check-jsonschema"
	elif command -v ajv >/dev/null 2>&1; then
		schema_validator="ajv"
	fi
fi

SBOM_PATH="$sbom_abs" SBOM_ROOT="$root" SCHEMA_PATH="$schema" \
	SCHEMA_VALIDATOR="$schema_validator" AGAINST_REPO="$against_repo" \
	FORMAT="$format" python3 - <<'PY'
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import uuid

path = pathlib.Path(os.environ["SBOM_PATH"])
root = pathlib.Path(os.environ["SBOM_ROOT"])
schema_path = os.environ["SCHEMA_PATH"]
schema_validator = os.environ["SCHEMA_VALIDATOR"]
against_repo = os.environ["AGAINST_REPO"] == "1"
format_ = os.environ["FORMAT"]

errors = []
notes = []
checks = 0


def check(condition, message):
    global checks
    checks += 1
    if not condition:
        errors.append(message)
    return condition


def note(message):
    notes.append(message)


# ── parse ────────────────────────────────────────────────────────────────────

try:
    document = json.loads(path.read_text(encoding="utf-8"))
except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
    print("ERROR: SBOM is not readable JSON: " + str(exc), file=sys.stderr)
    raise SystemExit(1)

check(isinstance(document, dict), "top level must be a JSON object")
if not isinstance(document, dict):
    print("ERROR: top level is " + type(document).__name__ + ", not an object", file=sys.stderr)
    raise SystemExit(1)

# ── CycloneDX 1.5 required and enumerated fields ─────────────────────────────
# Each check cites the field it defends. These are the requirements a consumer
# actually trips over, not an exhaustive transcription of the schema.

check(document.get("bomFormat") == "CycloneDX",
      "bomFormat must be the literal 'CycloneDX' (got %r)" % document.get("bomFormat"))
spec = document.get("specVersion")
check(isinstance(spec, str) and re.match(r"^1\.[0-6]$", spec or ""),
      "specVersion must be a CycloneDX version string like '1.5' (got %r)" % spec)

serial = document.get("serialNumber", "")
check(bool(re.match(r"^urn:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", serial)),
      "serialNumber must be a urn:uuid (got %r)" % serial)
if re.match(r"^urn:uuid:", serial or ""):
    value = serial[len("urn:uuid:"):]
    try:
        parsed = uuid.UUID(value)
        check(parsed.variant == uuid.RFC_4122,
              "serialNumber UUID must be RFC 4122 variant (got variant %r)" % parsed.variant)
    except ValueError as exc:
        errors.append("serialNumber is not a parseable UUID: " + str(exc))

version = document.get("version")
check(isinstance(version, int) and version >= 1,
      "version must be a positive integer BOM revision (got %r)" % version)

metadata = document.get("metadata")
check(isinstance(metadata, dict), "metadata is required by CycloneDX 1.x and must be an object")
if isinstance(metadata, dict):
    ts = metadata.get("timestamp")
    check(isinstance(ts, str) and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?([+-]\d{2}:\d{2})?$", ts or ""),
          "metadata.timestamp must be an RFC 3339 timestamp (got %r)" % ts)
    tools = metadata.get("tools")
    check(tools is not None, "metadata.tools should record the generator; without it a reader cannot judge the document")
    meta_component = metadata.get("component")
    check(isinstance(meta_component, dict) and bool(meta_component.get("name")),
          "metadata.component must identify the subject of the SBOM")

# ── components ───────────────────────────────────────────────────────────────

COMPONENT_TYPES = {
    "application", "framework", "library", "container", "platform",
    "operating-system", "device", "device-driver", "firmware", "file",
    "machine-learning-model", "data", "cryptographic-asset",
}
SCOPES = {"required", "optional", "excluded"}
HASH_ALGS = {"MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512", "SHA3-256", "SHA3-384", "SHA3-512",
             "BLAKE2b-256", "BLAKE2b-384", "BLAKE2b-512", "BLAKE3"}

components = document.get("components")
check(isinstance(components, list) and len(components) > 0,
      "components must be a non-empty array")
components = components if isinstance(components, list) else []

refs = []
purl_re = re.compile(r"^pkg:[a-z][a-z0-9.+\-]*/[^\s@]+(@[^\s@]+)?(\?[^\s#]*)?(#.*)?$")
for index, component in enumerate(components):
    where = "components[%d]" % index
    if not isinstance(component, dict):
        errors.append(where + " is not an object")
        continue
    ctype = component.get("type")
    check(ctype in COMPONENT_TYPES, where + ".type %r is not a CycloneDX component type" % ctype)
    name = component.get("name")
    check(isinstance(name, str) and name.strip() != "", where + ".name is required and must be non-empty")
    if "scope" in component:
        check(component["scope"] in SCOPES, where + ".scope %r is not one of %s" % (component["scope"], sorted(SCOPES)))
    if "version" in component:
        check(isinstance(component["version"], str) and component["version"].strip() != "",
              where + ".version, when present, must be a non-empty string")
    purl = component.get("purl")
    if purl is not None:
        check(bool(purl_re.match(purl)), where + ".purl is not a well-formed package URL: %r" % purl)
        if purl.startswith("pkg:golang/"):
            check("@" in purl, where + " is a Go module purl with no version: " + purl)
    bref = component.get("bom-ref")
    check(isinstance(bref, str) and bref.strip() != "",
          where + " has no bom-ref; without one no dependency entry can point at it")
    if isinstance(bref, str):
        refs.append(bref)
    for h in component.get("hashes", []) or []:
        check(isinstance(h, dict) and h.get("alg") in HASH_ALGS,
              where + " has a hash with an unknown alg: %r" % (h.get("alg") if isinstance(h, dict) else h))
        content = h.get("content") if isinstance(h, dict) else None
        expected_len = {"MD5": 32, "SHA-1": 40, "SHA-256": 64, "SHA-384": 96, "SHA-512": 128}.get(
            h.get("alg") if isinstance(h, dict) else "", None)
        if expected_len is not None:
            check(isinstance(content, str) and bool(re.match(r"^[0-9a-fA-F]{%d}$" % expected_len, content or "")),
                  where + " hash content is not a %d-hex %s digest" % (expected_len, h.get("alg")))

main_ref = None
if isinstance(metadata, dict) and isinstance(metadata.get("component"), dict):
    main_ref = metadata["component"].get("bom-ref")
    check(isinstance(main_ref, str) and main_ref.strip() != "",
          "metadata.component needs a bom-ref to participate in the dependency graph")
    if isinstance(main_ref, str):
        refs.append(main_ref)

duplicates = sorted({r for r in refs if refs.count(r) > 1})
check(not duplicates, "bom-ref values must be unique; duplicates: " + ", ".join(duplicates[:5]))

# ── dependency graph ─────────────────────────────────────────────────────────

dependencies = document.get("dependencies", [])
check(isinstance(dependencies, list), "dependencies, when present, must be an array")
dependencies = dependencies if isinstance(dependencies, list) else []

known = set(refs)
adjacency = {}
for index, entry in enumerate(dependencies):
    where = "dependencies[%d]" % index
    if not isinstance(entry, dict):
        errors.append(where + " is not an object")
        continue
    ref = entry.get("ref")
    check(isinstance(ref, str) and ref.strip() != "", where + ".ref is required")
    if isinstance(ref, str):
        check(ref in known, where + ".ref %r does not match any bom-ref in this document" % ref)
    depends_on = entry.get("dependsOn", [])
    check(isinstance(depends_on, list), where + ".dependsOn must be an array")
    for target in depends_on if isinstance(depends_on, list) else []:
        check(target in known, where + " depends on %r which is not a bom-ref in this document" % target)
    if isinstance(ref, str):
        adjacency.setdefault(ref, set()).update(depends_on if isinstance(depends_on, list) else [])

# CycloneDX 1.4+ requires an acyclic dependency graph. Go's module graph is
# cyclic, so scripts/sbom.sh removes back edges; this check proves it did.
WHITE, GRAY, BLACK = 0, 1, 2
color = {node: WHITE for node in adjacency}
cycles = []
for start in sorted(adjacency):
    if color[start] != WHITE:
        continue
    stack = [(start, iter(sorted(adjacency[start])))]
    color[start] = GRAY
    path_stack = [start]
    while stack:
        node, children = stack[-1]
        advanced = False
        for child in children:
            if color.get(child) == WHITE:
                color[child] = GRAY
                path_stack.append(child)
                stack.append((child, iter(sorted(adjacency.get(child, set())))))
                advanced = True
                break
            if color.get(child) == GRAY:
                try:
                    cycles.append(path_stack[path_stack.index(child):] + [child])
                except ValueError:
                    cycles.append([node, child])
        if not advanced:
            color[node] = BLACK
            stack.pop()
            path_stack.pop()
check(not cycles,
      "dependency graph must be acyclic; found %d cycle(s), e.g. %s"
      % (len(cycles), " -> ".join(cycles[0]) if cycles else ""))

# ── drift: does the document still describe this tree? ───────────────────────

repo_check = "not run"
if against_repo:
    go = shutil.which("go", path=os.environ.get("PATH", ""))
    if not go:
        note("--against-repo requested but `go` is not on PATH; the drift check did NOT run")
        repo_check = "unavailable: no go on PATH"
    elif not (root / "go.mod").is_file():
        note("--against-repo requested but no go.mod under %s; the drift check DID NOT run" % root)
        repo_check = "unavailable: no go.mod"
    else:
        completed = subprocess.run([go, "list", "-m", "all"], cwd=str(root),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if completed.returncode != 0:
            note("--against-repo: `go list -m all` failed; the drift check DID NOT run: "
                 + completed.stderr.decode("utf-8", "replace").strip().splitlines()[0])
            repo_check = "unavailable: go list failed"
        else:
            live = set()
            for line in completed.stdout.decode("utf-8", "replace").splitlines():
                # `go list -m all` prints "<module path> <version>" per line, and
                # the main module is printed with no version at all.
                fields = line.split()
                if not fields:
                    continue
                module_path = fields[0]
                if len(fields) < 2:
                    continue
                live.add(module_path)
            documented = {c.get("name") for c in components if isinstance(c, dict)}
            missing = sorted(live - documented)
            extra = sorted(documented - live)
            check(not missing,
                  "SBOM is stale: %d module(s) in the build list are absent from the document, e.g. %s"
                  % (len(missing), ", ".join(missing[:5])))
            check(not extra,
                  "SBOM lists %d module(s) no longer in the build list, e.g. %s"
                  % (len(extra), ", ".join(extra[:5])))
            repo_check = "ran: %d live module(s) vs %d documented" % (len(live), len(documented))

# ── optional full JSON-Schema validation ────────────────────────────────────

schema_check = "not run: no --schema given"
if schema_path:
    if not schema_validator:
        schema_check = (
            "NOT RUN: no JSON-Schema validator found. Looked for: python3 `jsonschema` module, "
            "`check-jsonschema`, `ajv` on PATH. A structural pass is NOT a schema pass; "
            "install one to close this gap."
        )
        note(schema_check)
    else:
        completed = None
        if schema_validator == "python3-jsonschema":
            completed = subprocess.run(
                [sys.executable, "-c",
                 "import json,sys,jsonschema;"
                 "schema=json.load(open(sys.argv[1]));doc=json.load(open(sys.argv[2]));"
                 "jsonschema.validate(doc,schema)",
                 schema_path, str(path)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        elif schema_validator == "check-jsonschema":
            completed = subprocess.run(
                ["check-jsonschema", "--schemafile", schema_path, str(path)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        elif schema_validator == "ajv":
            completed = subprocess.run(
                ["ajv", "validate", "-s", schema_path, "-d", str(path), "--spec=draft7"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if completed is None:
            schema_check = "NOT RUN: validator %s could not be invoked" % schema_validator
            note(schema_check)
        elif completed.returncode == 0:
            schema_check = "ran and passed (%s)" % schema_validator
        else:
            schema_check = "ran and FAILED (%s)" % schema_validator
            errors.append("JSON Schema validation failed: "
                          + completed.stderr.decode("utf-8", "replace").strip()[:2000])

# ── report ───────────────────────────────────────────────────────────────────

status = "valid" if not errors else "invalid"

if format_ == "json":
    print(json.dumps({
        "sbom": str(path),
        "specVersion": spec,
        "serialNumber": serial,
        "components": len(components),
        "dependencyEntries": len(dependencies),
        "structuralChecks": checks,
        "structuralErrors": errors,
        "schemaValidation": schema_check,
        "repoDriftCheck": repo_check,
        "notes": notes,
        "status": status,
    }, indent=2, sort_keys=True))
    raise SystemExit(0 if not errors else 1)

print("sbom-validate: " + str(path))
print("  specVersion          : " + str(spec))
print("  serialNumber         : " + str(serial))
print("  components           : %d" % len(components))
print("  dependency entries   : %d" % len(dependencies))
print("  structural checks    : %d" % checks)
print("  full schema check    : " + schema_check)
print("  drift vs repo        : " + repo_check)
for n in notes:
    if n != schema_check:
        print("  note                 : " + n)
print()
if errors:
    print("INVALID: %d structural problem(s):" % len(errors))
    for e in errors:
        print("  x " + e)
    raise SystemExit(1)

print("VALID (structural conformance to the enumerated CycloneDX 1.5 requirements)")
if not schema_path or not schema_validator:
    print("       NOT full JSON-Schema validated — see the schema check line above; do not")
    print("       read this pass as stronger than it is.")
PY
