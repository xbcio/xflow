#!/usr/bin/env bash
#
# pin-audit.sh checks that every reference this repository reaches out to at
# build/CI/release time is pinned to something immutable and machine-checkable:
#
#   * GitHub Actions `uses:` -> 40-hex commit SHA (a mutable tag is not a pin);
#   * container images in CI service blocks and the integration compose file
#     -> an `@sha256:` digest (a tag is a moving pointer, not a pin);
#   * tool installs performed by CI or the Makefile (go install, npx, curl'd
#     release binaries) -> an explicit immutable version.
#
# SCOPE, stated plainly because a gate that overstates itself is worse than no
# gate. This is a SOURCE-TEXT audit. It proves that a human wrote an immutable
# reference into a tracked file. It proves nothing about PROVENANCE: it does
# not verify a signature, it does not confirm that a commit SHA belongs to the
# tag written next to it, and it does not confirm that a digest names the image
# someone believes it names. All three require network or keys and are out of
# scope here. docs/references/supply-chain-pins.md carries the same list.
#
# Files scanned (add more with --workflow / --compose):
#   .github/workflows/*.yml, *.yaml
#   test/env/docker-compose.yml, docker-compose.yaml
#
# A mutable reference is NOT automatically a failure. It must be DECLARED in
# the allowlist (default: scripts/pins-allowlist.txt) with a reason, an owner
# and an expiry date. Then:
#
#   default   find nothing, report the declared gaps, exit 0  (`make pin-audit`)
#   --strict  any declared-and-still-mutable reference is a FAILURE. This is
#             the release gate: it fails today, by design, and keeps failing
#             until the digests are pinned or the expiry is re-justified.
#
# The split is deliberate. The default target must stay green so it can be wired
# into ordinary CI without either breaking the build or being ignored; the
# strict target is what a release gate actually wants, and it is honest about
# the current state.
#
# Usage:
#   scripts/pin-audit.sh [--root DIR] [--allowlist FILE] [--strict]
#                        [--format text|json] [--quiet]

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
allowlist=""
strict=0
format="text"
quiet=0

usage() {
	cat <<'USAGE'
usage: scripts/pin-audit.sh [options]

  --root DIR         repository root (default: this script's parent directory)
  --allowlist FILE   known-mutable-pin inventory (default: scripts/pins-allowlist.txt)
  --strict           treat every declared-but-still-mutable reference as a failure
  --format text|json output format (default: text)
  --quiet            print only failures and the summary line

Exit codes:
  0  pass (no violations; declared gaps reported unless --strict)
  1  violations found (or, under --strict, declared mutable references remain)
  2  usage error / unreadable allowlist / no scan targets found
USAGE
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--root)
		[ "$#" -ge 2 ] || { echo "ERROR: --root needs a path" >&2; exit 2; }
		root="$2"
		shift 2
		;;
	--allowlist)
		[ "$#" -ge 2 ] || { echo "ERROR: --allowlist needs a path" >&2; exit 2; }
		allowlist="$2"
		shift 2
		;;
	--strict)
		strict=1
		shift
		;;
	--format)
		[ "$#" -ge 2 ] || { echo "ERROR: --format needs a value" >&2; exit 2; }
		format="$2"
		shift 2
		;;
	--quiet)
		quiet=1
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
text | json) ;;
*)
	echo "ERROR: --format must be text or json, got $format" >&2
	exit 2
	;;
esac

if [ ! -d "$root" ]; then
	echo "ERROR: --root is not a directory: $root" >&2
	exit 2
fi
root="$(cd "$root" && pwd)"

if [ -z "$allowlist" ]; then
	allowlist="$root/scripts/pins-allowlist.txt"
fi
if [ ! -f "$allowlist" ]; then
	echo "ERROR: allowlist not found: $allowlist" >&2
	echo "       A missing allowlist would silently downgrade every known gap to a violation;" >&2
	echo "       refusing to guess. Pass --allowlist explicitly to scan without one." >&2
	exit 2
fi

ROOT="$root" ALLOWLIST="$allowlist" STRICT="$strict" FORMAT="$format" QUIET="$quiet" python3 - <<'PY'
import datetime
import json
import os
import pathlib
import re
import sys

root = pathlib.Path(os.environ["ROOT"])
allowlist_path = pathlib.Path(os.environ["ALLOWLIST"])
strict = os.environ["STRICT"] == "1"
format_ = os.environ["FORMAT"]
quiet = os.environ["QUIET"] == "1"
today = datetime.date.today()

SHA40 = re.compile(r"^[0-9a-f]{40}$")
# An immutable tool version: plain semver, or the Go pseudo-version form, both
# optionally v-prefixed. Ranges (`^1.2.3`, `~1.2`, `>=1`) and dist-tags
# (`latest`, `next`) are neither immutable nor machine-checkable, so they are
# not accepted as pins.
VERSION_PIN = re.compile(r"^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.\-]+)?$|^v\d+\.\d+\.\d+-\d{14}-[0-9a-f]{12}$")
# `owner/repo@ref` for Actions; the ref may be absent (means the default branch).
USES = re.compile(r"^\s*(?:-\s*)?uses:\s*([^\s#]+)(?:\s*#\s*(.*))?$")
IMAGE = re.compile(r"^\s*(?:-\s*)?image:\s*[\"']?([^\s\"'#]+)[\"']?\s*(?:#\s*(.*))?$")
# A `run:` scalar line that installs a tool. Only shell lines are considered,
# and only ones that reference the tool by its registry path.
GO_INSTALL = re.compile(r"\bgo\s+install\s+(.*)$")
NPX = re.compile(r"\bnpx\b(?:\s+--?[\w=.,\-]+)*\s+(@?[A-Za-z0-9._\-/]+(?:@[^\s]+)?)")
# `-o` is not a word character, so \b does not sit between a space and a hyphen
# under Python's re either; match the surrounding whitespace explicitly.
CURL_DOWNLOAD = re.compile(r"\bcurl\b.*?(?:\s)(-o|--output|-O)(?:\s|$)")

SCAN_GLOBS = [".github/workflows/*.yml", ".github/workflows/*.yaml"]
COMPOSE_CANDIDATES = ["test/env/docker-compose.yml", "test/env/docker-compose.yaml"]

findings = []  # {kind, key, path, line, text, detail}
allow_errors = []


def rel(p):
    try:
        return str(p.relative_to(root))
    except ValueError:
        return str(p)


def add(kind, key, path, line, text, detail, section=""):
    findings.append(
        {
            "kind": kind,
            "section": section,
            "key": key,
            "path": rel(path),
            "line": line,
            "text": text.strip(),
            "detail": detail,
        }
    )


# ── allowlist ────────────────────────────────────────────────────────────────
#
# INI-ish, deliberately tiny: `[kind]` sections, then `key = reason | owner=... |
# expires=YYYY-MM-DD`. A duplicate key is an error rather than a silent
# last-one-wins, because a security inventory with two answers is not an
# inventory.

allow = {}
section = None
for lineno, raw in enumerate(allowlist_path.read_text(encoding="utf-8").splitlines(), 1):
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    if line.startswith("[") and line.endswith("]"):
        section = line[1:-1].strip()
        if section not in ("actions", "images", "tools"):
            allow_errors.append(f"{allowlist_path.name}:{lineno}: unknown section [{section}]")
            section = None
        continue
    if section is None:
        allow_errors.append(f"{allowlist_path.name}:{lineno}: entry outside any [section]")
        continue
    if "=" not in line:
        allow_errors.append(f"{allowlist_path.name}:{lineno}: expected `key = fields`")
        continue
    key, rest = line.split("=", 1)
    key = key.strip()
    fields = {}
    for part in rest.split("|"):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            fname, fvalue = part.split("=", 1)
            fields[fname.strip()] = fvalue.strip()
        else:
            fields.setdefault("reason", part)
    if not fields.get("reason"):
        allow_errors.append(f"{allowlist_path.name}:{lineno}: {key} has no reason")
    if not fields.get("owner"):
        allow_errors.append(f"{allowlist_path.name}:{lineno}: {key} has no owner")
    expires = fields.get("expires", "")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expires):
        allow_errors.append(f"{allowlist_path.name}:{lineno}: {key} needs expires=YYYY-MM-DD")
    elif datetime.date.fromisoformat(expires) < today:
        # Expiry is the mechanism that stops this file from becoming a
        # permanent excuse: a lapsed entry is a real failure, not a note.
        add(
            "allowlist-expired",
            key,
            allowlist_path,
            lineno,
            raw,
            f"declared gap expired {expires} (reason: {fields.get('reason')}, owner: {fields.get('owner')})",
        )
    composite = f"{section}:{key}"
    if composite in allow:
        allow_errors.append(f"{allowlist_path.name}:{lineno}: duplicate entry {composite}")
        continue
    allow[composite] = {"expires": expires, **fields}

if allow_errors:
    for err in allow_errors:
        print("ERROR: " + err, file=sys.stderr)
    raise SystemExit(2)

# The decoy trap. A never-fires check cannot be told apart from a passing one
# (secret-scan.yml makes exactly this argument for gitleaks). If the sentinel
# key below is present in the allowlist, the scan is misconfigured or is being
# fed a fabricated inventory, and every "pass" it produces is worthless.
if "actions:SELFTEST-SENTINEL-MUST-NEVER-BE-DECLARED" in allow:
    print(
        "ERROR: allowlist contains the self-test sentinel key; the allowlist is a "
        "fabricated or corrupted inventory and cannot be trusted",
        file=sys.stderr,
    )
    raise SystemExit(2)


def declared(kind, key):
    entry = allow.get(f"{kind}:{key}")
    return entry


def check_or_declare(kind, key, path, line, text, detail):
    """Record either a declared gap (advisory) or a violation."""
    entry = declared(kind, key)
    if entry is None:
        add("violation", kind, path, line, text, detail)
        return
    add(
        "declared", key, path, line, text,
        f"{detail}; declared: {entry['reason']} (owner {entry['owner']}, expires {entry['expires']})",
        section=kind,
    )


# ── scan targets ─────────────────────────────────────────────────────────────

targets = []
for pattern in SCAN_GLOBS:
    targets.extend(sorted(root.glob(pattern)))
for candidate in COMPOSE_CANDIDATES:
    p = root / candidate
    if p.is_file():
        targets.append(p)
# De-duplicate, keeping a stable order.
seen = set()
targets = [t for t in targets if not (t in seen or seen.add(t))]

if not targets:
    print(
        "ERROR: no scan targets matched under " + str(root) +
        "; expected .github/workflows/*.yml and test/env/docker-compose.yml",
        file=sys.stderr,
    )
    raise SystemExit(2)


# ── action pin drift ─────────────────────────────────────────────────────────
#
# Every `uses:` must be SHA-pinned. Additionally, the same action must not be
# pinned to two different SHAs in one tree: that is a half-finished upgrade and
# is exactly the state a reviewer cannot see by reading one file.

action_shas = {}
for path in targets:
    if path.suffix not in (".yml", ".yaml") or ".github/workflows" not in str(path):
        continue
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        m = USES.match(raw)
        if not m:
            continue
        ref = m.group(1)
        comment = (m.group(2) or "").strip()
        if ref.startswith("./") or ref.startswith("docker://"):
            # A local composite action is already pinned by the checked-out
            # commit; a docker:// reference is checked by the image rules.
            continue
        if "@" not in ref:
            check_or_declare(
                "actions", f"{ref}@<unset-ref>", path, lineno, raw,
                "uses: has no @ref; it resolves whatever the default branch holds at run time",
            )
            continue
        name, ref_value = ref.rsplit("@", 1)
        if not SHA40.match(ref_value):
            check_or_declare(
                "actions", f"{name}@{ref_value}", path, lineno, raw,
                f"uses: {name} is pinned to the mutable ref '{ref_value}', not a commit SHA",
            )
            continue
        key = f"{name}@{ref_value}"
        if not comment:
            # A bare SHA is a pin, but an unreviewable one: nothing records what
            # it is a pin OF, so nobody can tell a legitimate upgrade from a
            # substitution pointed at a fork.
            check_or_declare(
                "actions", f"{name}@{ref_value}", path, lineno, raw,
                f"uses: {name} is SHA-pinned but carries no `# <tag>` comment, so the reviewed tag cannot be read back",
            )
        action_shas.setdefault(name, {}).setdefault(ref_value, []).append((path, lineno))

for name, shas in sorted(action_shas.items()):
    if len(shas) > 1:
        locations = ", ".join(f"{rel(p)}:{ln}" for sha in sorted(shas) for p, ln in shas[sha])
        add(
            "violation", "actions", root / ".github/workflows", 0,
            name,
            f"{name} is pinned to {len(shas)} different SHAs in one tree ({locations}); "
            "a partially applied upgrade means the reviewed pin is not the one that runs",
        )


# ── container images ─────────────────────────────────────────────────────────
#
# Reference grammar: [registry/]repo[:tag][@sha256:<64 hex>]. A digest anywhere
# in the reference is the pin; a tag alone is not, no matter how specific it
# looks, because a tag can be repointed at different bytes.

DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")


def image_key(reference):
    """Normalise a reference to `registry/repo:tag`, applying docker's defaults.

    `redis:7.2`, `library/redis:7.2` and `docker.io/library/redis:7.2` are the
    same image, and a report that lists them as three different pins invites
    the reader to conclude one of them is the fine one. Note the direction of
    the normalisation: it is display/report only. A reference that NAMES a
    registry (`docker.io/apache/kafka:3.7.2`) keeps that registry — the
    previous attempt appended a second `docker.io/` to it, which is how a
    bogus key gets into an inventory nobody re-reads.
    """
    name, _, _digest = reference.partition("@")
    repo_part = name
    registry = "docker.io"
    slash = name.find("/")
    if slash != -1:
        head = name[:slash]
        # A registry component contains a dot or a port; a bare word is a
        # namespace on Docker Hub (`apache/kafka`), not a host.
        if "." in head or ":" in head or head == "localhost":
            registry = head
            repo_part = name[slash + 1:]
    if ":" in repo_part:
        repo, tag = repo_part.rsplit(":", 1)
    else:
        repo, tag = repo_part, "latest"
    if registry == "docker.io" and "/" not in repo:
        repo = "library/" + repo
    return f"{registry}/{repo}:{tag}"


for path in targets:
    text = path.read_text(encoding="utf-8")
    in_services = False
    for lineno, raw in enumerate(text.splitlines(), 1):
        stripped = raw.strip()
        if stripped == "services:":
            in_services = True
            continue
        m = IMAGE.match(raw)
        if not m:
            continue
        # Only `image:` lines that declare a container runtime image. A compose
        # `image:` next to a `build:` is a produced image name, not a consumed
        # reference, so it is reported as a declared advisory rather than
        # silently skipped.
        reference = m.group(1)
        key = image_key(reference)
        if DIGEST.search(reference):
            continue
        where = "workflow service" if ".github/workflows" in str(path) else "compose service"
        if reference.endswith(":latest") or ":" not in reference.rsplit("/", 1)[-1]:
            detail = f"{where} image {reference} floats on the implicit ':latest' tag"
            expected = key
        else:
            expected = f"{key}@sha256:<64 hex>"
            detail = f"{where} image {reference} is pinned to a mutable tag; expected {expected}"
        check_or_declare("images", key, path, lineno, raw, detail)


# ── tool installs ────────────────────────────────────────────────────────────
#
# Everything CI or the Makefile fetches to run a check. An unpinned fetch here
# silently changes what the gate meant between two runs of the same commit.

tool_targets = [p for p in targets if ".github/workflows" in str(p)]
makefile = root / "Makefile"
if makefile.is_file():
    tool_targets.append(makefile)

for path in tool_targets:
    # Indentation of the `run:` key whose block literal we are inside, or None.
    # Tracking the key's own indent (rather than assuming 8) is what makes the
    # block end where the YAML block ends, so a step written as a list item at
    # any nesting depth is still scanned.
    run_block_indent = None
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw
        # Shell only: a `run: |` block, a `run: <one-liner>`, or a Makefile
        # recipe line. YAML keys that merely mention a tool are not installs.
        if path.name == "Makefile":
            if not raw.startswith("\t"):
                continue
        else:
            block = re.match(r"^(\s*)(?:-\s*)?run:\s*\|", raw)
            if block:
                run_block_indent = len(raw) - len(raw.lstrip())
                continue
            if run_block_indent is not None:
                # Inside a block literal: scan every more-indented line. A line
                # at or left of the key's own indent ends the block.
                if not raw.strip():
                    continue
                if len(raw) - len(raw.lstrip()) <= run_block_indent:
                    run_block_indent = None
                else:
                    line = raw
            if run_block_indent is None:
                one_liner = re.match(r"^\s*(?:-\s*)?run:\s*(.+)$", raw)
                if one_liner:
                    line = one_liner.group(1)
                else:
                    continue

        m = GO_INSTALL.search(line)
        if m:
            args = m.group(1).strip()
            spec = None
            for token in args.split():
                if "@" in token or "/" in token:
                    spec = token
                    break
            if spec is None or "@" not in spec:
                check_or_declare(
                    "tools", f"{rel(path)}:go-install", path, lineno, raw,
                    f"`go install {args}` has no @version; it fetches the tool at whatever the proxy serves then",
                )
            else:
                pkg, version = spec.rsplit("@", 1)
                if not VERSION_PIN.match(version):
                    check_or_declare(
                        "tools", f"{rel(path)}:go-install:{pkg}@{version}", path, lineno, raw,
                        f"`go install {pkg}@{version}` is not an immutable version",
                    )

        for npx_match in NPX.finditer(line):
            spec = npx_match.group(1)
            if spec.startswith("-"):
                continue
            if "@" not in spec:
                check_or_declare(
                    "tools", f"{rel(path)}:npx:{spec}", path, lineno, raw,
                    f"`npx {spec}` has no @version; npm resolves the latest published version at run time",
                )
            else:
                pkg, version = spec.rsplit("@", 1)
                if not pkg or not VERSION_PIN.match(version):
                    check_or_declare(
                        "tools", f"{rel(path)}:npx:{spec}", path, lineno, raw,
                        f"`npx {spec}` uses a range or dist-tag rather than an immutable version",
                    )

        if CURL_DOWNLOAD.search(line) and path.name != "Makefile":
            # The neighbouring line(s) usually hold the URL. Look ahead a little
            # so `curl ... \\\n  URL` is still attributed.
            window = "\n".join(
                (path.read_text(encoding="utf-8").splitlines()[lineno - 1: lineno + 3])
            )
            if re.search(r"/releases/download/|\.tar\.gz|\.zip|\.tgz", window) and not re.search(
                r"sha256|SHA256|checksum|--checksum", window
            ):
                check_or_declare(
                    "tools", f"{rel(path)}:curl-download", path, lineno, raw,
                    "curl fetches a release archive with no checksum or signature check in the surrounding lines",
                )


# ── report ───────────────────────────────────────────────────────────────────

violations = [f for f in findings if f["kind"] == "violation"]
expired = [f for f in findings if f["kind"] == "allowlist-expired"]
declared_findings = [f for f in findings if f["kind"] == "declared"]
# An allowlist entry that matches nothing in the tree is dead inventory: either
# the reference was pinned (good — delete the entry) or its spelling drifted
# (bad — the entry silently stopped protecting anything). Either way it must be
# visible, and under --strict it is a failure.
used_allow = {f"{f['section']}:{f['key']}" for f in declared_findings}
unused_allow = sorted(k for k in allow if k not in used_allow)

failed = bool(violations) or bool(expired) or (strict and (bool(declared_findings) or bool(unused_allow)))

if format_ == "json":
    print(json.dumps(
        {
            "root": str(root),
            "allowlist": rel(allowlist_path),
            "strict": strict,
            "scanned": [rel(t) for t in targets],
            "violations": violations,
            "expiredAllowlistEntries": expired,
            "declaredMutablePins": declared_findings,
            "unusedAllowlistEntries": unused_allow,
            "status": "fail" if failed else "pass",
        },
        indent=2,
        sort_keys=True,
    ))
    raise SystemExit(1 if failed else 0)

def emit(heading, items, marker):
    if not items:
        return
    print(heading)
    for item in items:
        where = f"{item['path']}:{item['line']}" if item["line"] else item["path"]
        print(f"  {marker} {where}: {item['detail']}")
        if item["text"]:
            print(f"      {item['text']}")
    print()

if not quiet:
    print(f"pin-audit: scanning {len(targets)} file(s) under {root}")
    for t in targets:
        print(f"  - {rel(t)}")
    print()

emit("FAIL: undeclared or unmachine-checkable references:", violations, "x")
emit("FAIL: allowlist entries past their expiry:", expired, "x")

if strict:
    if declared_findings:
        print("FAIL (--strict): declared-but-still-mutable references remain:")
        for item in declared_findings:
            where = f"{item['path']}:{item['line']}" if item["line"] else item["path"]
            print(f"  x {where}: {item['detail']}")
        print()
    if unused_allow:
        print("FAIL (--strict): allowlist entries that no longer match any reference "
              "(delete them so the inventory keeps meaning something):")
        for key in unused_allow:
            print(f"  x {key}")
        print()
elif declared_findings and not quiet:
    print("KNOWN GAPS (declared in the allowlist, not yet pinned):")
    for item in declared_findings:
        where = f"{item['path']}:{item['line']}" if item["line"] else item["path"]
        print(f"  ! {where}: {item['key']} -> {item['detail'].split('; declared:')[0]}")
    print()
    print("  These are reported, not enforced. `make pin-audit-strict` is the release")
    print("  gate that fails on every one of them.")
    print()

if unused_allow and not strict and not quiet:
    print("NOTE: allowlist entries matching nothing in this tree (remove or fix):")
    for key in unused_allow:
        print(f"  ? {key}")
    print()

print(
    f"pin-audit: {len(violations)} violation(s), {len(expired)} expired allowlist "
    f"entr(ies), {len(declared_findings)} declared mutable reference(s)"
    + (", strict" if strict else "")
    + f" -> {'FAIL' if failed else 'PASS'}"
)
raise SystemExit(1 if failed else 0)
PY
