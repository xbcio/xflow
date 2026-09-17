#!/usr/bin/env bash
#
# evidence-images.sh derives the container-image spec for the schema-v3
# `release.container_images` block of a G0 evidence artifact.
#
# The list of images comes from the tracked compose file that defines this
# repository's integration dependencies (test/env/docker-compose.yml). Registry
# digests are resolved best-effort from a local docker/podman daemon: a digest
# is a fact about the registry, not about this repository, so it cannot be
# derived from source. When no daemon can answer, the image is emitted WITHOUT
# a digest and the verifier records it as resolved=false. That distinction is
# the whole point — an unresolved digest must never be readable as a pinned one.
#
# Output (default):
#   redis=docker.io/library/redis:7.2@sha256:<64 hex>;mysql=docker.io/library/mysql:8.0
#
# With --format make:
#   EVIDENCE_CONTAINER_IMAGES='redis=...;mysql=...'
#
# Usage:
#   scripts/evidence-images.sh [--compose PATH] [--format spec|make] [--no-resolve]

set -euo pipefail

compose="test/env/docker-compose.yml"
format="spec"
resolve=1

usage() {
	cat <<'USAGE'
usage: scripts/evidence-images.sh [--compose PATH] [--format spec|make] [--no-resolve]

  --compose PATH   compose file to read image references from
                   (default: test/env/docker-compose.yml)
  --format spec    print the component=reference[@digest] spec (default)
  --format make    print an EVIDENCE_CONTAINER_IMAGES='...' assignment
  --no-resolve     do not query docker/podman for registry digests
USAGE
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--compose)
		[ "$#" -ge 2 ] || { echo "ERROR: --compose needs a path" >&2; exit 2; }
		compose="$2"
		shift 2
		;;
	--format)
		[ "$#" -ge 2 ] || { echo "ERROR: --format needs a value" >&2; exit 2; }
		format="$2"
		shift 2
		;;
	--no-resolve)
		resolve=0
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
spec | make) ;;
*)
	echo "ERROR: --format must be spec or make, got $format" >&2
	exit 2
	;;
esac

if [ ! -f "$compose" ]; then
	echo "ERROR: compose file not found: $compose" >&2
	exit 1
fi

COMPOSE="$compose" RESOLVE="$resolve" FORMAT="$format" python3 - <<'PY'
import os
import pathlib
import re
import shutil
import subprocess
import sys

compose = pathlib.Path(os.environ["COMPOSE"])
resolve = os.environ["RESOLVE"] == "1"
format_ = os.environ["FORMAT"]

# Known service -> component names. Anything else keeps its service name, so a
# newly added dependency shows up in the evidence instead of being dropped.
KNOWN = {"redis": "redis", "mysql": "mysql", "kafka": "kafka"}

SERVICE = re.compile(r"^  ([A-Za-z0-9_.-]+):\s*$")
IMAGE = re.compile(r"^\s+image:\s*(\S+)\s*$")


def parse_services(text):
    """Return {service: image} for the top-level `services:` block.

    Deliberately a two-pattern line scanner rather than a YAML parse: the file
    is tracked and hand-maintained, and adding a YAML dependency to the release
    gate would be a bigger risk than the parse it replaces.
    """
    services = {}
    current = None
    in_services = False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line:
            continue
        if not in_services:
            if line.strip() == "services:":
                in_services = True
            continue
        if not raw.startswith(" "):
            # Back at column 0: the services block ended.
            break
        service = SERVICE.match(line)
        if service:
            current = service.group(1)
            continue
        image = IMAGE.match(line)
        if image and current:
            services[current] = image.group(1)
    return services


def registry_digest(reference):
    """Best-effort registry digest for reference, or None.

    Never raises and never blocks the gate: the caller records resolved=false
    when this returns None, which is the honest answer when no daemon (or no
    image) is available.
    """
    if not resolve:
        return None
    commands = []
    if shutil.which("docker"):
        commands.append(["docker", "image", "inspect", "--format", "{{range .RepoDigests}}{{println .}}{{end}}", reference])
    if shutil.which("podman"):
        commands.append(["podman", "image", "inspect", "--format", "{{range .RepoDigests}}{{println .}}{{end}}", reference])
    for command in commands:
        try:
            completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=15, check=False)
        except (OSError, subprocess.SubprocessError):
            continue
        if completed.returncode != 0:
            continue
        for line in completed.stdout.decode("utf-8", "replace").splitlines():
            match = re.search(r"(sha256:[0-9a-f]{64})\s*$", line.strip())
            if match:
                return match.group(1)
    return None


services = parse_services(compose.read_text(encoding="utf-8"))
if not services:
    print("ERROR: no `image:` entries found in the services block of " + str(compose), file=sys.stderr)
    raise SystemExit(1)

components = {}
for service, reference in services.items():
    component = KNOWN.get(service.lower(), service)
    if component in components:
        print("ERROR: services " + service + " and another map to component " + component, file=sys.stderr)
        raise SystemExit(1)
    components[component] = reference

entries = []
unresolved = []
for component in sorted(components):
    reference = components[component]
    digest = registry_digest(reference)
    if digest:
        entries.append(component + "=" + reference + "@" + digest)
    else:
        entries.append(component + "=" + reference)
        unresolved.append(component)

spec = ";".join(entries)
if format_ == "make":
    print("EVIDENCE_CONTAINER_IMAGES='" + spec + "'")
else:
    print(spec)

if unresolved and resolve:
    print(
        "evidence-images: no registry digest resolved for " + ", ".join(unresolved) +
        "; the artifact will record resolved=false (set EVIDENCE_CONTAINER_IMAGES to pin them)",
        file=sys.stderr,
    )
PY
