#!/usr/bin/env bash
#
# pin-audit-selftest.sh proves scripts/pin-audit.sh can actually fail.
#
# Why this exists: a check that has never been observed failing is indis-
# tinguishable from a check that always passes. This repository has already
# been bitten by exactly that — .github/workflows/secret-scan.yml carries a
# comment describing a sweep for internal addresses that "came back clean
# against a tree that provably contained them". So each case below plants a
# specific defect in a throwaway fixture tree and asserts the auditor reports
# it. If a future edit makes the auditor blind, this script goes red instead
# of the gate quietly going green.
#
# The cases, and what each one is defending:
#
#   1  mutable action tag                  -> uses: must be a 40-hex SHA
#   2  SHA-pinned action with no tag comment -> the pin must stay readable
#   3  same action pinned to two SHAs      -> a half-applied upgrade
#   4  bare `uses:` with no ref at all     -> resolves the default branch
#   5  tag-only container image            -> must have an @sha256: digest
#   6  digests present                     -> the check does not fire on a
#                                             correctly pinned tree (no
#                                             always-fail "check")
#   7  unpinned npx tool                   -> no @version
#   8  unpinned `go install`               -> no @version
#   9  curl'd release archive, no checksum -> artifact trusted on TLS alone
#  10  declared-but-unpinned image         -> default mode reports, --strict fails
#  11  allowlist entry past its expiry     -> hard failure in BOTH modes
#  12  self-test sentinel in the allowlist -> refuses to run on a fabricated
#                                             inventory
#  13  missing allowlist file              -> refuses to guess
#
# Usage: scripts/pin-audit-selftest.sh [--keep]
# Exit:  0 every case behaved as asserted; 1 any case did not.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
auditor="$repo_root/scripts/pin-audit.sh"
keep=0

while [ "$#" -gt 0 ]; do
	case "$1" in
	--keep)
		keep=1
		shift
		;;
	-h | --help)
		echo "usage: scripts/pin-audit-selftest.sh [--keep]"
		exit 0
		;;
	*)
		echo "ERROR: unknown argument: $1" >&2
		exit 2
		;;
	esac
done

if [ ! -x "$auditor" ]; then
	echo "ERROR: auditor not executable: $auditor" >&2
	exit 1
fi

# mktemp -d, never anything under .tmp/: sibling agents are running tests there
# right now and this script must not be able to touch their caches.
work="$(mktemp -d "${TMPDIR:-/tmp}/pin-audit-selftest.XXXXXX")"
cleanup() {
	if [ "$keep" = "1" ]; then
		echo "pin-audit-selftest: fixture kept at $work"
	else
		rm -rf "$work"
	fi
}
trap cleanup EXIT

pass=0
fail=0

ok() {
	pass=$((pass + 1))
	echo "  ok   $1"
}

bad() {
	fail=$((fail + 1))
	echo "  FAIL $1" >&2
}

# expect_exit <description> <expected-exit> <actual-exit>
expect_exit() {
	if [ "$2" = "$3" ]; then
		ok "$1 (exit $3)"
	else
		bad "$1: expected exit $2, got $3"
	fi
}

# expect_contains <description> <needle> <haystack-file>
expect_contains() {
	if grep -qF -- "$2" "$3"; then
		ok "$1"
	else
		bad "$1: output does not contain '$2'"
		sed 's/^/       | /' "$3" >&2
	fi
}

run_audit() {
	# run_audit <fixture-root> <allowlist> <out-file> [extra args...]
	local root="$1" allow="$2" out="$3"
	shift 3
	local status=0
	"$auditor" --root "$root" --allowlist "$allow" "$@" >"$out" 2>&1 || status=$?
	return "$status"
}

echo "pin-audit-selftest: fixture root $work"
echo

# ── fixture: a tree containing one instance of every defect ──────────────────
#
# Written out whole rather than generated from the real workflows, so the cases
# cannot silently disappear when a real file is renamed or restructured.

fixture="$work/bad-tree"
mkdir -p "$fixture/.github/workflows" "$fixture/test/env" "$fixture/scripts"

cat >"$fixture/.github/workflows/ci.yml" <<'YAML'
name: fixture
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7.2
      pinned:
        image: docker.io/library/redis:7.2@sha256:1111111111111111111111111111111111111111111111111111111111111111
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff
      - uses: actions/cache@1111111111111111111111111111111111111111 # v4
      - uses: actions/cache@2222222222222222222222222222222222222222 # v4
      - uses: some/action
      - run: |
          go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
          go install golang.org/x/vuln/cmd/govulncheck
          npx --yes @redocly/cli@1.34.2 lint --config .redocly.yaml api/openapi/xflow-v1.yaml
          npx @stoplight/spectral-cli lint api/openapi/xflow-v1.yaml
          curl -sSfL -o /tmp/tool.tar.gz "https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/gitleaks_8.30.1_linux_x64.tar.gz"
          tar -xzf /tmp/tool.tar.gz -C /usr/local/bin tool
YAML

cat >"$fixture/test/env/docker-compose.yml" <<'YAML'
services:
  redis:
    image: redis:7.2
  mysql:
    image: docker.io/library/mysql:8.0@sha256:2222222222222222222222222222222222222222222222222222222222222222
YAML

# ── case 10/11: an allowlist that declares one image and has lapsed entries ──

cat >"$work/allowlist-declared.txt" <<'TXT'
[images]
docker.io/library/redis:7.2 = reason=fixture: declared on purpose | owner=nobody | expires=2999-01-01
TXT

cat >"$work/allowlist-expired.txt" <<'TXT'
[images]
docker.io/library/redis:7.2 = reason=fixture: declared on purpose | owner=nobody | expires=2000-01-01
TXT

cat >"$work/allowlist-sentinel.txt" <<'TXT'
[images]
docker.io/library/redis:7.2 = reason=fixture | owner=nobody | expires=2999-01-01

[actions]
SELFTEST-SENTINEL-MUST-NEVER-BE-DECLARED = reason=fabricated inventory probe | owner=nobody | expires=2999-01-01
TXT

cat >"$work/allowlist-missing-fields.txt" <<'TXT'
[images]
docker.io/library/redis:7.2 = reason=fixture: no owner and no expiry
TXT

# ── cases 1-9: the defect fixture is reported ────────────────────────────────

out="$work/out-bad.txt"
status=0
run_audit "$fixture" "$work/allowlist-declared.txt" "$out" || status=$?
expect_exit "unpinned fixture is rejected" 1 "$status"

echo "  -- defect classes detected in the unpinned fixture --"

# 1: actions/checkout@v4
expect_contains "mutable action tag is reported" \
	"uses: actions/checkout is pinned to the mutable ref 'v4', not a commit SHA" "$out"

# 2: SHA with no `# vN` comment
expect_contains "SHA-pinned action with no tag comment is reported" \
	"uses: actions/setup-go is SHA-pinned but carries no" "$out"

# 3: same action at two SHAs
expect_contains "action pinned to two SHAs in one tree is reported" \
	"actions/cache is pinned to 2 different SHAs in one tree" "$out"

# 4: `uses:` with no @ref
expect_contains "uses: with no @ref is reported" \
	"uses: has no @ref" "$out"

# 5: tag-only image, and the digest-pinned sibling must NOT be reported
expect_contains "tag-only workflow service image is reported" \
	"workflow service image redis:7.2 is pinned to a mutable tag" "$out"
if grep -qF "1111111111111111111111111111111111111111111111111111111111111111" "$out"; then
	bad "an @sha256-pinned image was reported as unpinned"
else
	ok "an @sha256-pinned image is not reported"
fi
if grep -qF "mysql:8.0" "$out"; then
	bad "an @sha256-pinned compose image was reported as unpinned"
else
	ok "an @sha256-pinned compose image is not reported"
fi

# 6/7: tools
expect_contains "unpinned npx tool is reported" \
	'npx @stoplight/spectral-cli` uses a range or dist-tag' "$out"
if grep -qF "@1.34.2" "$out"; then
	bad "a version-pinned npx tool was reported as unpinned"
else
	ok "a version-pinned npx tool is not reported"
fi
expect_contains "unpinned go install is reported" \
	"has no @version; it fetches the tool at whatever the proxy serves then" "$out"
if grep -qF "govulncheck@v1.8.0" "$out"; then
	bad "a version-pinned go install was reported as unpinned"
else
	ok "a version-pinned go install is not reported"
fi

# 8: curl'd archive with no checksum
expect_contains "checksum-less release download is reported" \
	"curl fetches a release archive with no checksum or signature check" "$out"

echo
echo "  -- declared vs enforced --"

# case 10: declared image is advisory in default mode, fatal under --strict
out_declared="$work/out-declared.txt"
status=0
run_audit "$fixture" "$work/allowlist-declared.txt" "$out_declared" || status=$?
# Everything else in the fixture is still a violation, so the run must fail —
# but the DECLARED redis image must be absent from the violation list.
if grep -qE "^  x .*image redis:7\.2" "$out_declared"; then
	bad "a declared image was reported as a violation in default mode"
else
	ok "a declared image is not a violation in default mode"
fi
expect_contains "declared image is listed as a known gap" \
	"KNOWN GAPS" "$out_declared"

# A clean-tree check: same fixture, but every defect declared. Here we only need
# the strict-mode semantics, so use the full real allowlist shape is overkill —
# instead assert that a single declared entry becomes fatal under --strict by
# grepping the strict banner.
out_strict="$work/out-strict.txt"
status=0
run_audit "$fixture" "$work/allowlist-declared.txt" "$out_strict" --strict --quiet || status=$?
expect_exit "--strict fails on a declared mutable reference" 1 "$status"
expect_contains "--strict names the declared reference" \
	"FAIL (--strict): declared-but-still-mutable references remain" "$out_strict"

# case 11: lapsed expiry fails in BOTH modes
for mode in "" "--strict"; do
	# shellcheck disable=SC2086
	status=0
	run_audit "$fixture" "$work/allowlist-expired.txt" "$work/out-expired.txt" --quiet $mode || status=$?
	expect_exit "expired allowlist entry fails (mode '${mode:-default}')" 1 "$status"
	expect_contains "expired entry is named (mode '${mode:-default}')" \
		"declared gap expired 2000-01-01" "$work/out-expired.txt"
done

echo
echo "  -- the auditor refuses to run on an untrustworthy inventory --"

# case 12: the sentinel trap. A never-fires check is indistinguishable from a
# passing one; if the sentinel is present the inventory was fabricated.
out_sentinel="$work/out-sentinel.txt"
status=0
run_audit "$fixture" "$work/allowlist-sentinel.txt" "$out_sentinel" || status=$?
expect_exit "fabricated allowlist is refused" 2 "$status"
expect_contains "refusal names the sentinel" \
	"self-test sentinel key" "$out_sentinel"

# allowlist entries missing owner/expires are refused rather than defaulted
status=0
run_audit "$fixture" "$work/allowlist-missing-fields.txt" "$work/out-fields.txt" || status=$?
expect_exit "allowlist entry without owner/expires is refused" 2 "$status"

# case 13: a missing allowlist must never be read as "everything is declared"
status=0
run_audit "$fixture" "$work/does-not-exist.txt" "$work/out-noallow.txt" || status=$?
expect_exit "missing allowlist is refused" 2 "$status"
expect_contains "missing allowlist refusal explains why" \
	"refusing to guess" "$work/out-noallow.txt"

# no scan targets must also be refused, not reported as clean
mkdir -p "$work/empty-tree"
status=0
run_audit "$work/empty-tree" "$work/allowlist-declared.txt" "$work/out-empty.txt" || status=$?
expect_exit "a tree with no scan targets is refused, not passed" 2 "$status"

echo
echo "  -- the auditor passes on a correctly pinned tree --"

# The negative control. Without this, every assertion above is satisfied by a
# script that simply always fails.
good="$work/good-tree"
mkdir -p "$good/.github/workflows" "$good/test/env"
cat >"$good/.github/workflows/ci.yml" <<'YAML'
name: fixture
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7.2@sha256:1111111111111111111111111111111111111111111111111111111111111111
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
      - run: |
          go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
          npx --yes @redocly/cli@1.34.2 lint api/openapi/xflow-v1.yaml
YAML
cat >"$good/test/env/docker-compose.yml" <<'YAML'
services:
  redis:
    image: docker.io/library/redis:7.2@sha256:1111111111111111111111111111111111111111111111111111111111111111
YAML

out_good="$work/out-good.txt"
status=0
run_audit "$good" "$work/allowlist-declared.txt" "$out_good" || status=$?
expect_exit "a correctly pinned tree passes" 0 "$status"
expect_contains "the pass is stated explicitly" "-> PASS" "$out_good"
# The declared entry protects nothing here, which must be visible.
expect_contains "an allowlist entry that matches nothing is flagged" \
	"allowlist entries matching nothing in this tree" "$out_good"

status=0
run_audit "$good" "$work/allowlist-declared.txt" "$work/out-good-strict.txt" --strict || status=$?
expect_exit "--strict fails when a stale allowlist entry protects nothing" 1 "$status"

echo
echo "pin-audit-selftest: $pass passed, $fail failed"
if [ "$fail" -ne 0 ]; then
	exit 1
fi
exit 0
