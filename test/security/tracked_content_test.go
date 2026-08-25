package security

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// This file guards the two ways internal detail has actually escaped into this
// repository, both of which a reviewer reading a diff will miss.
//
// The first is a binary: a screenshot committed alongside unrelated code showed
// a person's name, a bookmarks folder named after the company, and most of an
// internal cloud console. Every text search run over this repository was blind
// to it, because it is not text. The second is an address: internal broker and
// database IPs sat quoted inside example output for weeks, surviving reviews
// that were looking for credentials and finding none, because an IP is not a
// credential -- what it leaks is topology.
//
// Both checks are deliberately allowlist-free. An allowlist is where a guard
// goes to die: entries outlive their reason, and the next real hit lands next
// to five stale exemptions and reads like a sixth. If a file legitimately needs
// to break one of these rules, that is worth a conversation and an explicit
// change here, not a line in a table.

// documentationIPPrefix is the one private range this repository uses on
// purpose. RFC 5737 reserves 192.0.2.0/24 and friends for documentation, but
// this repository's fixtures need addresses that *look* like the private ones
// they stand in for -- a DSN test asserting "this host is not local" is
// pointless against a public documentation address. 10.0.0.0/24 is the
// convention here: close enough to be a realistic stand-in, far enough from any
// real allocation to be obviously fake.
const documentationIPPrefix = "10.0.0."

// privateIPPattern matches RFC 1918 addresses, and only complete four-octet
// ones. Three-octet matches would swamp the result with version numbers --
// "pnpm@10.10.0", "node: >=10.13.0" -- and a check that reports mostly noise is
// a check people stop reading.
var privateIPPattern = regexp.MustCompile(
	`\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}` +
		`|192\.168\.\d{1,3}\.\d{1,3}` +
		`|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b`,
)

// selfPath is skipped by the content checks: this file states the patterns it
// searches for, so it necessarily contains matches for them.
const selfPath = "test/security/tracked_content_test.go"

// cidrSuffix matches the "/24" that turns an address into a range.
//
// A range and a host are different disclosures. "sudo ufw allow from
// 192.168.1.0/24" names nobody's machine -- it is how everyone writes "the
// local network" -- whereas 10.20.30.41 is somewhere a packet can arrive. Only
// the second is worth failing a build over.
//
// The cost of this exemption is worth stating plainly rather than discovering
// later: an internal subnet written as 10.42.0.0/24 also goes unreported, and
// that does describe the network. The judgement is that subnet declarations are
// both rarer and less actionable than host addresses, not that they are safe.
var cidrSuffix = regexp.MustCompile(`^/\d{1,2}\b`)

// repoRoot returns the working tree root, or skips when git is unavailable --
// as it is in a module downloaded as a dependency, where there is no tree to
// scan and nothing this test could say.
func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("not a git working tree, nothing to scan: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// trackedFiles lists what is actually in the index. This is the whole point of
// shelling out to git rather than walking the filesystem: .gitignore does not
// protect a file that was committed before the rule existed, so "what is
// ignored" and "what is published" are different questions. Only the index
// answers the second one.
func trackedFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "-z")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var paths []string
	for _, p := range strings.Split(string(out), "\x00") {
		if p != "" {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		t.Fatal("git ls-files returned nothing; the scan would pass vacuously")
	}
	return paths
}

// reportableIPs returns the addresses in content that this guard considers a
// disclosure, after both exemptions.
//
// The scan and its self-test both go through here on purpose. A self-test that
// exercised privateIPPattern directly would be checking something the scan does
// not use, and would keep passing while the exemptions above it silently
// swallowed every real hit.
func reportableIPs(content string) []string {
	var out []string
	for _, loc := range privateIPPattern.FindAllStringIndex(content, -1) {
		hit := content[loc[0]:loc[1]]
		if strings.HasPrefix(hit, documentationIPPrefix) {
			continue
		}
		if cidrSuffix.MatchString(content[loc[1]:]) {
			continue
		}
		out = append(out, hit)
	}
	return out
}

// TestTrackedFilesAreText fails on any tracked file containing a NUL byte.
//
// The rule is coarse on purpose. It does not try to decide whether a particular
// image is safe, because that judgement is exactly what failed last time: the
// screenshots were added as UI references, five of them genuinely were, and the
// sixth was a whole desktop. Reviewing image contents is not something a diff
// affords, so the rule is that binaries do not land here without someone
// deciding to change this test.
func TestTrackedFilesAreText(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range trackedFiles(t, root) {
		body, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			// A tracked path can be absent from the working tree during a
			// partial checkout; that is not this test's business.
			continue
		}
		if i := strings.IndexByte(string(body), 0); i >= 0 {
			t.Errorf("%s is binary (NUL byte at offset %d).\n"+
				"Binary files cannot be reviewed in a diff, and an image can carry far more "+
				"than the code around it. If this file must be tracked, say why here.", rel, i)
		}
	}
}

// TestTrackedFilesHaveNoInternalIPs fails on RFC 1918 addresses outside the
// documentation range.
//
// It reports the file and the offending address but never the surrounding line:
// a check that prints context is a check whose failure output has to be handled
// carefully, and the path is enough to find it.
func TestTrackedFilesHaveNoInternalIPs(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range trackedFiles(t, root) {
		if rel == selfPath {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			continue
		}
		for _, hit := range reportableIPs(string(body)) {
			t.Errorf("%s contains the private address %s.\n"+
				"Use a %sx address for fixtures and examples. Real internal addresses "+
				"describe the network's shape even when no credential is present.",
				rel, hit, documentationIPPrefix)
		}
	}
}

// TestScanPatternsCanFail is the reason to trust the two tests above.
//
// Both of them pass by finding nothing, and a search that cannot match anything
// also finds nothing. That is not hypothetical here: the sweep this file grew
// out of first came back clean against a tree that provably contained internal
// addresses, because the pattern used \b under POSIX ERE, where it is not a
// word boundary, and matched zero bytes everywhere. The empty result looked
// exactly like success.
//
// So the patterns are exercised against inputs whose verdict is known. If a
// future edit breaks a pattern, this fails immediately rather than quietly
// turning the guard into a formality.
func TestScanPatternsCanFail(t *testing.T) {
	mustMatch := []string{
		"broker at 10.20.30.40:9092",
		`"root:pw@tcp(10.20.30.41:3306)/app"`,
		"192.168.99.99",
		"172.31.255.5",
		"10.20.30.41/api/v1/health", // a path, not a prefix length
	}
	for _, s := range mustMatch {
		if len(reportableIPs(s)) == 0 {
			t.Errorf("nothing reportable in %q; the guard is inert", s)
		}
	}

	mustNotMatch := []string{
		"pnpm@10.10.0",                     // version, three octets
		"node: '>=10.13.0'",                // version inside a constraint
		"engines: {node: ^10.13.0 || ^12}", // several versions on one line
		"127.0.0.1:6379",                   // loopback is not RFC 1918
		"10.0.0.9",                         // documentation range
		"8.8.8.8",                          // public
		"sudo ufw allow from 192.168.1.0/24 to any port 8080", // a range
		"sudo ufw allow from 10.0.0.0/8",                      // a range
		"172.16.0.0/12",                                       // a range
	}
	for _, s := range mustNotMatch {
		if hits := reportableIPs(s); len(hits) != 0 {
			t.Errorf("reported %q in %q; noise this loud gets ignored", hits, s)
		}
	}

	// The binary check has one degree of freedom worth pinning: NUL is what
	// separates it from text, and PNG puts one in its eight-byte signature.
	if !strings.Contains("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR", "\x00") {
		t.Error("a PNG header no longer contains a NUL byte; TestTrackedFilesAreText would miss images")
	}
}
