package protocol

import "testing"

// TestLinkedXflowVersionReportsUnknownInThisRepository pins the answer a binary
// built from xflow itself gives.
//
// The test binary for this package has xflow as its MAIN module, so there is no
// dependency entry to read and the main module's version is not a release. That
// is the same shape a runner built against a filesystem replacement has, and
// both must answer "" rather than a string a control plane could compare: the
// entire value of the version label is that agreement means the two sides link
// the same release, so a working tree asserting a release tag would manufacture
// exactly the false agreement the label exists to prevent.
func TestLinkedXflowVersionReportsUnknownInThisRepository(t *testing.T) {
	if got := LinkedXflowVersion(); got != "" {
		t.Fatalf("LinkedXflowVersion() = %q, want \"\" for a binary built from this repository: "+
			"a working tree cannot name the release it is", got)
	}
}

// TestComparableModuleVersionAcceptsOnlyReleases pins which strings two
// deployments are allowed to compare as versions.
func TestComparableModuleVersionAcceptsOnlyReleases(t *testing.T) {
	cases := []struct {
		name    string
		version string
		want    string
	}{
		{"release tag", "v0.0.31", "v0.0.31"},
		{"pseudo-version", "v0.0.31-0.20260924120000-a0b2ebe1234", "v0.0.31-0.20260924120000-a0b2ebe1234"},
		{"surrounding space is not part of the version", "  v1.2.3\n", "v1.2.3"},
		{"devel names a working tree", "(devel)", ""},
		{"empty names a filesystem replacement", "", ""},
		{"a bare semver is not a module version", "0.0.31", ""},
		{"anything else is not a version", "unknown", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := comparableModuleVersion(tc.version); got != tc.want {
				t.Fatalf("comparableModuleVersion(%q) = %q, want %q", tc.version, got, tc.want)
			}
		})
	}
}
