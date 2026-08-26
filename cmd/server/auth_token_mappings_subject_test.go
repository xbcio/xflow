package main

import "testing"

// TestLoadAuthTokenMappingsRejectsMissingSubject pins the r.Subject == ""
// half of the "each mapping requires token, subject, and namespace" guard in
// loadAuthTokenMappings. TestLoadAuthTokenMappingsRejectsMissingFields already
// covers a mapping missing namespace (token+subject present); it never
// exercises a mapping with token+namespace present but subject empty, so that
// half of the guard could be deleted without any test going red.
//
// An admitted mapping with an empty Subject would let a token bind to no
// operator identity at all: every audit row written under that principal
// records an empty "acted as" field, so the write cannot be attributed to
// anyone after the fact (the WHO in a WHO/WHAT/WHEN audit trail is gone).
func TestLoadAuthTokenMappingsRejectsMissingSubject(t *testing.T) {
	path := writeTokenFile(t, "tokens.json", `[{"token":"t","namespace":"ns"}]`, 0600)
	if _, err := loadAuthTokenMappings(serverConfig{authTokensFile: path}); err == nil {
		t.Fatal("loadAuthTokenMappings() error = nil, want error for missing subject")
	}
}
