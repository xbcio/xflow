package apiserver

import (
	"net/http"
	"testing"
)

// The duplicate-token guard in NewBearerPrincipalAuthMulti (authz.go:288-292)
// was documented and never executed. grep -rn "duplicate auth token" over the
// repo returns exactly one hit — the panic string's own definition. Thirteen
// files call NewBearerPrincipalAuthMulti and every one of them passes
// pairwise-distinct token strings, so deleting the guard leaves the whole
// suite green.
//
// The registry is built once at boot from an operator-supplied file
// (cmd/server/main.go:401, no recover around it — the panic is a deliberate
// fail-fast). Without the guard, two mappings that accidentally share a token
// value — a copy-paste in the tokens file, a rotation script that fails to
// regenerate one tenant's secret, a secret reused across environments — do not
// fail to start. The map simply keeps whichever mapping came last, and the
// earlier principal's subject, namespace and scopes are discarded with no
// error and no log line. Every holder of that token then authenticates as the
// last-listed principal: wrong namespace, wrong scopes, and the audit rows
// name the wrong subject.

func TestNewBearerPrincipalAuthMultiRejectsDuplicateToken(t *testing.T) {
	const shared = "PLACEHOLDER-SHARED-TOKEN"

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewBearerPrincipalAuthMulti accepted two mappings that share " +
				"a token: the second silently shadows the first, so the token's " +
				"holder authenticates with the other principal's namespace and scopes")
		}
	}()

	NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: shared, Subject: "tenant-a", Namespace: "ns-a", Scopes: []string{"workflow"}},
		{Token: shared, Subject: "tenant-b", Namespace: "ns-b", Scopes: []string{"workflow", "management.read"}},
	})
}

// TestNewBearerPrincipalAuthMultiKeepsDistinctTokensApart is the positive
// control: without it, a constructor that panics unconditionally satisfies the
// test above and the server can never boot with a tokens file at all.
//
// It also pins the binding itself — each token must resolve to ITS OWN
// subject, namespace and scopes, which is the property the duplicate guard
// exists to protect.
func TestNewBearerPrincipalAuthMultiKeepsDistinctTokensApart(t *testing.T) {
	auth := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "PLACEHOLDER-TOKEN-A", Subject: "tenant-a", Namespace: "ns-a", Scopes: []string{"workflow"}},
		{Token: "PLACEHOLDER-TOKEN-B", Subject: "tenant-b", Namespace: "ns-b", Scopes: []string{"workflow", "management.read"}},
	})

	for _, tc := range []struct {
		token       string
		subject     string
		namespaceID string
		scopes      []string
	}{
		{"PLACEHOLDER-TOKEN-A", "tenant-a", "ns-a", []string{"workflow"}},
		{"PLACEHOLDER-TOKEN-B", "tenant-b", "ns-b", []string{"workflow", "management.read"}},
	} {
		req, err := http.NewRequest(http.MethodGet, "http://example.invalid/v1/workflows", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tc.token)

		p, err := auth.Authenticate(req)
		if err != nil {
			t.Fatalf("Authenticate(%s) error = %v", tc.subject, err)
		}
		if p.Subject != tc.subject {
			t.Errorf("subject = %q, want %q: the token resolved to another "+
				"tenant's principal", p.Subject, tc.subject)
		}
		if p.Namespace != tc.namespaceID {
			t.Errorf("namespace = %q, want %q: every namespace-scoped store read "+
				"and audit row for this caller lands in the wrong tenant",
				p.Namespace, tc.namespaceID)
		}
		if len(p.Scopes) != len(tc.scopes) {
			t.Errorf("scopes = %v, want %v", p.Scopes, tc.scopes)
			continue
		}
		for i := range tc.scopes {
			if p.Scopes[i] != tc.scopes[i] {
				t.Errorf("scopes = %v, want %v", p.Scopes, tc.scopes)
				break
			}
		}
	}
}
