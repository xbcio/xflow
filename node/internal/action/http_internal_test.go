package action

import (
	"net/http"
	"testing"
)

// applyHTTPAuth is the only code path in the repo that attaches credentials to
// an outbound HTTP request, and no test executed it. http_test.go's
// TestHTTP_Factory only checks that the builder stored
// params["authentication"] == "my_cred"; it never issues a request or looks at
// an outgoing header. test/integration/action_parity_http_test.go mentions auth
// nowhere. So every scheme below could be wrong — wrong prefix, swapped
// username and password, wrong header name — and the suite stays green while
// every workflow that authenticates to a third-party API gets rejected by it.
//
// The credential values here are obvious placeholders, not real secrets.

func TestApplyHTTPAuthBearerUsesTheBearerScheme(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyHTTPAuth(req, map[string]any{"type": "bearer", "token": "PLACEHOLDER-TOKEN"})

	if got := req.Header.Get("Authorization"); got != "Bearer PLACEHOLDER-TOKEN" {
		t.Fatalf("Authorization = %q, want %q: the scheme prefix is not "+
			"decoration, a server rejects a bare token",
			got, "Bearer PLACEHOLDER-TOKEN")
	}
}

// An empty token must leave the header unset rather than send "Bearer ".
// A literal "Bearer " reads to the server as a malformed credential; no header
// at all is the honest signal that none was configured.
func TestApplyHTTPAuthBearerWithoutTokenSetsNoHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyHTTPAuth(req, map[string]any{"type": "bearer", "token": ""})

	if _, ok := req.Header["Authorization"]; ok {
		t.Fatalf("Authorization = %q, want the header to be absent",
			req.Header.Get("Authorization"))
	}
}

// The username and password are both plain strings in the same map, so swapping
// the two cast.ToString calls compiles and changes nothing that any other test
// can see. It sends the password as the username — which, on a server that logs
// failed usernames, writes the password into that server's logs.
func TestApplyHTTPAuthBasicDoesNotSwapUserAndPassword(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyHTTPAuth(req, map[string]any{
		"type":     "basic",
		"username": "placeholder-user",
		"password": "PLACEHOLDER-PASS",
	})

	user, pass, ok := req.BasicAuth()
	if !ok {
		t.Fatalf("BasicAuth() not set; Authorization = %q", req.Header.Get("Authorization"))
	}
	if user != "placeholder-user" || pass != "PLACEHOLDER-PASS" {
		t.Fatalf("BasicAuth() = %q/%q, want placeholder-user/PLACEHOLDER-PASS",
			user, pass)
	}
}

func TestApplyHTTPAuthAPIKeyHeaderAndDefault(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyHTTPAuth(req, map[string]any{
		"type": "api_key", "header": "X-Custom-Key", "value": "PLACEHOLDER-KEY",
	})
	if got := req.Header.Get("X-Custom-Key"); got != "PLACEHOLDER-KEY" {
		t.Fatalf("X-Custom-Key = %q, want PLACEHOLDER-KEY", got)
	}
	// The credential must not also leak into the standard slot under a name the
	// caller did not ask for.
	if got := req.Header.Get("Authorization"); got != "" {
		t.Fatalf("Authorization = %q, want empty for an api_key credential", got)
	}

	// Omitting the header name falls back to X-API-Key (http.go:406-408). No
	// fixture exercised the fallback, so it could name any header at all.
	def, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyHTTPAuth(def, map[string]any{"type": "api_key", "value": "PLACEHOLDER-KEY"})
	if got := def.Header.Get("X-API-Key"); got != "PLACEHOLDER-KEY" {
		t.Fatalf("default api-key header = %q, want it on X-API-Key", got)
	}
}

// A nil credential map and an unrecognized type must both leave the request
// untouched, rather than sending a partially-formed credential.
func TestApplyHTTPAuthNilAndUnknownTypeAddNoHeaders(t *testing.T) {
	for name, cred := range map[string]map[string]any{
		"nil":     nil,
		"unknown": {"type": "oauth2", "token": "PLACEHOLDER-TOKEN"},
	} {
		req, err := http.NewRequest(http.MethodGet, "http://example.invalid/", nil)
		if err != nil {
			t.Fatal(err)
		}
		before := len(req.Header)
		applyHTTPAuth(req, cred)
		if len(req.Header) != before {
			t.Fatalf("%s credential added headers %#v", name, req.Header)
		}
	}
}
