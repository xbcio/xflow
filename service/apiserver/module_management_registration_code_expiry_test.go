package apiserver

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
)

// TestResolveRegistrationCodeExpiry walks the whole truth table of
// (deployment ceiling x requested lifetime). The two axes are independent and
// each has a default that must stay passive, so the interesting cases are all
// at their intersections — a per-branch test would miss exactly the pairs that
// matter.
func TestResolveRegistrationCodeExpiry(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	secs := func(v int64) *int64 { return &v }

	cases := []struct {
		name      string
		ceiling   time.Duration
		requested *int64
		want      time.Time
		wantErr   bool
	}{
		{
			// The plain-upgrade case: new binary, no new flag, no request
			// field. Nothing may change, or an operator's first mint after
			// upgrading quietly acquires a deadline they never asked for.
			name: "no ceiling, no request -> never expires",
			want: time.Time{},
		},
		{
			name:      "no ceiling, explicit 0 -> never expires",
			requested: secs(0),
			want:      time.Time{},
		},
		{
			// Without a ceiling the request is the only authority, so a
			// caller may still bound its own code. Expiry is available to
			// careful operators before the deployment mandates it.
			name:      "no ceiling, positive request -> honored",
			requested: secs(3600),
			want:      now.Add(time.Hour),
		},
		{
			name:    "ceiling, no request -> ceiling is the default",
			ceiling: 24 * time.Hour,
			want:    now.Add(24 * time.Hour),
		},
		{
			// Refused, not reinterpreted as the ceiling. 0 means "never
			// expires"; the deployment has declared no such code may exist,
			// and rewriting the request would hide that the caller asked for
			// something the server will not do.
			name:      "ceiling, explicit 0 -> refused",
			ceiling:   24 * time.Hour,
			requested: secs(0),
			wantErr:   true,
		},
		{
			name:      "ceiling, request within it -> honored",
			ceiling:   24 * time.Hour,
			requested: secs(3600),
			want:      now.Add(time.Hour),
		},
		{
			// The boundary is inclusive: asking for exactly the ceiling is
			// the most common scripted request there is, and refusing it
			// would make the documented cap unusable at its own value.
			name:      "ceiling, request exactly at it -> honored",
			ceiling:   24 * time.Hour,
			requested: secs(86400),
			want:      now.Add(24 * time.Hour),
		},
		{
			// Refused rather than clamped down. A caller that asked for 90
			// days and silently got 24 hours has a code that dies two months
			// before it expects to, with nothing in the response saying so.
			name:      "ceiling, request over it -> refused",
			ceiling:   24 * time.Hour,
			requested: secs(86401),
			wantErr:   true,
		},
		{
			name:      "negative request -> refused",
			requested: secs(-1),
			wantErr:   true,
		},
		{
			// Without the overflow guard this multiplication wraps negative
			// and mints a code that is already expired — which reads to the
			// caller as "the server ignored my request".
			name:      "request beyond time.Duration's range -> refused",
			requested: secs(math.MaxInt64),
			wantErr:   true,
		},
		{
			name:      "request beyond time.Duration's range, under a ceiling -> refused",
			ceiling:   24 * time.Hour,
			requested: secs(math.MaxInt64),
			wantErr:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveRegistrationCodeExpiry(now, tc.ceiling, tc.requested)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("err = nil, want an error; got expiry %v", got)
				}
				if !got.IsZero() {
					t.Fatalf("returned %v alongside an error; the caller must not persist it", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("expiry = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolveRegistrationCodeExpiryErrorsCarryNoSecret guards the repo rule
// that error text reaching a caller carries no caller-supplied secret. These
// errors are returned verbatim in a 400 body, unlike the enroll path's
// deliberately opaque single verdict — the difference is that a lifetime
// request is not a credential. The requested number and the server's own cap
// are exactly what the caller needs to correct the request; nothing else may
// appear.
func TestResolveRegistrationCodeExpiryErrorsCarryNoSecret(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	over := int64(999999)
	_, err := resolveRegistrationCodeExpiry(now, time.Hour, &over)
	if err == nil {
		t.Fatal("expected a refusal")
	}
	msg := err.Error()
	if strings.Contains(msg, now.Format(time.RFC3339)) || strings.Contains(msg, "2026") {
		t.Fatalf("error leaks the server clock: %q", msg)
	}
	if !strings.Contains(msg, "999999") || !strings.Contains(msg, "1h") {
		t.Fatalf("error omits the two facts a caller needs to fix the request: %q", msg)
	}
}

// newRegistrationCodeTTLServer is newRegistrationCodeTestServer with a
// deployment ceiling and, optionally, the _global scopes. Both variants are
// needed by the same tests below, and the point of the _global variant is
// that it must behave identically.
func newRegistrationCodeTTLServer(t *testing.T, ttl time.Duration, global bool) *registrationCodeTestServer {
	t.Helper()
	codes := control.NewMemoryRegistrationCodeStore()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.codes = codes
	m.registrationCodeTTL = ttl
	scopes := []string{
		"management.registration_code.create",
		"management.registration_code.list",
		"management.registration_code.revoke",
		"management.registration_code.audit",
	}
	if global {
		scopes = append(scopes,
			"management.registration_code.create_global",
			"management.registration_code.list_global",
			"management.registration_code.revoke_global",
			"management.registration_code.audit_global",
		)
	}
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "ops", Namespace: "namespaceA", Scopes: scopes,
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return &registrationCodeTestServer{srv: httptest.NewServer(mux), codes: codes}
}

// listExpiresAt creates a code with the given request body and returns the
// expires_at the LIST endpoint reports for it. Reading the value back through
// list rather than the store is deliberate: it is the only view an operator
// has, and a deadline that is persisted but not surfaced is a deadline they
// cannot plan around.
func listExpiresAt(t *testing.T, h *registrationCodeTestServer, body string) string {
	t.Helper()
	h.doJSON(t, http.MethodPost, "/v1/management/registration-codes", body, http.StatusOK)
	raw := h.doJSON(t, http.MethodGet, "/v1/management/registration-codes", "", http.StatusOK)
	var listed struct {
		Data []struct {
			ExpiresAt string `json:"expires_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &listed); err != nil {
		t.Fatalf("decode list: %v; body = %q", err, raw)
	}
	if len(listed.Data) != 1 {
		t.Fatalf("list len = %d, want 1; body = %q", len(listed.Data), raw)
	}
	return listed.Data[0].ExpiresAt
}

func TestCreateRegistrationCodeAppliesDeploymentCeiling(t *testing.T) {
	body := `{"allowed_namespaces":["namespaceA"]}`

	t.Run("no ceiling leaves the code without a deadline", func(t *testing.T) {
		h := newRegistrationCodeTTLServer(t, 0, false)
		if got := listExpiresAt(t, h, body); got != "" {
			t.Fatalf("expires_at = %q, want absent", got)
		}
	})

	t.Run("ceiling becomes the default deadline", func(t *testing.T) {
		h := newRegistrationCodeTTLServer(t, time.Hour, false)
		got := listExpiresAt(t, h, body)
		if got == "" {
			t.Fatal("expires_at absent; the ceiling was not applied")
		}
		parsed, err := time.Parse(time.RFC3339, got)
		if err != nil {
			t.Fatalf("expires_at %q is not RFC3339: %v", got, err)
		}
		// A minute of slack: the handler stamps now+ceiling from its own
		// clock, which is not the test's.
		if d := time.Until(parsed); d < 59*time.Minute || d > time.Hour+time.Minute {
			t.Fatalf("expires_at is %v out, want ~1h", d)
		}
	})

	t.Run("a shorter request is honored", func(t *testing.T) {
		h := newRegistrationCodeTTLServer(t, time.Hour, false)
		got := listExpiresAt(t, h,
			`{"allowed_namespaces":["namespaceA"],"expires_in_seconds":60}`)
		parsed, err := time.Parse(time.RFC3339, got)
		if err != nil {
			t.Fatalf("expires_at %q is not RFC3339: %v", got, err)
		}
		if d := time.Until(parsed); d > 2*time.Minute {
			t.Fatalf("expires_at is %v out, want ~1m; the request was ignored", d)
		}
	})

	t.Run("a longer request is refused with 400", func(t *testing.T) {
		h := newRegistrationCodeTTLServer(t, time.Hour, false)
		h.doJSON(t, http.MethodPost, "/v1/management/registration-codes",
			`{"allowed_namespaces":["namespaceA"],"expires_in_seconds":7200}`,
			http.StatusBadRequest)
		// The refusal must be total: a code the caller was told it could not
		// have must not exist afterwards.
		raw := h.doJSON(t, http.MethodGet, "/v1/management/registration-codes", "", http.StatusOK)
		if !strings.Contains(raw, `"data":[]`) && !strings.Contains(raw, `"data":null`) {
			t.Fatalf("a refused create still minted a code: %q", raw)
		}
	})

	t.Run("an explicit never-expires request is refused with 400", func(t *testing.T) {
		h := newRegistrationCodeTTLServer(t, time.Hour, false)
		h.doJSON(t, http.MethodPost, "/v1/management/registration-codes",
			`{"allowed_namespaces":["namespaceA"],"expires_in_seconds":0}`,
			http.StatusBadRequest)
	})
}

// TestCreateRegistrationCodeCeilingBindsGlobalPrincipals is the security
// property the whole ceiling rests on. If a create_global holder could opt out
// of the cap, the cap would be an authorization boundary rather than a
// deployment one — and "grant this subject create_global" would quietly become
// "grant this subject permanent registration codes". Widening the cap must
// stay an act on the host, visible in the process's flags.
func TestCreateRegistrationCodeCeilingBindsGlobalPrincipals(t *testing.T) {
	h := newRegistrationCodeTTLServer(t, time.Hour, true)
	// _global's actual privilege — naming a namespace outside its own — still
	// works, so this test fails for the right reason: the ceiling, not a
	// scope rejection.
	h.doJSON(t, http.MethodPost, "/v1/management/registration-codes",
		`{"allowed_namespaces":["namespaceB"],"expires_in_seconds":60}`,
		http.StatusOK)
	h.doJSON(t, http.MethodPost, "/v1/management/registration-codes",
		`{"allowed_namespaces":["namespaceB"],"expires_in_seconds":7200}`,
		http.StatusBadRequest)
	h.doJSON(t, http.MethodPost, "/v1/management/registration-codes",
		`{"allowed_namespaces":["namespaceB"],"expires_in_seconds":0}`,
		http.StatusBadRequest)
}

// TestEnrollRejectsExpiredCodeWithoutSayingWhy holds the no-existence-oracle
// line across the new lifecycle state. An expired code must be as
// indistinguishable from an unknown one as a revoked code already is — the
// server's own reason survives only in the audit trail.
func TestEnrollRejectsExpiredCodeWithoutSayingWhy(t *testing.T) {
	codes := control.NewMemoryRegistrationCodeStore()
	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(t.Context(), control.RegistrationCode{
		ID: id, CodeHash: control.HashSecret(plaintext),
		AllowedNamespaces: []string{"namespaceA"}, AllowedNodeTypes: []string{"*"},
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
		ExpiresAt: time.Now().UTC().Add(-time.Hour),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, expiredErr := codes.ResolveByPlaintext(t.Context(), plaintext)
	if !errors.Is(expiredErr, control.ErrRegistrationCodeExpired) {
		t.Fatalf("expired code resolved to %v, want ErrRegistrationCodeExpired", expiredErr)
	}
	_, unknownErr := codes.ResolveByPlaintext(t.Context(), "not-a-code")
	if !errors.Is(unknownErr, control.ErrRegistrationCodeUnknown) {
		t.Fatalf("unknown code resolved to %v, want ErrRegistrationCodeUnknown", unknownErr)
	}
	// The two are distinct HERE, inside the server, so the audit trail can
	// tell an operator which one happened. They must stop being distinct at
	// the enroll face; that collapse is Core.Enroll's job and is asserted in
	// service/control. What this guards is the store-level precondition: the
	// expired verdict is its own sentinel and was not folded into "unknown"
	// as a shortcut, which would have destroyed the audit distinction.
	if errors.Is(expiredErr, control.ErrRegistrationCodeUnknown) {
		t.Fatal("expired collapsed into unknown; the audit trail can no longer distinguish them")
	}
}

// TestNewInjectsRegistrationCodeTTLIntoManagementModule closes the last hop of
// the wiring chain. Every other test in this file sets
// managementModule.registrationCodeTTL by hand, so severing New's assignment
// would leave all of them green while every real deployment silently lost its
// ceiling — the exact failure the field-injection shape (rather than a
// constructor argument) makes possible.
func TestNewInjectsRegistrationCodeTTLIntoManagementModule(t *testing.T) {
	const ttl = 36 * time.Hour
	srv, err := New(Config{
		Concurrency:         1,
		RegistrationCodes:   control.NewMemoryRegistrationCodeStore(),
		IssuedIdentities:    control.NewMemoryIssuedIdentityStore(),
		RegistrationCodeTTL: ttl,
	}, WithManagement())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var found *managementModule
	for _, m := range srv.modules {
		if mm, ok := m.(*managementModule); ok {
			found = mm
			break
		}
	}
	if found == nil {
		t.Fatal("no managementModule in the server's modules")
	}
	if found.registrationCodeTTL != ttl {
		t.Fatalf("registrationCodeTTL = %v, want %v; New dropped Config.RegistrationCodeTTL",
			found.registrationCodeTTL, ttl)
	}
}
