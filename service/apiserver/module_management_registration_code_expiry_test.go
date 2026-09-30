package apiserver

import (
	"math"
	"strings"
	"testing"
	"time"
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
