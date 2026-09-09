package store

import (
	"testing"
	"time"
)

// TestRegistrationCodeIsExpired pins the predicate at its boundary, which is
// the one place a store contract test cannot reach: neither store exposes its
// clock through the RegistrationCodeStore interface, so a contract test can
// only assert the direction of the comparison ("an hour ago is expired"), not
// what happens at the exact instant a code comes due.
//
// The boundary must stay "not After" — expired AT the deadline, not one tick
// later — because IssuedIdentityAuthenticator makes the identical check for
// issued identities. If these two drifted, a fleet could observe a code and an
// identity minted with the same deadline dying at different moments, and the
// operator would have no way to tell which rule they were looking at.
func TestRegistrationCodeIsExpired(t *testing.T) {
	deadline := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		expiresAt time.Time
		now       time.Time
		want      bool
	}{
		{
			// The pre-feature shape. Every code minted before the server grew
			// a TTL carries this, and an upgrade must not kill them.
			name: "zero deadline never expires",
			now:  deadline.Add(100 * 365 * 24 * time.Hour),
			want: false,
		},
		{
			name:      "before the deadline",
			expiresAt: deadline,
			now:       deadline.Add(-time.Nanosecond),
			want:      false,
		},
		{
			name:      "exactly at the deadline",
			expiresAt: deadline,
			now:       deadline,
			want:      true,
		},
		{
			name:      "after the deadline",
			expiresAt: deadline,
			now:       deadline.Add(time.Nanosecond),
			want:      true,
		},
		{
			// A deadline read out of the database arrives in whatever location
			// the driver chose. Comparison must be by instant, not by wall
			// clock reading, or a server running in a non-UTC zone would
			// honor codes for hours past their deadline.
			name:      "same instant in another location",
			expiresAt: deadline.In(time.FixedZone("east", 8*3600)),
			now:       deadline,
			want:      true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := RegistrationCode{ExpiresAt: tc.expiresAt}
			if got := c.IsExpired(tc.now); got != tc.want {
				t.Fatalf("IsExpired(%v) with ExpiresAt=%v = %v, want %v",
					tc.now, tc.expiresAt, got, tc.want)
			}
		})
	}
}
