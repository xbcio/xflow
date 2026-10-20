package supplyenc

import (
	"encoding/json"
	"testing"
)

// EnvelopeKeyID is a diagnostic classifier, not a completion criterion, but
// its partition must still be exact: a row it labels with a kid is treated as
// an envelope by every caller, and a row it labels plaintext is passed through
// unchanged by Open. These cases pin the contract the storage layer relies on.
func TestEnvelopeKeyIDClassifiesStoredValues(t *testing.T) {
	tests := []struct {
		name      string
		stored    string
		wantKid   string
		wantEnvel bool
	}{
		{
			name:      "not an envelope",
			stored:    `{"rules":["legacy"]}`,
			wantEnvel: false,
		},
		{
			name:      "too short to be an envelope",
			stored:    `{"v":`,
			wantEnvel: false,
		},
		{
			name:      "looks like an envelope but is truncated",
			stored:    `{"v":1,"alg":"aes-256-gcm","kid":"abcd1234","data":`,
			wantEnvel: true,
		},
		{
			name:      "envelope without a kid",
			stored:    `{"v":1,"alg":"aes-256-gcm","data":"bm90LWEtcmVhbC1wYXlsb2Fk"}`,
			wantEnvel: true,
		},
		{
			name:      "frozen envelope",
			stored:    frozenEnvelope,
			wantKid:   frozenKeyID,
			wantEnvel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kid, isEnvelope := EnvelopeKeyID([]byte(tt.stored))
			if isEnvelope != tt.wantEnvel {
				t.Fatalf("EnvelopeKeyID(%q) isEnvelope = %v, want %v", tt.stored, isEnvelope, tt.wantEnvel)
			}
			if kid != tt.wantKid {
				t.Fatalf("EnvelopeKeyID(%q) kid = %q, want %q", tt.stored, kid, tt.wantKid)
			}
		})
	}
}

// The method exists so the storage layer can consume the classifier through an
// interface without importing this package. It must agree with the package
// function exactly, including the frozen format.
func TestAtRestEnvelopeKeyIDMatchesPackageFunction(t *testing.T) {
	a := NewAtRest([32]byte{})
	for _, stored := range []string{
		`{"rules":[]}`,
		`{"v":1,"alg":"aes-256-gcm","data":"bm90LWEtcmVhbC1wYXlsb2Fk"}`,
		frozenEnvelope,
	} {
		wantKid, wantEnvelope := EnvelopeKeyID([]byte(stored))
		gotKid, gotEnvelope := a.EnvelopeKeyID([]byte(stored))
		if gotKid != wantKid || gotEnvelope != wantEnvelope {
			t.Fatalf("AtRest.EnvelopeKeyID(%q) = (%q, %v), package function = (%q, %v)",
				stored, gotKid, gotEnvelope, wantKid, wantEnvelope)
		}
	}
}

// A row whose declared kid is the current one but whose ciphertext was sealed
// under another key is the case a kid census cannot see. EnvelopeKeyID reports
// the claimed kid (it is only a diagnostic); only Open can expose the lie.
func TestEnvelopeKeyIDReportsSelfDeclaredKid(t *testing.T) {
	otherKey := KeyFromBytes([32]byte{9, 9, 9})
	currentKey := KeyFromBytes([32]byte{1, 2, 3})
	sealed, err := Encrypt(otherKey, []byte(`{"row":"lying"}`))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Same-package test: use the envelope struct itself so the field order (and
	// therefore the `{"v":` prefix IsEncrypted sniffs) survives re-marshalling.
	var env envelope
	if err := json.Unmarshal(sealed, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	env.KeyID = currentKey.ID
	lying, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal lied envelope: %v", err)
	}

	kid, isEnvelope := EnvelopeKeyID(lying)
	if !isEnvelope || kid != currentKey.ID {
		t.Fatalf("EnvelopeKeyID(lying) = (%q, %v), want (%q, true)", kid, isEnvelope, currentKey.ID)
	}
	if _, err := NewAtRest([32]byte{1, 2, 3}).Open(lying); err == nil {
		t.Fatal("Open succeeded on an envelope whose kid lies about its ciphertext key")
	}
}
