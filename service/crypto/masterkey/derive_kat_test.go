package masterkey

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// The two existing Derive tests are self-referential: one checks that two info
// strings disagree, the other that one info string agrees with itself. Both are
// satisfied by SHA256(masterKey || info), which is a plain Merkle-Damgard hash
// and therefore length-extendable -- given one leaked derived key (the supply
// transport key travels to every runner, the master key does not), an attacker
// can compute a derived key for a purpose they never saw. That is exactly the
// cross-purpose compromise the package doc says Derive prevents, and no
// assertion anywhere in the repo would have noticed the substitution.
//
// Pinning the algorithm needs an oracle that is not the implementation under
// test. hkdfSHA256Reference is HKDF written out from RFC 5869 section 2 over
// crypto/hmac, independent of crypto/hkdf; TestReferenceHKDFMatchesRFC5869
// below validates the reference itself against the RFC's published vectors, so
// a mistake in the reference shows up as its own failure rather than as a
// wrongly-passing Derive test.

// hkdfSHA256Reference implements RFC 5869 HKDF-SHA256 (Extract then Expand).
func hkdfSHA256Reference(ikm, salt, info []byte, n int) []byte {
	if len(salt) == 0 {
		// RFC 5869 2.2: absent salt is HashLen zero bytes.
		salt = make([]byte, sha256.Size)
	}
	ext := hmac.New(sha256.New, salt)
	ext.Write(ikm)
	prk := ext.Sum(nil)

	var out, block []byte
	for counter := byte(1); len(out) < n; counter++ {
		exp := hmac.New(sha256.New, prk)
		exp.Write(block)
		exp.Write(info)
		exp.Write([]byte{counter})
		block = exp.Sum(nil)
		out = append(out, block...)
	}
	return out[:n]
}

// TestReferenceHKDFMatchesRFC5869 is the reference implementation's own test.
// Without it, an error in hkdfSHA256Reference would make TestDeriveIsHKDFSHA256
// fail for the wrong reason -- or, if the same error existed in both, pass for
// the wrong reason.
func TestReferenceHKDFMatchesRFC5869(t *testing.T) {
	cases := []struct {
		name            string
		ikm, salt, info string
		n               int
		want            string
	}{
		{
			// RFC 5869 Appendix A.1 -- Test Case 1 (SHA-256, basic).
			name: "A.1",
			ikm:  "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b",
			salt: "000102030405060708090a0b0c",
			info: "f0f1f2f3f4f5f6f7f8f9",
			n:    42,
			want: "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865",
		},
		{
			// RFC 5869 Appendix A.3 -- Test Case 3 (SHA-256, zero-length salt
			// and info). This is the shape Derive uses: no salt.
			name: "A.3",
			ikm:  "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b",
			salt: "",
			info: "",
			n:    42,
			want: "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hkdfSHA256Reference(mustHex(t, tc.ikm), mustHex(t, tc.salt), mustHex(t, tc.info), tc.n)
			if want := mustHex(t, tc.want); !bytes.Equal(got, want) {
				t.Fatalf("reference HKDF-SHA256 = %x, want %x", got, want)
			}
		})
	}
}

// TestDeriveIsHKDFSHA256 pins Derive to HKDF-SHA256 with an absent salt and the
// info string passed through verbatim. Any of these break it: swapping HKDF for
// a bare hash, changing the hash, introducing a salt, wrapping or truncating
// the info string, or returning a different slice of the output.
func TestDeriveIsHKDFSHA256(t *testing.T) {
	k := mustLoad(t, validKeyB64())
	for _, info := range []string{
		"",
		"supply.transport",
		"xflow-supply-content-v1",
		"xflow-artifact-v1",
	} {
		got := k.Derive(info)
		want := hkdfSHA256Reference(k.raw[:], nil, []byte(info), 32)
		if !bytes.Equal(got[:], want) {
			t.Errorf("Derive(%q) = %x, want HKDF-SHA256(ikm=masterKey, salt=nil, info=%q) = %x",
				info, got, info, want)
		}
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return b
}
