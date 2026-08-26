package supplyenc

import (
	"errors"
	"strings"
	"testing"
)

// Three things in this package were configured and never read back.
//
// SupplyContentInfo is the HKDF info string that scopes the supply at-rest
// key. Its own comment says changing it makes every previously stored row
// unreadable — and nothing asserted the value. It has exactly one production
// reference (cmd/server/main.go's mk.Derive call), so a typo in it is not a
// failure, it is a silent, total, permanent loss of every encrypted supply row:
// the derived DEK changes, every stored envelope's GCM tag stops verifying, and
// there is no way back because the old string is gone.
//
// The envelope version check was likewise unexercised: no test anywhere ever
// constructed an envelope whose v field is not 1, so ErrUnsupportedVersion had
// no reachable path in the suite. It is the only safety net for a future
// incompatible format — without it an old build would parse a v2 envelope with
// v1 field semantics.
//
// IsEncrypted's five-byte prefix comparison had no counterexample that could
// tell the five bytes apart. The existing negative case is `{"rules":[]}`,
// whose third byte is 'r', so a sniff weakened to three bytes still answers
// correctly for it. That matters because AtRest.Open uses IsEncrypted as the
// ONLY thing deciding pass-through versus decrypt: a false positive turns
// legitimate plaintext into a hard "open stored content" error, which the
// supply gate reads as an unusable supply.

func TestSupplyContentInfoIsFrozen(t *testing.T) {
	// Compared against a literal, not against itself: the whole point is that
	// this exact byte string is baked into every stored row's key derivation.
	const want = "xflow-supply-content-v1"
	if SupplyContentInfo != want {
		t.Fatalf("SupplyContentInfo = %q, want %q: the at-rest DEK is derived "+
			"from this string, so changing it re-keys the store and makes every "+
			"supply row already written undecryptable, with no migration path "+
			"because the previous string is what would have been needed to read "+
			"them", SupplyContentInfo, want)
	}
	// The version suffix is the migration mechanism the comment describes: a
	// future format change adds -v2 rather than editing this in place, which is
	// what lets a reader hold both keys during a rollover.
	if !strings.HasSuffix(SupplyContentInfo, "-v1") {
		t.Fatalf("SupplyContentInfo = %q has no version suffix: there is then no "+
			"way to derive both the old and the new key during a rotation",
			SupplyContentInfo)
	}
}

func TestDecryptRejectsAnUnsupportedEnvelopeVersion(t *testing.T) {
	// The frozen envelope with only its version changed: every other field is
	// still valid, so this isolates the version check from the "looks nothing
	// like an envelope" path.
	future := strings.Replace(frozenEnvelope, `{"v":1,`, `{"v":2,`, 1)
	if future == frozenEnvelope {
		t.Fatal("failed to build a v2 envelope from the frozen fixture")
	}

	kr := NewKeyring(frozenKey())
	_, err := kr.Decrypt([]byte(future))
	if err == nil {
		t.Fatal("Decrypt() error = nil for a v2 envelope: a future incompatible " +
			"format was parsed with v1 field semantics, which is the one thing " +
			"the version field exists to prevent")
	}
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Decrypt() error = %v, want ErrUnsupportedVersion", err)
	}
	// Distinguishing the two sentinels is the load-bearing part. ErrNotEncrypted
	// means "this was never an envelope", and AtRest treats that class as a
	// shape problem; a version it does not understand is a real envelope it must
	// refuse, not a blob it should reconsider as plaintext.
	if errors.Is(err, ErrNotEncrypted) {
		t.Fatalf("Decrypt() reported ErrNotEncrypted for a well-formed envelope "+
			"with an unknown version: %v", err)
	}
}

func TestIsEncryptedNeedsEveryByteOfTheVersionKey(t *testing.T) {
	// Each negative differs from `{"v":` at exactly one position, so no single
	// byte of the comparison can be dropped without one of them flipping. All
	// are padded past the length floor so that check is not what rejects them.
	const pad = `,"padding":"aaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	for _, tc := range []struct {
		name string
		in   string
		want bool
	}{
		{"the frozen envelope", frozenEnvelope, true},
		{"first key is value", `{"value":1` + pad, false},
		{"first key is version", `{"version":1` + pad, false},
		{"key is two vs", `{"vv":1` + pad, false},
		{"no quote after brace", `{v":1` + pad, false},
		{"array not object", `[{"v":1}` + pad, false},
		{"leading whitespace is tolerated", "  \n\t" + frozenEnvelope, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEncrypted([]byte(tc.in)); got != tc.want {
				if tc.want {
					t.Fatalf("IsEncrypted() = false for %s: AtRest.Open returns "+
						"these ciphertext bytes to the caller unchanged, with a nil "+
						"error, and the guest is handed base64 as if it were rules",
						tc.name)
				}
				t.Fatalf("IsEncrypted() = true for %s: AtRest.Open sends plaintext "+
					"down the decrypt path, and the resulting parse failure is "+
					"reported as a damaged envelope rather than the readable "+
					"content it is", tc.name)
			}
		})
	}
}

func TestIsEncryptedRejectsInputTooShortToBeAnEnvelope(t *testing.T) {
	// A real envelope carries a base64 nonce, ciphertext and tag, so it cannot
	// be short. Without the floor, a tiny JSON document whose first key happens
	// to be "v" is sent to Decrypt and its content is refused.
	const tiny = `{"v":1}`
	if IsEncrypted([]byte(tiny)) {
		t.Fatalf("IsEncrypted(%q) = true: no envelope is that short, and treating "+
			"it as one turns a readable row into an open error", tiny)
	}
	if len(tiny) >= 20 {
		t.Fatalf("the fixture is %d bytes, at or above the length floor: it no "+
			"longer probes the floor", len(tiny))
	}
}

func TestOpenPassesThroughPlaintextWhoseFirstKeyStartsWithV(t *testing.T) {
	// The end-to-end consequence of the sniff. Rows written before encryption
	// was enabled are plaintext and must survive the upgrade; Open's own comment
	// says failing them would make every existing supply unreadable at the
	// moment of upgrade, and the supply gate would then decline every
	// activation, so no runner would host any trigger.
	a := NewAtRest(testDEK())
	plain := []byte(`{"value":"not encrypted","rules":["placeholder"]}`)

	got, err := a.Open(plain)
	if err != nil {
		t.Fatalf("Open() error = %v for plaintext whose first key starts with a "+
			"v: this row predates encryption and is now unreadable", err)
	}
	if string(got) != string(plain) {
		t.Fatalf("Open() = %q, want the input unchanged", got)
	}
}
