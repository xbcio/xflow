package supplyenc

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"
)

// The $enc envelope is a wire format in two senses, and neither of them was
// pinned to anything but the code that produces it.
//
//   - It crosses a version boundary between two independently deployed
//     binaries: the control plane seals (service/control/supply_encryption.go:99)
//     and the runner opens, so during any rolling upgrade an old reader meets a
//     new writer.
//   - It is also a storage format. AtRest.Seal writes this exact JSON into the
//     supply content column, so every row already on disk was written by some
//     earlier build of this file.
//
// Every existing test round-trips Encrypt through Decrypt, which moves both
// sides together: envelopeVersion, the `algorithm` constant, all four struct
// tags and the key-ID derivation can each be changed and the whole suite stays
// green. The only literal envelopes in the package are the two inside
// TestIsEncrypted, and IsEncrypted never reads `alg`, `kid`, or the *value* of
// `v` — it sniffs five bytes and returns.
//
// So the assertions below are deliberately not round-trips. The envelope is
// frozen: it was produced by the build at the time this test was written and is
// hard-coded here, together with the key that opens it. A reader that can no
// longer open it has broken compatibility with data that already exists,
// whether that data is in flight or in a table.
//
// The key is a fixed 0x00..0x1f pattern, not a secret.
const (
	frozenEnvelope = `{"v":1,"alg":"aes-256-gcm","kid":"630dcd29",` +
		`"data":"toMjljBzCYFwzIB1OrV0eTeYvu52bjhMJTxPjvfRSE8vbj18uNO+P5F+xH7+Cj4EUpgdSk4="}`
	frozenPlaintext = `{"rules":["placeholder"]}`
	frozenKeyID     = "630dcd29"
)

func frozenKey() *Key {
	var raw [32]byte
	for i := range raw {
		raw[i] = byte(i)
	}
	return KeyFromBytes(raw)
}

// TestDecryptOpensAFrozenEnvelope is the compatibility direction: this exact
// byte string is what an older writer emits, and the current reader must still
// open it.
func TestDecryptOpensAFrozenEnvelope(t *testing.T) {
	key := frozenKey()
	if key.ID != frozenKeyID {
		t.Fatalf("KeyFromBytes derived kid %q for the fixed key, want %q: the "+
			"key ID is how a receiver selects a key without trial decryption, "+
			"so changing its derivation orphans every envelope already sealed "+
			"and every key already handed to a running runner", key.ID, frozenKeyID)
	}

	got, err := NewKeyring(key).Decrypt([]byte(frozenEnvelope))
	if err != nil {
		t.Fatalf("Decrypt(frozen envelope) = %v: the reader can no longer open "+
			"an envelope this package itself produced. Every supply row already "+
			"stored is sealed in that format, and during a rolling upgrade every "+
			"runner still on the old build is producing it", err)
	}
	if string(got) != frozenPlaintext {
		t.Fatalf("Decrypt(frozen envelope) = %q, want %q", got, frozenPlaintext)
	}
}

// TestEncryptEmitsTheFrozenEnvelopeShape is the other direction: the current
// writer must still emit what an older reader parses. Checked on the decoded
// JSON rather than on the raw bytes, because field order is not part of the
// contract but the field NAMES are — a renamed tag is invisible to every
// round-trip test in this package and fatal to a reader that did not move with
// it.
func TestEncryptEmitsTheFrozenEnvelopeShape(t *testing.T) {
	sealed, err := Encrypt(frozenKey(), []byte(frozenPlaintext))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(sealed, &fields); err != nil {
		t.Fatalf("Encrypt did not produce a JSON object: %v (%s)", err, sealed)
	}

	names := make([]string, 0, len(fields))
	for k := range fields {
		names = append(names, k)
	}
	sort.Strings(names)
	want := []string{"alg", "data", "kid", "v"}
	if len(names) != len(want) {
		t.Fatalf("envelope fields = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("envelope fields = %v, want %v: a renamed or added field "+
				"changes what an already-deployed reader sees", names, want)
		}
	}

	var version int
	if err := json.Unmarshal(fields["v"], &version); err != nil {
		t.Fatalf("v is not a number: %s", fields["v"])
	}
	if version != 1 {
		t.Fatalf("envelope version = %d, want 1: Decrypt rejects any version "+
			"it does not equal, so bumping this constant makes every reader on "+
			"the previous build refuse every envelope from this one", version)
	}

	var alg string
	if err := json.Unmarshal(fields["alg"], &alg); err != nil {
		t.Fatalf("alg is not a string: %s", fields["alg"])
	}
	if alg != "aes-256-gcm" {
		t.Fatalf("envelope alg = %q, want aes-256-gcm: the field exists so a "+
			"receiver can detect an algorithm it does not implement, which it "+
			"cannot do if the name drifts", alg)
	}

	var kid string
	if err := json.Unmarshal(fields["kid"], &kid); err != nil {
		t.Fatalf("kid is not a string: %s", fields["kid"])
	}
	if kid != frozenKeyID {
		t.Fatalf("envelope kid = %q, want %q", kid, frozenKeyID)
	}
}

// TestIsEncryptedMatchesWhatEncryptProduces ties the two halves of the sniff
// together. IsEncrypted hard-codes the five bytes `{"v":` while the field name
// lives in a struct tag; nothing in the package makes one follow the other, and
// they are read by different code paths.
//
// AtRest.Open uses IsEncrypted as the ONLY thing deciding pass-through versus
// decrypt (atrest.go:50). If the sniff stops recognising a real envelope, Open
// does not fail — it returns the sealed bytes unchanged, and the caller hands
// base64 ciphertext to the wasm guest as if it were rule content.
func TestIsEncryptedMatchesWhatEncryptProduces(t *testing.T) {
	sealed, err := Encrypt(frozenKey(), []byte(frozenPlaintext))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if !IsEncrypted(sealed) {
		t.Fatalf("IsEncrypted said no to this package's own Encrypt output: "+
			"AtRest.Open would return these ciphertext bytes to the caller "+
			"unchanged and report no error: %s", sealed)
	}
	if !IsEncrypted([]byte(frozenEnvelope)) {
		t.Fatal("IsEncrypted said no to the frozen envelope: every supply row " +
			"already sealed on disk would be passed through as plaintext")
	}
}

// TestAtRestOpensAFrozenEnvelope is the end-to-end version of the two tests
// above, through the path that actually reads stored rows. It is here because
// Open's failure mode is silence: a sniff that stops matching, a tag that gets
// renamed, or a version bump all turn a stored envelope into "not encrypted",
// and Open then returns it with a nil error.
func TestAtRestOpensAFrozenEnvelope(t *testing.T) {
	var dek [32]byte
	for i := range dek {
		dek[i] = byte(i)
	}
	got, err := NewAtRest(dek).Open([]byte(frozenEnvelope))
	if err != nil {
		t.Fatalf("AtRest.Open(frozen envelope): %v", err)
	}
	if bytes.Equal(got, []byte(frozenEnvelope)) {
		t.Fatal("AtRest.Open returned the sealed envelope unchanged: " +
			"IsEncrypted no longer recognises it, so the pass-through branch " +
			"for pre-encryption rows swallowed a row that IS encrypted, with " +
			"no error")
	}
	if string(got) != frozenPlaintext {
		t.Fatalf("AtRest.Open = %q, want %q", got, frozenPlaintext)
	}
}
