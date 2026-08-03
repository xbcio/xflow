package supplyenc

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestGenerateKey(t *testing.T) {
	k, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if len(k.ID) != 8 {
		t.Fatalf("key ID length = %d, want 8", len(k.ID))
	}
	// Ensure non-zero key.
	var zero [32]byte
	if k.Raw == zero {
		t.Fatal("key is all zeros")
	}
}

func TestKeyFromBase64Roundtrip(t *testing.T) {
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	encoded := k.ToBase64()
	k2, err := KeyFromBase64(encoded)
	if err != nil {
		t.Fatalf("KeyFromBase64: %v", err)
	}
	if k.ID != k2.ID {
		t.Fatalf("ID mismatch: %s vs %s", k.ID, k2.ID)
	}
	if k.Raw != k2.Raw {
		t.Fatal("raw key mismatch")
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte(`{"rules":[{"kind":"clean","field":"authorization"}]}`)

	ciphertext, err := Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Must look like encrypted.
	if !IsEncrypted(ciphertext) {
		t.Fatal("IsEncrypted returned false for ciphertext")
	}

	kr := NewKeyring(key)
	got, err := kr.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("plaintext mismatch:\n got: %s\nwant: %s", got, plaintext)
	}
}

func TestEncryptNonDeterministic(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte("same input")

	c1, _ := Encrypt(key, plaintext)
	c2, _ := Encrypt(key, plaintext)
	if bytes.Equal(c1, c2) {
		t.Fatal("two encryptions of same plaintext produced identical ciphertext")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	ciphertext, _ := Encrypt(key1, []byte("secret"))
	kr := NewKeyring(key2)
	_, err := kr.Decrypt(ciphertext)
	if err == nil {
		t.Fatal("expected error decrypting with wrong key")
	}
}

func TestKeyringRotation(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	// Encrypt with key1 (old), then rotate to key2.
	ct1, _ := Encrypt(key1, []byte("msg-old"))
	ct2, _ := Encrypt(key2, []byte("msg-new"))

	kr := NewKeyring(key1)
	kr.Rotate(key2) // key2 = current, key1 = previous

	// Both should decrypt.
	p1, err := kr.Decrypt(ct1)
	if err != nil {
		t.Fatalf("decrypt with previous key: %v", err)
	}
	if string(p1) != "msg-old" {
		t.Fatalf("got %q, want %q", p1, "msg-old")
	}

	p2, err := kr.Decrypt(ct2)
	if err != nil {
		t.Fatalf("decrypt with current key: %v", err)
	}
	if string(p2) != "msg-new" {
		t.Fatalf("got %q, want %q", p2, "msg-new")
	}
}

func TestKeyringRotationEvicts(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()
	key3, _ := GenerateKey()

	ct1, _ := Encrypt(key1, []byte("oldest"))

	kr := NewKeyring(key1)
	kr.Rotate(key2)
	kr.Rotate(key3) // key3=current, key2=previous, key1=evicted

	_, err := kr.Decrypt(ct1)
	if err == nil {
		t.Fatal("expected error: key1 should have been evicted")
	}
}

func TestIsEncrypted(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want bool
	}{
		{"envelope", []byte(`{"v":1,"alg":"aes-256-gcm","kid":"abcd1234","data":"..."}`), true},
		{"plain json", []byte(`{"rules":[]}`), false},
		{"empty", nil, false},
		{"binary", func() []byte { b := make([]byte, 50); rand.Read(b); return b }(), false},
		{"whitespace prefix", []byte(`  {"v":1,"alg":"aes-256-gcm","kid":"x","data":"y"}`), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEncrypted(tc.data); got != tc.want {
				t.Fatalf("IsEncrypted = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecryptNotEncrypted(t *testing.T) {
	key, _ := GenerateKey()
	kr := NewKeyring(key)
	_, err := kr.Decrypt([]byte(`{"rules":[]}`))
	if err == nil {
		t.Fatal("expected error for non-envelope data")
	}
}

func TestEncryptEmptyPlaintext(t *testing.T) {
	key, _ := GenerateKey()
	ct, err := Encrypt(key, []byte{})
	if err != nil {
		t.Fatalf("Encrypt empty: %v", err)
	}
	kr := NewKeyring(key)
	pt, err := kr.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt empty: %v", err)
	}
	if len(pt) != 0 {
		t.Fatalf("expected empty plaintext, got %d bytes", len(pt))
	}
}

func TestKeyFromBase64InvalidLength(t *testing.T) {
	_, err := KeyFromBase64("dG9vc2hvcnQ=") // "tooshort"
	if err == nil {
		t.Fatal("expected error for short key")
	}
}
