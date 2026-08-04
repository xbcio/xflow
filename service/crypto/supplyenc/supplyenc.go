// Package supplyenc implements AES-256-GCM envelope encryption for supply
// content transmitted between the control plane and runners. The encrypted
// envelope is a compact JSON object ("$enc envelope") that carries a version
// tag, algorithm identifier, key ID, and base64-encoded ciphertext. The key ID
// allows the receiver to select the correct key from a keyring without trial
// decryption.
package supplyenc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// envelopeVersion is bumped only when the wire format changes incompatibly.
const envelopeVersion = 1

// algorithm is the AEAD algorithm name carried in the envelope. Only
// aes-256-gcm is supported; the field exists for forward-compat detection.
const algorithm = "aes-256-gcm"

// Key is a 32-byte AES-256 key with a short identifier derived from its hash.
type Key struct {
	// ID is the first 4 bytes of SHA-256(Raw), hex-encoded (8 chars). It is
	// transmitted in the envelope so the receiver can locate the key without
	// trial decryption.
	ID  string
	Raw [32]byte
}

// String and GoString guard against key leakage through fmt. Raw is an
// EXPORTED field, so %v and %+v print all 32 bytes unless an explicit method
// intercepts them — a wider exposure than the sibling masterkey.Key, whose
// equivalent redaction this mirrors. A *Key reaches many call sites here
// (runner keyrings, the control plane's SupplyEncryptor), so an accidental
// %+v on a struct carrying one is plausible.
func (k *Key) String() string { return "supplyenc.Key(redacted)" }

// GoString guards the %#v verb the same way String guards %v and %+v.
func (k *Key) GoString() string { return "supplyenc.Key(redacted)" }

// GenerateKey creates a new random 32-byte key.
func GenerateKey() (*Key, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("supplyenc: generate key: %w", err)
	}
	return KeyFromBytes(raw), nil
}

// KeyFromBytes constructs a Key from raw bytes, computing the key ID.
func KeyFromBytes(raw [32]byte) *Key {
	sum := sha256.Sum256(raw[:])
	return &Key{ID: hex.EncodeToString(sum[:4]), Raw: raw}
}

// KeyFromBase64 decodes a base64-encoded 32-byte key (as transmitted in the
// RegisterRunnerResponse.SupplyKey field).
func KeyFromBase64(encoded string) (*Key, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("supplyenc: decode key: %w", err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("supplyenc: key length %d, want 32", len(raw))
	}
	var arr [32]byte
	copy(arr[:], raw)
	return KeyFromBytes(arr), nil
}

// ToBase64 encodes the key for wire transmission.
func (k *Key) ToBase64() string {
	return base64.StdEncoding.EncodeToString(k.Raw[:])
}

// envelope is the JSON wire format for encrypted supply content.
type envelope struct {
	Version   int    `json:"v"`
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Data      string `json:"data"` // base64(nonce || ciphertext || tag)
}

// Encrypt encrypts plaintext with the given key and returns a JSON $enc
// envelope. Each call generates a fresh random nonce.
func Encrypt(key *Key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key.Raw[:])
	if err != nil {
		return nil, fmt.Errorf("supplyenc: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("supplyenc: new gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("supplyenc: random nonce: %w", err)
	}
	// Seal appends ciphertext+tag after nonce.
	sealed := gcm.Seal(nonce, nonce, plaintext, nil)
	env := envelope{
		Version:   envelopeVersion,
		Algorithm: algorithm,
		KeyID:     key.ID,
		Data:      base64.StdEncoding.EncodeToString(sealed),
	}
	return json.Marshal(env)
}

// Decryption errors.
var (
	ErrNotEncrypted      = errors.New("supplyenc: not an encrypted envelope")
	ErrUnsupportedVersion = errors.New("supplyenc: unsupported envelope version")
	ErrUnknownKey        = errors.New("supplyenc: no key matches kid")
	ErrDecryptFailed     = errors.New("supplyenc: decryption failed")
)

// Keyring holds up to 2 keys (current + previous) for decryption. It is
// safe for concurrent use.
type Keyring struct {
	mu   sync.RWMutex
	keys []*Key // index 0 = current, index 1 = previous (if any)
}

// NewKeyring creates a keyring with the given keys. The first key is current.
func NewKeyring(keys ...*Key) *Keyring {
	kr := &Keyring{}
	for _, k := range keys {
		if k != nil {
			kr.keys = append(kr.keys, k)
		}
	}
	return kr
}

// Rotate installs a new current key, demoting the old current to previous.
// Any prior previous key is discarded.
func (kr *Keyring) Rotate(newKey *Key) {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	if len(kr.keys) == 0 {
		kr.keys = []*Key{newKey}
	} else {
		kr.keys = []*Key{newKey, kr.keys[0]}
	}
}

// HasKeys reports whether the keyring has at least one key.
func (kr *Keyring) HasKeys() bool {
	if kr == nil {
		return false
	}
	kr.mu.RLock()
	defer kr.mu.RUnlock()
	return len(kr.keys) > 0
}

// Decrypt decrypts a $enc envelope using the key identified by the kid field.
// Returns ErrNotEncrypted if data is not a valid envelope.
func (kr *Keyring) Decrypt(data []byte) ([]byte, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, ErrNotEncrypted
	}
	if env.Version == 0 || env.Algorithm == "" || env.Data == "" {
		return nil, ErrNotEncrypted
	}
	if env.Version != envelopeVersion {
		return nil, fmt.Errorf("%w: v%d", ErrUnsupportedVersion, env.Version)
	}

	key := kr.findKey(env.KeyID)
	if key == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKey, env.KeyID)
	}

	sealed, err := base64.StdEncoding.DecodeString(env.Data)
	if err != nil {
		return nil, fmt.Errorf("supplyenc: decode data: %w", err)
	}

	block, err := aes.NewCipher(key.Raw[:])
	if err != nil {
		return nil, fmt.Errorf("supplyenc: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("supplyenc: new gcm: %w", err)
	}
	if len(sealed) < gcm.NonceSize() {
		return nil, ErrDecryptFailed
	}
	nonce := sealed[:gcm.NonceSize()]
	ciphertext := sealed[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrDecryptFailed
	}
	return plaintext, nil
}

func (kr *Keyring) findKey(kid string) *Key {
	kr.mu.RLock()
	defer kr.mu.RUnlock()
	for _, k := range kr.keys {
		if k.ID == kid {
			return k
		}
	}
	return nil
}

// IsEncrypted performs a fast check on whether data looks like a $enc envelope.
// It checks for the JSON opening and version field without full parsing.
func IsEncrypted(data []byte) bool {
	// Minimal heuristic: starts with `{"v":` and contains "alg".
	if len(data) < 20 {
		return false
	}
	// Trim leading whitespace.
	for len(data) > 0 && (data[0] == ' ' || data[0] == '\t' || data[0] == '\n' || data[0] == '\r') {
		data = data[1:]
	}
	return len(data) > 5 && data[0] == '{' && data[1] == '"' && data[2] == 'v' && data[3] == '"' && data[4] == ':'
}
