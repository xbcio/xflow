// Package masterkey loads the process-wide root encryption key (KEK) and
// derives purpose-scoped subkeys from it.
//
// The key never comes from the repository or from generated state: it is
// injected at deploy time via XFLOW_MASTER_KEY or --master-key-file. A key
// stored alongside the data it protects (in MySQL, in the image, in git)
// offers no protection against the threat that motivates encrypting at rest
// in the first place — an attacker holding a database dump or the source.
//
// Position in the main line: masterkey is the root of the server's key
// hierarchy. cmd/server loads it at startup and passes it to
// service/control.ControlPlane, which calls Derive("supply.transport") to
// produce the AES-256 key managed by service/crypto/supplyenc. No other
// package receives the raw Key; all downstream packages receive only the
// purpose-scoped derived bytes, so a compromise of one derived key cannot
// expose keys derived for other purposes. ErrNotConfigured is the expected
// signal on a dev deployment that deliberately runs without encryption.
package masterkey

import (
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

// keyLen is the only accepted decoded key length: 32 bytes is an AES-256 key,
// so the KEK needs no further stretching before use.
const keyLen = 32

// ErrNotConfigured means neither source supplied a key. Callers decide whether
// that is fatal: production fails closed, dev continues with plaintext storage.
var ErrNotConfigured = errors.New("masterkey: not configured")

// Key is the root key. It is deliberately not exported as raw bytes: callers
// derive purpose-scoped subkeys instead of encrypting with the root directly,
// so a single leaked ciphertext key never compromises other purposes.
type Key struct {
	raw [32]byte
}

// Load resolves the master key. envValue (XFLOW_MASTER_KEY) takes precedence
// over filePath (--master-key-file) so a deployment can override a mounted
// file without rewriting it.
//
// Both empty returns ErrNotConfigured. Any other problem — bad base64, wrong
// length, loose file permissions — is an error, never a silent fallback: a
// misconfigured key that is quietly ignored writes plaintext to disk while
// everything appears to work.
func Load(envValue string, filePath string) (*Key, error) {
	encoded := envValue
	if encoded == "" {
		if filePath == "" {
			return nil, ErrNotConfigured
		}
		info, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("masterkey: %w", err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			return nil, fmt.Errorf("masterkey: %s is group/world readable (mode %o); chmod 0600", filePath, mode)
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("masterkey: %w", err)
		}
		encoded = string(data)
	}
	// Trim so a key file written with a trailing newline (every editor, and
	// `openssl rand -base64 32 > f`) is not rejected for a stray byte.
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return nil, ErrNotConfigured
	}

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// Never echo the value: it is the key. Only say what shape was wanted.
		return nil, fmt.Errorf("masterkey: value is not valid base64; generate one with: openssl rand -base64 32")
	}
	if len(raw) != keyLen {
		return nil, fmt.Errorf("masterkey: decoded key is %d bytes, want %d; generate one with: openssl rand -base64 32", len(raw), keyLen)
	}
	k := &Key{}
	copy(k.raw[:], raw)
	return k, nil
}

// ErrPreviousEqualsCurrent means the previous-key input holds the same value
// as the current key. That is always an operator mistake during a rotation
// (the old and new values were swapped or copied into both slots), and
// accepting it would let a reseal job report success while re-sealing nothing.
var ErrPreviousEqualsCurrent = errors.New("masterkey: previous key equals current key")

// LoadPrevious resolves the key being rotated OUT (XFLOW_MASTER_KEY_PREVIOUS /
// --master-key-previous-file). It exists only for the rotation window: while
// it is configured, rows sealed under it stay readable, and a reseal pass
// rewrites them under current so it can be removed afterwards.
//
// It applies exactly the same validation as Load — env over file, 0600 file,
// 32 decoded bytes, value never echoed — and returns ErrNotConfigured when
// neither source is set, which is the normal steady state. current must be
// non-nil: a previous key without a current key has nothing to rotate to.
func LoadPrevious(envValue, filePath string, current *Key) (*Key, error) {
	if current == nil {
		if envValue != "" || filePath != "" {
			return nil, errors.New("masterkey: previous key configured without a current key")
		}
		return nil, ErrNotConfigured
	}
	prev, err := Load(envValue, filePath)
	if err != nil {
		if errors.Is(err, ErrNotConfigured) {
			return nil, err
		}
		return nil, fmt.Errorf("previous %w", err)
	}
	if subtle.ConstantTimeCompare(prev.raw[:], current.raw[:]) == 1 {
		return nil, ErrPreviousEqualsCurrent
	}
	return prev, nil
}

// Derive returns a purpose-scoped 32-byte subkey via HKDF-SHA256. Distinct
// info strings yield unrelated keys, so the generically-named master key can
// protect several kinds of data without one leak exposing the others.
//
// The result is deterministic: the same master key and info always derive the
// same subkey, which is what makes previously stored ciphertext readable after
// a restart.
func (k *Key) Derive(info string) [32]byte {
	// No salt: the master key is already full-entropy random, which is the
	// condition under which HKDF-Extract's salt is optional (RFC 5869 §3.1).
	out, err := hkdf.Key(sha256.New, k.raw[:], nil, info, keyLen)
	if err != nil {
		// Only reachable on an invalid length argument, which is a constant here.
		panic("masterkey: hkdf: " + err.Error())
	}
	var arr [32]byte
	copy(arr[:], out)
	return arr
}

// String and GoString guard against key leakage through fmt: %v and %+v
// reach unexported struct fields via reflection regardless of export status,
// so an explicit method is the only reliable redaction — relevant once this
// package is wired into cmd/server, where an accidental %+v on a config
// struct embedding a *Key is plausible.
func (k *Key) String() string { return "masterkey.Key(redacted)" }

// GoString guards the %#v verb the same way String guards %v and %+v.
func (k *Key) GoString() string { return "masterkey.Key(redacted)" }
