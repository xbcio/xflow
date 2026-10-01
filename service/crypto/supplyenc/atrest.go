package supplyenc

import (
	"encoding/json"
	"errors"
	"fmt"
)

// SupplyContentInfo is the HKDF info string that scopes the supply at-rest
// key. Changing it makes every previously stored row unreadable, so it carries
// a version suffix instead of ever being edited in place.
const SupplyContentInfo = "xflow-supply-content-v1"

// AtRest encrypts supply content for storage. It reuses the same $enc envelope
// format as the transport path, so there is one wire format to reason about,
// but the key is different: the transport key is short-lived and reissued at
// will, while this one must stay derivable for the lifetime of the stored data.
type AtRest struct {
	key     *Key
	keyring *Keyring
}

// NewAtRest builds an encryptor from a DEK, normally
// masterkey.Key.Derive(SupplyContentInfo).
func NewAtRest(dek [32]byte) *AtRest {
	key := KeyFromBytes(dek)
	return &AtRest{key: key, keyring: NewKeyring(key)}
}

// ErrSameDEK means NewAtRestWithPrevious was handed the same DEK twice. A
// rotation whose old and new keys are identical re-seals nothing while
// reporting success, so it is refused rather than tolerated.
var ErrSameDEK = errors.New("supplyenc: previous DEK equals current DEK")

// NewAtRestWithPrevious builds the encryptor used during a KEK rotation window.
// Seal always uses current; Open accepts rows sealed under either key, so
// stored content stays readable until a reseal pass has rewritten it.
//
// Two keys cover exactly one rotation. The previous key is meant to be removed
// once NeedsReseal reports false for every stored row; a second rotation
// started before that would strand rows sealed under the oldest key.
func NewAtRestWithPrevious(current, previous [32]byte) (*AtRest, error) {
	if current == previous {
		return nil, ErrSameDEK
	}
	cur := KeyFromBytes(current)
	return &AtRest{key: cur, keyring: NewKeyring(cur, KeyFromBytes(previous))}, nil
}

// CurrentKeyID is the fingerprint Seal stamps into every new envelope. It is a
// 4-byte hash of the DEK, safe to log or print from an operator tool.
func (a *AtRest) CurrentKeyID() string { return a.key.ID }

// NeedsReseal reports whether stored is not yet sealed under the current key:
// either pre-encryption plaintext, or an envelope carrying another kid. It
// never decrypts, so a damaged envelope whose kid still parses as current
// reports false here and surfaces as an Open error on the normal read path.
//
// Envelope classification is the same IsEncrypted prefix test Open uses, so
// the two can never disagree about which rows are plaintext.
func (a *AtRest) NeedsReseal(stored []byte) bool {
	if !IsEncrypted(stored) {
		return true
	}
	var env struct {
		KeyID string `json:"kid"`
	}
	if err := json.Unmarshal(stored, &env); err != nil {
		// Claims to be an envelope but is damaged. Report it so the reseal
		// pass attempts Open and records the failure instead of skipping the
		// row as if it were already current.
		return true
	}
	return env.KeyID != a.key.ID
}

// Seal encrypts plaintext into a storable envelope.
func (a *AtRest) Seal(plaintext []byte) ([]byte, error) {
	return Encrypt(a.key, plaintext)
}

// Open decrypts a stored value.
//
// Whether to pass data through unchanged is decided ONLY by IsEncrypted up
// front: data that does not look like an envelope is returned unchanged, to
// support the upgrade path where rows written before encryption was enabled
// are plaintext, and failing them would make every existing supply unreadable
// at the moment of upgrade — the supply gate would then decline every
// activation, so no runner would host any trigger.
//
// Once data has passed that check it claims to be an envelope, so past this
// point every error is an error, including a JSON parse failure. A parse
// failure here does not mean "it was actually plaintext"; it means the
// envelope is damaged (truncation, a bad migration, a bit flip in a JSON
// structural character), and silently returning the damaged bytes would hand
// the wasm guest unparseable content that looks like "no rules" instead of
// "decryption failed".
func (a *AtRest) Open(stored []byte) ([]byte, error) {
	if !IsEncrypted(stored) {
		return stored, nil
	}
	plaintext, err := a.keyring.Decrypt(stored)
	if err != nil {
		return nil, fmt.Errorf("supplyenc: open stored content: %w", err)
	}
	return plaintext, nil
}
