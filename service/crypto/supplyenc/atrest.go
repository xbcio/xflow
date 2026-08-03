package supplyenc

import (
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
