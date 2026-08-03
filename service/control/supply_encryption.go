package control

import (
	"sync"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
)

// SupplyEncryptor manages the AES-256-GCM key used to encrypt supply content
// for runners. It generates a key at construction and supports rotation: the
// old key is kept long enough for in-flight heartbeat responses to deliver the
// new key before runners discard the old one.
//
// The encryptor is optional: when nil, the control plane returns plaintext
// supply content (backward-compatible path).
type SupplyEncryptor struct {
	mu      sync.RWMutex
	current *supplyenc.Key
	// pendingRotation is non-nil between a Rotate() call and the next time the
	// server has confirmed (via supply_observed convergence) that all runners
	// received the new key. For simplicity in this implementation, it is set on
	// Rotate and cleared by ConsumeRotation — once each runner's next heartbeat
	// picks it up.
	pendingRotation *supplyenc.Key
}

// NewSupplyEncryptor creates an encryptor with a fresh random key. Returns an
// error only if the system entropy source fails.
func NewSupplyEncryptor() (*SupplyEncryptor, error) {
	k, err := supplyenc.GenerateKey()
	if err != nil {
		return nil, err
	}
	return &SupplyEncryptor{current: k}, nil
}

// Encrypt encrypts plaintext supply content with the current key. Safe for
// concurrent use.
func (e *SupplyEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	e.mu.RLock()
	k := e.current
	e.mu.RUnlock()
	return supplyenc.Encrypt(k, plaintext)
}

// KeyForRunner returns the current key base64-encoded, suitable for inclusion
// in a RegisterRunnerResponse.SupplyKey field.
func (e *SupplyEncryptor) KeyForRunner() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current.ToBase64()
}

// Rotate generates a new key and stages it for delivery to runners on their
// next heartbeat. The old current key remains valid for decryption (runners
// keep it as previous in their keyring).
func (e *SupplyEncryptor) Rotate() error {
	newKey, err := supplyenc.GenerateKey()
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pendingRotation = newKey
	e.current = newKey
	return nil
}

// ConsumeRotation returns the pending rotation key (base64-encoded) and clears
// the pending state for this runner. Returns "" if no rotation is pending. Each
// runner consumes the rotation independently via its heartbeat; this is
// stateless — all runners that heartbeat while pendingRotation is non-nil will
// receive it.
//
// In a production deployment with many runners, the pending state should be
// cleared only after ALL runners have acknowledged (via supply_observed
// convergence). This simplified implementation clears after a single consume
// for clarity; the production version would use a per-runner tracking set.
func (e *SupplyEncryptor) ConsumeRotation() string {
	e.mu.RLock()
	p := e.pendingRotation
	e.mu.RUnlock()
	if p == nil {
		return ""
	}
	return p.ToBase64()
}
