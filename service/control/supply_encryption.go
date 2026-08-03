package control

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
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

// NewSupplyEncryptorShared resolves the transport key through Redis so every
// replica encrypts with the same key. Without this, a runner that registers
// against replica A and fetches supply content from replica B decrypts with
// the wrong key, the supply gate declines, and the runner hosts no triggers
// at all -- while heartbeating perfectly healthily.
//
// SET NX rather than leader election: election answers "who does the work",
// while key distribution only needs "everyone converges on one value". SET NX
// gives that atomically, with no startup window in which non-leader replicas
// have to wait for a leader to finish generating.
//
// The transport key is deliberately the one key kept in Redis: it is
// short-lived and self-healing (losing it only forces re-registration), which
// matches Redis' durability characteristics. The at-rest key must never live
// here -- losing it would make already-stored ciphertext permanently
// unreadable.
func NewSupplyEncryptorShared(ctx context.Context, rdb redis.Cmdable, key string) (*SupplyEncryptor, error) {
	candidate, err := supplyenc.GenerateKey()
	if err != nil {
		return nil, err
	}
	ok, err := rdb.SetNX(ctx, key, candidate.ToBase64(), 0).Result()
	if err != nil {
		return nil, fmt.Errorf("supply encryption key: %w", err)
	}
	if ok {
		return &SupplyEncryptor{current: candidate}, nil
	}
	// Another replica won the race (or a previous run stored one): adopt it.
	stored, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("supply encryption key: %w", err)
	}
	adopted, err := supplyenc.KeyFromBase64(stored)
	if err != nil {
		return nil, fmt.Errorf("supply encryption key: stored value is unusable")
	}
	return &SupplyEncryptor{current: adopted}, nil
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

// ConsumeRotation returns the pending rotation key (base64-encoded) and
// clears the pending state, so the rotation is delivered once rather than on
// every heartbeat.
//
// Redelivering it would be actively harmful, not merely wasteful: the
// runner's installSupplyKey calls Keyring.Rotate on each delivery, so a
// second delivery of the same key demotes the key it just promoted and
// evicts the previous one. Content encrypted before the rotation then fails
// to decrypt -- exactly what keeping a previous key exists to prevent.
//
// Known limitation: clearing the pending state here means only the next
// runner to heartbeat after a Rotate() receives the rotation key -- every
// other runner never gets it. That is different from the goal this type's
// original doc comment described (clear only after all runners converge via
// supply_observed). This fix addresses only the "infinite redelivery
// destroys the keyring" defect, which is the one actively causing harm.
// Per-runner tracking so every runner receives the rotation exactly once is
// out of scope here.
func (e *SupplyEncryptor) ConsumeRotation() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pendingRotation == nil {
		return ""
	}
	out := e.pendingRotation.ToBase64()
	e.pendingRotation = nil
	return out
}

// supplyEncryptionKeyRedisKey is where replicas rendezvous on one transport key.
const supplyEncryptionKeyRedisKey = "xflow:supply:transport-key"

// resolveSupplyEncryptor picks the key source for the backend in use.
//
// With Redis, replicas must share one key: a runner that registers against
// replica A and fetches supply content from replica B would otherwise hold
// key A and receive ciphertext under key B. The fetch fails, the supply gate
// declines, and the runner hosts no triggers -- while heartbeating healthily.
//
// Without Redis the backend is the in-memory one, which is single-replica by
// construction, so a process-local key is consistent by definition.
func resolveSupplyEncryptor(ctx context.Context, b backend.Provider) (*SupplyEncryptor, error) {
	if rc, ok := b.(redisClientProvider); ok && rc.RedisClient() != nil {
		return NewSupplyEncryptorShared(ctx, rc.RedisClient(), supplyEncryptionKeyRedisKey)
	}
	return NewSupplyEncryptor()
}
