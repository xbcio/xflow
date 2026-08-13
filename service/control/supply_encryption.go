package control

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
)

// SupplyEncryptor manages the AES-256-GCM key used to encrypt supply content
// for runners. It generates a key at construction and supports rotation.
//
// Delivery is convergent, not bookkept: a runner reports the key ID it holds on
// every heartbeat and the server hands back the full key only when that ID does
// not match the current one (see RotationForHolder). Nothing per-runner is
// tracked, so a runner that was down during a rotation, one that restarted, and
// one that joined afterwards all converge on their next heartbeat. The earlier
// design — stage a rotation, deliver it once, clear it — reached exactly one
// runner per rotation and stranded every other runner on a key that no longer
// decrypts anything.
//
// The encryptor is optional: when nil, the control plane returns plaintext
// supply content (backward-compatible path).
type SupplyEncryptor struct {
	mu      sync.RWMutex
	current *supplyenc.Key

	// rdb and redisKey are set only for the shared (multi-replica) encryptor.
	// They make a rotation visible to the other replicas: Rotate writes the new
	// key back, and Refresh adopts whatever another replica rotated to. Without
	// them a rotation would live and die inside one process — lost on restart,
	// invisible to every peer, and actively harmful because that process hands
	// runners a key its peers do not encrypt with.
	rdb      redis.Cmdable
	redisKey string
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
// at all — while heartbeating perfectly healthily.
//
// SET NX rather than leader election: election answers "who does the work",
// while key distribution only needs "everyone converges on one value". SET NX
// gives that atomically, with no startup window in which non-leader replicas
// have to wait for a leader to finish generating.
//
// The transport key is deliberately the one key kept in Redis: it is
// short-lived and self-healing (losing it only forces re-registration), which
// matches Redis' durability characteristics. The at-rest key must never live
// here — losing it would make already-stored ciphertext permanently
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
		return &SupplyEncryptor{current: candidate, rdb: rdb, redisKey: key}, nil
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
	return &SupplyEncryptor{current: adopted, rdb: rdb, redisKey: key}, nil
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

// CurrentKeyID returns the ID of the key currently used for encryption. It is
// the value a runner echoes back on each heartbeat so the server can tell
// whether that runner still needs the rotation.
//
// The ID is the first 4 bytes of SHA-256(key) — a fingerprint, not key
// material. It is safe on the wire and in logs; the raw key never is.
func (e *SupplyEncryptor) CurrentKeyID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current.ID
}

// RotationForHolder returns the current key base64-encoded when the caller
// holds a different one, and "" when the caller is already current.
//
// heldKeyID == "" means the runner did not report one: either an old runner
// that predates the field, or one that has no key at all. Returning "" for
// that case is deliberate — an old runner cannot be told apart from a
// converged one, and pushing a rotation at every heartbeat would be the
// keyring-destroying redelivery this design exists to avoid. Old runners keep
// the key they received at registration, exactly as before.
func (e *SupplyEncryptor) RotationForHolder(heldKeyID string) string {
	if heldKeyID == "" {
		return ""
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if heldKeyID == e.current.ID {
		return ""
	}
	return e.current.ToBase64()
}

// Rotate generates a new key, installs it as current, and — for the shared
// encryptor — publishes it so every other replica adopts it too.
//
// The Redis write happens BEFORE the local swap. Publishing first means the
// worst case is a replica encrypting with a key its peers already have;
// swapping first would mean encrypting with a key no peer can obtain, which no
// runner could ever decrypt. On a Redis failure the local key is left
// untouched and the error is returned, so a failed rotation is a no-op rather
// than a partial one.
func (e *SupplyEncryptor) Rotate(ctx context.Context) error {
	newKey, err := supplyenc.GenerateKey()
	if err != nil {
		return err
	}
	if e.rdb != nil {
		if err := e.rdb.Set(ctx, e.redisKey, newKey.ToBase64(), 0).Err(); err != nil {
			return fmt.Errorf("supply encryption key: publish rotation: %w", err)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.current = newKey
	return nil
}

// Refresh adopts the transport key currently stored in Redis, so a replica
// that did not perform the rotation stops encrypting with the superseded key.
// It reports whether the key changed.
//
// A no-op (false, nil) for the process-local encryptor, which has no peers.
//
// A missing Redis key is treated as an error rather than a reason to generate
// a replacement: two replicas each generating one after an eviction would
// diverge, which is the exact failure the shared key exists to prevent. The
// caller logs and retries on the next tick.
func (e *SupplyEncryptor) Refresh(ctx context.Context) (bool, error) {
	if e.rdb == nil {
		return false, nil
	}
	stored, err := e.rdb.Get(ctx, e.redisKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, fmt.Errorf("supply encryption key: not present in Redis")
		}
		return false, fmt.Errorf("supply encryption key: %w", err)
	}
	adopted, err := supplyenc.KeyFromBase64(stored)
	if err != nil {
		return false, fmt.Errorf("supply encryption key: stored value is unusable")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.current != nil && e.current.ID == adopted.ID {
		return false, nil
	}
	e.current = adopted
	return true, nil
}

// supplyEncryptionKeyRedisKey is where replicas rendezvous on one transport key.
const supplyEncryptionKeyRedisKey = "xflow:supply:transport-key"

// resolveSupplyEncryptor picks the key source for the backend in use.
//
// With Redis, replicas must share one key: a runner that registers against
// replica A and fetches supply content from replica B would otherwise hold
// key A and receive ciphertext under key B. The fetch fails, the supply gate
// declines, and the runner hosts no triggers — while heartbeating healthily.
//
// Without Redis the backend is the in-memory one, which is single-replica by
// construction, so a process-local key is consistent by definition.
func resolveSupplyEncryptor(ctx context.Context, b backend.Provider) (*SupplyEncryptor, error) {
	if rc, ok := b.(redisClientProvider); ok && rc.RedisClient() != nil {
		return NewSupplyEncryptorShared(ctx, rc.RedisClient(), supplyEncryptionKeyRedisKey)
	}
	return NewSupplyEncryptor()
}
