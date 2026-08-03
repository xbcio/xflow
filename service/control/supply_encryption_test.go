package control

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
)

// redisBackendStub wraps a plain backend.Provider and adds a RedisClient
// method so it satisfies redisClientProvider. Using the local backend plus
// this stub -- rather than spinning up backend/providers/distributed, which
// needs a bound queue/transport just to expose a Redis client -- keeps the
// test focused on resolveSupplyEncryptor's branch selection.
type redisBackendStub struct {
	backend.Provider
	rdb redis.Cmdable
}

func (s redisBackendStub) RedisClient() redis.Cmdable { return s.rdb }

func newMiniRedis(t *testing.T) redis.Cmdable {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// This is the most damaging defect in the current implementation: each
// replica generates its own key, a runner registers against replica A and
// gets key A, then fetches supply content from replica B and receives
// ciphertext encrypted under key B, decryption fails, the gate declines, and
// the runner hosts no triggers at all -- while heartbeating perfectly
// healthily.
func TestSharedEncryptorConvergesAcrossReplicas(t *testing.T) {
	rdb := newMiniRedis(t)
	ctx := context.Background()

	a, err := NewSupplyEncryptorShared(ctx, rdb, "xflow:supply:enckey")
	if err != nil {
		t.Fatalf("replica A: %v", err)
	}
	b, err := NewSupplyEncryptorShared(ctx, rdb, "xflow:supply:enckey")
	if err != nil {
		t.Fatalf("replica B: %v", err)
	}
	if a.KeyForRunner() != b.KeyForRunner() {
		t.Fatal("two replicas hold different keys; a runner registering on one " +
			"and fetching from the other cannot decrypt")
	}
}

// After a restart the process must recover the same key, otherwise every
// rolling deploy causes in-flight runners to fail decryption.
func TestSharedEncryptorSurvivesRestart(t *testing.T) {
	rdb := newMiniRedis(t)
	ctx := context.Background()

	first, err := NewSupplyEncryptorShared(ctx, rdb, "k")
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	before := first.KeyForRunner()

	second, err := NewSupplyEncryptorShared(ctx, rdb, "k")
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if second.KeyForRunner() != before {
		t.Fatal("the key changed across a restart")
	}
}

// ConsumeRotation's doc comment claims it clears the pending state, but the
// function body contains no assignment clearing it. The consequence: every
// heartbeat redelivers the same rotation key, and the runner's
// installSupplyKey calls Keyring.Rotate on each delivery, so the keyring
// becomes [new, new] -- the previous key gets evicted on the very next
// heartbeat, and any in-flight ciphertext encrypted before the rotation can
// no longer be decrypted.
func TestConsumeRotationClearsPending(t *testing.T) {
	e, err := NewSupplyEncryptor()
	if err != nil {
		t.Fatalf("NewSupplyEncryptor: %v", err)
	}
	if err := e.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := e.ConsumeRotation(); got == "" {
		t.Fatal("first ConsumeRotation returned nothing after a Rotate")
	}
	if got := e.ConsumeRotation(); got != "" {
		t.Fatal("ConsumeRotation kept returning the rotation; every heartbeat " +
			"would redeliver it and evict the runner's previous key")
	}
}

func TestNoRotationPendingReturnsEmpty(t *testing.T) {
	e, err := NewSupplyEncryptor()
	if err != nil {
		t.Fatalf("NewSupplyEncryptor: %v", err)
	}
	if got := e.ConsumeRotation(); got != "" {
		t.Errorf("ConsumeRotation = %q with no rotation pending, want empty", got)
	}
}

// --memory 模式没有 Redis，且本就是单副本，所以退回进程内生成是正确的 --
// 但必须真的退回，而不是启动失败。
func TestResolveSupplyEncryptorFallsBackWithoutRedis(t *testing.T) {
	enc, err := resolveSupplyEncryptor(context.Background(), backendlocal.New())
	if err != nil {
		t.Fatalf("resolveSupplyEncryptor on a backend without Redis: %v", err)
	}
	if enc == nil {
		t.Fatal("no encryptor was built; encryption would silently stay off")
	}
	if enc.KeyForRunner() == "" {
		t.Error("the fallback encryptor has no usable key")
	}
}

// 有 Redis 时必须走共享路径，否则多副本各持一把 key 的缺陷原样保留。
func TestResolveSupplyEncryptorSharesViaRedis(t *testing.T) {
	rdb := newMiniRedis(t)
	ctx := context.Background()

	// Seed the shared key directly, simulating a replica that already
	// registered it in Redis.
	seeded, err := NewSupplyEncryptorShared(ctx, rdb, supplyEncryptionKeyRedisKey)
	if err != nil {
		t.Fatalf("seed the shared key: %v", err)
	}

	// resolveSupplyEncryptor -- the function under test -- must adopt the
	// seeded key rather than generate its own. If it silently bypassed Redis
	// (e.g. always falling back to NewSupplyEncryptor()), KeyForRunner()
	// would differ from the seeded value and this assertion would fail.
	provider := redisBackendStub{Provider: backendlocal.New(), rdb: rdb}
	enc, err := resolveSupplyEncryptor(ctx, provider)
	if err != nil {
		t.Fatalf("resolveSupplyEncryptor with a Redis-backed provider: %v", err)
	}
	if enc.KeyForRunner() != seeded.KeyForRunner() {
		t.Fatal("resolveSupplyEncryptor did not adopt the key already stored in Redis")
	}
}
