package control

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

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
