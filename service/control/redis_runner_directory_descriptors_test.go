package control

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisRunnerDirectoryDescriptorContract(t *testing.T) {
	runRunnerDescriptorDirectoryContract(t, func(t *testing.T) descriptorContractDirectory {
		_, rdb := newRedisRunnerDirectoryTestClient(t)
		return NewRedisRunnerDirectory(rdb)
	})
}

// TestRedisRunnerDirectoryDescriptorContractRealRedis runs the same contract
// against a real Redis, so the register and remove Lua are exercised outside
// miniredis' interpreter too.
func TestRedisRunnerDirectoryDescriptorContractRealRedis(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis descriptor contract")
	}
	runRunnerDescriptorDirectoryContract(t, func(t *testing.T) descriptorContractDirectory {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			_ = rdb.Close()
			t.Fatalf("ping real Redis at %s: %v", addr, err)
		}
		return newRealRedisRunnerDirectory(t, rdb)
	})
}

// TestRedisRunnerDirectoryDescriptorsWrittenWithSession pins that the
// descriptor field is part of the register transition: it is present exactly
// when the current session reported descriptors, and survives a directory
// recreation because Redis, not the process, holds it.
func TestRedisRunnerDirectoryDescriptorsWrittenWithSession(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	base := time.Unix(1_900_000_000, 0).UTC()

	contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
	raw, err := rdb.HGet(ctx, dir.keys.runnerDescriptors, "runner-1").Result()
	if err != nil || raw == "" {
		t.Fatalf("descriptor field = %q, err = %v; want the stored record", raw, err)
	}

	recreated := NewRedisRunnerDirectory(rdb)
	records, err := recreated.LiveRunnerDescriptors(ctx, base)
	if err != nil || len(records) != 1 || records[0].Descriptors[0].Hash != "hash-A" {
		t.Fatalf("recreated directory records = %+v, err = %v", records, err)
	}

	contractRegister(t, dir, "runner-1", base.Add(time.Second))
	if _, err := rdb.HGet(ctx, dir.keys.runnerDescriptors, "runner-1").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("descriptor field after an empty re-register: err = %v, want redis.Nil", err)
	}
}

func TestRedisRunnerDirectoryDescriptorsSkipCorruptOrSessionless(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	base := time.Unix(1_900_000_000, 0).UTC()

	contractRegister(t, dir, "runner-good", base, contractDescriptor("acme.a", 1, "A"))
	contractRegister(t, dir, "runner-bad", base, contractDescriptor("acme.a", 1, "A"))
	mr.HSet(dir.keys.runnerDescriptors, "runner-bad", "not-json{{")
	mr.HSet(dir.keys.runnerDescriptors, "runner-ghost", `{"registered_at_ms":0,"descriptors":[{"type":"acme.a","version":1,"hash":"h","descriptor":{}}]}`)

	records, err := dir.LiveRunnerDescriptors(ctx, base)
	if err != nil {
		t.Fatalf("LiveRunnerDescriptors: %v", err)
	}
	if len(records) != 1 || records[0].RunnerID != "runner-good" {
		t.Fatalf("records = %+v, want only runner-good", records)
	}
}

func TestRedisRunnerDirectoryDescriptorsPropagateRedisError(t *testing.T) {
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	mr.Close()
	if _, err := dir.LiveRunnerDescriptors(context.Background(), time.Now()); err == nil {
		t.Fatal("LiveRunnerDescriptors on a closed Redis returned nil error")
	}
}
