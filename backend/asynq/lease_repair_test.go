package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRepairLeaseIndexRestoresMissingCorrectsMismatchAndPrunesStale(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	issued := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	const leaseTTL = 30 * time.Second
	mustUpsertRunning(t, state, "repair-exec", "active", "token-active", issued, leaseTTL)

	index := leaseExpiryZSetKey("repair-exec")
	member := leaseExpiryMember("repair-exec", "active")
	if err := rdb.Del(ctx, index).Err(); err != nil {
		t.Fatalf("delete lease index: %v", err)
	}
	if reconciled, err := state.RepairLeaseIndex(ctx, 16); err != nil || reconciled != 1 {
		t.Fatalf("RepairLeaseIndex() reconciled=%d err=%v, want 1/nil", reconciled, err)
	}
	wantDeadline := float64(issued.Add(leaseTTL).UnixMilli())
	if got, err := rdb.ZScore(ctx, index, member).Result(); err != nil || got != wantDeadline {
		t.Fatalf("restored index score=%v err=%v, want %v/nil", got, err, wantDeadline)
	}

	if err := rdb.ZAdd(ctx, index, redis.Z{Score: wantDeadline + float64(time.Hour.Milliseconds()), Member: member}).Err(); err != nil {
		t.Fatalf("write mismatched index score: %v", err)
	}
	if reconciled, err := state.RepairLeaseIndex(ctx, 16); err != nil || reconciled != 1 {
		t.Fatalf("RepairLeaseIndex() mismatch reconciled=%d err=%v, want 1/nil", reconciled, err)
	}
	if got, err := rdb.ZScore(ctx, index, member).Result(); err != nil || got != wantDeadline {
		t.Fatalf("corrected index score=%v err=%v, want %v/nil", got, err, wantDeadline)
	}

	if err := rdb.Set(ctx, nodeStatusKey("repair-exec", "active"), "success", time.Minute).Err(); err != nil {
		t.Fatalf("mark node terminal: %v", err)
	}
	if reconciled, err := state.RepairLeaseIndex(ctx, 16); err != nil || reconciled != 0 {
		t.Fatalf("RepairLeaseIndex() stale reconciled=%d err=%v, want 0/nil", reconciled, err)
	}
	if _, err := rdb.ZScore(ctx, index, member).Result(); err != redis.Nil {
		t.Fatalf("terminal lease member remains indexed: %v", err)
	}
}

func TestRepairLeaseIndexBackfillsLegacyDeadline(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	issued := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	const leaseTTL = 15 * time.Second
	mustUpsertRunning(t, state, "legacy-repair", "active", "token-active", issued, leaseTTL)

	if err := rdb.HDel(ctx, nodeMetaKey("legacy-repair", "active"), "lease_deadline_ms").Err(); err != nil {
		t.Fatalf("delete legacy deadline: %v", err)
	}
	if err := rdb.Del(ctx, leaseExpiryZSetKey("legacy-repair")).Err(); err != nil {
		t.Fatalf("delete index: %v", err)
	}
	if reconciled, err := state.RepairLeaseIndex(ctx, 16); err != nil || reconciled != 1 {
		t.Fatalf("RepairLeaseIndex() reconciled=%d err=%v, want 1/nil", reconciled, err)
	}
	deadline, err := rdb.HGet(ctx, nodeMetaKey("legacy-repair", "active"), "lease_deadline_ms").Int64()
	if err != nil {
		t.Fatalf("read backfilled deadline: %v", err)
	}
	want := issued.Add(leaseTTL).UnixMilli()
	if deadline != want {
		t.Fatalf("backfilled deadline=%d, want %d", deadline, want)
	}
}
