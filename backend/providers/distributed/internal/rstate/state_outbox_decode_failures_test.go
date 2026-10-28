package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// These tests read OutboxMetrics before and after seeding and compare the two
// snapshots instead of asserting absolute values, so the same bodies run
// unchanged against a real Redis that other tests have already written into.

// decodeFailureExecID returns an execution ID no other test uses and registers
// a cleanup that deletes only the keys that execution owns.
func decodeFailureExecID(t *testing.T, state *Store, label string) types.ExecutionID {
	t.Helper()
	id := types.ExecutionID(fmt.Sprintf("decode-failures-%s-%d", label, time.Now().UnixNano()))
	t.Cleanup(func() {
		ctx := context.Background()
		// The namespace-wide readiness index holds this execution as a member and
		// outlives its per-execution keys, so it has to be pruned separately.
		_ = state.rdb.ZRem(ctx, outboxReadyIndexKey(namespace.Default), string(id)).Err()
		keys, err := redisx.ScanAll(ctx, state.rdb, execKey(namespace.Default, id, "*"), 128)
		if err != nil || len(keys) == 0 {
			return
		}
		_ = state.rdb.Del(ctx, keys...).Err()
	})
	return id
}

// seedValidOutbox persists an execution with n well-formed outbox entries.
func seedValidOutbox(t *testing.T, state *Store, id types.ExecutionID, n int) {
	t.Helper()
	entries := make([]engine.OutboxEntry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, engine.OutboxEntry{
			ID: fmt.Sprintf("root/%s/start/%d", id, i),
			Task: engine.Task{
				ExecutionID: id,
				NodeName:    "start",
				NodeIdx:     i,
				Type:        engine.TaskTypeNodeExec,
			},
		})
	}
	if err := state.CreateExecutionWithOutbox(context.Background(), &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
	}, entries); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}
}

// seedBadOutboxBody adds a ready member whose stored body is not valid JSON.
func seedBadOutboxBody(t *testing.T, state *Store, id types.ExecutionID, member string) {
	t.Helper()
	ctx := context.Background()
	if err := state.rdb.HSet(ctx, outboxBodyKey(namespace.Default, id), member, "{not json").Err(); err != nil {
		t.Fatalf("HSet bad outbox body: %v", err)
	}
	if err := state.rdb.ZAdd(ctx, outboxReadyKey(namespace.Default, id), redis.Z{
		Score:  float64(time.Now().UTC().UnixMilli()),
		Member: member,
	}).Err(); err != nil {
		t.Fatalf("ZAdd bad outbox member: %v", err)
	}
}

func readOutboxMetrics(t *testing.T, state *Store) engine.OutboxMetricsSnapshot {
	t.Helper()
	snap, err := state.OutboxMetrics(context.Background())
	if err != nil {
		t.Fatalf("OutboxMetrics() error = %v", err)
	}
	return snap
}

func checkOutboxMetricsCountsDecodeFailures(t *testing.T, state *Store) {
	base := readOutboxMetrics(t, state)

	// Two executions, one with two undecodable bodies and one with a single one:
	// the count is a running total across the whole scan, not per execution.
	idA := decodeFailureExecID(t, state, "count-a")
	seedValidOutbox(t, state, idA, 1)
	seedBadOutboxBody(t, state, idA, "bad-a-1")
	seedBadOutboxBody(t, state, idA, "bad-a-2")
	idB := decodeFailureExecID(t, state, "count-b")
	seedValidOutbox(t, state, idB, 1)
	seedBadOutboxBody(t, state, idB, "bad-b-1")

	got := readOutboxMetrics(t, state)
	if delta := got.DecodeFailures - base.DecodeFailures; delta != 3 {
		t.Fatalf("DecodeFailures delta = %d (before %d, after %d), want 3", delta, base.DecodeFailures, got.DecodeFailures)
	}
}

func checkOutboxMetricsPendingUnaffectedByBadBody(t *testing.T, state *Store) {
	id := decodeFailureExecID(t, state, "pending")
	seedValidOutbox(t, state, id, 2)
	before := readOutboxMetrics(t, state)

	seedBadOutboxBody(t, state, id, "bad-pending-1")
	after := readOutboxMetrics(t, state)

	// The bad member is still a ready member: Pending is the ZSET cardinality
	// and moves by exactly one for it. The failure counter must not add a second.
	if delta := after.Pending - before.Pending; delta != 1 {
		t.Fatalf("Pending delta = %d (before %d, after %d), want 1 for the one added member", delta, before.Pending, after.Pending)
	}
	if delta := after.DecodeFailures - before.DecodeFailures; delta != 1 {
		t.Fatalf("DecodeFailures delta = %d, want 1", delta)
	}
	// A body that cannot be decoded has no CreatedAt to contribute, so the age of
	// the valid entries is untouched.
	if !after.OldestPendingAt.Equal(before.OldestPendingAt) {
		t.Fatalf("OldestPendingAt moved from %s to %s after adding an undecodable body",
			before.OldestPendingAt.Format(time.RFC3339Nano), after.OldestPendingAt.Format(time.RFC3339Nano))
	}
}

func checkOutboxMetricsZeroOnValidBodies(t *testing.T, state *Store) {
	base := readOutboxMetrics(t, state)

	id := decodeFailureExecID(t, state, "valid")
	seedValidOutbox(t, state, id, 3)

	got := readOutboxMetrics(t, state)
	if got.Pending-base.Pending != 3 {
		t.Fatalf("Pending delta = %d, want 3 so the scan actually visited the seeded entries", got.Pending-base.Pending)
	}
	if got.DecodeFailures != base.DecodeFailures {
		t.Fatalf("DecodeFailures moved from %d to %d with only well-formed bodies seeded", base.DecodeFailures, got.DecodeFailures)
	}
}

func TestOutboxMetricsCountsDecodeFailures(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	checkOutboxMetricsCountsDecodeFailures(t, state)
}

func TestOutboxMetricsPendingUnaffectedByBadBody(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	checkOutboxMetricsPendingUnaffectedByBadBody(t, state)
}

func TestOutboxMetricsZeroOnValidBodies(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	checkOutboxMetricsZeroOnValidBodies(t, state)
	// With nothing else in the keyspace the absolute value is zero too.
	if got := readOutboxMetrics(t, state); got.DecodeFailures != 0 {
		t.Fatalf("DecodeFailures = %d on a keyspace holding only valid bodies, want 0", got.DecodeFailures)
	}
}

// TestOutboxMetricsDecodeFailuresRealRedis runs the same three checks against a
// real Redis, to confirm the in-memory fake and the server agree on how an
// undecodable body is read back. ENV-gated.
func TestOutboxMetricsDecodeFailuresRealRedis(t *testing.T) {
	state := newRealRedisState(t, realRedisAddr(t))
	t.Run("counts_decode_failures", func(t *testing.T) { checkOutboxMetricsCountsDecodeFailures(t, state) })
	t.Run("pending_unaffected_by_bad_body", func(t *testing.T) { checkOutboxMetricsPendingUnaffectedByBadBody(t, state) })
	t.Run("zero_on_valid_bodies", func(t *testing.T) { checkOutboxMetricsZeroOnValidBodies(t, state) })
}
