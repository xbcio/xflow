package rstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// entryActivationIndexFaultClient is a redis.UniversalClient wrapper that fails
// selected commands on demand, simulating a partial Redis fault (one command
// family erroring) without a real failing server. A nil error delegates to the
// wrapped client.
type entryActivationIndexFaultClient struct {
	redis.UniversalClient
	sAddErr   error
	existsErr error
}

func (c *entryActivationIndexFaultClient) SAdd(ctx context.Context, key string, members ...any) *redis.IntCmd {
	if c.sAddErr != nil {
		return redis.NewIntResult(0, c.sAddErr)
	}
	return c.UniversalClient.SAdd(ctx, key, members...)
}

func (c *entryActivationIndexFaultClient) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	if c.existsErr != nil {
		return redis.NewIntResult(0, c.existsErr)
	}
	return c.UniversalClient.Exists(ctx, keys...)
}

func newEntryActivationIndexFaultClient(t *testing.T) (*entryActivationIndexFaultClient, *redis.Client) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)
	base := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = base.Close() })
	return &entryActivationIndexFaultClient{UniversalClient: base}, base
}

// assertEntryActivationIndexTTLCoversMembers asserts the invariant the write
// path must preserve: the index key must not expire before any member record it
// has to expose. A permanent index covers everything; a permanent member
// requires a permanent index; otherwise the index's remaining TTL must be at
// least the member's. go-redis maps Redis' PTTL -1 (no expiry) and -2 (missing
// key) replies to time.Duration(-1) and time.Duration(-2).
func assertEntryActivationIndexTTLCoversMembers(t *testing.T, rdb *redis.Client, indexKey string, members ...string) {
	t.Helper()
	ctx := context.Background()
	indexTTL, err := rdb.PTTL(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("PTTL %q: %v", indexKey, err)
	}
	if indexTTL == -2 {
		t.Fatalf("index key %q does not exist", indexKey)
	}
	for _, member := range members {
		memberTTL, err := rdb.PTTL(ctx, member).Result()
		if err != nil {
			t.Fatalf("PTTL %q: %v", member, err)
		}
		switch {
		case memberTTL == -2:
			// The member is gone; there is nothing left to cover.
		case memberTTL == -1:
			if indexTTL != -1 {
				t.Fatalf("index TTL %v would expire under the permanent member %q", indexTTL, member)
			}
		case indexTTL == -1:
			// A permanent index covers a TTL'd member.
		default:
			if indexTTL < memberTTL {
				t.Fatalf("index TTL %v is shorter than member %q remaining TTL %v", indexTTL, member, memberTTL)
			}
		}
	}
}

// The workflow enumeration set is the one index link with no rebuild behind it
// once the ready gate is set, so tag registration is fail-closed: a failed SADD
// aborts the write instead of dropping the tag, and the abort leaves nothing
// half-applied — the record does not exist afterwards, and a retry once Redis
// recovers succeeds.
func TestEntryActivationIndexTagRegistrationIsFailClosed(t *testing.T) {
	ctx := context.Background()

	t.Run("upsert aborts before writing the record", func(t *testing.T) {
		client, base := newEntryActivationIndexFaultClient(t)
		store := NewEntryActivationStore(client, time.Hour)
		act := engine.EntryActivation{
			Namespace:       "index-failclosed-upsert-ns",
			WorkflowID:      "wf-failclosed-upsert",
			WorkflowVersion: "v1",
			EntryUnitID:     "entry",
			PackageHash:     "pkg-failclosed",
			Desired:         true,
		}
		key := entryActivationKeyFromActivation(act)

		client.sAddErr = errors.New("sadd unavailable")
		if err := store.Upsert(ctx, act); err == nil {
			t.Fatal("Upsert must fail when the workflow tag cannot be registered")
		}
		if exists, err := base.Exists(ctx, store.keyFor(key)).Result(); err != nil || exists != 0 {
			t.Fatalf("record must not be written after the tag registration failed: exists=%d err=%v", exists, err)
		}
		if exists, err := base.Exists(ctx, entryActivationWorkflowSetRedisKey(act.Namespace)).Result(); err != nil || exists != 0 {
			t.Fatalf("the half-applied enumeration set must not exist: exists=%d err=%v", exists, err)
		}

		client.sAddErr = nil
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert after the fault cleared: %v", err)
		}
		if exists, err := base.Exists(ctx, store.keyFor(key)).Result(); err != nil || exists != 1 {
			t.Fatalf("record after the retry: exists=%d err=%v", exists, err)
		}
	})

	t.Run("assign aborts before promoting a legacy record", func(t *testing.T) {
		client, base := newEntryActivationIndexFaultClient(t)
		store := NewEntryActivationStore(client, time.Hour)
		act := engine.EntryActivation{
			Namespace:       "index-failclosed-assign-ns",
			WorkflowID:      "wf-failclosed-assign",
			WorkflowVersion: "v1",
			EntryUnitID:     "entry",
			PackageHash:     "pkg-failclosed",
			Desired:         true,
		}
		seedLegacyEntryActivation(t, base, store, act, nil)
		key := entryActivationKeyFromActivation(act)

		client.sAddErr = errors.New("sadd unavailable")
		applied, err := store.Assign(ctx, key, "runner-1", "session-1", 1, time.Now().Add(time.Minute))
		if err == nil || applied {
			t.Fatalf("Assign must fail closed when the tag cannot be registered: applied=%v err=%v", applied, err)
		}
		if exists, err := base.Exists(ctx, store.keyFor(key)).Result(); err != nil || exists != 0 {
			t.Fatalf("assign must not promote the legacy record into the modern key: exists=%d err=%v", exists, err)
		}
		// The seeded legacy record has no runner_id field, so redis.Nil is the
		// untouched result.
		got, err := base.HGet(ctx, store.legacyKeyFor(key), "runner_id").Result()
		if err != nil && err != redis.Nil {
			t.Fatalf("read legacy runner_id: %v", err)
		}
		if got != "" {
			t.Fatalf("assign must leave the legacy record untouched: runner_id=%q", got)
		}

		client.sAddErr = nil
		applied, err = store.Assign(ctx, key, "runner-1", "session-1", 1, time.Now().Add(time.Minute))
		if err != nil || !applied {
			t.Fatalf("Assign after the fault cleared: applied=%v err=%v", applied, err)
		}
		if got, err := base.HGet(ctx, store.keyFor(key), "runner_id").Result(); err != nil || got != "runner-1" {
			t.Fatalf("modern record after the retry: runner_id=%q err=%v", got, err)
		}
	})
}

// A tag lost from the enumeration set (a path this process never observed)
// hides the workflow from the indexed read; the next successful write must
// restore it, because every write re-registers its tag unconditionally.
func TestEntryActivationIndexTagLossIsRestoredByTheNextWrite(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-tag-loss-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-tag-loss",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-tag-loss",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuildEntryActivationIndex: %v", err)
	}

	listed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (steady state): %v", err)
	}
	if len(listed) != 1 || listed[0].EntryUnitID != act.EntryUnitID {
		t.Fatalf("steady-state List lost the record: %+v", listed)
	}

	tag := entryActivationWorkflowTag(ns, act.WorkflowID)
	if err := rdb.SRem(ctx, entryActivationWorkflowSetRedisKey(ns), tag).Err(); err != nil {
		t.Fatalf("drop workflow tag: %v", err)
	}
	listed, err = store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (tag lost): %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("a lost tag must hide the workflow from the indexed read: %+v", listed)
	}

	restored := act
	restored.PackageHash = "pkg-tag-restored"
	if err := store.Upsert(ctx, restored); err != nil {
		t.Fatalf("Upsert (restore): %v", err)
	}
	listed, err = store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (restored): %v", err)
	}
	if len(listed) != 1 || listed[0].PackageHash != "pkg-tag-restored" {
		t.Fatalf("the write did not restore the workflow tag: %+v", listed)
	}
}

// The write path may only ever extend an index TTL. A rebuild derives each
// index expiry from its members' remaining TTLs, and shortening that derivation
// with the current store TTL would let the index expire before a record written
// under a longer previous TTL; leaving a permanent index alone preserves the
// same derivation for members that never expire. Both the upsert tail and
// touch_activation (through Assign) must honor that rule.
func TestEntryActivationIndexTTLExtendOnly(t *testing.T) {
	ctx := context.Background()
	newFixture := func(t *testing.T) (*EntryActivationStore, *redis.Client, engine.EntryActivation, string) {
		t.Helper()
		store, rdb, _ := newEntryActivationIndexTestStore(t, 24*time.Hour)
		act := engine.EntryActivation{
			Namespace:       "index-extend-only-ns",
			WorkflowID:      "wf-extend-only",
			WorkflowVersion: "v1",
			EntryUnitID:     "entry",
			PackageHash:     "pkg-extend-only",
			Desired:         true,
		}
		return store, rdb, act, store.keyFor(entryActivationKeyFromActivation(act))
	}

	t.Run("upsert does not shorten a longer index ttl", func(t *testing.T) {
		store, rdb, act, member := newFixture(t)
		indexKey := entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)
		// An index derived when a longer TTL was in effect (or by a rebuild over
		// a member written under a longer previous TTL).
		if err := rdb.SAdd(ctx, indexKey, member).Err(); err != nil {
			t.Fatalf("preseed index member: %v", err)
		}
		if err := rdb.PExpire(ctx, indexKey, 48*time.Hour).Err(); err != nil {
			t.Fatalf("preseed index ttl: %v", err)
		}

		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		pttl, err := rdb.PTTL(ctx, indexKey).Result()
		if err != nil {
			t.Fatalf("PTTL %q: %v", indexKey, err)
		}
		if pttl <= 47*time.Hour {
			t.Fatalf("the write shortened a longer index TTL: %v, want ~48h", pttl)
		}
		assertEntryActivationIndexTTLCoversMembers(t, rdb, indexKey, member)
	})

	t.Run("assign does not shorten a longer index ttl", func(t *testing.T) {
		store, rdb, act, member := newFixture(t)
		indexKey := entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)
		if err := rdb.SAdd(ctx, indexKey, member).Err(); err != nil {
			t.Fatalf("preseed index member: %v", err)
		}
		if err := rdb.PExpire(ctx, indexKey, 48*time.Hour).Err(); err != nil {
			t.Fatalf("preseed index ttl: %v", err)
		}
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		key := entryActivationKeyFromActivation(act)
		applied, err := store.Assign(ctx, key, "runner-1", "session-1", 1, time.Now().Add(time.Minute))
		if err != nil || !applied {
			t.Fatalf("Assign: applied=%v err=%v", applied, err)
		}
		pttl, err := rdb.PTTL(ctx, indexKey).Result()
		if err != nil {
			t.Fatalf("PTTL %q: %v", indexKey, err)
		}
		if pttl <= 47*time.Hour {
			t.Fatalf("the transition shortened a longer index TTL: %v, want ~48h", pttl)
		}
		assertEntryActivationIndexTTLCoversMembers(t, rdb, indexKey, member)
	})

	t.Run("upsert leaves a permanent index permanent", func(t *testing.T) {
		store, rdb, act, member := newFixture(t)
		indexKey := entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)
		if err := rdb.SAdd(ctx, indexKey, member).Err(); err != nil {
			t.Fatalf("preseed permanent index: %v", err)
		}

		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if ttl, err := rdb.PTTL(ctx, indexKey).Result(); err != nil || ttl != -1 {
			t.Fatalf("a permanent index acquired a TTL: ttl=%v err=%v, want -1", ttl, err)
		}
	})

	t.Run("assign leaves a permanent index permanent", func(t *testing.T) {
		store, rdb, act, member := newFixture(t)
		indexKey := entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)
		if err := rdb.SAdd(ctx, indexKey, member).Err(); err != nil {
			t.Fatalf("preseed permanent index: %v", err)
		}
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		key := entryActivationKeyFromActivation(act)
		applied, err := store.Assign(ctx, key, "runner-1", "session-1", 1, time.Now().Add(time.Minute))
		if err != nil || !applied {
			t.Fatalf("Assign: applied=%v err=%v", applied, err)
		}
		if ttl, err := rdb.PTTL(ctx, indexKey).Result(); err != nil || ttl != -1 {
			t.Fatalf("a permanent index acquired a TTL: ttl=%v err=%v, want -1", ttl, err)
		}
	})
}

// A failed readiness probe degrades List to the scan path instead of failing
// the read: the reconciler aborts its whole pass on a List error, and the scan
// result is the pre-index behaviour — correct, just slower. The failed probe
// must not be answered with a rebuild either, so a Redis erroring on EXISTS is
// not asked to SCAN for it too.
func TestEntryActivationListFallsBackWhenTheReadinessProbeFails(t *testing.T) {
	ctx := context.Background()
	client, base := newEntryActivationIndexFaultClient(t)
	healthy := NewEntryActivationStore(base, time.Hour)
	faulty := NewEntryActivationStore(client, time.Hour)
	ns := namespace.Namespace("index-probe-fallback-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-probe-fallback",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-probe-fallback",
		Desired:         true,
	}
	if err := healthy.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	client.existsErr = errors.New("exists unavailable")
	listed, err := faulty.List(ctx, ns)
	if err != nil {
		t.Fatalf("List must degrade to the scan path when the readiness probe fails: %v", err)
	}
	if len(listed) != 1 || listed[0].EntryUnitID != act.EntryUnitID {
		t.Fatalf("scan-path fallback lost the record: %+v", listed)
	}
	// Give a wrongly-triggered rebuild goroutine time to take its lock and flip
	// the gate before asserting neither happened.
	time.Sleep(100 * time.Millisecond)
	if exists, err := base.Exists(ctx, entryActivationIndexRebuildLockRedisKey(ns)).Result(); err != nil || exists != 0 {
		t.Fatalf("a failed probe must not start a rebuild: lock exists=%d err=%v", exists, err)
	}
	if exists, err := base.Exists(ctx, entryActivationIndexReadyRedisKey(ns)).Result(); err != nil || exists != 0 {
		t.Fatalf("a failed probe must not flip the ready gate: exists=%d err=%v", exists, err)
	}
}

// Deleting the ready gate is the manual recovery lever for index state lost
// after the gate was set: List serves the index alone, so a dropped member
// hides its record until the gate is cleared and a rebuild re-derives every
// set from a full scan.
func TestEntryActivationIndexManualRecoveryLeversRebuildsLostState(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-manual-lever-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-manual-lever",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-manual-lever",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuildEntryActivationIndex: %v", err)
	}
	listed, err := store.List(ctx, ns)
	if err != nil || len(listed) != 1 {
		t.Fatalf("steady-state List: %+v err=%v", listed, err)
	}

	// Simulate index state lost while the ready gate stays set: the record
	// exists and every write path is healthy, but the index no longer names it.
	key := entryActivationKeyFromActivation(act)
	indexKey := entryActivationWorkflowIndexRedisKey(ns, act.WorkflowID)
	if err := rdb.SRem(ctx, indexKey, store.keyFor(key)).Err(); err != nil {
		t.Fatalf("drop index member: %v", err)
	}
	listed, err = store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (member lost): %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("the indexed read must be the only source once ready: %+v", listed)
	}

	if err := rdb.Del(ctx, entryActivationIndexReadyRedisKey(ns)).Err(); err != nil {
		t.Fatalf("clear ready gate: %v", err)
	}
	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuild after clearing the gate: %v", err)
	}
	listed, err = store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (after recovery): %v", err)
	}
	if len(listed) != 1 || listed[0].EntryUnitID != act.EntryUnitID {
		t.Fatalf("the recovery rebuild did not restore the record: %+v", listed)
	}
}

// The indexed path must agree with the scan path for namespaces whose raw form
// contains Redis glob metacharacters: the modern layout escapes the namespace
// and stays exactly matchable, while the legacy layout's pattern is built from
// the raw namespace. The legacy-only record's visibility therefore follows the
// raw pattern's existing (pre-index) semantics — invisible when "?" or "["
// makes the pattern miss its own key — and the rebuild must reproduce the scan
// path exactly, not "improve" it.
func TestEntryActivationIndexEscapedNamespaceParity(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		ns            string
		legacyVisible bool
	}{
		{ns: "ns with space/slash", legacyVisible: true},
		{ns: "ns*star?q[b]", legacyVisible: false},
		{ns: "ns{a}b}c", legacyVisible: true},
		{ns: "ns%percent", legacyVisible: true},
	}
	for _, tc := range cases {
		t.Run(tc.ns, func(t *testing.T) {
			store, rdb := newEntryActivationRevisionRedisStore(t)
			ns := namespace.Namespace(tc.ns)
			modern := engine.EntryActivation{
				Namespace:       ns,
				WorkflowID:      "wf-escaped",
				WorkflowVersion: "v1",
				EntryUnitID:     "modern-entry",
				PackageHash:     "pkg-modern",
				Desired:         true,
			}
			legacyOnly := modern
			legacyOnly.EntryUnitID = "legacy-entry"
			legacyOnly.PackageHash = "pkg-legacy"
			seedLegacyEntryActivation(t, rdb, store, legacyOnly, nil)
			if err := store.Upsert(ctx, modern); err != nil {
				t.Fatalf("Upsert: %v", err)
			}

			scanned, err := store.scanEntryActivations(ctx, ns)
			if err != nil {
				t.Fatalf("scanEntryActivations: %v", err)
			}
			wantLen := 1
			if tc.legacyVisible {
				wantLen = 2
			}
			if len(scanned) != wantLen {
				t.Fatalf("scan path returned %d activations, want %d: %+v", len(scanned), wantLen, scanned)
			}
			if !entryActivationSetContainsUnit(scanned, "modern-entry") {
				t.Fatalf("the escaped modern key must be visible for every namespace: %+v", scanned)
			}

			if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
				t.Fatalf("rebuildEntryActivationIndex: %v", err)
			}
			indexed, err := store.List(ctx, ns)
			if err != nil {
				t.Fatalf("List (indexed): %v", err)
			}
			compareEntryActivationSets(t, scanned, indexed)
		})
	}
}

func entryActivationSetContainsUnit(activations []engine.EntryActivation, unit string) bool {
	for _, act := range activations {
		if act.EntryUnitID == unit {
			return true
		}
	}
	return false
}
