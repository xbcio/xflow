package rstate

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func newEntryActivationIndexTestStore(t *testing.T, ttl time.Duration) (*EntryActivationStore, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewEntryActivationStore(rdb, ttl), rdb, srv
}

func waitForEntryActivationIndexCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func seedLegacyEntryActivation(t *testing.T, rdb *redis.Client, store *EntryActivationStore, act engine.EntryActivation, fields map[string]any) {
	t.Helper()
	seed := map[string]any{
		"namespace":        string(act.Namespace),
		"workflow_id":      string(act.WorkflowID),
		"workflow_version": act.WorkflowVersion,
		"entry_unit_id":    act.EntryUnitID,
		"node_type":        "kafka.source",
		"package_hash":     "pkg-legacy",
		"desired":          "1",
	}
	for name, value := range fields {
		seed[name] = value
	}
	if err := rdb.HSet(context.Background(), store.legacyKeyFor(entryActivationKeyFromActivation(act)), seed).Err(); err != nil {
		t.Fatalf("seed legacy activation: %v", err)
	}
}

// Upsert must maintain the per-workflow index and the workflow enumeration set
// in the same write: the index carries the modern key name, the enumeration
// carries the digest tag, and both stay alive at least as long as the record.
func TestEntryActivationIndexMaintainedOnUpsert(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	act := engine.EntryActivation{
		Namespace:       "index-upsert-ns",
		WorkflowID:      "wf-index-upsert",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		NodeType:        "http.request",
		PackageHash:     "pkg-upsert",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	indexKey := entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)
	members, err := rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("SMEMBERS %q: %v", indexKey, err)
	}
	wantMember := store.keyFor(entryActivationKeyFromActivation(act))
	if len(members) != 1 || members[0] != wantMember {
		t.Fatalf("workflow index members = %v, want [%q]", members, wantMember)
	}

	tags, err := rdb.SMembers(ctx, entryActivationWorkflowSetRedisKey(act.Namespace)).Result()
	if err != nil {
		t.Fatalf("SMEMBERS workflow enumeration set: %v", err)
	}
	wantTag := entryActivationWorkflowTag(act.Namespace, act.WorkflowID)
	if len(tags) != 1 || tags[0] != wantTag {
		t.Fatalf("workflow enumeration set = %v, want [%q]", tags, wantTag)
	}

	// The load-bearing invariant is one-sided: the index must never expire
	// before a record it has to expose, so its remaining TTL is at least its
	// member's. A longer index TTL is legal — a rebuild derives it from a member
	// written under a longer previous TTL — and a range assertion like (0, ttl]
	// would pin the write path to shortening that derivation.
	assertEntryActivationIndexTTLCoversMembers(t, rdb, indexKey, wantMember)
}

// A legacy-only record promoted by Assign/Renew/Fence must land in the
// per-workflow index in the same Lua call that creates the modern record — in
// a promotion the SADD cannot come from the Upsert path, which never ran.
func TestEntryActivationIndexMaintainedOnLegacyPromotion(t *testing.T) {
	transitions := []struct {
		name string
		run  func(context.Context, *EntryActivationStore, engine.EntryActivationKey, time.Time) error
	}{
		{
			name: "assign",
			run: func(ctx context.Context, store *EntryActivationStore, key engine.EntryActivationKey, deadline time.Time) error {
				ok, err := store.Assign(ctx, key, "runner-new", "session-new", 5, deadline)
				if err == nil && !ok {
					err = fmt.Errorf("Assign returned false")
				}
				return err
			},
		},
		{
			name: "renew",
			run: func(ctx context.Context, store *EntryActivationStore, key engine.EntryActivationKey, deadline time.Time) error {
				ok, err := store.Renew(ctx, key, 4, deadline)
				if err == nil && !ok {
					err = fmt.Errorf("Renew returned false")
				}
				return err
			},
		},
		{
			name: "fence",
			run: func(ctx context.Context, store *EntryActivationStore, key engine.EntryActivationKey, _ time.Time) error {
				return store.Fence(ctx, key, 4)
			},
		},
	}

	for _, transition := range transitions {
		t.Run(transition.name, func(t *testing.T) {
			ctx := context.Background()
			store, rdb := newEntryActivationRevisionRedisStore(t)
			act := engine.EntryActivation{
				Namespace:       namespace.Namespace("index-promotion-ns-" + transition.name),
				WorkflowID:      "wf-index-promotion",
				WorkflowVersion: "v1",
				EntryUnitID:     "entry",
			}
			key := entryActivationKeyFromActivation(act)
			seedLegacyEntryActivation(t, rdb, store, act, map[string]any{
				"runner_id":      "runner-legacy",
				"session_id":     "session-legacy",
				"generation":     "4",
				"lease_deadline": "0",
			})

			if err := transition.run(ctx, store, key, time.Now().Add(time.Minute)); err != nil {
				t.Fatalf("%s legacy activation: %v", transition.name, err)
			}

			members, err := rdb.SMembers(ctx, entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)).Result()
			if err != nil {
				t.Fatalf("SMEMBERS workflow index: %v", err)
			}
			wantMember := store.keyFor(key)
			if len(members) != 1 || members[0] != wantMember {
				t.Fatalf("workflow index members after %s = %v, want [%q]", transition.name, members, wantMember)
			}

			tags, err := rdb.SMembers(ctx, entryActivationWorkflowSetRedisKey(act.Namespace)).Result()
			if err != nil {
				t.Fatalf("SMEMBERS workflow enumeration set: %v", err)
			}
			wantTag := entryActivationWorkflowTag(act.Namespace, act.WorkflowID)
			if len(tags) != 1 || tags[0] != wantTag {
				t.Fatalf("workflow enumeration set after %s = %v, want [%q]", transition.name, tags, wantTag)
			}
		})
	}
}

// The indexed read path must return exactly what the scan path returned for the
// same store: mixed modern/legacy layouts, a duplicated identity, sibling
// replicas, a watermark-stale record, and a dangling index member.
func TestEntryActivationIndexedListMatchesScannedList(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-parity-ns")

	base := engine.EntryActivation{
		Namespace:        ns,
		WorkflowID:       "wf-parity-a",
		WorkflowVersion:  "v1",
		EntryUnitID:      "entry-a",
		NodeType:         "http.request",
		PackageHash:      "pkg-a",
		Desired:          true,
		RegistryRevision: 3,
	}
	replica := base
	replica.ReplicaIndex = 1
	duplicated := base
	duplicated.EntryUnitID = "entry-dup"
	duplicated.PackageHash = "pkg-dup-modern"
	legacyOnly := base
	legacyOnly.EntryUnitID = "entry-legacy"
	stale := base
	stale.WorkflowID = "wf-parity-b"
	stale.EntryUnitID = "entry-b"
	stale.RegistryRevision = 1

	// Legacy hashes exist before the modern records, as during the layout
	// transition: the duplicated identity must collapse to the modern record.
	seedLegacyEntryActivation(t, rdb, store, duplicated, map[string]any{"package_hash": "pkg-dup-legacy"})
	seedLegacyEntryActivation(t, rdb, store, legacyOnly, map[string]any{"package_hash": "pkg-legacy-only"})

	for _, act := range []engine.EntryActivation{base, replica, duplicated, stale} {
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert %s/%s: %v", act.WorkflowID, act.EntryUnitID, err)
		}
	}
	if err := store.AdvanceWorkflowRevision(ctx, ns, stale.WorkflowID, 2); err != nil {
		t.Fatalf("AdvanceWorkflowRevision: %v", err)
	}

	// A dangling member: the index names a record that does not exist. It must
	// survive the rebuild (which only adds) and be skipped on read.
	dangling := "xflow:ns:" + entryActivationNamespacePath(ns) +
		":entryact:{" + entryActivationWorkflowTag(ns, stale.WorkflowID) + "}:act:deadbeef:replica:0"
	if err := rdb.SAdd(ctx, entryActivationWorkflowIndexRedisKey(ns, stale.WorkflowID), dangling).Err(); err != nil {
		t.Fatalf("seed dangling index member: %v", err)
	}

	scanned, err := store.scanEntryActivations(ctx, ns)
	if err != nil {
		t.Fatalf("scanEntryActivations: %v", err)
	}
	if len(scanned) != 5 {
		t.Fatalf("scan fixture returned %d activations, want 5: %+v", len(scanned), scanned)
	}

	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuildEntryActivationIndex: %v", err)
	}
	if exists, err := rdb.Exists(ctx, entryActivationIndexReadyRedisKey(ns)).Result(); err != nil || exists != 1 {
		t.Fatalf("ready gate after rebuild: exists=%d err=%v", exists, err)
	}
	if exists, err := rdb.Exists(ctx, entryActivationIndexRebuildLockRedisKey(ns)).Result(); err != nil || exists != 0 {
		t.Fatalf("rebuild must release its lock on success: exists=%d err=%v", exists, err)
	}

	indexed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (indexed): %v", err)
	}
	compareEntryActivationSets(t, scanned, indexed)

	// The duplicated identity must expose the modern record's payload.
	for _, act := range indexed {
		if act.EntryUnitID == "entry-dup" && act.PackageHash != "pkg-dup-modern" {
			t.Fatalf("duplicated identity lost modern priority: %+v", act)
		}
		if act.EntryUnitID == "entry-b" && act.Desired {
			t.Fatalf("watermark-stale record listed as desired: %+v", act)
		}
	}

	// A second rebuild over a live index is a no-op for readers.
	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	again, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List after second rebuild: %v", err)
	}
	compareEntryActivationSets(t, scanned, again)
}

// Indexed reads must skip members whose record is gone, and they must really be
// reading the index: removing a live member hides its record until the next
// write or rebuild puts it back.
func TestEntryActivationIndexedListSkipsDanglingMembers(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-dangling-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-dangling",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-dangling",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	key := entryActivationKeyFromActivation(act)
	indexKey := entryActivationWorkflowIndexRedisKey(ns, act.WorkflowID)

	dangling := "xflow:ns:" + entryActivationNamespacePath(ns) +
		":entryact:{" + entryActivationWorkflowTag(ns, act.WorkflowID) + "}:act:deadbeef:replica:0"
	if err := rdb.SAdd(ctx, indexKey, dangling).Err(); err != nil {
		t.Fatalf("seed dangling index member: %v", err)
	}
	if err := rdb.Set(ctx, entryActivationIndexReadyRedisKey(ns), "1", 0).Err(); err != nil {
		t.Fatalf("flip ready gate: %v", err)
	}

	listed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List with dangling member: %v", err)
	}
	if len(listed) != 1 || listed[0].EntryUnitID != act.EntryUnitID {
		t.Fatalf("dangling member surfaced or hid the live record: %+v", listed)
	}

	if err := rdb.SRem(ctx, indexKey, store.keyFor(key)).Err(); err != nil {
		t.Fatalf("remove live index member: %v", err)
	}
	listed, err = store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List after member removal: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("List did not read the index (record still visible after its member was removed): %+v", listed)
	}
}

// Every write must refresh the index TTL together with the record's, or an
// index written once and refreshed often would expire out from under a live
// record. The refreshed TTL is subject to the extend-only rule — it may only
// grow to this write's TTL, never shrink below a member's remaining TTL — so
// the real invariant asserted here is "index TTL ≥ member remaining TTL" after
// every write, not a range.
func TestEntryActivationIndexTTLRefreshesOnWrite(t *testing.T) {
	ctx := context.Background()
	store, rdb, srv := newEntryActivationIndexTestStore(t, time.Hour)
	act := engine.EntryActivation{
		Namespace:       "index-ttl-ns",
		WorkflowID:      "wf-ttl",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-ttl",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	indexKey := entryActivationWorkflowIndexRedisKey(act.Namespace, act.WorkflowID)
	member := store.keyFor(entryActivationKeyFromActivation(act))
	assertEntryActivationIndexTTLCoversMembers(t, rdb, indexKey, member)
	initial, err := rdb.TTL(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("TTL initial: %v", err)
	}

	srv.FastForward(30 * time.Minute)
	aged, err := rdb.TTL(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("TTL aged: %v", err)
	}
	if aged >= initial {
		t.Fatalf("index TTL did not decay with its record: initial=%v aged=%v", initial, aged)
	}
	if _, ok, err := store.Get(ctx, entryActivationKeyFromActivation(act)); err != nil || !ok {
		t.Fatalf("record must outlive the fast-forward: ok=%v err=%v", ok, err)
	}
	assertEntryActivationIndexTTLCoversMembers(t, rdb, indexKey, member)

	refreshed := act
	refreshed.PackageHash = "pkg-ttl-rewritten"
	if err := store.Upsert(ctx, refreshed); err != nil {
		t.Fatalf("Upsert (refresh): %v", err)
	}
	afterWrite, err := rdb.TTL(ctx, indexKey).Result()
	if err != nil {
		t.Fatalf("TTL after write: %v", err)
	}
	if afterWrite <= aged {
		t.Fatalf("write did not refresh the index TTL: aged=%v after=%v", aged, afterWrite)
	}
	assertEntryActivationIndexTTLCoversMembers(t, rdb, indexKey, member)
}

// List must keep serving the scan result and ask for a rebuild exactly once the
// ready gate is missing; the rebuild then flips the gate and later calls take
// the indexed path.
func TestEntryActivationListTriggersIndexRebuild(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-trigger-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-trigger",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-trigger",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	readyKey := entryActivationIndexReadyRedisKey(ns)
	if exists, err := rdb.Exists(ctx, readyKey).Result(); err != nil || exists != 0 {
		t.Fatalf("ready gate must start unset: exists=%d err=%v", exists, err)
	}

	listed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (scan path): %v", err)
	}
	if len(listed) != 1 || listed[0].EntryUnitID != act.EntryUnitID {
		t.Fatalf("scan path lost the record: %+v", listed)
	}

	waitForEntryActivationIndexCondition(t, func() bool {
		exists, err := rdb.Exists(ctx, readyKey).Result()
		return err == nil && exists == 1
	}, "the background rebuild to flip the ready gate")

	members, err := rdb.SMembers(ctx, entryActivationWorkflowIndexRedisKey(ns, act.WorkflowID)).Result()
	if err != nil {
		t.Fatalf("SMEMBERS workflow index: %v", err)
	}
	if wantMember := store.keyFor(entryActivationKeyFromActivation(act)); len(members) != 1 || members[0] != wantMember {
		t.Fatalf("rebuilt index members = %v, want [%q]", members, wantMember)
	}

	indexed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List (indexed path): %v", err)
	}
	compareEntryActivationSets(t, listed, indexed)
}

// A rebuild that loses the lock must touch nothing: another process owns the
// rebuild, and deleting its lock would let a third one run concurrently.
func TestEntryActivationIndexRebuildHonorsLock(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-lock-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-lock",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-lock",
		Desired:         true,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	lockKey := entryActivationIndexRebuildLockRedisKey(ns)
	if err := rdb.Set(ctx, lockKey, "other-process", entryActivationIndexRebuildLockTTL).Err(); err != nil {
		t.Fatalf("seed rebuild lock: %v", err)
	}

	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuild with the lock held: %v", err)
	}
	if exists, err := rdb.Exists(ctx, entryActivationIndexReadyRedisKey(ns)).Result(); err != nil || exists != 0 {
		t.Fatalf("rebuild must not flip the ready gate while another process holds the lock: exists=%d err=%v", exists, err)
	}
	if got, err := rdb.Get(ctx, lockKey).Result(); err != nil || got != "other-process" {
		t.Fatalf("rebuild must leave the other process's lock alone: value=%q err=%v", got, err)
	}
}

// An empty namespace is a valid rebuild result: the ready gate flips, the
// indexed read returns an empty set, and neither path writes a stray index key.
func TestEntryActivationIndexEmptyNamespaceRebuild(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-empty-ns")

	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuildEntryActivationIndex on empty namespace: %v", err)
	}
	if exists, err := rdb.Exists(ctx, entryActivationIndexReadyRedisKey(ns)).Result(); err != nil || exists != 1 {
		t.Fatalf("ready gate after empty rebuild: exists=%d err=%v", exists, err)
	}

	listed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List on empty namespace: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("empty namespace listed %d activations: %+v", len(listed), listed)
	}
}

// The rebuild splits one scan's keys into modern and legacy by key name alone,
// so the classifier must accept exactly the modern layout of this namespace and
// nothing that merely resembles it.
func TestEntryActivationIndexKeyTagClassification(t *testing.T) {
	ns := namespace.Namespace("classify-ns")
	wf := types.WorkflowID("wf-classify")
	tag := entryActivationWorkflowTag(ns, wf)

	modern := workflowScopedEntryActivationRedisKey(ns, wf, "v1", "entry", 0)
	if got := modernEntryActivationKeyTag(ns, modern); got != tag {
		t.Fatalf("modern key %q classified as %q, want tag %q", modern, got, tag)
	}

	legacy := entryActivationRedisKey(ns, wf, "v1", "entry", 0)
	if got := modernEntryActivationKeyTag(ns, legacy); got != "" {
		t.Fatalf("legacy key %q classified as modern tag %q", legacy, got)
	}

	foreign := workflowScopedEntryActivationRedisKey("other-ns", wf, "v1", "entry", 0)
	if got := modernEntryActivationKeyTag(ns, foreign); got != "" {
		t.Fatalf("foreign modern key %q classified as tag %q", foreign, got)
	}

	// A legacy workflow ID can be shaped exactly like a digest tag. Its key is
	// still legacy: the first '}' closes the tag early and the remainder is
	// "tail|v1|entry}", never ":act:".
	digestShaped := "wf-" + strings.Repeat("a", 64)
	pretender := entryActivationRedisKey(ns, types.WorkflowID(digestShaped+"}tail"), "v1", "entry", 0)
	if got := modernEntryActivationKeyTag(ns, pretender); got != "" {
		t.Fatalf("legacy key with a digest-shaped workflow ID %q classified as tag %q", pretender, got)
	}

	// Correct tag length but non-hex characters.
	nonHex := "xflow:ns:classify-ns:entryact:{wf-" + strings.Repeat("z", 64) + "}:act:deadbeef:replica:0"
	if got := modernEntryActivationKeyTag(ns, nonHex); got != "" {
		t.Fatalf("non-hex tag key %q classified as tag %q", nonHex, got)
	}

	// The modern layout always continues with ":act:" after the tag.
	noAct := "xflow:ns:classify-ns:entryact:{" + tag + "}:elsewhere"
	if got := modernEntryActivationKeyTag(ns, noAct); got != "" {
		t.Fatalf("key without the :act: segment %q classified as tag %q", noAct, got)
	}
}

// A record written without a TTL has no remaining time to derive from, so its
// index must not acquire one — a TTL'd index would expire while a live record
// still depended on it.
func TestEntryActivationIndexPermanentMemberKeepsIndexPermanent(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	ns := namespace.Namespace("index-permanent-ns")
	act := engine.EntryActivation{
		Namespace:       ns,
		WorkflowID:      "wf-permanent",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
		PackageHash:     "pkg-permanent",
		Desired:         true,
	}
	key := entryActivationKeyFromActivation(act)
	// Written by hand without EXPIRE, like a record from a writer that manages
	// its own lifetime.
	if err := rdb.HSet(ctx, store.keyFor(key), map[string]any{
		"namespace":        string(ns),
		"workflow_id":      string(act.WorkflowID),
		"workflow_version": act.WorkflowVersion,
		"entry_unit_id":    act.EntryUnitID,
		"package_hash":     act.PackageHash,
		"desired":          "1",
	}).Err(); err != nil {
		t.Fatalf("seed permanent record: %v", err)
	}

	if err := store.rebuildEntryActivationIndex(ctx, ns); err != nil {
		t.Fatalf("rebuildEntryActivationIndex: %v", err)
	}
	indexKey := entryActivationWorkflowIndexRedisKey(ns, act.WorkflowID)
	if ttl, err := rdb.TTL(ctx, indexKey).Result(); err != nil || ttl != -1 {
		t.Fatalf("index TTL over a permanent member = %v (err=%v), want -1 (no expiry)", ttl, err)
	}

	listed, err := store.List(ctx, ns)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 || listed[0].EntryUnitID != act.EntryUnitID {
		t.Fatalf("permanent member not listed: %+v", listed)
	}
}

// compareEntryActivationSets asserts both paths returned the same activations,
// comparing sets because List does not promise an order.
func compareEntryActivationSets(t *testing.T, want, got []engine.EntryActivation) {
	t.Helper()
	sortEntryActivations(want)
	sortEntryActivations(got)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("indexed list differs from scanned list:\nscanned = %+v\nindexed = %+v", want, got)
	}
}

func sortEntryActivations(activations []engine.EntryActivation) {
	sort.Slice(activations, func(i, j int) bool {
		left, right := activations[i], activations[j]
		if left.WorkflowID != right.WorkflowID {
			return left.WorkflowID < right.WorkflowID
		}
		if left.EntryUnitID != right.EntryUnitID {
			return left.EntryUnitID < right.EntryUnitID
		}
		return left.ReplicaIndex < right.ReplicaIndex
	})
}
