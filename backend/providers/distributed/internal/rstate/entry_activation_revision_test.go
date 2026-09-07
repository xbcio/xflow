package rstate

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
)

func newEntryActivationRevisionRedisStore(t *testing.T) (*EntryActivationStore, *redis.Client) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewEntryActivationStore(rdb, time.Hour), rdb
}

func TestRedisEntryActivationRevisionOrdering(t *testing.T) {
	ctx := context.Background()
	store, _ := newEntryActivationRevisionRedisStore(t)
	current := engine.EntryActivation{
		Namespace:        "revision-ns",
		WorkflowID:       "wf-ordering",
		WorkflowVersion:  "v1",
		EntryUnitID:      "entry",
		NodeType:         "kafka.source",
		PackageHash:      "pkg-current",
		Desired:          true,
		RegistryRevision: 2,
	}
	key := entryActivationKeyFromActivation(current)
	if err := store.Upsert(ctx, current); err != nil {
		t.Fatalf("Upsert revision 2: %v", err)
	}

	for _, revision := range []uint64{1, 0} {
		stale := current
		stale.RegistryRevision = revision
		stale.PackageHash = "pkg-stale"
		stale.Desired = false
		if err := store.Upsert(ctx, stale); err != nil {
			t.Fatalf("Upsert stale revision %d: %v", revision, err)
		}
	}
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get current activation: ok=%v err=%v", ok, err)
	}
	if got.RegistryRevision != 2 || got.PackageHash != "pkg-current" || !got.Desired {
		t.Fatalf("stale write changed revision 2 record: %+v", got)
	}

	equal := current
	equal.PackageHash = "pkg-equal-retry"
	equal.Desired = false
	if err := store.Upsert(ctx, equal); err != nil {
		t.Fatalf("Upsert equal revision: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RegistryRevision != 2 || got.PackageHash != "pkg-equal-retry" || got.Desired {
		t.Fatalf("equal revision was not applied: %+v", got)
	}
}

func TestRedisEntryActivationWorkflowWatermark(t *testing.T) {
	ctx := context.Background()
	store, _ := newEntryActivationRevisionRedisStore(t)
	old := engine.EntryActivation{
		Namespace:        "revision-ns",
		WorkflowID:       "wf-watermark",
		WorkflowVersion:  "v1",
		EntryUnitID:      "old-entry",
		NodeType:         "kafka.source",
		PackageHash:      "pkg-old",
		Desired:          true,
		RegistryRevision: 1,
	}
	if err := store.Upsert(ctx, old); err != nil {
		t.Fatalf("Upsert revision 1: %v", err)
	}
	if err := store.AdvanceWorkflowRevision(ctx, old.Namespace, old.WorkflowID, 2); err != nil {
		t.Fatalf("AdvanceWorkflowRevision: %v", err)
	}

	// Deliberately omit a revision-2 Upsert. The watermark itself must make the
	// old desired record non-authoritative for both point and namespace reads.
	got, ok, err := store.Get(ctx, entryActivationKeyFromActivation(old))
	if err != nil || !ok {
		t.Fatalf("Get old activation: ok=%v err=%v", ok, err)
	}
	if got.Desired || got.RegistryRevision != 1 {
		t.Fatalf("Get exposed stale desired activation: %+v", got)
	}
	listed, err := store.List(ctx, old.Namespace)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	matching := activationsMatchingKey(listed, entryActivationKeyFromActivation(old))
	if len(matching) != 1 || matching[0].Desired || matching[0].RegistryRevision != 1 {
		t.Fatalf("List exposed stale desired activation: %+v", matching)
	}

	for _, revision := range []uint64{1, 0} {
		stale := old
		stale.EntryUnitID = "absent-stale-" + string(rune('0'+revision))
		stale.RegistryRevision = revision
		if err := store.Upsert(ctx, stale); err != nil {
			t.Fatalf("Upsert absent stale revision %d: %v", revision, err)
		}
		if _, exists, err := store.Get(ctx, entryActivationKeyFromActivation(stale)); err != nil || exists {
			t.Fatalf("stale revision %d created absent key: exists=%v err=%v", revision, exists, err)
		}
	}
}

func TestRedisEntryActivationLegacyCompatibilityAndDeduplication(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	act := engine.EntryActivation{
		Namespace:       "legacy-revision-ns",
		WorkflowID:      "wf-legacy-layout",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-entry",
	}
	key := entryActivationKeyFromActivation(act)
	legacyKey := store.legacyKeyFor(key)
	if err := rdb.HSet(ctx, legacyKey, map[string]any{
		"namespace":             string(act.Namespace),
		"workflow_id":           string(act.WorkflowID),
		"workflow_version":      act.WorkflowVersion,
		"entry_unit_id":         act.EntryUnitID,
		"node_type":             "kafka.source",
		"package_hash":          "pkg-legacy",
		"desired":               "1",
		"runner_id":             "runner-legacy",
		"session_id":            "session-legacy",
		"generation":            "4",
		"lease_deadline":        "0",
		"assigned_package_hash": "pkg-legacy",
	}).Err(); err != nil {
		t.Fatalf("seed legacy hash: %v", err)
	}

	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get legacy-only activation: ok=%v err=%v", ok, err)
	}
	if got.PackageHash != "pkg-legacy" || !got.Desired || got.Generation != 4 {
		t.Fatalf("legacy-only activation decoded incorrectly: %+v", got)
	}
	listed, err := store.List(ctx, act.Namespace)
	if err != nil {
		t.Fatalf("List legacy-only activation: %v", err)
	}
	if matching := activationsMatchingKey(listed, key); len(matching) != 1 || matching[0].PackageHash != "pkg-legacy" {
		t.Fatalf("legacy-only activation disappeared or duplicated: %+v", matching)
	}

	deadline := time.Now().Add(time.Minute).Truncate(time.Second)
	if claimed, err := store.Assign(ctx, key, "runner-upgraded", "session-upgraded", 5, deadline); err != nil || !claimed {
		t.Fatalf("Assign legacy activation: claimed=%v err=%v", claimed, err)
	}
	if renewed, err := store.Renew(ctx, key, 5, deadline.Add(time.Minute)); err != nil || !renewed {
		t.Fatalf("Renew legacy activation: renewed=%v err=%v", renewed, err)
	}
	if err := store.Fence(ctx, key, 5); err != nil {
		t.Fatalf("Fence legacy activation: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "" || got.Generation != 5 {
		t.Fatalf("legacy Fence did not update legacy hash: %+v", got)
	}
	if claimed, err := store.Assign(ctx, key, "runner-copy", "session-copy", 6, deadline); err != nil || !claimed {
		t.Fatalf("reassign legacy activation: claimed=%v err=%v", claimed, err)
	}

	// The first modern Upsert leaves the legacy hash in place, preserves its
	// assignment fields, and becomes the authoritative duplicate.
	modern := act
	modern.NodeType = "kafka.source"
	modern.PackageHash = "pkg-modern"
	modern.Desired = true
	if err := store.Upsert(ctx, modern); err != nil {
		t.Fatalf("Upsert modern activation: %v", err)
	}
	if exists, err := rdb.Exists(ctx, legacyKey).Result(); err != nil || exists != 1 {
		t.Fatalf("legacy hash was removed: exists=%d err=%v", exists, err)
	}
	got, ok, err = store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get modern activation: ok=%v err=%v", ok, err)
	}
	if got.PackageHash != "pkg-modern" || got.RunnerID != "runner-copy" || got.Generation != 6 || got.AssignedPackageHash != "pkg-legacy" {
		t.Fatalf("modern record did not win/preserve assignment: %+v", got)
	}
	listed, err = store.List(ctx, act.Namespace)
	if err != nil {
		t.Fatalf("List duplicate layouts: %v", err)
	}
	matching := activationsMatchingKey(listed, key)
	if len(matching) != 1 || matching[0].PackageHash != "pkg-modern" {
		t.Fatalf("new/legacy layouts were not deduplicated with modern priority: %+v", matching)
	}
}

func TestRedisEntryActivationLegacyRecordHonorsWatermark(t *testing.T) {
	ctx := context.Background()
	store, rdb := newEntryActivationRevisionRedisStore(t)
	act := engine.EntryActivation{
		Namespace:       "legacy-watermark-ns",
		WorkflowID:      "wf-legacy-watermark",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry",
	}
	key := entryActivationKeyFromActivation(act)
	if err := rdb.HSet(ctx, store.legacyKeyFor(key), map[string]any{
		"namespace":         string(act.Namespace),
		"workflow_id":       string(act.WorkflowID),
		"workflow_version":  act.WorkflowVersion,
		"entry_unit_id":     act.EntryUnitID,
		"package_hash":      "pkg-revision-1",
		"desired":           "1",
		"registry_revision": "1",
	}).Err(); err != nil {
		t.Fatalf("seed revisioned legacy hash: %v", err)
	}
	if err := store.AdvanceWorkflowRevision(ctx, act.Namespace, act.WorkflowID, 2); err != nil {
		t.Fatalf("AdvanceWorkflowRevision: %v", err)
	}
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok || got.Desired || got.RegistryRevision != 1 {
		t.Fatalf("Get legacy activation after watermark: got=%+v ok=%v err=%v", got, ok, err)
	}
	listed, err := store.List(ctx, act.Namespace)
	if err != nil {
		t.Fatalf("List legacy activation after watermark: %v", err)
	}
	matching := activationsMatchingKey(listed, key)
	if len(matching) != 1 || matching[0].Desired {
		t.Fatalf("List exposed stale legacy desired activation: %+v", matching)
	}
}

func TestRedisEntryActivationRevisionUsesFullUint64(t *testing.T) {
	ctx := context.Background()
	store, _ := newEntryActivationRevisionRedisStore(t)
	act := engine.EntryActivation{
		Namespace:        "revision-ns",
		WorkflowID:       "wf-uint64",
		WorkflowVersion:  "v1",
		EntryUnitID:      "entry",
		PackageHash:      "pkg-max-minus-one",
		Desired:          true,
		RegistryRevision: math.MaxUint64 - 1,
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert MaxUint64-1: %v", err)
	}
	act.RegistryRevision = math.MaxUint64
	act.PackageHash = "pkg-max"
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert MaxUint64: %v", err)
	}
	act.RegistryRevision = math.MaxUint64 - 1
	act.PackageHash = "pkg-stale"
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert stale MaxUint64-1: %v", err)
	}
	got, ok, err := store.Get(ctx, entryActivationKeyFromActivation(act))
	if err != nil || !ok {
		t.Fatalf("Get max revision: ok=%v err=%v", ok, err)
	}
	if got.RegistryRevision != math.MaxUint64 || got.PackageHash != "pkg-max" {
		t.Fatalf("uint64 revision comparison lost precision: %+v", got)
	}
}

func TestRedisEntryActivationWorkflowTagIsSafe(t *testing.T) {
	ctx := context.Background()
	store, _ := newEntryActivationRevisionRedisStore(t)
	act := engine.EntryActivation{
		Namespace:        "tenant{attacker}*",
		WorkflowID:       "wf{attacker}",
		WorkflowVersion:  "v1",
		EntryUnitID:      "entry",
		Desired:          true,
		RegistryRevision: 1,
	}
	activationKey := store.keyFor(entryActivationKeyFromActivation(act))
	watermarkKey := entryActivationWorkflowRevisionRedisKey(act.Namespace, act.WorkflowID)
	if got, want := firstRedisHashTag(activationKey), entryActivationWorkflowTag(act.Namespace, act.WorkflowID); got != want {
		t.Fatalf("activation first hash tag = %q, want safe workflow tag %q (key %q)", got, want, activationKey)
	}
	if got, want := firstRedisHashTag(watermarkKey), entryActivationWorkflowTag(act.Namespace, act.WorkflowID); got != want {
		t.Fatalf("watermark first hash tag = %q, want safe workflow tag %q (key %q)", got, want, watermarkKey)
	}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert escaped namespace: %v", err)
	}
	listed, err := store.List(ctx, act.Namespace)
	if err != nil {
		t.Fatalf("List escaped namespace: %v", err)
	}
	if matching := activationsMatchingKey(listed, entryActivationKeyFromActivation(act)); len(matching) != 1 || !matching[0].Desired {
		t.Fatalf("escaped namespace activation not listed: %+v", matching)
	}
}

func activationsMatchingKey(activations []engine.EntryActivation, key engine.EntryActivationKey) []engine.EntryActivation {
	var matching []engine.EntryActivation
	for _, act := range activations {
		if entryActivationKeyFromActivation(act) == key {
			matching = append(matching, act)
		}
	}
	return matching
}

func firstRedisHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return ""
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end < 0 {
		return ""
	}
	return key[start+1 : start+1+end]
}
