package workflowreg

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func newIndexTestRegistry(t *testing.T) (*Registry, *redis.Client) {
	t.Helper()

	reg, srv := newTestRegistry(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return reg, rdb
}

// indexRecord builds a record whose Key deliberately excludes the namespace.
// Two namespaces can therefore hold the *same* logical key for the same name,
// which is the adversarial shape for a per-namespace index: any leakage between
// the two namespaces shows up as a cross-namespace id in the enumeration.
func indexRecord(id types.WorkflowID, ns, name, version, hash string) backend.WorkflowRecord {
	return backend.WorkflowRecord{
		ID:             id,
		Key:            "shared/" + name + "@" + version,
		Namespace:      ns,
		Name:           name,
		Version:        version,
		DefinitionHash: hash,
	}
}

func indexContext(ns string) context.Context {
	return namespace.WithNamespace(context.Background(), namespace.Namespace(ns))
}

func listIndexIDs(t *testing.T, ctx context.Context, reg *Registry, ns namespace.Namespace, opts backend.WorkflowListOptions) []types.WorkflowID {
	t.Helper()

	ids, err := reg.ListWorkflows(ctx, ns, opts)
	if err != nil {
		t.Fatalf("ListWorkflows(%q): %v", ns, err)
	}
	return ids
}

func assertIndexIDs(t *testing.T, got []types.WorkflowID, want ...types.WorkflowID) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("ListWorkflows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ListWorkflows = %v, want %v", got, want)
		}
	}
}

func indexScore(t *testing.T, rdb *redis.Client, ns namespace.Namespace, id types.WorkflowID) (float64, bool) {
	t.Helper()

	score, err := rdb.ZScore(context.Background(), workflowIndexKey(ns), string(id)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("ZScore(%q): %v", id, err)
	}
	return score, true
}

// TestWorkflowIndexSharesNamespaceSlotWithRecords extends the cluster-safety
// guarantee to the new index key. The whole atomicity argument is that index
// maintenance runs inside the same Lua script as the record mutation, and a
// script in Redis Cluster may only touch one slot — so the index must live in
// the namespace digest slot alongside the records it indexes.
func TestWorkflowIndexSharesNamespaceSlotWithRecords(t *testing.T) {
	tn := namespace.Namespace("namespace-{attacker}")
	indexKey := workflowIndexKey(tn)

	recordSlot := slotTag(workflowByKeyKey(tn, "shared/wf@v1"))
	if got := slotTag(indexKey); got != recordSlot {
		t.Fatalf("index slot = %q, want record slot %q (key %q)", got, recordSlot, indexKey)
	}
	if want := registryPrefix(tn) + "index"; indexKey != want {
		t.Fatalf("index key %q, want %q", indexKey, want)
	}
	if !strings.HasPrefix(indexKey, "xflow:wfreg:v2:{ns:") {
		t.Fatalf("index key %q does not follow the xflow:wfreg:v2 convention", indexKey)
	}
	if slotTag(workflowIndexKey(namespace.Namespace("namespace-{other}"))) == slotTag(indexKey) {
		t.Fatal("different namespaces unexpectedly share the index slot")
	}
}

// TestWorkflowIndexPopulatedByAdd pins the create path: the record's id becomes
// a member of its namespace's index, scored from the revision the add
// allocated, inside the same script run that wrote the record.
func TestWorkflowIndexPopulatedByAdd(t *testing.T) {
	ctx := indexContext("index-add")
	ns := namespace.FromContext(ctx)
	reg, rdb := newIndexTestRegistry(t)

	stored := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-a", string(ns), "a", "v1", "sha256:a"))

	score, ok := indexScore(t, rdb, ns, stored.ID)
	if !ok {
		t.Fatalf("index has no member for %q", stored.ID)
	}
	if want := -float64(stored.RegistryRevision); score != want {
		t.Fatalf("index score = %v, want %v", score, want)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), stored.ID)

	// An idempotent re-add must not duplicate the membership.
	again := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord(stored.ID, string(ns), "a", "v1", "sha256:a"))
	if again.ID != stored.ID {
		t.Fatalf("idempotent add ID = %q, want %q", again.ID, stored.ID)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), stored.ID)
}

// TestWorkflowIndexRescoredByDefinitionHashChange covers UpdateDefinitionHash:
// the id keeps exactly one membership and its score follows the fresh revision.
func TestWorkflowIndexRescoredByDefinitionHashChange(t *testing.T) {
	ctx := indexContext("index-hash")
	ns := namespace.FromContext(ctx)
	reg, rdb := newIndexTestRegistry(t)

	stored := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-a", string(ns), "a", "v1", "sha256:old"))
	if err := reg.UpdateDefinitionHash(ctx, stored.ID, "sha256:old", "sha256:new"); err != nil {
		t.Fatalf("UpdateDefinitionHash: %v", err)
	}

	updated := getDistributedAtomicWorkflow(t, ctx, reg, stored.ID)
	score, ok := indexScore(t, rdb, ns, stored.ID)
	if !ok {
		t.Fatalf("index lost %q after hash change", stored.ID)
	}
	if want := -float64(updated.RegistryRevision); score != want {
		t.Fatalf("index score = %v, want %v", score, want)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), stored.ID)
}

// TestWorkflowIndexMaintainedByCompareAndReplace covers the replace path in
// both directions: a committed replacement retires the previous membership and
// installs the destination one, while a conflicted replacement leaves the index
// exactly as it was — the index must never describe a mutation that did not
// commit.
func TestWorkflowIndexMaintainedByCompareAndReplace(t *testing.T) {
	ctx := indexContext("index-replace")
	ns := namespace.FromContext(ctx)

	t.Run("committed", func(t *testing.T) {
		reg, rdb := newIndexTestRegistry(t)
		original := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-old", string(ns), "old", "v1", "sha256:old"))

		result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
			MutationID:  "mutation-replace",
			Expected:    backend.RevisionOfWorkflow(original),
			Replacement: indexRecord("id-new", string(ns), "new", "v2", "sha256:new"),
		})
		if err != nil {
			t.Fatalf("CompareAndReplaceWorkflow: %v", err)
		}
		if result.Status != backend.WorkflowReplaceReplaced {
			t.Fatalf("status = %q, want %q", result.Status, backend.WorkflowReplaceReplaced)
		}

		if _, ok := indexScore(t, rdb, ns, original.ID); ok {
			t.Fatalf("index still contains retired id %q after replace", original.ID)
		}
		score, ok := indexScore(t, rdb, ns, result.Current.ID)
		if !ok {
			t.Fatalf("index has no member for replacement %q", result.Current.ID)
		}
		if want := -float64(result.Current.RegistryRevision); score != want {
			t.Fatalf("index score = %v, want %v", score, want)
		}
		assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), result.Current.ID)
	})

	t.Run("conflicted", func(t *testing.T) {
		reg, _ := newIndexTestRegistry(t)
		original := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-old", string(ns), "old", "v1", "sha256:old"))

		if _, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
			MutationID:  "mutation-first",
			Expected:    backend.RevisionOfWorkflow(original),
			Replacement: indexRecord("id-new", string(ns), "new", "v2", "sha256:new"),
		}); err != nil {
			t.Fatalf("first replace: %v", err)
		}

		// Replaying the now-consumed revision must conflict and must not touch
		// the index: the winning member stays and the loser never appears.
		_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
			MutationID:  "mutation-second",
			Expected:    backend.RevisionOfWorkflow(original),
			Replacement: indexRecord("id-loser", string(ns), "loser", "v3", "sha256:loser"),
		})
		assertDistributedWorkflowConflict(t, err, backend.WorkflowReplaceConflictStaleRevision)

		assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), "id-new")
	})
}

// TestWorkflowIndexDroppedByRemove covers the delete path, including the
// negative case: a remove that finds nothing must not disturb the index.
func TestWorkflowIndexDroppedByRemove(t *testing.T) {
	ctx := indexContext("index-remove")
	ns := namespace.FromContext(ctx)
	reg, rdb := newIndexTestRegistry(t)

	stored := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-a", string(ns), "a", "v1", "sha256:a"))
	other := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-b", string(ns), "b", "v1", "sha256:b"))

	if err := reg.RemoveWorkflow(ctx, stored.ID); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	if _, ok := indexScore(t, rdb, ns, stored.ID); ok {
		t.Fatalf("index still contains removed id %q", stored.ID)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), other.ID)

	if err := reg.RemoveWorkflow(ctx, stored.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("second RemoveWorkflow = %v, want ErrWorkflowNotFound", err)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), other.ID)
}

// TestWorkflowIndexEnumerationIsNamespaceScoped is the leakage test. Both
// namespaces hold the same logical key, so only a genuinely namespace-scoped
// index can keep them apart.
func TestWorkflowIndexEnumerationIsNamespaceScoped(t *testing.T) {
	firstCtx := indexContext("index-ns-one")
	secondCtx := indexContext("index-ns-two")
	firstNS := namespace.FromContext(firstCtx)
	secondNS := namespace.FromContext(secondCtx)
	reg, _ := newIndexTestRegistry(t)

	first := addDistributedAtomicWorkflow(t, firstCtx, reg, indexRecord("id-one", string(firstNS), "wf", "v1", "sha256:one"))
	second := addDistributedAtomicWorkflow(t, secondCtx, reg, indexRecord("id-two", string(secondNS), "wf", "v1", "sha256:two"))
	if first.Key != second.Key {
		t.Fatalf("test premise broken: keys %q and %q differ", first.Key, second.Key)
	}

	// Each namespace lists itself, and the scope is the argument rather than the
	// ambient context namespace.
	assertIndexIDs(t, listIndexIDs(t, firstCtx, reg, firstNS, backend.WorkflowListOptions{}), first.ID)
	assertIndexIDs(t, listIndexIDs(t, secondCtx, reg, secondNS, backend.WorkflowListOptions{}), second.ID)
	assertIndexIDs(t, listIndexIDs(t, secondCtx, reg, firstNS, backend.WorkflowListOptions{}), first.ID)

	// A third namespace owns nothing.
	assertIndexIDs(t, listIndexIDs(t, firstCtx, reg, namespace.Namespace("index-ns-three"), backend.WorkflowListOptions{}))
}

// TestWorkflowIndexEnumerationEmptyForUnknownNamespace pins the "unknown means
// empty, not error" half of the contract.
func TestWorkflowIndexEnumerationEmptyForUnknownNamespace(t *testing.T) {
	ctx := indexContext("index-known")
	ns := namespace.FromContext(ctx)
	reg, _ := newIndexTestRegistry(t)
	addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-a", string(ns), "a", "v1", "sha256:a"))

	ids := listIndexIDs(t, ctx, reg, namespace.Namespace("index-never-used"), backend.WorkflowListOptions{})
	if len(ids) != 0 {
		t.Fatalf("unknown namespace returned %v, want empty", ids)
	}
}

// TestWorkflowIndexRejectsUnscopedNamespace pins the other half: an empty or
// malformed namespace is refused outright rather than read as "everything",
// which is what makes the scope non-optional by construction.
func TestWorkflowIndexRejectsUnscopedNamespace(t *testing.T) {
	ctx := indexContext("index-guarded")
	ns := namespace.FromContext(ctx)
	reg, _ := newIndexTestRegistry(t)
	addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-a", string(ns), "a", "v1", "sha256:a"))

	for _, raw := range []string{"", "index-bad:ns", "index-bad{ns}", "index-bad*ns"} {
		if _, err := reg.ListWorkflows(ctx, namespace.Namespace(raw), backend.WorkflowListOptions{}); err == nil {
			t.Fatalf("ListWorkflows(%q) = nil error, want a rejection", raw)
		}
	}
	if _, err := reg.ListWorkflows(ctx, ns, backend.WorkflowListOptions{Limit: -1}); err == nil {
		t.Fatal("ListWorkflows(limit=-1) = nil error, want a rejection")
	}
	if _, err := reg.ListWorkflows(ctx, ns, backend.WorkflowListOptions{Offset: -1}); err == nil {
		t.Fatal("ListWorkflows(offset=-1) = nil error, want a rejection")
	}
}

// TestWorkflowIndexBackfillsRecordsWrittenBeforeIt covers the upgrade case: a
// record whose membership predates the index is still enumerable, and the
// membership is restored by the read itself.
func TestWorkflowIndexBackfillsRecordsWrittenBeforeIt(t *testing.T) {
	ctx := indexContext("index-backfill")
	ns := namespace.FromContext(ctx)
	reg, rdb := newIndexTestRegistry(t)

	stored := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-a", string(ns), "a", "v1", "sha256:a"))
	// Simulate a registry written before this index key existed.
	if err := rdb.ZRem(ctx, workflowIndexKey(ns), string(stored.ID)).Err(); err != nil {
		t.Fatalf("ZRem: %v", err)
	}
	if _, ok := indexScore(t, rdb, ns, stored.ID); ok {
		t.Fatal("precondition failed: index still has the member")
	}

	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), stored.ID)
	if _, ok := indexScore(t, rdb, ns, stored.ID); !ok {
		t.Fatalf("index was not repaired for %q", stored.ID)
	}

	// The repair is idempotent: repeating it converges on the same single
	// membership rather than duplicating or dropping anything.
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), stored.ID)
	cardinality, err := rdb.ZCard(ctx, workflowIndexKey(ns)).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if cardinality != 1 {
		t.Fatalf("index cardinality = %d, want 1", cardinality)
	}
}

// TestWorkflowIndexReconcilePrunesStaleMembers covers the prune half of the
// repair: a membership with no live record must never be handed to a caller,
// and once nothing live remains the repair removes it for good.
func TestWorkflowIndexReconcilePrunesStaleMembers(t *testing.T) {
	ctx := indexContext("index-stale")
	ns := namespace.FromContext(ctx)
	reg, rdb := newIndexTestRegistry(t)

	if err := rdb.ZAdd(ctx, workflowIndexKey(ns), redis.Z{Score: -9, Member: "id-ghost"}).Err(); err != nil {
		t.Fatalf("ZAdd ghost: %v", err)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}))
	if _, ok := indexScore(t, rdb, ns, "id-ghost"); ok {
		t.Fatal("stale membership survived the repair")
	}

	// A stale member beside a live one is skipped rather than returned, and a
	// read does not write: pruning stays the repair path's job.
	live := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-live", string(ns), "live", "v1", "sha256:live"))
	if err := rdb.ZAdd(ctx, workflowIndexKey(ns), redis.Z{Score: -99, Member: "id-ghost-2"}).Err(); err != nil {
		t.Fatalf("ZAdd ghost: %v", err)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), live.ID)
	if _, ok := indexScore(t, rdb, ns, "id-ghost-2"); !ok {
		t.Fatal("a read pruned a member; reads must not write")
	}
}

// TestWorkflowIndexEnumerationIsNewestFirstAndPaged pins the ordering and the
// offset/limit contract the future list endpoint will page over.
func TestWorkflowIndexEnumerationIsNewestFirstAndPaged(t *testing.T) {
	ctx := indexContext("index-order")
	ns := namespace.FromContext(ctx)
	reg, _ := newIndexTestRegistry(t)

	first := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-1", string(ns), "n1", "v1", "sha256:1"))
	second := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-2", string(ns), "n2", "v1", "sha256:2"))
	third := addDistributedAtomicWorkflow(t, ctx, reg, indexRecord("id-3", string(ns), "n3", "v1", "sha256:3"))
	if !(first.RegistryRevision < second.RegistryRevision && second.RegistryRevision < third.RegistryRevision) {
		t.Fatalf("revisions are not monotonic: %d %d %d", first.RegistryRevision, second.RegistryRevision, third.RegistryRevision)
	}

	want := []types.WorkflowID{third.ID, second.ID, first.ID}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), want...)
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{Limit: 2}), want[:2]...)
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{Limit: 2, Offset: 2}), want[2:]...)
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{Offset: 3}))
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{Offset: 99}))

	// Replacing an older record moves it to the front, which is what makes the
	// order a function of the index alone rather than of insertion time.
	replaced, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  "mutation-order",
		Expected:    backend.RevisionOfWorkflow(first),
		Replacement: indexRecord(first.ID, string(ns), "n1", "v2", "sha256:1b"),
	})
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow: %v", err)
	}
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), replaced.Current.ID, third.ID, second.ID)
}

// TestWorkflowIndexPopulatedByLegacyImport covers the lazy v1 import path: an
// imported record must be enumerable in v2 without a separate backfill.
func TestWorkflowIndexPopulatedByLegacyImport(t *testing.T) {
	ctx := indexContext("tenant")
	ns := namespace.FromContext(ctx)
	reg, rdb := newIndexTestRegistry(t)

	legacy := distributedAtomicRecord("legacy-id", "tenant/wf@v1", "v1", "sha256:legacy")
	seedLegacyWorkflow(t, ctx, reg, legacy)

	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}))

	imported := getDistributedAtomicWorkflow(t, ctx, reg, legacy.ID)
	assertIndexIDs(t, listIndexIDs(t, ctx, reg, ns, backend.WorkflowListOptions{}), imported.ID)
	if _, ok := indexScore(t, rdb, ns, imported.ID); !ok {
		t.Fatalf("imported record %q is not in the index", imported.ID)
	}
}

// TestWorkflowIndexReconcileScriptStaysInsideOneNamespace is a static guard on
// the repair script. It must address Redis purely through KEYS: its only SCAN
// pattern is the namespace-prefixed byid pattern passed in as KEYS[1], which is
// what keeps the traversal inside one namespace and one Cluster slot instead of
// becoming a keyspace-wide walk.
//
// The five record-mutation scripts cannot be introspected this way (go-redis
// does not expose a Script's source), so their index maintenance is pinned
// behaviourally by the tests above instead.
func TestWorkflowIndexReconcileScriptStaysInsideOneNamespace(t *testing.T) {
	for _, fragment := range []string{
		"redis.call('SCAN', cursor, 'MATCH', KEYS[2]",
		"redis.call('ZADD', KEYS[3]",
		"redis.call('ZREM', KEYS[3]",
		"redis.call('ZRANGE', KEYS[3], 0, -1, 'WITHSCORES')",
	} {
		if !strings.Contains(reconcileWorkflowIndexScript, fragment) {
			t.Errorf("reconcile script does not contain %q", fragment)
		}
	}
	// The script may build Lua *table* keys by concatenation, but no redis.call
	// may receive a key assembled from data: the file's invariant is that every
	// Redis key a script touches arrives through KEYS.
	for _, line := range strings.Split(reconcileWorkflowIndexScript, "\n") {
		if !strings.Contains(line, "redis.call(") || !strings.Contains(line, "..") {
			continue
		}
		t.Errorf("reconcile script builds a Redis key from data: %s", strings.TrimSpace(line))
	}
	// It must never write authority state; the index is its only writable key.
	for _, forbidden := range []string{"'HSET'", "'SET'", "'DEL'", "'INCR'", "'SADD'"} {
		if strings.Contains(reconcileWorkflowIndexScript, forbidden) {
			t.Errorf("reconcile script writes authority state via %s", forbidden)
		}
	}
}
