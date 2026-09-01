package workflowreg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestDistributedWorkflowRegistryAssignsMonotonicRevisions(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	first := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("first", "tenant/first@v1", "v1", "hash:first"))
	if first.RegistryRevision == 0 {
		t.Fatal("first AddWorkflow revision is zero")
	}

	idempotent := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("ignored-id", first.Key, "v1", first.DefinitionHash))
	if idempotent.ID != first.ID || idempotent.RegistryRevision != first.RegistryRevision {
		t.Fatalf("idempotent AddWorkflow = id %q revision %d, want id %q revision %d", idempotent.ID, idempotent.RegistryRevision, first.ID, first.RegistryRevision)
	}

	second := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("second", "tenant/second@v1", "v1", "hash:second"))
	if second.RegistryRevision <= first.RegistryRevision {
		t.Fatalf("second revision = %d, want > %d", second.RegistryRevision, first.RegistryRevision)
	}

	if err := reg.UpdateDefinitionHash(ctx, first.ID, first.DefinitionHash, first.DefinitionHash); err != nil {
		t.Fatalf("unchanged UpdateDefinitionHash: %v", err)
	}
	if got := getDistributedAtomicWorkflow(t, ctx, reg, first.ID); got.RegistryRevision != first.RegistryRevision {
		t.Fatalf("unchanged hash revision = %d, want %d", got.RegistryRevision, first.RegistryRevision)
	}
	if err := reg.UpdateDefinitionHash(ctx, first.ID, first.DefinitionHash, "hash:first-v2"); err != nil {
		t.Fatalf("changed UpdateDefinitionHash: %v", err)
	}
	if got := getDistributedAtomicWorkflow(t, ctx, reg, first.ID); got.RegistryRevision <= second.RegistryRevision {
		t.Fatalf("updated revision = %d, want > %d", got.RegistryRevision, second.RegistryRevision)
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceIndexesAtomically(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("old-id", "tenant/flow@v1", "v1", "hash:a"))

	unchangedRequest := backend.WorkflowReplaceRequest{
		MutationID:  "unchanged",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: original,
	}
	unchangedRequest.Replacement.RegistryRevision = 999
	beforeRevision := distributedRegistryRevision(t, ctx, reg)
	unchanged, err := reg.CompareAndReplaceWorkflow(ctx, unchangedRequest)
	if err != nil {
		t.Fatalf("unchanged CompareAndReplaceWorkflow: %v", err)
	}
	if unchanged.Status != backend.WorkflowReplaceUnchanged || unchanged.Current.RegistryRevision != original.RegistryRevision {
		t.Fatalf("unchanged result = status %q revision %d", unchanged.Status, unchanged.Current.RegistryRevision)
	}
	if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
		t.Fatalf("unchanged replacement advanced revision from %d to %d", beforeRevision, got)
	}

	replacement := original
	replacement.ID = "new-id"
	replacement.Key = "tenant/renamed@v2"
	replacement.Name = "renamed"
	replacement.Version = "v2"
	replacement.DefinitionHash = "hash:b"
	replacement.RegistryRevision = 0
	replaced, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  "rename-and-change-id",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	})
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if replaced.Status != backend.WorkflowReplaceReplaced || replaced.Current.ID != replacement.ID || replaced.Current.Key != replacement.Key {
		t.Fatalf("replacement result = %#v", replaced)
	}
	if replaced.Current.RegistryRevision <= original.RegistryRevision {
		t.Fatalf("replacement revision = %d, want > %d", replaced.Current.RegistryRevision, original.RegistryRevision)
	}
	assertDistributedWorkflowNotFoundByID(t, ctx, reg, original.ID)
	assertDistributedWorkflowNotFoundByKey(t, ctx, reg, original.Key)
	byID := getDistributedAtomicWorkflow(t, ctx, reg, replacement.ID)
	byKey := getDistributedAtomicWorkflowByKey(t, ctx, reg, replacement.Key)
	if byID.ID != byKey.ID || byID.Key != byKey.Key || byID.RegistryRevision != byKey.RegistryRevision {
		t.Fatalf("new indexes disagree: byID=%#v byKey=%#v", byID, byKey)
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceValidatesEntireExpectedRevision(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*backend.WorkflowRevision)
	}{
		{name: "id", mutate: func(expected *backend.WorkflowRevision) { expected.ID = "missing" }},
		{name: "key", mutate: func(expected *backend.WorkflowRevision) { expected.Key += "-stale" }},
		{name: "version", mutate: func(expected *backend.WorkflowRevision) { expected.Version = "stale" }},
		{name: "hash", mutate: func(expected *backend.WorkflowRevision) { expected.DefinitionHash = "hash:stale" }},
		{name: "revision", mutate: func(expected *backend.WorkflowRevision) { expected.RegistryRevision++ }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, reg, srv := newAtomicTestRegistry(t)
			original := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
			expected := backend.RevisionOfWorkflow(original)
			tt.mutate(&expected)
			replacement := original
			replacement.DefinitionHash = "hash:b"
			replacement.RegistryRevision = 0
			beforeRevision := distributedRegistryRevision(t, ctx, reg)

			_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
				MutationID:  "stale-" + tt.name,
				Expected:    expected,
				Replacement: replacement,
			})
			assertDistributedWorkflowConflict(t, err, backend.WorkflowReplaceConflictStaleRevision)
			got := getDistributedAtomicWorkflow(t, ctx, reg, original.ID)
			if got.DefinitionHash != original.DefinitionHash || got.RegistryRevision != original.RegistryRevision {
				t.Fatalf("stale replacement changed record: %#v", got)
			}
			if gotRevision := distributedRegistryRevision(t, ctx, reg); gotRevision != beforeRevision {
				t.Fatalf("stale replacement advanced revision from %d to %d", beforeRevision, gotRevision)
			}
			if srv.Exists(workflowOperationKey(namespace.FromContext(ctx), "stale-"+tt.name)) {
				t.Fatal("stale replacement wrote an operation ledger entry")
			}
		})
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceConflictsPreserveBothRecords(t *testing.T) {
	tests := []struct {
		name        string
		replacement func(a, b backend.WorkflowRecord) backend.WorkflowRecord
		want        backend.WorkflowReplaceConflictKind
	}{
		{
			name: "destination key",
			replacement: func(a, b backend.WorkflowRecord) backend.WorkflowRecord {
				a.ID = "free-id"
				a.Key = b.Key
				return a
			},
			want: backend.WorkflowReplaceConflictDestinationKey,
		},
		{
			name: "destination id",
			replacement: func(a, b backend.WorkflowRecord) backend.WorkflowRecord {
				a.ID = b.ID
				a.Key = "tenant/free@v1"
				return a
			},
			want: backend.WorkflowReplaceConflictDestinationID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, reg, _ := newAtomicTestRegistry(t)
			a := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("a-id", "tenant/a@v1", "v1", "hash:a"))
			b := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("b-id", "tenant/b@v1", "v1", "hash:b"))
			replacement := tt.replacement(a, b)
			replacement.DefinitionHash = "hash:replacement"
			replacement.RegistryRevision = 0
			beforeRevision := distributedRegistryRevision(t, ctx, reg)
			_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
				MutationID:  "conflict",
				Expected:    backend.RevisionOfWorkflow(a),
				Replacement: replacement,
			})
			assertDistributedWorkflowConflict(t, err, tt.want)
			if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
				t.Fatalf("conflict advanced revision from %d to %d", beforeRevision, got)
			}
			assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, a.ID), a)
			assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, b.ID), b)
		})
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceReplayAndMutationReuse(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
	replacement := original
	replacement.DefinitionHash = "hash:b"
	replacement.RegistryRevision = 0
	request := backend.WorkflowReplaceRequest{MutationID: "stable-operation", Expected: backend.RevisionOfWorkflow(original), Replacement: replacement}

	first, err := reg.CompareAndReplaceWorkflow(ctx, request)
	if err != nil {
		t.Fatalf("first replacement: %v", err)
	}
	revisionAfterFirst := distributedRegistryRevision(t, ctx, reg)
	request.Expected = backend.RevisionOfWorkflow(first.Current)
	replay, err := reg.CompareAndReplaceWorkflow(ctx, request)
	if err != nil {
		t.Fatalf("response-loss replay: %v", err)
	}
	if replay.Status != first.Status || replay.Previous != first.Previous || replay.Current.ID != first.Current.ID || replay.Current.RegistryRevision != first.Current.RegistryRevision {
		t.Fatalf("replay = %#v, want original result %#v", replay, first)
	}
	if got := distributedRegistryRevision(t, ctx, reg); got != revisionAfterFirst {
		t.Fatalf("replay advanced revision from %d to %d", revisionAfterFirst, got)
	}

	request.Replacement.DefinitionHash = "hash:different-request"
	_, err = reg.CompareAndReplaceWorkflow(ctx, request)
	assertDistributedWorkflowConflict(t, err, backend.WorkflowReplaceConflictMutationIDReuse)
	got := getDistributedAtomicWorkflow(t, ctx, reg, first.Current.ID)
	if got.DefinitionHash != first.Current.DefinitionHash || got.RegistryRevision != first.Current.RegistryRevision {
		t.Fatalf("MutationID reuse changed record: %#v", got)
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceConcurrentExpectedHasOneWinner(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
	const contenders = 8
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			replacement := original
			replacement.DefinitionHash = fmt.Sprintf("hash:contender-%d", i)
			replacement.RegistryRevision = 0
			_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
				MutationID:  fmt.Sprintf("contender-%d", i),
				Expected:    backend.RevisionOfWorkflow(original),
				Replacement: replacement,
			})
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	var succeeded, stale int
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case distributedWorkflowConflictKind(err) == backend.WorkflowReplaceConflictStaleRevision:
			stale++
		default:
			t.Fatalf("concurrent replacement error = %v", err)
		}
	}
	if succeeded != 1 || stale != contenders-1 {
		t.Fatalf("concurrent outcomes = %d success, %d stale; want 1 and %d", succeeded, stale, contenders-1)
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceRevisionPreventsABA(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
	toB := original
	toB.DefinitionHash = "hash:b"
	toB.RegistryRevision = 0
	b, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{MutationID: "to-b", Expected: backend.RevisionOfWorkflow(original), Replacement: toB})
	if err != nil {
		t.Fatalf("A -> B: %v", err)
	}
	backToA := b.Current
	backToA.DefinitionHash = original.DefinitionHash
	backToA.RegistryRevision = 0
	aAgain, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{MutationID: "back-to-a", Expected: backend.RevisionOfWorkflow(b.Current), Replacement: backToA})
	if err != nil {
		t.Fatalf("B -> A: %v", err)
	}
	if aAgain.Current.RegistryRevision == original.RegistryRevision {
		t.Fatalf("ABA reused revision %d", original.RegistryRevision)
	}
	stale := original
	stale.DefinitionHash = "hash:stale"
	stale.RegistryRevision = 0
	_, err = reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{MutationID: "stale-after-aba", Expected: backend.RevisionOfWorkflow(original), Replacement: stale})
	assertDistributedWorkflowConflict(t, err, backend.WorkflowReplaceConflictStaleRevision)
	got := getDistributedAtomicWorkflow(t, ctx, reg, original.ID)
	if got.DefinitionHash != original.DefinitionHash || got.RegistryRevision != aAgain.Current.RegistryRevision {
		t.Fatalf("stale ABA writer changed record: %#v", got)
	}
}

func TestDistributedWorkflowRegistryCompareAndReplaceRejectsEmptyMutationID(t *testing.T) {
	ctx, reg, srv := newAtomicTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
	replacement := original
	replacement.DefinitionHash = "hash:b"
	replacement.RegistryRevision = 0
	beforeRevision := distributedRegistryRevision(t, ctx, reg)
	if _, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{Expected: backend.RevisionOfWorkflow(original), Replacement: replacement}); err == nil {
		t.Fatal("empty MutationID replacement succeeded")
	}
	if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
		t.Fatalf("empty MutationID advanced revision from %d to %d", beforeRevision, got)
	}
	if srv.Exists(workflowOperationKey(namespace.FromContext(ctx), "")) {
		t.Fatal("empty MutationID wrote an operation ledger entry")
	}
	assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, original.ID), original)
}

func TestDistributedWorkflowRegistryLazilyImportsLegacyWithoutDeletingIt(t *testing.T) {
	ctx, reg, srv := newAtomicTestRegistry(t)
	legacy := distributedAtomicRecord("legacy-id", "tenant/legacy@v1", "v1", "hash:legacy")
	legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)

	imported, err := reg.GetWorkflow(ctx, legacy.ID)
	if err != nil {
		t.Fatalf("GetWorkflow legacy import: %v", err)
	}
	if imported.ID != legacy.ID || imported.Key != legacy.Key || imported.RegistryRevision == 0 {
		t.Fatalf("imported record = %#v", imported)
	}
	assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
	ns := namespace.FromContext(ctx)
	for _, key := range []string{
		workflowByIDKey(ns, imported.Key, imported.ID),
		workflowByKeyKey(ns, imported.Key),
		workflowIDMapKey(ns, imported.ID),
		workflowLegacyByKeyMarker(ns, imported.Key),
		workflowLegacyByIDMarker(ns, imported.ID),
	} {
		if !srv.Exists(key) {
			t.Errorf("import did not create %q", key)
		}
	}
	byKey, err := reg.GetWorkflowByKey(ctx, legacy.Key)
	if err != nil {
		t.Fatalf("GetWorkflowByKey after import: %v", err)
	}
	if byKey.RegistryRevision != imported.RegistryRevision || byKey.ID != imported.ID {
		t.Fatalf("by-key import view = %#v, want revision %d", byKey, imported.RegistryRevision)
	}

	if err := reg.RemoveWorkflow(ctx, imported.ID); err != nil {
		t.Fatalf("RemoveWorkflow imported record: %v", err)
	}
	assertDistributedWorkflowNotFoundByID(t, ctx, reg, imported.ID)
	assertDistributedWorkflowNotFoundByKey(t, ctx, reg, imported.Key)
	assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
}

func TestDistributedWorkflowRegistryConcurrentLegacyImportAllocatesOneRevision(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	legacy := distributedAtomicRecord("legacy-id", "tenant/legacy@v1", "v1", "hash:legacy")
	legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)

	const readers = 10
	type result struct {
		record backend.WorkflowRecord
		err    error
	}
	results := make(chan result, readers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				record, err := reg.GetWorkflow(ctx, legacy.ID)
				results <- result{record: record, err: err}
				return
			}
			record, err := reg.GetWorkflowByKey(ctx, legacy.Key)
			results <- result{record: record, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	var revision uint64
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent legacy import: %v", result.err)
		}
		if result.record.RegistryRevision == 0 {
			t.Fatal("concurrent legacy import returned revision zero")
		}
		if revision == 0 {
			revision = result.record.RegistryRevision
		} else if result.record.RegistryRevision != revision {
			t.Fatalf("concurrent imports returned revisions %d and %d", revision, result.record.RegistryRevision)
		}
	}
	if got := distributedRegistryRevision(t, ctx, reg); got != revision {
		t.Fatalf("revision counter = %d, want single imported revision %d", got, revision)
	}
	assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
}

func TestDistributedWorkflowRegistryAddHonorsLegacyKeyAndIDOccupancy(t *testing.T) {
	t.Run("key", func(t *testing.T) {
		ctx, reg, _ := newAtomicTestRegistry(t)
		legacy := distributedAtomicRecord("legacy-id", "tenant/legacy@v1", "v1", "hash:legacy")
		legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)
		candidate := distributedAtomicRecord("new-id", legacy.Key, "v1", "hash:new")
		if _, err := reg.AddWorkflow(ctx, candidate); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("AddWorkflow over legacy key = %v, want ErrWorkflowConflict", err)
		}
		assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
		assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, legacy.ID), legacy)
	})

	t.Run("id", func(t *testing.T) {
		ctx, reg, _ := newAtomicTestRegistry(t)
		legacy := distributedAtomicRecord("legacy-id", "tenant/legacy@v1", "v1", "hash:legacy")
		legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)
		candidate := distributedAtomicRecord(legacy.ID, "tenant/new@v1", "v1", "hash:new")
		if _, err := reg.AddWorkflow(ctx, candidate); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("AddWorkflow over legacy ID = %v, want ErrWorkflowConflict", err)
		}
		assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
		assertDistributedWorkflowNotFoundByKey(t, ctx, reg, candidate.Key)
		assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, legacy.ID), legacy)
	})
}

func TestDistributedWorkflowRegistryReplaceHonorsLegacyDestinationOccupancy(t *testing.T) {
	tests := []struct {
		name        string
		replacement func(source, legacy backend.WorkflowRecord) backend.WorkflowRecord
		want        backend.WorkflowReplaceConflictKind
	}{
		{
			name: "key",
			replacement: func(source, legacy backend.WorkflowRecord) backend.WorkflowRecord {
				source.ID = "free-id"
				source.Key = legacy.Key
				return source
			},
			want: backend.WorkflowReplaceConflictDestinationKey,
		},
		{
			name: "id",
			replacement: func(source, legacy backend.WorkflowRecord) backend.WorkflowRecord {
				source.ID = legacy.ID
				source.Key = "tenant/free@v1"
				return source
			},
			want: backend.WorkflowReplaceConflictDestinationID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, reg, srv := newAtomicTestRegistry(t)
			source := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("source-id", "tenant/source@v1", "v1", "hash:source"))
			legacy := distributedAtomicRecord("legacy-id", "tenant/legacy@v1", "v1", "hash:legacy")
			legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)
			replacement := tt.replacement(source, legacy)
			replacement.DefinitionHash = "hash:replacement"
			replacement.RegistryRevision = 0
			beforeRevision := distributedRegistryRevision(t, ctx, reg)
			_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
				MutationID:  "legacy-destination-" + tt.name,
				Expected:    backend.RevisionOfWorkflow(source),
				Replacement: replacement,
			})
			assertDistributedWorkflowConflict(t, err, tt.want)
			if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
				t.Fatalf("legacy destination conflict advanced revision from %d to %d", beforeRevision, got)
			}
			assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, source.ID), source)
			assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
			if srv.Exists(workflowOperationKey(namespace.FromContext(ctx), "legacy-destination-"+tt.name)) {
				t.Fatal("legacy destination conflict wrote operation ledger")
			}
		})
	}
}

func newAtomicTestRegistry(t *testing.T) (context.Context, *Registry, interface{ Exists(string) bool }) {
	t.Helper()
	reg, srv := newTestRegistry(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("tenant"))
	return ctx, reg, srv
}

func distributedAtomicRecord(id types.WorkflowID, key, version, hash string) backend.WorkflowRecord {
	return backend.WorkflowRecord{
		ID:             id,
		Key:            key,
		Namespace:      "tenant",
		Name:           key,
		Version:        version,
		DefinitionHash: hash,
	}
}

func addDistributedAtomicWorkflow(t *testing.T, ctx context.Context, reg *Registry, record backend.WorkflowRecord) backend.WorkflowRecord {
	t.Helper()
	stored, err := reg.AddWorkflow(ctx, record)
	if err != nil {
		t.Fatalf("AddWorkflow(%q): %v", record.Key, err)
	}
	return stored
}

func getDistributedAtomicWorkflow(t *testing.T, ctx context.Context, reg *Registry, id types.WorkflowID) backend.WorkflowRecord {
	t.Helper()
	record, err := reg.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("GetWorkflow(%q): %v", id, err)
	}
	return record
}

func getDistributedAtomicWorkflowByKey(t *testing.T, ctx context.Context, reg *Registry, key string) backend.WorkflowRecord {
	t.Helper()
	record, err := reg.GetWorkflowByKey(ctx, key)
	if err != nil {
		t.Fatalf("GetWorkflowByKey(%q): %v", key, err)
	}
	return record
}

func assertDistributedWorkflowNotFoundByID(t *testing.T, ctx context.Context, reg *Registry, id types.WorkflowID) {
	t.Helper()
	if _, err := reg.GetWorkflow(ctx, id); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow(%q) = %v, want ErrWorkflowNotFound", id, err)
	}
}

func assertDistributedWorkflowNotFoundByKey(t *testing.T, ctx context.Context, reg *Registry, key string) {
	t.Helper()
	if _, err := reg.GetWorkflowByKey(ctx, key); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowByKey(%q) = %v, want ErrWorkflowNotFound", key, err)
	}
}

func assertDistributedWorkflowIdentity(t *testing.T, got, want backend.WorkflowRecord) {
	t.Helper()
	if got.ID != want.ID || got.Key != want.Key || got.Version != want.Version || got.DefinitionHash != want.DefinitionHash {
		t.Fatalf("workflow = id %q key %q version %q hash %q, want id %q key %q version %q hash %q", got.ID, got.Key, got.Version, got.DefinitionHash, want.ID, want.Key, want.Version, want.DefinitionHash)
	}
}

func assertDistributedWorkflowConflict(t *testing.T, err error, want backend.WorkflowReplaceConflictKind) {
	t.Helper()
	if got := distributedWorkflowConflictKind(err); got != want {
		t.Fatalf("conflict kind = %q (err %v), want %q", got, err, want)
	}
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("error %v does not unwrap to ErrWorkflowConflict", err)
	}
}

func distributedWorkflowConflictKind(err error) backend.WorkflowReplaceConflictKind {
	var conflict *backend.WorkflowReplaceConflictError
	if !errors.As(err, &conflict) {
		return ""
	}
	return conflict.Kind
}

func distributedRegistryRevision(t *testing.T, ctx context.Context, reg *Registry) uint64 {
	t.Helper()
	value, err := reg.rdb.Get(ctx, workflowRevisionKey(namespace.FromContext(ctx))).Result()
	if errors.Is(err, redis.Nil) {
		return 0
	}
	if err != nil {
		t.Fatalf("read registry revision: %v", err)
	}
	revision, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("parse registry revision %q: %v", value, err)
	}
	return revision
}

func seedLegacyWorkflow(t *testing.T, ctx context.Context, reg *Registry, record backend.WorkflowRecord) map[string]string {
	t.Helper()
	payload, err := marshalWorkflowRecordPayload(record)
	if err != nil {
		t.Fatalf("marshal legacy workflow: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("decode legacy payload: %v", err)
	}
	delete(fields, "registry_revision")
	payload, err = json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode legacy payload: %v", err)
	}
	ns := namespace.FromContext(ctx)
	values := map[string]string{
		legacyWorkflowByKeyKey(ns, record.Key):              string(record.ID),
		legacyWorkflowByIDKey(ns, record.Key, record.ID):    string(payload),
		legacyWorkflowIDMapKey(ns, record.ID):               record.Key,
		legacyWorkflowDefHashKey(ns, record.Key, record.ID): record.DefinitionHash,
	}
	for key, value := range values {
		if err := reg.rdb.Set(ctx, key, value, 0).Err(); err != nil {
			t.Fatalf("seed legacy key %q: %v", key, err)
		}
	}
	return values
}

func assertLegacyWorkflowUnchanged(t *testing.T, ctx context.Context, reg *Registry, snapshot map[string]string) {
	t.Helper()
	for key, want := range snapshot {
		got, err := reg.rdb.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("read legacy key %q: %v", key, err)
		}
		if got != want {
			t.Fatalf("legacy key %q changed: got %q, want %q", key, got, want)
		}
	}
}
