package local

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/types"
)

func TestWorkflowRegistryAssignsAndRollsRevisions(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()

	first := addAtomicTestWorkflow(t, reg, backend.WorkflowRecord{
		ID:               "first",
		Key:              "tenant/first@v1",
		Namespace:        "tenant",
		Name:             "first",
		Version:          "v1",
		DefinitionHash:   "hash:first",
		RegistryRevision: 999,
	})
	if first.RegistryRevision == 0 || first.RegistryRevision == 999 {
		t.Fatalf("AddWorkflow revision = %d, want non-zero registry-assigned value", first.RegistryRevision)
	}

	idempotent := addAtomicTestWorkflow(t, reg, backend.WorkflowRecord{
		Key:            first.Key,
		DefinitionHash: first.DefinitionHash,
	})
	if idempotent.RegistryRevision != first.RegistryRevision {
		t.Fatalf("idempotent AddWorkflow revision = %d, want %d", idempotent.RegistryRevision, first.RegistryRevision)
	}

	second := addAtomicTestWorkflow(t, reg, backend.WorkflowRecord{
		ID:             "second",
		Key:            "tenant/second@v1",
		Namespace:      "tenant",
		Name:           "second",
		Version:        "v1",
		DefinitionHash: "hash:second",
	})
	if second.RegistryRevision <= first.RegistryRevision {
		t.Fatalf("second revision = %d, want > %d", second.RegistryRevision, first.RegistryRevision)
	}

	if err := reg.UpdateDefinitionHash(ctx, first.ID, first.DefinitionHash, first.DefinitionHash); err != nil {
		t.Fatalf("unchanged UpdateDefinitionHash: %v", err)
	}
	unchanged := getAtomicTestWorkflow(t, reg, first.ID)
	if unchanged.RegistryRevision != first.RegistryRevision {
		t.Fatalf("unchanged hash revision = %d, want %d", unchanged.RegistryRevision, first.RegistryRevision)
	}

	if err := reg.UpdateDefinitionHash(ctx, first.ID, first.DefinitionHash, "hash:first-v2"); err != nil {
		t.Fatalf("changed UpdateDefinitionHash: %v", err)
	}
	updated := getAtomicTestWorkflow(t, reg, first.ID)
	if updated.RegistryRevision <= second.RegistryRevision {
		t.Fatalf("updated revision = %d, want > %d", updated.RegistryRevision, second.RegistryRevision)
	}
}

func TestWorkflowRegistryCompareAndReplaceSupportsUnchangedRenameAndIDChange(t *testing.T) {
	reg := newWorkflowRegistry()
	original := addAtomicTestWorkflow(t, reg, atomicTestRecord("old-id", "tenant/flow@v1", "v1", "hash:a"))

	unchangedReq := backend.WorkflowReplaceRequest{
		MutationID:  "unchanged",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: original,
	}
	unchangedReq.Replacement.RegistryRevision = 12345
	unchanged, err := reg.CompareAndReplaceWorkflow(context.Background(), unchangedReq)
	if err != nil {
		t.Fatalf("unchanged CompareAndReplaceWorkflow: %v", err)
	}
	if unchanged.Status != backend.WorkflowReplaceUnchanged {
		t.Fatalf("unchanged status = %q, want %q", unchanged.Status, backend.WorkflowReplaceUnchanged)
	}
	if unchanged.Current.RegistryRevision != original.RegistryRevision {
		t.Fatalf("unchanged revision = %d, want %d", unchanged.Current.RegistryRevision, original.RegistryRevision)
	}

	sameIDReplacement := original
	sameIDReplacement.DefinitionHash = "hash:b"
	sameIDReplacement.RegistryRevision = 0
	sameID, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		MutationID:  "same-id",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: sameIDReplacement,
	})
	if err != nil {
		t.Fatalf("same-ID CompareAndReplaceWorkflow: %v", err)
	}
	if sameID.Status != backend.WorkflowReplaceReplaced || sameID.Current.ID != original.ID {
		t.Fatalf("same-ID result = status %q id %q", sameID.Status, sameID.Current.ID)
	}
	if sameID.Current.RegistryRevision <= original.RegistryRevision {
		t.Fatalf("same-ID revision = %d, want > %d", sameID.Current.RegistryRevision, original.RegistryRevision)
	}

	renamedRecord := sameID.Current
	renamedRecord.ID = "new-id"
	renamedRecord.Key = "tenant/renamed@v2"
	renamedRecord.Name = "renamed"
	renamedRecord.Version = "v2"
	renamedRecord.DefinitionHash = "hash:c"
	renamedRecord.RegistryRevision = 0
	renamed, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		MutationID:  "rename-and-change-id",
		Expected:    backend.RevisionOfWorkflow(sameID.Current),
		Replacement: renamedRecord,
	})
	if err != nil {
		t.Fatalf("rename CompareAndReplaceWorkflow: %v", err)
	}
	if renamed.Current.ID != "new-id" || renamed.Current.Key != "tenant/renamed@v2" {
		t.Fatalf("renamed current = id %q key %q", renamed.Current.ID, renamed.Current.Key)
	}
	assertAtomicTestNotFoundByID(t, reg, original.ID)
	assertAtomicTestNotFoundByKey(t, reg, original.Key)
	byID := getAtomicTestWorkflow(t, reg, renamed.Current.ID)
	byKey := getAtomicTestWorkflowByKey(t, reg, renamed.Current.Key)
	if byID != byKey {
		t.Fatalf("by-ID and by-key records differ: %#v != %#v", byID, byKey)
	}
}

func TestWorkflowRegistryCompareAndReplaceConflictsDoNotModifyIndexes(t *testing.T) {
	tests := []struct {
		name        string
		replacement func(a, b backend.WorkflowRecord) backend.WorkflowRecord
		wantKind    backend.WorkflowReplaceConflictKind
	}{
		{
			name: "destination key",
			replacement: func(a, b backend.WorkflowRecord) backend.WorkflowRecord {
				a.ID = "free-id"
				a.Key = b.Key
				return a
			},
			wantKind: backend.WorkflowReplaceConflictDestinationKey,
		},
		{
			name: "destination id",
			replacement: func(a, b backend.WorkflowRecord) backend.WorkflowRecord {
				a.ID = b.ID
				a.Key = "tenant/free@v1"
				return a
			},
			wantKind: backend.WorkflowReplaceConflictDestinationID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := newWorkflowRegistry()
			a := addAtomicTestWorkflow(t, reg, atomicTestRecord("a-id", "tenant/a@v1", "v1", "hash:a"))
			b := addAtomicTestWorkflow(t, reg, atomicTestRecord("b-id", "tenant/b@v1", "v1", "hash:b"))
			beforeRevision := reg.revision
			replacement := tt.replacement(a, b)
			replacement.DefinitionHash = "hash:replacement"
			replacement.RegistryRevision = 0

			_, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
				MutationID:  "conflict",
				Expected:    backend.RevisionOfWorkflow(a),
				Replacement: replacement,
			})
			assertAtomicTestConflict(t, err, tt.wantKind)
			if reg.revision != beforeRevision {
				t.Fatalf("revision advanced from %d to %d on conflict", beforeRevision, reg.revision)
			}
			if got := getAtomicTestWorkflow(t, reg, a.ID); got != a {
				t.Fatalf("source changed on conflict: %#v != %#v", got, a)
			}
			if got := getAtomicTestWorkflow(t, reg, b.ID); got != b {
				t.Fatalf("destination changed on conflict: %#v != %#v", got, b)
			}
		})
	}
}

func TestWorkflowRegistryCompareAndReplaceValidatesEntireExpectedRevision(t *testing.T) {
	original := atomicTestRecord("old-id", "tenant/flow@v1", "v1", "hash:a")
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
			reg := newWorkflowRegistry()
			stored := addAtomicTestWorkflow(t, reg, original)
			expected := backend.RevisionOfWorkflow(stored)
			tt.mutate(&expected)
			replacement := stored
			replacement.DefinitionHash = "hash:new"
			replacement.RegistryRevision = 0

			_, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
				MutationID:  "stale-" + tt.name,
				Expected:    expected,
				Replacement: replacement,
			})
			assertAtomicTestConflict(t, err, backend.WorkflowReplaceConflictStaleRevision)
			if got := getAtomicTestWorkflow(t, reg, stored.ID); got != stored {
				t.Fatalf("record changed after stale %s: %#v != %#v", tt.name, got, stored)
			}
		})
	}
}

func TestWorkflowRegistryCompareAndReplaceMutationIDIsIdempotent(t *testing.T) {
	reg := newWorkflowRegistry()
	original := addAtomicTestWorkflow(t, reg, atomicTestRecord("old-id", "tenant/flow@v1", "v1", "hash:a"))
	replacement := original
	replacement.DefinitionHash = "hash:b"
	replacement.RegistryRevision = 0
	req := backend.WorkflowReplaceRequest{
		MutationID:  "mutation-1",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	}

	first, err := reg.CompareAndReplaceWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("first CompareAndReplaceWorkflow: %v", err)
	}
	revisionAfterFirst := reg.revision
	retry := req
	// Model response loss recovery: the caller re-read the now-current record
	// and supplied its new mutable CAS fields. The source ID and semantic
	// replacement are unchanged, so the operation ledger must replay before CAS.
	retry.Expected = backend.RevisionOfWorkflow(first.Current)
	replay, err := reg.CompareAndReplaceWorkflow(context.Background(), retry)
	if err != nil {
		t.Fatalf("response-loss replay CompareAndReplaceWorkflow: %v", err)
	}
	if replay != first {
		t.Fatalf("response-loss replay = %#v, want %#v", replay, first)
	}
	if reg.revision != revisionAfterFirst {
		t.Fatalf("response-loss replay advanced revision from %d to %d", revisionAfterFirst, reg.revision)
	}

	reused := retry
	reused.Replacement.DefinitionHash = "hash:different-fingerprint"
	_, err = reg.CompareAndReplaceWorkflow(context.Background(), reused)
	assertAtomicTestConflict(t, err, backend.WorkflowReplaceConflictMutationIDReuse)
	if got := getAtomicTestWorkflow(t, reg, first.Current.ID); got != first.Current {
		t.Fatalf("mutation-ID reuse changed current record: %#v != %#v", got, first.Current)
	}
}

func TestWorkflowRegistryCompareAndReplaceConcurrentExpectedHasOneWinner(t *testing.T) {
	reg := newWorkflowRegistry()
	original := addAtomicTestWorkflow(t, reg, atomicTestRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))

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
			_, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
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

	succeeded := 0
	stale := 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case atomicTestConflictKind(err) == backend.WorkflowReplaceConflictStaleRevision:
			stale++
		default:
			t.Fatalf("concurrent replacement error = %v", err)
		}
	}
	if succeeded != 1 || stale != contenders-1 {
		t.Fatalf("concurrent outcomes = %d success, %d stale; want 1 and %d", succeeded, stale, contenders-1)
	}
}

func TestWorkflowRegistryCompareAndReplaceRevisionPreventsABA(t *testing.T) {
	reg := newWorkflowRegistry()
	original := addAtomicTestWorkflow(t, reg, atomicTestRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))

	toB := original
	toB.DefinitionHash = "hash:b"
	toB.RegistryRevision = 0
	b, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		MutationID:  "to-b",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: toB,
	})
	if err != nil {
		t.Fatalf("A -> B: %v", err)
	}

	backToA := b.Current
	backToA.DefinitionHash = original.DefinitionHash
	backToA.RegistryRevision = 0
	aAgain, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		MutationID:  "back-to-a",
		Expected:    backend.RevisionOfWorkflow(b.Current),
		Replacement: backToA,
	})
	if err != nil {
		t.Fatalf("B -> A: %v", err)
	}
	if aAgain.Current.DefinitionHash != original.DefinitionHash || aAgain.Current.RegistryRevision == original.RegistryRevision {
		t.Fatalf("ABA current = hash %q revision %d; original revision %d", aAgain.Current.DefinitionHash, aAgain.Current.RegistryRevision, original.RegistryRevision)
	}

	staleReplacement := original
	staleReplacement.DefinitionHash = "hash:stale-writer"
	staleReplacement.RegistryRevision = 0
	_, err = reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		MutationID:  "stale-after-aba",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: staleReplacement,
	})
	assertAtomicTestConflict(t, err, backend.WorkflowReplaceConflictStaleRevision)
	if got := getAtomicTestWorkflow(t, reg, original.ID); got != aAgain.Current {
		t.Fatalf("stale ABA writer changed record: %#v != %#v", got, aAgain.Current)
	}
}

func TestWorkflowRegistryCompareAndReplaceCanceledContextDoesNotCommit(t *testing.T) {
	reg := newWorkflowRegistry()
	original := addAtomicTestWorkflow(t, reg, atomicTestRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
	replacement := original
	replacement.DefinitionHash = "hash:b"
	replacement.RegistryRevision = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  "canceled",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replacement error = %v, want context.Canceled", err)
	}
	if got := getAtomicTestWorkflow(t, reg, original.ID); got != original {
		t.Fatalf("canceled replacement changed record: %#v != %#v", got, original)
	}
	if len(reg.mutations) != 0 {
		t.Fatalf("canceled replacement recorded %d mutations, want 0", len(reg.mutations))
	}
}

func TestWorkflowRegistryCompareAndReplaceRejectsEmptyMutationID(t *testing.T) {
	reg := newWorkflowRegistry()
	original := addAtomicTestWorkflow(t, reg, atomicTestRecord("flow-id", "tenant/flow@v1", "v1", "hash:a"))
	replacement := original
	replacement.DefinitionHash = "hash:b"
	replacement.RegistryRevision = 0

	_, err := reg.CompareAndReplaceWorkflow(context.Background(), backend.WorkflowReplaceRequest{
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	})
	if err == nil {
		t.Fatal("empty MutationID replacement succeeded")
	}
	if got := getAtomicTestWorkflow(t, reg, original.ID); got != original {
		t.Fatalf("empty MutationID replacement changed record: %#v != %#v", got, original)
	}
	if len(reg.mutations) != 0 {
		t.Fatalf("empty MutationID replacement recorded %d mutations, want 0", len(reg.mutations))
	}
}

func atomicTestRecord(id types.WorkflowID, key, version, hash string) backend.WorkflowRecord {
	return backend.WorkflowRecord{
		ID:             id,
		Key:            key,
		Namespace:      "tenant",
		Name:           key,
		Version:        version,
		DefinitionHash: hash,
	}
}

func addAtomicTestWorkflow(t *testing.T, reg *workflowRegistry, rec backend.WorkflowRecord) backend.WorkflowRecord {
	t.Helper()
	stored, err := reg.AddWorkflow(context.Background(), rec)
	if err != nil {
		t.Fatalf("AddWorkflow(%q): %v", rec.Key, err)
	}
	return stored
}

func getAtomicTestWorkflow(t *testing.T, reg *workflowRegistry, id types.WorkflowID) backend.WorkflowRecord {
	t.Helper()
	rec, err := reg.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow(%q): %v", id, err)
	}
	return rec
}

func getAtomicTestWorkflowByKey(t *testing.T, reg *workflowRegistry, key string) backend.WorkflowRecord {
	t.Helper()
	rec, err := reg.GetWorkflowByKey(context.Background(), key)
	if err != nil {
		t.Fatalf("GetWorkflowByKey(%q): %v", key, err)
	}
	return rec
}

func assertAtomicTestNotFoundByID(t *testing.T, reg *workflowRegistry, id types.WorkflowID) {
	t.Helper()
	_, err := reg.GetWorkflow(context.Background(), id)
	if !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow(%q) error = %v, want ErrWorkflowNotFound", id, err)
	}
}

func assertAtomicTestNotFoundByKey(t *testing.T, reg *workflowRegistry, key string) {
	t.Helper()
	_, err := reg.GetWorkflowByKey(context.Background(), key)
	if !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowByKey(%q) error = %v, want ErrWorkflowNotFound", key, err)
	}
}

func assertAtomicTestConflict(t *testing.T, err error, want backend.WorkflowReplaceConflictKind) {
	t.Helper()
	if got := atomicTestConflictKind(err); got != want {
		t.Fatalf("conflict kind = %q (err %v), want %q", got, err, want)
	}
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("error %v does not unwrap to ErrWorkflowConflict", err)
	}
}

func atomicTestConflictKind(err error) backend.WorkflowReplaceConflictKind {
	var conflict *backend.WorkflowReplaceConflictError
	if !errors.As(err, &conflict) {
		return ""
	}
	return conflict.Kind
}
