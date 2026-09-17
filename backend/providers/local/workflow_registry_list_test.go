package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// listRecord builds a record the way the repo's callers do, with the namespace
// embedded in the logical key (service/apiserver's workflowRegistryKey is
// "ns/name@version").
//
// The key deliberately embeds the namespace because this registry's by-key map
// is global rather than per-namespace: two records sharing a logical key across
// namespaces cannot coexist here at all, which is a pre-existing property of the
// in-memory provider and not something enumeration can change. The adversarial
// property this helper preserves is that two namespaces hold the *same workflow
// name*, so a listing that leaked by name rather than by namespace would show
// the foreign record.
func listRecord(id types.WorkflowID, ns, name, version, hash string) backend.WorkflowRecord {
	return backend.WorkflowRecord{
		ID:             id,
		Key:            ns + "/" + name + "@" + version,
		Namespace:      ns,
		Name:           name,
		Version:        version,
		DefinitionHash: hash,
	}
}

func listIDs(t *testing.T, reg *workflowRegistry, ns namespace.Namespace, opts backend.WorkflowListOptions) []types.WorkflowID {
	t.Helper()

	ids, err := reg.ListWorkflows(context.Background(), ns, opts)
	if err != nil {
		t.Fatalf("ListWorkflows(%q): %v", ns, err)
	}
	return ids
}

func assertListIDs(t *testing.T, got []types.WorkflowID, want ...types.WorkflowID) {
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

// TestWorkflowRegistryListWorkflowsTracksLifecycle pins the in-memory registry
// against the same enumeration contract the distributed registry implements: an
// added record is listed, a replaced record's retired id disappears and the
// destination id appears, and a removed record drops out.
func TestWorkflowRegistryListWorkflowsTracksLifecycle(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()

	original := addAtomicTestWorkflow(t, reg, listRecord("id-old", "tenant", "wf", "v1", "hash:old"))
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}), original.ID)

	result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  "mutation-list",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: listRecord("id-new", "tenant", "wf", "v2", "hash:new"),
	})
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow: %v", err)
	}
	if result.Status != backend.WorkflowReplaceReplaced {
		t.Fatalf("status = %q, want %q", result.Status, backend.WorkflowReplaceReplaced)
	}
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}), result.Current.ID)

	// A hash-only update keeps the id listed exactly once.
	if err := reg.UpdateDefinitionHash(ctx, result.Current.ID, "hash:new", "hash:newer"); err != nil {
		t.Fatalf("UpdateDefinitionHash: %v", err)
	}
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}), result.Current.ID)

	if err := reg.RemoveWorkflow(ctx, result.Current.ID); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}))
}

// TestWorkflowRegistryListWorkflowsIsNamespaceScoped is the leakage test: the
// same logical key exists in two namespaces and neither listing shows the
// other's id.
func TestWorkflowRegistryListWorkflowsIsNamespaceScoped(t *testing.T) {
	reg := newWorkflowRegistry()

	first := addAtomicTestWorkflow(t, reg, listRecord("id-one", "ns-one", "wf", "v1", "hash:one"))
	second := addAtomicTestWorkflow(t, reg, listRecord("id-two", "ns-two", "wf", "v1", "hash:two"))
	if first.Name != second.Name || first.Version != second.Version {
		t.Fatalf("test premise broken: identities %q/%q and %q/%q differ", first.Name, first.Version, second.Name, second.Version)
	}

	assertListIDs(t, listIDs(t, reg, "ns-one", backend.WorkflowListOptions{}), first.ID)
	assertListIDs(t, listIDs(t, reg, "ns-two", backend.WorkflowListOptions{}), second.ID)
	assertListIDs(t, listIDs(t, reg, "ns-three", backend.WorkflowListOptions{}))
}

// TestWorkflowRegistryListWorkflowsTreatsEmptyRecordNamespaceAsDefault pins the
// compatibility rule: records written by embedded callers that never populated
// Namespace belong to namespace.Default rather than becoming invisible.
func TestWorkflowRegistryListWorkflowsTreatsEmptyRecordNamespaceAsDefault(t *testing.T) {
	reg := newWorkflowRegistry()

	legacy := addAtomicTestWorkflow(t, reg, backend.WorkflowRecord{
		ID:             "legacy",
		Key:            "default/wf@v1",
		DefinitionHash: "hash:legacy",
	})

	assertListIDs(t, listIDs(t, reg, namespace.Default, backend.WorkflowListOptions{}), legacy.ID)
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}))
}

// TestWorkflowRegistryListWorkflowsRejectsUnscopedNamespace pins the fail-closed
// half of the contract: an empty or malformed namespace is refused rather than
// read as "every namespace".
func TestWorkflowRegistryListWorkflowsRejectsUnscopedNamespace(t *testing.T) {
	reg := newWorkflowRegistry()
	addAtomicTestWorkflow(t, reg, listRecord("id-a", "tenant", "a", "v1", "hash:a"))

	for _, raw := range []string{"", "bad:ns", "bad{ns}", "bad*ns"} {
		if _, err := reg.ListWorkflows(context.Background(), namespace.Namespace(raw), backend.WorkflowListOptions{}); err == nil {
			t.Fatalf("ListWorkflows(%q) = nil error, want a rejection", raw)
		}
	}
	if _, err := reg.ListWorkflows(context.Background(), "tenant", backend.WorkflowListOptions{Limit: -1}); err == nil {
		t.Fatal("ListWorkflows(limit=-1) = nil error, want a rejection")
	}
	if _, err := reg.ListWorkflows(context.Background(), "tenant", backend.WorkflowListOptions{Offset: -1}); err == nil {
		t.Fatal("ListWorkflows(offset=-1) = nil error, want a rejection")
	}
}

// TestWorkflowRegistryListWorkflowsIsNewestFirstAndPaged pins the ordering and
// the offset/limit contract.
func TestWorkflowRegistryListWorkflowsIsNewestFirstAndPaged(t *testing.T) {
	reg := newWorkflowRegistry()

	first := addAtomicTestWorkflow(t, reg, listRecord("id-1", "tenant", "n1", "v1", "hash:1"))
	second := addAtomicTestWorkflow(t, reg, listRecord("id-2", "tenant", "n2", "v1", "hash:2"))
	third := addAtomicTestWorkflow(t, reg, listRecord("id-3", "tenant", "n3", "v1", "hash:3"))
	if !(first.RegistryRevision < second.RegistryRevision && second.RegistryRevision < third.RegistryRevision) {
		t.Fatalf("revisions are not monotonic: %d %d %d", first.RegistryRevision, second.RegistryRevision, third.RegistryRevision)
	}

	want := []types.WorkflowID{third.ID, second.ID, first.ID}
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}), want...)
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{Limit: 2}), want[:2]...)
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{Limit: 2, Offset: 2}), want[2:]...)
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{Offset: 3}))
	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{Offset: 99}))
}

// TestWorkflowRegistryListWorkflowsDoesNotReturnTheCallersSlice guards the
// contract that the caller owns the returned slice: mutating it must not reach
// back into registry state.
func TestWorkflowRegistryListWorkflowsDoesNotReturnTheCallersSlice(t *testing.T) {
	reg := newWorkflowRegistry()
	addAtomicTestWorkflow(t, reg, listRecord("id-1", "tenant", "n1", "v1", "hash:1"))
	addAtomicTestWorkflow(t, reg, listRecord("id-2", "tenant", "n2", "v1", "hash:2"))

	ids := listIDs(t, reg, "tenant", backend.WorkflowListOptions{})
	if len(ids) != 2 {
		t.Fatalf("ListWorkflows = %v, want 2 ids", ids)
	}
	ids[0] = "mutated"

	assertListIDs(t, listIDs(t, reg, "tenant", backend.WorkflowListOptions{}), "id-2", "id-1")
}
