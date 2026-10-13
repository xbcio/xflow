package control

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// subWorkflowResolverTestGraph compiles a graph a resolver answer can carry.
func subWorkflowResolverTestGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "child",
		Nodes: []types.NodeDef{{Name: "work", Type: "xflow.noop"}},
	})
	if err != nil {
		t.Fatalf("compile test graph: %v", err)
	}
	return g
}

// registerSubWorkflowTestRecord adds one registry record and returns it with
// its registry-assigned identity.
func registerSubWorkflowTestRecord(t *testing.T, reg backend.WorkflowRegistry, rec backend.WorkflowRecord) backend.WorkflowRecord {
	t.Helper()
	stored, err := reg.AddWorkflow(context.Background(), rec)
	if err != nil {
		t.Fatalf("AddWorkflow(%q): %v", rec.Key, err)
	}
	return stored
}

// TestSubWorkflowResolverServesAPinnedRevision is the dispatch shape: an
// unbound resolver reads in the namespace the engine supplied and answers with
// the pinned record's graph, hash and version.
func TestSubWorkflowResolverServesAPinnedRevision(t *testing.T) {
	reg := local.New().WorkflowRegistry()
	g := subWorkflowResolverTestGraph(t)
	child := registerSubWorkflowTestRecord(t, reg, backend.WorkflowRecord{
		Key: "ns/child@v1", Namespace: "ns", Name: "child", Version: "v1",
		DefinitionHash: "sha256:h1", Graph: g,
	})
	resolver := NewSubWorkflowResolver(reg)

	target, err := resolver.ResolveSubWorkflow(context.Background(), graph.SubWorkflowRef{
		Namespace: "ns", Name: "child", Version: "v1", DefinitionHash: child.DefinitionHash,
	})
	if err != nil {
		t.Fatalf("ResolveSubWorkflow: %v", err)
	}
	if target.Graph != g {
		t.Fatal("resolver answered with a different graph than the registered record carries")
	}
	if target.DefinitionHash != child.DefinitionHash || target.Version != "v1" {
		t.Fatalf("target = %+v, want version v1 and hash %q", target, child.DefinitionHash)
	}
}

// TestSubWorkflowResolverServesTheLatestForAVersionlessReference is the
// compile-time shape: an author reference with no version resolves to the most
// recently registered revision of that NAME, and only that name.
func TestSubWorkflowResolverServesTheLatestForAVersionlessReference(t *testing.T) {
	ctx := context.Background()
	reg := local.New().WorkflowRegistry()
	g := subWorkflowResolverTestGraph(t)
	registerSubWorkflowTestRecord(t, reg, backend.WorkflowRecord{
		Key: "ns/child@v1", Namespace: "ns", Name: "child", Version: "v1",
		DefinitionHash: "sha256:h1", Graph: g,
	})
	registerSubWorkflowTestRecord(t, reg, backend.WorkflowRecord{
		Key: "ns/other@v1", Namespace: "ns", Name: "other", Version: "v1",
		DefinitionHash: "sha256:ho", Graph: g,
	})
	newest := registerSubWorkflowTestRecord(t, reg, backend.WorkflowRecord{
		Key: "ns/child@v2", Namespace: "ns", Name: "child", Version: "v2",
		DefinitionHash: "sha256:h2", Graph: g,
	})
	resolver := NewSubWorkflowResolver(reg).BoundToNamespace("ns")

	target, err := resolver.ResolveSubWorkflow(ctx, graph.SubWorkflowRef{Name: "child"})
	if err != nil {
		t.Fatalf("ResolveSubWorkflow: %v", err)
	}
	if target.Version != "v2" || target.DefinitionHash != newest.DefinitionHash {
		t.Fatalf("target = %+v, want the newest child revision v2/%q", target, newest.DefinitionHash)
	}

	if _, err := resolver.ResolveSubWorkflow(ctx, graph.SubWorkflowRef{Name: "absent"}); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("ResolveSubWorkflow(absent) error = %v, want ErrWorkflowNotFound", err)
	}
}

// TestSubWorkflowResolverNormalizesTheEmptyRegisteredVersion pins the two-door
// registration reality: the HTTP path stores an absent version as "", the SDK
// defaults it to "v1", and the resolver treats them as the one version they are
// — a v1 reference finds the empty-version record, and the answer reports "v1"
// so the compiled binding round-trips through a key search that tries both.
func TestSubWorkflowResolverNormalizesTheEmptyRegisteredVersion(t *testing.T) {
	reg := local.New().WorkflowRegistry()
	registerSubWorkflowTestRecord(t, reg, backend.WorkflowRecord{
		Key: "ns/child@", Namespace: "ns", Name: "child", Version: "",
		DefinitionHash: "sha256:h0", Graph: subWorkflowResolverTestGraph(t),
	})
	resolver := NewSubWorkflowResolver(reg).BoundToNamespace("ns")

	target, err := resolver.ResolveSubWorkflow(context.Background(), graph.SubWorkflowRef{Name: "child", Version: "v1"})
	if err != nil {
		t.Fatalf("ResolveSubWorkflow: %v", err)
	}
	if target.Version != "v1" || target.DefinitionHash != "sha256:h0" {
		t.Fatalf("target = %+v, want the empty-version record served as v1", target)
	}

	// The dispatch half of the round trip: the binding the compile path just
	// produced must resolve again under its normalized version.
	if _, err := resolver.ResolveSubWorkflow(context.Background(), graph.SubWorkflowRef{
		Name: "child", Version: target.Version, DefinitionHash: target.DefinitionHash,
	}); err != nil {
		t.Fatalf("re-resolve under the normalized binding: %v", err)
	}
}

// TestSubWorkflowResolverRefusesAPinMismatch pins the resolver's half of the
// pin contract: a reference that names a definition hash is served exactly that
// revision, and a record with a different hash is a refusal rather than a
// quiet upgrade.
func TestSubWorkflowResolverRefusesAPinMismatch(t *testing.T) {
	reg := local.New().WorkflowRegistry()
	registerSubWorkflowTestRecord(t, reg, backend.WorkflowRecord{
		Key: "ns/child@v1", Namespace: "ns", Name: "child", Version: "v1",
		DefinitionHash: "sha256:h1", Graph: subWorkflowResolverTestGraph(t),
	})
	resolver := NewSubWorkflowResolver(reg).BoundToNamespace("ns")

	_, err := resolver.ResolveSubWorkflow(context.Background(), graph.SubWorkflowRef{
		Name: "child", Version: "v1", DefinitionHash: "sha256:other",
	})
	if err == nil || !strings.Contains(err.Error(), "pins") {
		t.Fatalf("ResolveSubWorkflow error = %v, want a pin-mismatch refusal", err)
	}
}

// TestSubWorkflowResolverRefusesCrossNamespaceReferences pins the compile-time
// boundary: a bound resolver answers in its own namespace only, so a reference
// naming another one is refused before any read happens.
func TestSubWorkflowResolverRefusesCrossNamespaceReferences(t *testing.T) {
	reg := local.New().WorkflowRegistry()
	resolver := NewSubWorkflowResolver(reg).BoundToNamespace("ns")

	_, err := resolver.ResolveSubWorkflow(context.Background(), graph.SubWorkflowRef{
		Namespace: "other", Name: "child", Version: "v1",
	})
	if err == nil || !strings.Contains(err.Error(), "cross-namespace") {
		t.Fatalf("ResolveSubWorkflow error = %v, want a cross-namespace refusal", err)
	}
}

// TestSubWorkflowResolverRefusesANamespacelessDispatch pins the unbound half:
// the engine always supplies the execution's namespace, so its absence is a
// broken dispatch — refused, never widened to a default namespace read.
func TestSubWorkflowResolverRefusesANamespacelessDispatch(t *testing.T) {
	reg := local.New().WorkflowRegistry()
	resolver := NewSubWorkflowResolver(reg)

	_, err := resolver.ResolveSubWorkflow(context.Background(), graph.SubWorkflowRef{Name: "child", Version: "v1"})
	if err == nil || !strings.Contains(err.Error(), "names no namespace") {
		t.Fatalf("ResolveSubWorkflow error = %v, want a namespaceless refusal", err)
	}
}

// scopedNamespaceRegistry records the namespace the resolver read in, which is
// the part of the contract a namespace-blind in-memory registry cannot observe.
type scopedNamespaceRegistry struct {
	backend.WorkflowRegistry
	reads  []namespace.Namespace
	record backend.WorkflowRecord
}

func (r *scopedNamespaceRegistry) GetWorkflowByKey(ctx context.Context, key string) (backend.WorkflowRecord, error) {
	r.reads = append(r.reads, namespace.FromContext(ctx))
	return r.record, nil
}

func (r *scopedNamespaceRegistry) GetWorkflow(ctx context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	r.reads = append(r.reads, namespace.FromContext(ctx))
	return r.record, nil
}

func (r *scopedNamespaceRegistry) ListWorkflows(ctx context.Context, ns namespace.Namespace, _ backend.WorkflowListOptions) ([]types.WorkflowID, error) {
	r.reads = append(r.reads, namespace.FromContext(ctx))
	return []types.WorkflowID{"id-1"}, nil
}

// TestSubWorkflowResolverReadsInTheResolvedNamespace pins the context the
// registry read happens in: the distributed registry scopes its key by the
// context namespace, so the resolver must attach the scope it resolved — the
// caller's context cannot be trusted to carry it.
func TestSubWorkflowResolverReadsInTheResolvedNamespace(t *testing.T) {
	reg := &scopedNamespaceRegistry{record: backend.WorkflowRecord{
		Key: "tenant-a/child@v1", Name: "child", Version: "v1",
		DefinitionHash: "sha256:h1", Graph: subWorkflowResolverTestGraph(t),
	}}
	resolver := NewSubWorkflowResolver(reg).BoundToNamespace("tenant-a")

	// Caller context deliberately carries a different namespace.
	ctx := namespace.WithNamespace(context.Background(), "tenant-b")
	if _, err := resolver.ResolveSubWorkflow(ctx, graph.SubWorkflowRef{Name: "child", Version: "v1"}); err != nil {
		t.Fatalf("ResolveSubWorkflow: %v", err)
	}
	if len(reg.reads) != 1 || reg.reads[0] != "tenant-a" {
		t.Fatalf("registry reads happened in %v, want tenant-a", reg.reads)
	}
}
