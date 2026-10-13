package control

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
)

// subWorkflowDefaultVersion is the version an unversioned registry record is
// served as. The SDK's WorkflowBuilder defaults an absent version to "v1" before
// registration, while the HTTP registration path stores it empty; the two are
// the same workflow registered through two doors, so the resolver normalizes
// them onto this name on the way out and matches both registry keys on the way
// in (see subWorkflowRegistryKeys).
const subWorkflowDefaultVersion = "v1"

// subWorkflowLatestPageSize bounds one page of the latest-version scan. The
// scan only runs at compile time (a version-less reference), and it stops at
// the first name match, so the page size only decides how much of a large
// namespace is read before the match is found.
const subWorkflowLatestPageSize = 100

// SubWorkflowResolver adapts a workflow registry to graph.SubWorkflowResolver:
// it turns an xflow.subworkflow reference into the compiled graph and the
// definition hash the reference is pinned to.
//
// It serves both sides of the node's life. At COMPILE time the resolver is
// bound to one namespace (BoundToNamespace), because a WorkflowDef carries no
// namespace and the author's reference is resolved once, at registration. At
// DISPATCH time the engine hands it the parent execution's namespace, which is
// the authenticated scope the read must happen in — an unbound resolver
// REQUIRES that namespace rather than falling back to a default, so a
// namespace-less dispatch can never read across tenants.
type SubWorkflowResolver struct {
	registry backend.WorkflowRegistry
	// namespace binds every reference the resolver answers to one scope; empty
	// leaves the reference's own namespace authoritative (the dispatch shape).
	namespace namespace.Namespace
}

// NewSubWorkflowResolver returns an unbound resolver over registry. The engine
// takes this form: every dispatch names its namespace, and the resolver's
// answer is scoped to it.
func NewSubWorkflowResolver(registry backend.WorkflowRegistry) *SubWorkflowResolver {
	return &SubWorkflowResolver{registry: registry}
}

// BoundToNamespace returns a copy that resolves every reference in ns. It is
// the compile-time form: the compiler has no namespace to supply, so the caller
// that does — the registration path of the server or the SDK — supplies it
// here. A reference that names a different namespace is refused, which is what
// makes a cross-namespace sub-workflow unreachable at compile time.
func (r *SubWorkflowResolver) BoundToNamespace(ns namespace.Namespace) *SubWorkflowResolver {
	if r == nil {
		return nil
	}
	bound := *r
	bound.namespace = ns
	return &bound
}

// ResolveSubWorkflow implements graph.SubWorkflowResolver.
//
// The version-less reference is the "latest at compile time" form: it is
// resolved by name against the registry's newest-revision-first enumeration and
// the concrete version is returned, so the compiled binding pins it and no
// later registration moves the node. A reference that pins a definition hash
// must be served exactly that revision; a record with a different hash is a
// refusal, not an upgrade.
func (r *SubWorkflowResolver) ResolveSubWorkflow(ctx context.Context, ref graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	if r == nil || r.registry == nil {
		return graph.SubWorkflowTarget{}, errors.New("sub-workflow resolver: no workflow registry is configured")
	}
	name := strings.TrimSpace(ref.Name)
	if name == "" {
		return graph.SubWorkflowTarget{}, errors.New("sub-workflow resolver: the reference names no workflow")
	}
	ns, err := r.scope(ref.Namespace)
	if err != nil {
		return graph.SubWorkflowTarget{}, err
	}
	// The registry's own namespace-scoped read: rstate builds its key from the
	// context, so the resolved scope is attached here rather than left to
	// whatever the caller happened to carry.
	ctx = namespace.WithNamespace(ctx, ns)

	version := strings.TrimSpace(ref.Version)
	var rec backend.WorkflowRecord
	switch {
	case version == "" && ref.DefinitionHash == "":
		rec, err = r.latest(ctx, ns, name)
	case version == "":
		err = fmt.Errorf("sub-workflow %q: the reference pins definition %q but names no version",
			name, ref.DefinitionHash)
	default:
		rec, err = r.byVersion(ctx, ns, name, version, ref.DefinitionHash)
	}
	if err != nil {
		return graph.SubWorkflowTarget{}, fmt.Errorf("sub-workflow %q: %w", name, err)
	}
	if rec.Graph == nil {
		// Every registration path compiles before storing, so this is a
		// corrupt or imported record rather than a normal one; resolving it
		// without a graph would fail later with a much worse message.
		return graph.SubWorkflowTarget{}, fmt.Errorf("sub-workflow %q: registry record %q carries no compiled graph", name, rec.Key)
	}
	return graph.SubWorkflowTarget{
		Version:        effectiveSubWorkflowVersion(rec.Version),
		DefinitionHash: rec.DefinitionHash,
		Graph:          rec.Graph,
	}, nil
}

// scope decides the namespace one reference is read in: the bound one at
// compile time, the reference's own (the execution's) at dispatch, and a
// refusal whenever the two disagree.
func (r *SubWorkflowResolver) scope(refNamespace string) (namespace.Namespace, error) {
	ref := namespace.Namespace(strings.TrimSpace(refNamespace))
	if r.namespace == "" {
		if ref == "" {
			return "", errors.New("sub-workflow resolver: the reference names no namespace and the resolver is not bound to one")
		}
		if err := namespace.Validate(ref); err != nil {
			return "", fmt.Errorf("sub-workflow resolver: %w", err)
		}
		return ref, nil
	}
	if ref != "" && ref != r.namespace {
		return "", fmt.Errorf("sub-workflow resolver: cross-namespace reference: bound to %q, the reference names %q",
			r.namespace, ref)
	}
	if err := namespace.Validate(r.namespace); err != nil {
		return "", fmt.Errorf("sub-workflow resolver: %w", err)
	}
	return r.namespace, nil
}

// latest resolves a version-less reference to the most recently registered
// revision of name: the registry enumerates newest registry revision first, so
// the first name match is the answer. Compile time only — the engine always
// dispatches a pinned version.
func (r *SubWorkflowResolver) latest(ctx context.Context, ns namespace.Namespace, name string) (backend.WorkflowRecord, error) {
	for offset := 0; ; offset += subWorkflowLatestPageSize {
		ids, err := r.registry.ListWorkflows(ctx, ns, backend.WorkflowListOptions{
			Limit:  subWorkflowLatestPageSize,
			Offset: offset,
		})
		if err != nil {
			return backend.WorkflowRecord{}, fmt.Errorf("list workflows in %q: %w", ns, err)
		}
		for _, id := range ids {
			rec, err := r.registry.GetWorkflow(ctx, id)
			if err != nil {
				if errors.Is(err, backend.ErrWorkflowNotFound) {
					// Removed between the enumeration and the read; not a name
					// match, not an error.
					continue
				}
				return backend.WorkflowRecord{}, fmt.Errorf("read workflow %q: %w", id, err)
			}
			if rec.Name == name {
				return rec, nil
			}
		}
		if len(ids) < subWorkflowLatestPageSize {
			break
		}
	}
	return backend.WorkflowRecord{}, fmt.Errorf("no workflow named %q is registered: %w", name, backend.ErrWorkflowNotFound)
}

// byVersion resolves a versioned reference by registry key, honoring a pin.
//
// The key set is the normalization: a workflow registered through the SDK is
// stored as ns/name@v1, one registered through the HTTP path as ns/name@ — same
// name, same absent version, two keys — so a v1 reference consults both. With a
// pin, a candidate whose hash differs is skipped rather than returned: the hash
// identifies the exact revision, and when both keys exist the pin says which
// one the node was compiled against.
func (r *SubWorkflowResolver) byVersion(ctx context.Context, ns namespace.Namespace, name, version, pinHash string) (backend.WorkflowRecord, error) {
	var pinnedOut *backend.WorkflowRecord
	for _, key := range subWorkflowRegistryKeys(ns, name, version) {
		rec, err := r.registry.GetWorkflowByKey(ctx, key)
		if err != nil {
			if errors.Is(err, backend.ErrWorkflowNotFound) {
				continue
			}
			return backend.WorkflowRecord{}, fmt.Errorf("read workflow %q: %w", key, err)
		}
		if pinHash == "" || rec.DefinitionHash == pinHash {
			return rec, nil
		}
		if pinnedOut == nil {
			mismatch := rec
			pinnedOut = &mismatch
		}
	}
	if pinnedOut != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("workflow %q@%s is definition %q, the reference pins %q",
			name, version, pinnedOut.DefinitionHash, pinHash)
	}
	return backend.WorkflowRecord{}, fmt.Errorf("workflow %q@%s is not registered: %w", name, version, backend.ErrWorkflowNotFound)
}

// subWorkflowRegistryKeys returns the registry keys a versioned reference may
// live under, most specific first.
func subWorkflowRegistryKeys(ns namespace.Namespace, name, version string) []string {
	keys := []string{subWorkflowRegistryKey(ns, name, version)}
	if version == subWorkflowDefaultVersion {
		keys = append(keys, subWorkflowRegistryKey(ns, name, ""))
	}
	return keys
}

// subWorkflowRegistryKey builds the ns/name@version registry key, mirroring the
// SDK's workflow identity and the server's workflowRegistryKey.
func subWorkflowRegistryKey(ns namespace.Namespace, name, version string) string {
	return fmt.Sprintf("%s/%s@%s", ns, name, version)
}

// effectiveSubWorkflowVersion normalizes a stored version for a binding: the
// HTTP path's empty version and the SDK's "v1" are the same workflow, and the
// binding records the name both lookups accept.
func effectiveSubWorkflowVersion(stored string) string {
	if stored == "" {
		return subWorkflowDefaultVersion
	}
	return stored
}
