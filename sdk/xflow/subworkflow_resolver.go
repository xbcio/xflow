package xflow

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
// served as. WorkflowBuilder defaults an absent version to "v1" before
// registration, while a definition registered to a server with no version
// stores it empty; the two are the same workflow through two doors, so the
// resolver normalizes them onto this name on the way out and matches both
// registry keys on the way in (see subWorkflowRegistryKeys).
//
// It is deliberately the same rule service/control's resolver applies: in a
// server deployment both registrars write to the same registry, so a binding
// one of them pins must be one the other can serve.
const subWorkflowDefaultVersion = "v1"

// subWorkflowLatestPageSize bounds one page of the latest-version scan. The
// scan only runs at compile time (a version-less reference) and stops at the
// first name match.
const subWorkflowLatestPageSize = 100

// subWorkflowResolver adapts the SDK's workflow registry to
// graph.SubWorkflowResolver: it turns an xflow.subworkflow reference into the
// compiled graph and definition hash the reference is pinned to.
//
// It serves both sides of the node's life. At COMPILE time it is bound to one
// namespace, because a WorkflowDef carries no namespace and AddWorkflow is the
// one call that knows which one the definition belongs to. At DISPATCH time the
// engine hands it the parent execution's namespace — the authenticated scope
// the read must happen in — and an unbound resolver REQUIRES that namespace
// rather than falling back to a default, so a namespace-less dispatch can never
// read across tenants.
type subWorkflowResolver struct {
	registry backend.WorkflowRegistry
	// namespace binds every reference the resolver answers to one scope; empty
	// leaves the reference's own namespace authoritative (the dispatch shape).
	namespace namespace.Namespace
}

func newSubWorkflowResolver(registry backend.WorkflowRegistry) *subWorkflowResolver {
	return &subWorkflowResolver{registry: registry}
}

// compileSubWorkflowResolver returns the resolver AddWorkflow compiles
// xflow.subworkflow references against, bound to the definition's namespace, or
// nil when this engine has no registry. Nil is not a silent pass: such a node
// then compiles unresolved and the engine refuses it at submission.
func (e *Engine) compileSubWorkflowResolver(ns string) graph.SubWorkflowResolver {
	if e.workflowRegistry == nil {
		return nil
	}
	return newSubWorkflowResolver(e.workflowRegistry).boundToNamespace(ns)
}

// boundToNamespace returns a copy that resolves every reference in ns. It is
// the compile-time form: AddWorkflow supplies the definition's namespace, and a
// reference naming a different one is refused — which is what keeps a
// cross-namespace sub-workflow out of a registered graph.
func (r *subWorkflowResolver) boundToNamespace(ns string) *subWorkflowResolver {
	if r == nil {
		return nil
	}
	bound := *r
	bound.namespace = namespace.Namespace(ns)
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
func (r *subWorkflowResolver) ResolveSubWorkflow(ctx context.Context, ref graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
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
	// The registry's own namespace-scoped read: the distributed registry builds
	// its key from the context, so the resolved scope is attached here rather
	// than left to whatever the caller happened to carry.
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
func (r *subWorkflowResolver) scope(refNamespace string) (namespace.Namespace, error) {
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
func (r *subWorkflowResolver) latest(ctx context.Context, ns namespace.Namespace, name string) (backend.WorkflowRecord, error) {
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
// The key set is the normalization: a workflow built by this SDK is stored as
// ns/name@v1, one registered through a server's HTTP path as ns/name@ — same
// name, same absent version, two keys — so a v1 reference consults both. With a
// pin, a candidate whose hash differs is skipped rather than returned: the hash
// identifies the exact revision, and when both keys exist the pin says which
// one the node was compiled against.
func (r *subWorkflowResolver) byVersion(ctx context.Context, ns namespace.Namespace, name, version, pinHash string) (backend.WorkflowRecord, error) {
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

// subWorkflowRegistryKey builds the ns/name@version registry key, mirroring
// workflowKey (the SDK's own registration identity) and the server's
// workflowRegistryKey.
func subWorkflowRegistryKey(ns namespace.Namespace, name, version string) string {
	return fmt.Sprintf("%s/%s@%s", ns, name, version)
}

// effectiveSubWorkflowVersion normalizes a stored version for a binding: an
// empty stored version and "v1" are the same workflow, and the binding records
// the name both lookups accept.
func effectiveSubWorkflowVersion(stored string) string {
	if stored == "" {
		return subWorkflowDefaultVersion
	}
	return stored
}
