package graph

import (
	"context"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/types"
)

// SubWorkflowRef names a workflow to run as a child execution — the lookup
// request an xflow.subworkflow node compiles into, and the one the engine
// repeats at dispatch.
//
// It is deliberately dumb data: the compiler knows what the author wrote, the
// resolver knows what is registered, and neither learns the other's job.
type SubWorkflowRef struct {
	// Namespace is the namespace the reference is read in. The COMPILER leaves
	// it empty: a WorkflowDef carries no namespace, and a resolver is bound to
	// one namespace by its adapter (sdk/xflow, service/control). The ENGINE
	// fills in the parent execution's namespace at dispatch, which is what
	// gives a resolver its one chance to reject a mismatch — a cross-namespace
	// reference is unreachable by construction, not by a compile rule.
	Namespace string
	// Name is the referenced workflow's name, same namespace, trimmed.
	Name string
	// Version is the version the author pinned, or empty for "resolve the
	// latest at compile time". A resolver normalizes the empty form (the SDK
	// defaults to "v1", the HTTP registration path may leave it empty) and
	// returns the concrete version it resolved; the compiler pins THAT.
	Version string
	// DefinitionHash is the definition hash the caller has already pinned, or
	// empty when the caller has none — which is every compile-time call, since
	// nothing is pinned before the first resolution. A runtime call carries the
	// compiled binding's hash, and the caller (not this package) verifies the
	// returned target still matches it.
	DefinitionHash string
}

// SubWorkflowTarget is a resolver's answer for one SubWorkflowRef.
type SubWorkflowTarget struct {
	// Version is the concrete version that served the request. For a ref that
	// pinned one it must equal it; for an unversioned ref it is the latest at
	// resolution time, and pinning it here is what keeps "latest" from
	// drifting at every dispatch.
	Version string
	// DefinitionHash identifies the exact definition revision served. It is
	// opaque to this package — the registry's own definition hash, not
	// GraphHash — and it is the token the engine re-verifies against the
	// compiled binding before running a child.
	DefinitionHash string
	// Graph is the compiled target, used at compile time for the rules that
	// need the target's shape (does the entry exist, is it FAF or transient)
	// and at dispatch to run the child without a second registry round-trip.
	Graph *Graph
}

// SubWorkflowResolver resolves a sub-workflow reference to a concrete,
// compiled target. It is the one external dependency the compiler accepts, and
// it is narrow on purpose: everything the compiler and the engine need to know
// about other workflows comes through this method.
//
// Implementations must honor a non-empty ref.DefinitionHash by returning
// exactly that revision or an error — never a newer one. The engine verifies
// the hash it gets back anyway, but a resolver that ignores the pin would turn
// every verification failure from a loud refusal into a fleet-wide outage of
// long-running parents.
type SubWorkflowResolver interface {
	ResolveSubWorkflow(ctx context.Context, ref SubWorkflowRef) (SubWorkflowTarget, error)
}

// SubWorkflowBinding is the compile-time resolution of one xflow.subworkflow
// node's target, stored on NodeMeta so it travels with the graph through the
// wire format and into the graph hash automatically (the same placement rule
// as NodeMeta.Body).
//
// It is the pin: Name, the concrete Version, and the DefinitionHash the target
// had when this graph was compiled. A later release of the target workflow
// does not change what this node runs, and a runtime resolution that returns a
// different hash is refused rather than silently upgraded.
//
// The binding carries no namespace: the compiler has none to record, and the
// child runs in the parent execution's namespace, so a second copy could only
// disagree with the one the engine actually uses.
type SubWorkflowBinding struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	DefinitionHash string `json:"definition_hash"`
}

func cloneSubWorkflowBinding(b *SubWorkflowBinding) *SubWorkflowBinding {
	if b == nil {
		return nil
	}
	c := *b
	return &c
}

// compileSubWorkflowRefs resolves every xflow.subworkflow node's target through
// resolver and records the pinned binding on the node's NodeMeta.
//
// A nil resolver is not an error and not a silent pass: the node compiles
// UNRESOLVED (SubWorkflow stays nil), and the engine refuses to dispatch such a
// node with ErrSubWorkflowUnresolved. That is the deliberate difference from
// "treat it as an ordinary node" — an unresolved reference that runs as a plain
// no-op node would report success for work that never happened.
//
// The rules checked here are the ones that need the target definition: the
// entry exists, and (a later pass) the reference graph is acyclic and shallow
// enough. Rules decidable from the node itself live in
// validateSubWorkflowParams, which runs even without a resolver.
func compileSubWorkflowRefs(ctx context.Context, g *Graph, resolver SubWorkflowResolver) error {
	if resolver == nil {
		return nil
	}
	for i := range g.nodes {
		nd := &g.nodes[i]
		if nd.Type != types.SubWorkflowNodeType {
			continue
		}
		// Both values passed validateSubWorkflowParams in registerNodes: the
		// name is present and non-blank, and a present version is a string.
		name, _ := nd.Parameters["workflow"].(string)
		name = strings.TrimSpace(name)
		version, _ := nd.Parameters["version"].(string)
		target, err := resolver.ResolveSubWorkflow(ctx, SubWorkflowRef{Name: name, Version: strings.TrimSpace(version)})
		if err != nil {
			return fmt.Errorf("node %q: resolve sub-workflow %q: %w", nd.Name, name, err)
		}
		if target.Graph == nil {
			return fmt.Errorf("node %q: resolve sub-workflow %q: resolver returned no graph", nd.Name, name)
		}
		if target.Version == "" && target.DefinitionHash == "" {
			return fmt.Errorf("node %q: resolve sub-workflow %q: resolver returned neither a version nor a definition hash, "+
				"so the reference would be pinned to whatever is current at dispatch", nd.Name, name)
		}
		if entry, _ := nd.Parameters["entry"].(string); entry != "" {
			if _, ok := target.Graph.EntryIndex(entry); !ok {
				return fmt.Errorf("node %q: sub-workflow %q has no entry node %q", nd.Name, name, entry)
			}
		}
		nd.SubWorkflow = &SubWorkflowBinding{
			Name:           name,
			Version:        target.Version,
			DefinitionHash: target.DefinitionHash,
		}
	}
	return nil
}
