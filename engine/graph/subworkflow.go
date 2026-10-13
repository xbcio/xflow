package graph

import (
	"context"
	"errors"
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

// DefaultMaxSubWorkflowDepth bounds how deep sub-workflow references may nest.
// It is a COMPILE-TIME bound on the static reference graph: a chain longer than
// this is rejected at registration rather than discovered one execution at a
// time. The engine enforces the same number as a runtime bound on actual
// child-execution depth (WithMaxSubWorkflowDepth), which is what catches a
// chain the compiler could not see; the static bound stays a constant because
// it exists to reject a definition no deployment should run.
const DefaultMaxSubWorkflowDepth = 16

var (
	// ErrSubWorkflowCycle marks a reference cycle: a workflow whose
	// sub-workflow references lead back to a workflow already on the chain.
	// Cycles are judged by NAME, deliberately ignoring the version, because
	// what a cycle deadlocks is the concurrency slot — which is keyed by
	// namespace+name — so P@v1 -> P@v2 is the same cycle as P -> P.
	ErrSubWorkflowCycle = errors.New("sub-workflow reference cycle")
	// ErrSubWorkflowDepth marks a static reference chain deeper than
	// DefaultMaxSubWorkflowDepth.
	ErrSubWorkflowDepth = errors.New("sub-workflow reference chain exceeds the depth limit")
)

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

// subWorkflowRefKey identifies one resolver answer: the reference as the
// resolver sees it. Not the node — two nodes referencing the same revision
// share one answer, exactly as the registry would serve them.
type subWorkflowRefKey struct{ name, version string }

// subWorkflowResolverMemo memoizes resolver answers for the duration of one
// compile, and is shared by the binding pass and the reference walk.
//
// The walk revisits every direct reference the binding pass just resolved, so
// without the memo one reference would be resolved twice — and the two answers
// could differ if the registry changed in between, leaving the graph pinned to
// one revision while the walk judged a different one. One reference, one
// answer, one revision.
type subWorkflowResolverMemo struct {
	resolver SubWorkflowResolver
	answers  map[subWorkflowRefKey]SubWorkflowTarget
}

func newSubWorkflowResolverMemo(resolver SubWorkflowResolver) *subWorkflowResolverMemo {
	return &subWorkflowResolverMemo{
		resolver: resolver,
		answers:  make(map[subWorkflowRefKey]SubWorkflowTarget),
	}
}

func (m *subWorkflowResolverMemo) resolve(ctx context.Context, name, version string) (SubWorkflowTarget, error) {
	key := subWorkflowRefKey{name: name, version: version}
	if t, ok := m.answers[key]; ok {
		return t, nil
	}
	t, err := m.resolver.ResolveSubWorkflow(ctx, SubWorkflowRef{Name: name, Version: version})
	if err != nil {
		return SubWorkflowTarget{}, err
	}
	if t.Graph == nil {
		return SubWorkflowTarget{}, errors.New("resolver returned no graph")
	}
	m.answers[key] = t
	// Seed the PINNED form of the same reference as well. The walk reads a
	// node's binding (the resolved version) where the binding pass read the
	// author's request (often empty = latest), so without this the same
	// reference would be resolved a second time under its pinned version — the
	// exact re-resolution the memo exists to prevent.
	if t.Version != "" && t.Version != version {
		m.answers[subWorkflowRefKey{name: name, version: t.Version}] = t
	}
	return t, nil
}

// compileSubWorkflowRefs resolves every xflow.subworkflow node's target through
// the memo's resolver and records the pinned binding on the node's NodeMeta.
//
// A nil memo is not an error and not a silent pass: the node compiles
// UNRESOLVED (SubWorkflow stays nil), and the engine refuses to dispatch such a
// node with ErrSubWorkflowUnresolved. That is the deliberate difference from
// "treat it as an ordinary node" — an unresolved reference that runs as a plain
// no-op node would report success for work that never happened.
//
// The rules checked here are the ones that need the target definition: it
// exists, the entry exists, and the target is a shape a parent may wait on.
// Rules decidable from the node itself live in validateSubWorkflowParams, which
// runs even without a resolver; rules about the whole reference CHAIN live in
// checkSubWorkflowReferences.
func compileSubWorkflowRefs(ctx context.Context, g *Graph, memo *subWorkflowResolverMemo) error {
	if memo == nil {
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
		target, err := memo.resolve(ctx, name, strings.TrimSpace(version))
		if err != nil {
			return fmt.Errorf("node %q: resolve sub-workflow %q: %w", nd.Name, name, err)
		}
		if target.Version == "" && target.DefinitionHash == "" {
			return fmt.Errorf("node %q: resolve sub-workflow %q: resolver returned neither a version nor a definition hash, "+
				"so the reference would be pinned to whatever is current at dispatch", nd.Name, name)
		}
		// Two shapes a parent can never wait on. Both are checked on the DIRECT
		// target only, and that is enough: a workflow that is itself FAF cannot
		// contain a sub-workflow node at all (validateFAF rejects it), and a
		// transient target would have failed its own registration had any of its
		// own references been transient — so membership in the registry already
		// proves the property inductively for everything further down.
		if target.Graph.FAF() {
			return fmt.Errorf("node %q: sub-workflow %q is a fire-and-forget workflow: it persists no state, "+
				"so nothing could ever report the child's conclusion back to this node", nd.Name, name)
		}
		if target.Graph.Transient() {
			return fmt.Errorf("node %q: sub-workflow %q is transient: its state expires on a TTL, "+
				"so a wait on it could outlive the records that would resolve the wait", nd.Name, name)
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

// subWorkflowHop returns a sub-workflow node's referenced workflow name and
// version. It prefers the compiled binding — the version that was actually
// pinned — over the author's parameters, so the reference walk follows the
// same revision the node will run.
func subWorkflowHop(nd NodeMeta) (name, version string) {
	if nd.SubWorkflow != nil {
		return nd.SubWorkflow.Name, nd.SubWorkflow.Version
	}
	name, _ = nd.Parameters["workflow"].(string)
	version, _ = nd.Parameters["version"].(string)
	return strings.TrimSpace(name), strings.TrimSpace(version)
}

// checkSubWorkflowReferences walks the reference graph reachable from g and
// rejects a cycle (by name) or a chain deeper than DefaultMaxSubWorkflowDepth.
//
// The walk follows each referenced workflow's COMPILED graph, so it sees what
// the child will actually run: a hop is read off the target's own sub-workflow
// nodes, whose bindings were pinned when that target was registered. This is
// the static half of the deadlock story — a cycle P -> Q -> P would have each
// execution waiting on a child that can never start, because the concurrency
// slot it needs is held by its own ancestor. The runtime depth bound
// (WithMaxSubWorkflowDepth) is the other half, for chains the compiler cannot
// see.
//
// A nil memo skips the walk entirely, like compileSubWorkflowRefs: with no way
// to see the other side of a reference there is no cycle to find, and the node
// compiles unresolved (which the engine refuses to dispatch).
func checkSubWorkflowReferences(ctx context.Context, g *Graph, memo *subWorkflowResolverMemo) error {
	if memo == nil {
		return nil
	}
	// path holds the workflow names on the current chain, starting with this
	// graph's own name, so a reference back to the root is the shortest cycle.
	path := map[string]bool{}
	if name := g.Name(); name != "" {
		path[name] = true
	}
	chain := []string{g.Name()}

	var walk func(from *Graph, depth int) error
	walk = func(from *Graph, depth int) error {
		for i := 0; i < from.NodeCount(); i++ {
			nd := from.NodeAt(i)
			if nd.Type != types.SubWorkflowNodeType {
				continue
			}
			name, version := subWorkflowHop(nd)
			if name == "" {
				// Unreachable for a graph this compiler built (the parameter
				// shape is validated in registerNodes), but a projected or
				// foreign graph could carry one; there is no reference to
				// follow and nothing to resolve.
				continue
			}
			if path[name] {
				return fmt.Errorf("%w: %s", ErrSubWorkflowCycle,
					strings.Join(append(chain, name), " -> "))
			}
			if depth+1 > DefaultMaxSubWorkflowDepth {
				return fmt.Errorf("%w: %s would exceed %d",
					ErrSubWorkflowDepth, strings.Join(append(chain, name), " -> "), DefaultMaxSubWorkflowDepth)
			}
			target, err := memo.resolve(ctx, name, version)
			if err != nil {
				return fmt.Errorf("node %q of workflow %q: resolve sub-workflow %q: %w",
					nd.Name, from.Name(), name, err)
			}
			path[name] = true
			chain = append(chain, name)
			err = walk(target.Graph, depth+1)
			chain = chain[:len(chain)-1]
			delete(path, name)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return walk(g, 0)
}
