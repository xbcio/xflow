package graph

import (
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

// NodeBodyPackage is the projection of one node's declared body sub-graph into
// a self-contained SubgraphPackage, plus its canonical hash. Today only
// xflow.map declares a body, but nothing in this type is map-specific: it is
// the shape any body-bearing node type stores.
//
// A body and a node group are the same structure — a set of member nodes with a
// unique dominating entry and boundary outputs — so a body reuses
// SubgraphPackage rather than getting a parallel type. What differs is where the
// members come from: a group's live in the compiled Graph as a GroupMeta, a
// body's live only in its own node's "body" parameter.
type NodeBodyPackage struct {
	Package *SubgraphPackage
	Hash    string
	// OuterNodeRefs records every $nodes['name'] a body member aims at a node
	// that is NOT a body member -- i.e. at the outer graph. A body member has no
	// edge to such a node, so the reference is invisible in the topology and this
	// is the only record of the dependency: it is what validateBodyOuterRefs
	// checks and what the scheduler snapshots into the sub-execution.
	//
	// The referencing MEMBER travels alongside the referenced name because both
	// consumers need it. Compile-time: an error that says only "this body
	// references x" leaves an author with a multi-member body no way to find the
	// line. Run time: a snapshot fetch that fails must name who wanted the value.
	//
	// Only NAMES live here, following SubgraphPackage.VisibleSupplies. The
	// OUTPUTS are read per execution and must never enter a compile-time
	// artifact -- they are execution-scoped, and an upstream node's output is
	// routinely an HTTP response body carrying credentials.
	//
	// The explicit omitempty matters: this struct's other fields carry no json
	// tags, so a nil slice must serialize to nothing, or every already-persisted
	// graph containing a body would gain an "OuterNodeRefs":null and its
	// graphHash would move -- invalidating the hash recorded on every persisted
	// execution.
	OuterNodeRefs []BodyOuterRef `json:",omitempty"`
}

// BodyOuterRef is one body member's reference to one outer-graph node.
type BodyOuterRef struct {
	// Member is the body member that wrote the reference.
	Member string `json:"member"`
	// Node is the outer-graph node name it reads.
	Node string `json:"node"`
}

// bodyExitPort is the port a body member's output is collected from. A body has
// no boundary-output declarations to project — the whole body is one item's
// computation, and its result is whatever its terminal nodes emit on "main".
const bodyExitPort = "main"

// ProjectNodeBodyPackage projects the body declared on one map node.
//
// It is a separate entry point from ProjectGroupPackage rather than a widened
// version of it: that function reads members, boundary outputs, and entry index
// out of a GroupMeta stored in the compiled Graph, none of which a body has. The
// two converge on the same output type, which is what lets the executor stay
// agnostic about who asked.
//
// Called at compile time, so a malformed body is a compile error rather than a
// runtime surprise — the same reason the body's shape rules are enforced in
// validateNodeBody.
//
// visibleSupplies is the set of supply names the body's members may read. On
// the outer Compile pass it is the parent map node's OWN set — the sorted names
// g.SupplyRefsFor(mapNodeIdx) returns after buildDependencyEdges has run. When
// the map node is a group member the body is reprojected inside compileTrusted,
// whose Def structurally cannot carry the dependency edge, and projectNodeBodies
// unions in the enclosing group's VisibleSupplies there; see its doc for why
// that is not a loosening.
//
// A body member has no dependency edges of its own (it is never a top-level
// node in the outer graph), so without this the projected package's Def has no
// way to tell CompileProjectedPackage's validateSupplyUsage that a body member
// reading $supplies.<name> is allowed to: the dependency edge lives on the map
// node, and a body inherits it exactly the way a group member's OWN dependency
// edge already lets it through (buildVisibleSupplies does the same thing one
// layer up, from a group's members instead of a body's parent node). Passing
// nil here still compiles a body with no supply reads, which is the common
// case and the shape every existing caller of this function needs.
//
// wfCtx is the workflow-level Vars/Config the body's members must keep seeing —
// build it with projectedWorkflowContext(g). It is threaded in rather than read
// off a *Graph because a body has no Graph of its own; its members are compiled
// from the "body" parameter alone. Passing nil produces a body whose members
// see empty $vars and $config, which is what this function used to do
// unconditionally.
func ProjectNodeBodyPackage(mapNodeName string, params map[string]any, visibleSupplies []string, wfCtx *types.WorkflowContext) (*NodeBodyPackage, error) {
	bodyRaw, ok := params["body"]
	if !ok {
		return nil, fmt.Errorf("node %q has no body to project", mapNodeName)
	}
	bodyDef, err := decodeSubgraphBody(bodyRaw)
	if err != nil {
		return nil, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	nodes, conns, err := decodeSubgraphMembers(bodyDef.Parameters)
	if err != nil {
		return nil, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("node %q: body has no nodes", mapNodeName)
	}

	bg, entryIdx, outerRefs, err := compileBodyMembers(mapNodeName, nodes, conns)
	if err != nil {
		return nil, err
	}

	// Sorted member order keeps the hash stable across authoring order: two
	// bodies that differ only in how their nodes were listed must compile once,
	// not twice.
	memberNames := make([]string, 0, len(nodes))
	for _, nd := range nodes {
		memberNames = append(memberNames, nd.Name)
	}
	sort.Strings(memberNames)

	defNodes := make([]types.NodeDef, 0, len(memberNames))
	for _, name := range memberNames {
		nd := nodes[bg.index[name]]
		defNodes = append(defNodes, types.NodeDef{
			Name:       nd.Name,
			Type:       nd.Type,
			Kind:       nd.Kind,
			Version:    nd.Version,
			OnError:    nd.OnError,
			Parameters: cloneStringAnyMap(nd.Parameters),
			// Timeout crosses the body projection boundary so a map body member
			// keeps its own bound. A group's bound and a member's bound nest;
			// dropping this silently downgrades every map body member to the
			// global default.
			Timeout: nd.Timeout,
		})
	}

	exits := buildBodyExits(bg, memberNames)
	for _, exit := range exits {
		defNodes = append(defNodes, types.NodeDef{
			Name:    exit.CollectorNode,
			Type:    NodeTypeGroupExit,
			Version: SubgraphPackageVersion,
		})
	}

	pkg := &SubgraphPackage{
		Version:         SubgraphPackageVersion,
		GroupName:       mapNodeName,
		EntryNode:       nodes[entryIdx].Name,
		Def:             &types.WorkflowDef{Name: mapNodeName, Context: wfCtx, Nodes: defNodes, Connections: bodyConnections(conns, exits)},
		Exits:           exits,
		Requirements:    buildBodyRequirements(defNodes),
		VisibleSupplies: visibleSupplies,
		// The same names as OuterNodeRefs, flattened and deduplicated: that field
		// records who referenced what (needed for diagnostics), this one records
		// only what may be referenced (needed by the inner compile's existence
		// check). Deriving it here rather than making the inner compile walk
		// OuterNodeRefs keeps the package self-contained -- it is what travels to
		// a runner, and NodeBodyPackage does not.
		VisibleOuterNodes: outerRefNames(outerRefs),
	}
	hash, err := ComputePackageHash(pkg)
	if err != nil {
		return nil, fmt.Errorf("node %q: compute body package hash: %w", mapNodeName, err)
	}
	return &NodeBodyPackage{Package: pkg, Hash: hash, OuterNodeRefs: outerRefs}, nil
}

// outerRefNames flattens per-member outer references into the sorted,
// deduplicated name set the projected package carries. Two members reading the
// same outer node produce one name, and the order is the sorted order rather
// than authoring order so the package hash does not move when a body's members
// are relisted.
func outerRefNames(refs []BodyOuterRef) []string {
	if len(refs) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(refs))
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		if seen[ref.Node] {
			continue
		}
		seen[ref.Node] = true
		names = append(names, ref.Node)
	}
	sort.Strings(names)
	return names
}

// compileBodyMembers builds the minimal two-pass graph the entry rules need and
// returns it with the resolved entry index and the body's outer-graph $nodes
// references. Shared with validateNodeBody so the validation and the projection
// cannot disagree about what a valid body is.
func compileBodyMembers(mapNodeName string, nodes []types.NodeDef, conns types.Connections) (*Graph, int, []BodyOuterRef, error) {
	bodyDef := &types.WorkflowDef{Nodes: nodes, Connections: conns}
	n := len(nodes)
	bg := &Graph{
		nodes:        make([]NodeMeta, n),
		index:        make(map[string]int, n),
		entryIndexes: make(map[string]int),
		outEdges:     make([][]Edge, n),
		inEdges:      make([][]Edge, n),
		inDegree:     make([]int, n),
		startIdx:     -1,
	}
	if _, err := registerNodes(bodyDef, bg); err != nil {
		return nil, 0, nil, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	if _, err := buildEdges(bodyDef, bg); err != nil {
		return nil, 0, nil, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	members := make(map[int]bool, n)
	for i := 0; i < n; i++ {
		members[i] = true
	}
	entry, _, err := resolveGroupEntry(bg, members)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	if err := assertEntryDominates(bg, entry, members); err != nil {
		return nil, 0, nil, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	// I2: reuse the SAME portability validator group compilation uses
	// (validateGroupPortability's shared core) rather than letting a body
	// skip it. Without this, a body member of a non-portable type
	// (xflow.local/xflow.closure/xflow.inline) compiled cleanly even though it
	// cannot actually run once the body is projected onto a remote runner --
	// exactly the class of gap C1 fixed for $supplies, just for node types
	// instead. "body" is passed as kind so the error can never be confused
	// with a rejected GROUP even though both paths share the same code.
	//
	// collectExternal=true is where a body diverges from a group: an external
	// $nodes reference is COLLECTED rather than rejected, because whether it is
	// legal is a question about the outer graph (does the map node's ancestry
	// contain it?) that bg cannot answer -- bg holds only the body's own
	// members. validateBodyOuterRefs, a later pass that does hold the outer
	// graph, adjudicates every name collected here. Nothing reaches a compiled
	// graph unadjudicated: projectNodeBodies is the only production caller and
	// Compile runs that pass unconditionally.
	memberNames := make([]string, 0, n)
	for i := 0; i < n; i++ {
		memberNames = append(memberNames, nodes[i].Name)
	}
	outerRefs, err := checkPortability(bg, "body", mapNodeName, memberNames, true)
	if err != nil {
		// checkPortability already stamps "body %q: ..." (kind+name), so this
		// wraps with just "node %q:" rather than the "node %q: body: %w" other
		// body errors in this function use -- that would double up on the word
		// "body" (kind is already the outer noun here, unlike
		// resolveGroupEntry/assertEntryDominates, which say "group" internally
		// with no equivalent kind parameter to swap).
		return nil, 0, nil, fmt.Errorf("node %q: %w", mapNodeName, err)
	}
	return bg, entry, outerRefs, nil
}

// buildBodyExits attaches a collector to every member with no outgoing "main"
// edge. Those are the body's terminal nodes, and their outputs are what one
// item's result is: a body has no boundary declarations to consult, so the
// topology decides.
func buildBodyExits(bg *Graph, memberNames []string) []SubgraphPackageExit {
	var exits []SubgraphPackageExit
	for _, name := range memberNames {
		idx := bg.index[name]
		terminal := true
		for _, e := range bg.outEdges[idx] {
			if e.SrcPort == bodyExitPort {
				terminal = false
				break
			}
		}
		if !terminal {
			continue
		}
		exits = append(exits, SubgraphPackageExit{
			CollectorNode: fmt.Sprintf("__collector_%s_%s", name, bodyExitPort),
			SrcNode:       name,
			Port:          bodyExitPort,
		})
	}
	sort.Slice(exits, func(i, j int) bool { return exits[i].SrcNode < exits[j].SrcNode })
	return exits
}

// bodyConnections copies the body's own connections and appends one edge per
// exit so the collector nodes actually receive the terminal outputs.
func bodyConnections(conns types.Connections, exits []SubgraphPackageExit) types.Connections {
	out := make(types.Connections, len(conns)+len(exits))
	for src, ports := range conns {
		copied := make(map[string]types.PortConnections, len(ports))
		for port, pc := range ports {
			targets := append([]types.Connection(nil), pc.Targets...)
			copied[port] = types.PortConnections{Targets: targets}
		}
		out[src] = copied
	}
	for _, exit := range exits {
		ports, ok := out[exit.SrcNode]
		if !ok {
			ports = make(map[string]types.PortConnections, 1)
			out[exit.SrcNode] = ports
		}
		pc := ports[exit.Port]
		pc.Targets = append(pc.Targets, types.Connection{Node: exit.CollectorNode, Input: "main"})
		ports[exit.Port] = pc
	}
	return out
}

// buildBodyRequirements derives the capability requirements a runner must
// satisfy to run this body, deduplicated and sorted so the hash is stable.
// Collector nodes are excluded: they are the framework's own, present in every
// package, and a runner does not need to advertise them.
func buildBodyRequirements(nodes []types.NodeDef) []Requirement {
	type reqKey struct {
		nodeType    string
		nodeVersion int
	}
	seen := make(map[reqKey]bool, len(nodes))
	var reqs []Requirement
	for _, nd := range nodes {
		if nd.Type == NodeTypeGroupExit {
			continue
		}
		k := reqKey{nodeType: nd.Type, nodeVersion: nd.Version}
		if seen[k] {
			continue
		}
		seen[k] = true
		reqs = append(reqs, Requirement{NodeType: nd.Type, NodeVersion: nd.Version})
	}
	sort.Slice(reqs, func(i, j int) bool {
		if reqs[i].NodeType != reqs[j].NodeType {
			return reqs[i].NodeType < reqs[j].NodeType
		}
		return reqs[i].NodeVersion < reqs[j].NodeVersion
	})
	return reqs
}
