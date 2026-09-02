package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

const (
	NodeTypeGroupExit      = "xflow.group_exit"
	ReservedNodeTypePrefix = "xflow.group_"
	SubgraphPackageVersion = 1
	packageHashPrefix      = "pkg-sha256:v1:"
)

// Requirement is a graph-owned lower-level DTO describing what a member node
// needs from the runtime environment. It lives in engine/graph to avoid an
// import cycle with the engine package.
type Requirement struct {
	NodeType    string   `json:"node_type"`
	NodeVersion int      `json:"node_version,omitempty"`
	Runtime     string   `json:"runtime,omitempty"`
	Resource    string   `json:"resource,omitempty"`
	Credentials []string `json:"credentials,omitempty"`
}

// GroupArtifact describes a build artifact associated with a group member.
type GroupArtifact struct {
	NodeName string `json:"node_name"`
	Runtime  string `json:"runtime,omitempty"`
	Language string `json:"language,omitempty"`
	Digest   string `json:"digest,omitempty"`
	Size     int    `json:"size,omitempty"`
}

// SubgraphPackageExit describes a boundary output edge with a collector node.
type SubgraphPackageExit struct {
	CollectorNode string `json:"collector_node"`
	SrcNode       string `json:"src_node"`
	Port          string `json:"port"`
}

// SubgraphPackage is the deterministic projection of a co-location group into a
// self-contained package descriptor. It captures the group's member topology,
// boundary outputs, and requirements so that a runner can compile and execute
// the internal mini-graph independently.
type SubgraphPackage struct {
	Version      int                   `json:"version"`
	GroupName    string                `json:"group_name"`
	EntryNode    string                `json:"entry_node"`
	Def          *types.WorkflowDef    `json:"def"`
	Exits        []SubgraphPackageExit `json:"exits"`
	Artifacts    []GroupArtifact       `json:"artifacts,omitempty"`
	Requirements []Requirement         `json:"requirements"`
	// VisibleSupplies is the sorted set of supply node names the members may read
	// through $supplies.<name>. Only the names travel: the content is fetched by
	// the runner at activation time and deliberately stays out of the package, so
	// PackageHash does not move when a supply's content changes.
	//
	// This is the FLAT union across every member -- the fallback used when
	// NodeSupplyRefs is nil (see that field's doc). It stays the primary,
	// always-populated field rather than being replaced by NodeSupplyRefs
	// because most groups have exactly one supply shared by every member, where
	// the flat union already IS the per-node truth; NodeSupplyRefs exists only
	// to correct the cases where it is not.
	VisibleSupplies []string `json:"visible_supplies,omitempty"`
	// NodeSupplyRefs narrows VisibleSupplies to what each INDIVIDUAL member
	// actually declared a dependency edge to, keyed by member node name. It
	// exists because a projected package's Def structurally cannot carry a
	// dependency edge (a supply node is never a group member, so
	// buildPackageConnections keeps only member-to-member edges) -- without
	// SOME name list threaded through, CompileProjectedPackage's
	// validateSupplyUsage would see g.supplyRefs empty for every member and
	// reject a legitimate $supplies reference outright. VisibleSupplies alone
	// closes that gap, but it closes it too WIDE: it is the union across every
	// member, so a member that references $supplies.<name> only inside a body
	// it owns would pass validation against a sibling's edge to that name
	// rather than its own -- the flattening erases which member actually
	// declared what. NodeSupplyRefs is the per-node record that lets a
	// consumer recover that distinction; see compileTrusted's and
	// projectNodeBodies' visibleSuppliesForNode calls, which look up this map
	// before ever falling back to the flat list.
	//
	// nil, and therefore omitted from the JSON entirely, whenever it would not
	// narrow anything -- i.e. every member's own ref set already equals the
	// flat union (buildNodeSupplyRefs' doc has the exact condition). That is
	// the overwhelmingly common case (one supply shared by the whole group, or
	// no supply at all), and PackageHash = sha256(json.Marshal(pkg)) must not
	// move for it: a new field without omitempty would fence every group in
	// every workflow the moment this shipped, not just the ones this actually
	// changes behavior for.
	//
	// Once populated, it holds an entry for EVERY member, not just the ones
	// whose own set differs from the flat union. A missing key, once this map
	// is non-nil, means "this member declared zero edges of its own" --
	// leaving a homogeneous member out to save bytes would make it read as
	// zero-access the moment any sibling makes the map non-nil, which is a
	// correctness bug, not an optimization.
	//
	// encoding/json sorts map[string]... keys before marshaling -- a
	// documented guarantee of the encoding/json package, not an
	// implementation detail that could change under it -- so this field does
	// not need its own key-ordering pass for ComputePackageHash to stay
	// deterministic. Pinned directly by
	// TestNodeSupplyRefs_JSONKeyOrderIsDeterministic.
	NodeSupplyRefs map[string][]string `json:"node_supply_refs,omitempty"`
	// VisibleOuterNodes is the sorted set of OUTER-graph node names the members
	// may read through $nodes['<name>']. It exists for the same reason
	// VisibleSupplies does, one root over: this package is compiled on its own by
	// CompileProjectedPackage, whose graph contains only the members, so
	// buildNodesRefs would reject every outer name with "does not exist in the
	// workflow definition" -- which is what stalled a map node whose body read an
	// upstream ancestor.
	//
	// Populated only on a BODY package. A group member's outer references are
	// rejected outright (checkPortability with collectExternal=false), so a group
	// package leaves this nil and its bytes -- and hash -- do not move.
	//
	// Names only, and for the stronger of the two reasons VisibleSupplies gives:
	// a supply's content merely changes often, whereas an upstream node's output
	// is execution-scoped and routinely an HTTP response body carrying
	// credentials. It must never enter a compile-time artifact. The outputs are
	// snapshotted per execution and travel on the lease.
	VisibleOuterNodes []string `json:"visible_outer_nodes,omitempty"`
}

// ProjectGroupPackage projects a deterministic SubgraphPackage from a compiled
// Graph at the given unitIdx. The unitIdx must reference a UnitGroup unit.
// Returns the package, its canonical hash, and any error.
//
// The name says GROUP, not subgraph, even though the returned type says
// SubgraphPackage: the type is shared (a body projects into the same shape via
// ProjectNodeBodyPackage), but this entry point is not. It reads a GroupMeta out
// of the compiled Graph and rejects any unit that is not a UnitGroup, so it has
// nothing to do with the xflow.subgraph node type an author can write. Naming it
// after the shared output type once made it read as xflow.subgraph's projection
// entry, which is ProjectNodeBodyPackage.
// projectedWorkflowContext builds the Context a projected package carries so
// its members keep $vars and $config, the two workflow-level roots the DSL
// promises every node regardless of how deeply it is nested.
//
// Both projection entry points call this. They used to differ -- the group path
// carried a Context, the body path did not -- which made a map body's members
// see empty $vars and $config while the same members inside a group saw them.
// The spec's own map-body example writes `url: "{{ $vars.api_base_url }}/process"`,
// so the body path's omission rendered that URL without its host.
//
// Returns nil when the workflow declares neither, which keeps a bodiless
// workflow's package bytes -- and therefore its hash -- unchanged.
func projectedWorkflowContext(g *Graph) *types.WorkflowContext {
	if g == nil || (g.vars == nil && g.config == nil) {
		return nil
	}
	return &types.WorkflowContext{
		Vars:   cloneStringAnyMap(g.vars),
		Config: cloneStringAnyMap(g.config),
	}
}

func ProjectGroupPackage(g *Graph, unitIdx int) (*SubgraphPackage, string, error) {
	if unitIdx < 0 || unitIdx >= len(g.units) {
		return nil, "", fmt.Errorf("unit index %d out of range [0, %d)", unitIdx, len(g.units))
	}
	u := g.units[unitIdx]
	if u.Kind != UnitGroup {
		return nil, "", fmt.Errorf("unit %d is %v, not a group unit", unitIdx, u.Kind)
	}
	gm := g.groups[u.GroupIdx]

	memberSet := make(map[int]bool, len(gm.Members))
	for _, idx := range gm.Members {
		memberSet[idx] = true
	}

	// Build sorted member names for deterministic iteration.
	memberNames := make([]string, 0, len(gm.Members))
	for _, idx := range gm.Members {
		memberNames = append(memberNames, g.nodes[idx].Name)
	}
	sort.Strings(memberNames)

	// Build mini WorkflowDef: only member NodeDefs (sorted by name), no PinData,
	// no Groups, Retry=nil, RunnerSelector=nil on individual nodes.
	nodes := make([]types.NodeDef, 0, len(memberNames))
	for _, name := range memberNames {
		idx := g.index[name]
		n := g.nodes[idx]
		// The entry trigger's parameters are rendered here; every other member
		// keeps its source text for execution/params.go. See
		// evaluateEntryTriggerParams for why the split is at the entry.
		params, err := evaluateEntryTriggerParams(g, &gm, idx, n)
		if err != nil {
			return nil, "", err
		}
		nodes = append(nodes, types.NodeDef{
			Name:       n.Name,
			Type:       n.Type,
			Kind:       n.Kind,
			Version:    n.Version,
			OnError:    n.OnError,
			Parameters: params,
			// Timeout crosses the projection boundary even though Retry and
			// RunnerSelector deliberately do not (see the mini-def comment above).
			// Those two are dimensions the GROUP takes over from its members;
			// timeout is not -- a group bound and a member bound NEST, and the
			// executor takes the min of the two. Dropping it here would silently
			// downgrade every member to the global default with no diagnostic.
			Timeout: n.Timeout,
		})
	}

	// Build collector nodes for boundary outputs (xflow.group_exit type).
	exits := buildGroupExits(g, &gm, memberSet)

	// Add collector NodeDefs.
	for _, exit := range exits {
		nodes = append(nodes, types.NodeDef{
			Name:    exit.CollectorNode,
			Type:    NodeTypeGroupExit,
			Version: SubgraphPackageVersion,
		})
	}

	// Build internal connections: only edges where both endpoints are members,
	// plus edges from boundary-output src to collector.
	conns := buildPackageConnections(g, memberSet, exits)

	// Workflow-level Vars/Config (stripped of secrets).
	ctx := projectedWorkflowContext(g)

	def := &types.WorkflowDef{
		Name:        gm.Name,
		Context:     ctx,
		Nodes:       nodes,
		Connections: conns,
	}

	// Build requirements from member node types.
	reqs := buildPackageRequirements(g, memberSet)

	// Build the visible-supply name list from the members' g.supplyRefs.
	// Names only -- see the VisibleSupplies field doc for why content must
	// never enter the package.
	visibleSupplies := buildVisibleSupplies(g, memberSet)
	// nodeSupplyRefs is nil unless some member's own edges are a STRICT SUBSET
	// of the flat union -- see buildNodeSupplyRefs' doc for the exact condition
	// and why that keeps PackageHash stable for the common (homogeneous) case.
	nodeSupplyRefs := buildNodeSupplyRefs(g, memberSet, visibleSupplies)

	pkg := &SubgraphPackage{
		Version:         SubgraphPackageVersion,
		GroupName:       gm.Name,
		EntryNode:       g.nodes[gm.EntryIdx].Name,
		Def:             def,
		Exits:           exits,
		Requirements:    reqs,
		VisibleSupplies: visibleSupplies,
		NodeSupplyRefs:  nodeSupplyRefs,
	}

	hash, err := ComputePackageHash(pkg)
	if err != nil {
		return nil, "", fmt.Errorf("compute package hash: %w", err)
	}

	return pkg, hash, nil
}

func buildGroupExits(g *Graph, gm *GroupMeta, _ map[int]bool) []SubgraphPackageExit {
	type exitKey struct {
		srcNode string
		port    string
	}
	seen := map[exitKey]bool{}
	var exits []SubgraphPackageExit

	for _, be := range gm.BoundaryOutputs {
		srcName := g.nodes[be.Src.NodeIdx].Name
		k := exitKey{srcNode: srcName, port: be.Src.Port}
		if seen[k] {
			continue
		}
		seen[k] = true
		collectorName := fmt.Sprintf("__collector_%s_%s", srcName, be.Src.Port)
		exits = append(exits, SubgraphPackageExit{
			CollectorNode: collectorName,
			SrcNode:       srcName,
			Port:          be.Src.Port,
		})
	}

	sort.Slice(exits, func(i, j int) bool {
		if exits[i].SrcNode != exits[j].SrcNode {
			return exits[i].SrcNode < exits[j].SrcNode
		}
		return exits[i].Port < exits[j].Port
	})
	return exits
}

func buildPackageConnections(g *Graph, memberSet map[int]bool, exits []SubgraphPackageExit) types.Connections {
	conns := make(types.Connections)

	// Internal edges between members.
	for srcIdx := range g.outEdges {
		if !memberSet[srcIdx] {
			continue
		}
		for _, e := range g.outEdges[srcIdx] {
			if !memberSet[e.DstIdx] {
				continue
			}
			srcName := g.nodes[e.SrcIdx].Name
			dstName := g.nodes[e.DstIdx].Name
			if conns[srcName] == nil {
				conns[srcName] = make(map[string]types.PortConnections)
			}
			// A map index expression yields a non-addressable struct, so the
			// slice has to be lifted out, appended to, and written back.
			pc := conns[srcName][e.SrcPort]
			pc.Targets = append(pc.Targets, types.Connection{
				Node:  dstName,
				Input: e.DstPort,
			})
			conns[srcName][e.SrcPort] = pc
		}
	}

	// Edges from boundary-output source to collector.
	for _, exit := range exits {
		if conns[exit.SrcNode] == nil {
			conns[exit.SrcNode] = make(map[string]types.PortConnections)
		}
		pc := conns[exit.SrcNode][exit.Port]
		pc.Targets = append(pc.Targets, types.Connection{
			Node:  exit.CollectorNode,
			Input: "main",
		})
		conns[exit.SrcNode][exit.Port] = pc
	}

	// Sort connections for determinism.
	for src := range conns {
		for port := range conns[src] {
			pc := conns[src][port]
			sort.Slice(pc.Targets, func(i, j int) bool {
				if pc.Targets[i].Node != pc.Targets[j].Node {
					return pc.Targets[i].Node < pc.Targets[j].Node
				}
				return pc.Targets[i].Input < pc.Targets[j].Input
			})
			conns[src][port] = pc
		}
	}

	if len(conns) == 0 {
		return nil
	}
	return conns
}

// buildVisibleSupplies collects the sorted, de-duplicated set of supply node
// names referenced (via g.supplyRefs) by any member of the group. Only the
// names are sourced here -- g.supplyRefs never carries supply content, so
// this is inherently content-free.
func buildVisibleSupplies(g *Graph, memberSet map[int]bool) []string {
	seen := map[string]bool{}
	for idx := range memberSet {
		for _, name := range g.supplyRefs[idx] {
			seen[name] = true
		}
	}
	if len(seen) == 0 {
		return nil
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// buildNodeSupplyRefs reports every member's OWN sorted supply-name set, keyed
// by member name, but ONLY when at least one member's own set differs from
// the flat union flatVisible -- otherwise it returns nil.
//
// It is all-or-nothing across the group, not per-member, because
// visibleSuppliesForNode's contract is: nil map falls back to flat for every
// node, but once the map is non-nil, a key ABSENT from it means "this node's
// own set is empty" -- never "fall back to flat". A homogeneous member
// (its own set already equals flatVisible) would therefore be silently
// stripped of its real access the moment the map is populated for ANY
// heterogeneous sibling, unless that homogeneous member's entry is written
// too. Recording every member once heterogeneity is detected anywhere in the
// group is what keeps that from happening.
//
// Returns nil -- and therefore SubgraphPackage.NodeSupplyRefs stays omitted
// from the JSON, and PackageHash does not move -- when every member's own set
// equals flatVisible. That is deliberate: the flat field alone is already the
// correct per-node answer in that case, and the overwhelming majority of
// groups (one supply, read by every member, or no supply at all) are exactly
// this case.
func buildNodeSupplyRefs(g *Graph, memberSet map[int]bool, flatVisible []string) map[string][]string {
	own := make(map[int][]string, len(memberSet))
	homogeneous := true
	for idx := range memberSet {
		o := sortedSupplyNames(g.supplyRefs[idx])
		own[idx] = o
		if !equalStringSlices(o, flatVisible) {
			homogeneous = false
		}
	}
	if homogeneous {
		return nil
	}
	refs := make(map[string][]string, len(memberSet))
	for idx, o := range own {
		refs[g.nodes[idx].Name] = o
	}
	return refs
}

// sortedSupplyNames returns a sorted copy of names, or nil for an empty input
// -- matching buildVisibleSupplies' nil-for-empty convention so
// equalStringSlices(nil, nil) correctly reports "no own refs" as equal to "no
// flat refs" rather than needing a separate empty-vs-nil case.
func sortedSupplyNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	out := append([]string(nil), names...)
	sort.Strings(out)
	return out
}

// equalStringSlices reports whether a and b hold the same strings in the same
// order. Both g.supplyRefs entries and VisibleSupplies are already sorted, so
// an order-sensitive comparison is correct here and does not need its own sort
// step.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// visibleSuppliesForNode is the per-node lookup validateSupplyUsage and
// projectNodeBodies use in place of a flat, every-node-gets-everything name
// list.
//
// nodeSupplyRefs == nil means "no package-level narrowing exists for this
// compile" -- either this is an ungrouped Compile() (which never builds the
// map at all) or every member's own set already equalled the flat union (see
// buildNodeSupplyRefs) -- so falling back to flat is correct for EVERY node,
// not a loosening.
//
// Once nodeSupplyRefs is non-nil, a name absent from it for a given node means
// "this node's own set is empty", NOT "fall back to flat": that per-key
// fallback is exactly the flattening this fix removes. A node with a genuinely
// empty own set and a nil map entry both correctly resolve to nil here.
func visibleSuppliesForNode(name string, nodeSupplyRefs map[string][]string, flat []string) []string {
	if nodeSupplyRefs == nil {
		return flat
	}
	return nodeSupplyRefs[name]
}

func buildPackageRequirements(g *Graph, memberSet map[int]bool) []Requirement {
	type reqKey struct {
		nodeType    string
		nodeVersion int
		runtime     string
	}
	seen := map[reqKey]bool{}
	var reqs []Requirement

	for idx := range memberSet {
		n := g.nodes[idx]
		k := reqKey{nodeType: n.Type, nodeVersion: n.Version}
		if seen[k] {
			continue
		}
		seen[k] = true
		reqs = append(reqs, Requirement{
			NodeType:    n.Type,
			NodeVersion: n.Version,
		})
	}

	sort.Slice(reqs, func(i, j int) bool {
		if reqs[i].NodeType != reqs[j].NodeType {
			return reqs[i].NodeType < reqs[j].NodeType
		}
		if reqs[i].NodeVersion != reqs[j].NodeVersion {
			return reqs[i].NodeVersion < reqs[j].NodeVersion
		}
		return reqs[i].Runtime < reqs[j].Runtime
	})
	return reqs
}

// ComputePackageHash computes the canonical SHA-256 hash of a SubgraphPackage.
func ComputePackageHash(pkg *SubgraphPackage) (string, error) {
	data, err := json.Marshal(pkg)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return packageHashPrefix + hex.EncodeToString(sum[:]), nil
}

// CompileProjectedPackage is the trusted compile path for projected group
// packages. Unlike Compile, it permits reserved "xflow.group_*" node types
// (e.g. xflow.group_exit collectors). User-authored WorkflowDefs must use
// Compile which rejects these types.
func CompileProjectedPackage(pkg *SubgraphPackage) (*Graph, error) {
	if pkg == nil {
		return nil, fmt.Errorf("nil group package")
	}
	if pkg.Def == nil {
		return nil, fmt.Errorf("group package has nil Def")
	}
	return compileTrusted(pkg.Def, pkg.VisibleSupplies, pkg.VisibleOuterNodes, pkg.NodeSupplyRefs)
}

// compileTrusted is the internal compilation path that skips the reserved-type
// rejection. The trust boundary is expressed by the function call itself: only
// CompileProjectedPackage (called by the projection pipeline, not user input)
// reaches here.
//
// visibleSupplies widens validateSupplyUsage's declared set: a projected
// package's Def carries no dependency edges (they never enter g.outEdges),
// so the per-node g.supplyRefs computed here would always be empty. The
// caller-supplied name list -- sourced from the parent graph's g.supplyRefs
// at projection time -- is what lets a member's $supplies.<name> reference
// pass validation.
//
// nodeSupplyRefs is the per-node correction to that widening: visibleSupplies
// is the union across every member, so using it alone for every node would let
// a member read a sibling's supply merely because SOME member declared an
// edge to it. When nodeSupplyRefs is non-nil, visibleSuppliesForNode looks up
// each node's OWN set there instead and only falls back to visibleSupplies
// when the map itself is nil (see that function's doc). nodeSupplyRefs is nil
// for every ungrouped Compile() call and for any group where it would not
// narrow anything, which is what keeps this a strict tightening rather than a
// behavior change for those cases.
//
// visibleOuterNodes does the same for $nodes: a body member may read the map
// node's upstream ancestors, and those names exist in the OUTER graph only.
// Without the list, buildNodesRefs rejects them as nonexistent. Both lists are
// widenings of a validation set, never sources of data.
func compileTrusted(def *types.WorkflowDef, visibleSupplies, visibleOuterNodes []string, nodeSupplyRefs map[string][]string) (*Graph, error) {
	if def == nil {
		return nil, fmt.Errorf("workflow definition is nil")
	}
	n := len(def.Nodes)
	if n == 0 {
		return nil, fmt.Errorf("workflow has no nodes")
	}

	g := &Graph{
		name:            def.Name,
		workflowVersion: def.Version,
		compilerVersion: compilerVersion,
		nodes:           make([]NodeMeta, n),
		index:           make(map[string]int, n),
		entryIndexes:    make(map[string]int),
		outEdges:        make([][]Edge, n),
		inEdges:         make([][]Edge, n),
		inDegree:        make([]int, n),
		startIdx:        -1,
	}
	if def.Context != nil {
		g.vars = cloneStringAnyMap(def.Context.Vars)
		g.config = cloneStringAnyMap(def.Context.Config)
	}

	for i, nd := range def.Nodes {
		if _, dup := g.index[nd.Name]; dup {
			return nil, fmt.Errorf("duplicate node name: %s", nd.Name)
		}
		g.index[nd.Name] = i
		g.nodes[i] = NodeMeta{
			Name:               nd.Name,
			Type:               nd.Type,
			Kind:               nd.Kind,
			Version:            nd.Version,
			OnError:            nd.OnError,
			Parameters:         cloneStringAnyMap(nd.Parameters),
			GroupIdx:           -1,
			Timeout:            nd.Timeout,
			ActivationReplicas: nd.ActivationReplicas,
		}
		if nd.Type == "xflow.start" || nd.Kind == types.NodeKindTrigger {
			g.entryIndexes[nd.Name] = i
		}
	}

	if err := validateGraphValueDomain(g); err != nil {
		return nil, err
	}
	depPorts, err := buildEdges(def, g)
	if err != nil {
		return nil, err
	}
	if err := buildDependencyEdges(def, depPorts, g, visibleSupplies, nodeSupplyRefs); err != nil {
		return nil, err
	}
	// A group member that is itself an xflow.map node needs its body projected
	// here too, exactly as Compile's own pass ordering does (buildDependencyEdges
	// -> projectNodeBodies -> buildUnits): without this, a map compiled through
	// this trusted path keeps g.nodes[i].Body == nil, and the batch that expands
	// from it hits ErrNoMapBody at runtime instead of running its body. There is
	// no compileGroups call here (a projected package's Def carries only member
	// NodeDefs, never nested Groups), so the pass slots in right where Compile
	// would otherwise call compileGroups.
	//
	// visibleSupplies must be threaded in, unlike in Compile: the ordering alone
	// is not what makes Compile's pass correct — it is that Compile runs against
	// the OUTER graph, where g.supplyRefs carries the map node's dependency edge.
	// This Def cannot carry that edge (a supply node is never a group member and
	// buildPackageConnections keeps only member-to-member edges), so without the
	// widening the body is reprojected with no visible supplies and every batch
	// of a grouped map fails validation at runtime.
	//
	// nodeSupplyRefs is forwarded for the same reason it is forwarded to
	// buildDependencyEdges above: the map node whose body is being projected
	// here is itself one of the members, and its OWN visible set (not the flat
	// union) is what a sibling-owned supply must not leak into.
	if err := projectNodeBodies(def, g, visibleSupplies, nodeSupplyRefs); err != nil {
		return nil, err
	}
	// buildNodesRefs with skipCrossBranchWarning=true: projected packages are
	// sub-graphs of the outer graph; a reference that appears cross-branch in
	// the projection may be a deterministic ancestor in the outer topology.
	// Warning here would produce noise. Existence and forward-ref checks still
	// run — validatePortability guarantees members only reference each other,
	// so existence always passes, but forward-ref is still meaningful.
	if err := buildNodesRefs(g, true, visibleOuterNodes); err != nil {
		return nil, err
	}
	if err := buildUnits(g); err != nil {
		return nil, fmt.Errorf("build units: %w", err)
	}
	if err := assignGraphHash(g); err != nil {
		return nil, fmt.Errorf("hash graph: %w", err)
	}

	return g, nil
}

// assignPackageHashes computes and writes PackageHash for every group in the
// graph. Called during Compile after buildUnits and before assignGraphHash so
// that the package hash enters the graph hash.
func assignPackageHashes(g *Graph) error {
	for i := range g.groups {
		gm := &g.groups[i]
		if gm.PackageHash != "" {
			continue
		}
		_, hash, err := ProjectGroupPackage(g, gm.UnitIdx)
		if err != nil {
			return fmt.Errorf("group %q: %w", gm.Name, err)
		}
		gm.PackageHash = hash
	}
	return nil
}
