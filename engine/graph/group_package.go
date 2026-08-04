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
	GroupPackageVersion    = 1
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

// GroupPackageExit describes a boundary output edge with a collector node.
type GroupPackageExit struct {
	CollectorNode string `json:"collector_node"`
	SrcNode       string `json:"src_node"`
	Port          string `json:"port"`
}

// GroupPackage is the deterministic projection of a co-location group into a
// self-contained package descriptor. It captures the group's member topology,
// boundary outputs, and requirements so that a runner can compile and execute
// the internal mini-graph independently.
type GroupPackage struct {
	Version      int                `json:"version"`
	GroupName    string             `json:"group_name"`
	EntryNode    string             `json:"entry_node"`
	Def          *types.WorkflowDef `json:"def"`
	Exits        []GroupPackageExit `json:"exits"`
	Artifacts    []GroupArtifact    `json:"artifacts,omitempty"`
	Requirements []Requirement      `json:"requirements"`
	// VisibleSupplies is the sorted set of supply node names the members may read
	// through $supplies.<name>. Only the names travel: the content is fetched by
	// the runner at activation time and deliberately stays out of the package, so
	// PackageHash does not move when a supply's content changes.
	VisibleSupplies []string `json:"visible_supplies,omitempty"`
}

// ProjectGroupPackage projects a deterministic GroupPackage from a compiled
// Graph at the given unitIdx. The unitIdx must reference a UnitGroup unit.
// Returns the package, its canonical hash, and any error.
func ProjectGroupPackage(g *Graph, unitIdx int) (*GroupPackage, string, error) {
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
		nodes = append(nodes, types.NodeDef{
			Name:       n.Name,
			Type:       n.Type,
			Kind:       n.Kind,
			Version:    n.Version,
			OnError:    n.OnError,
			Parameters: cloneStringAnyMap(n.Parameters),
		})
	}

	// Build collector nodes for boundary outputs (xflow.group_exit type).
	exits := buildGroupExits(g, &gm, memberSet)

	// Add collector NodeDefs.
	for _, exit := range exits {
		nodes = append(nodes, types.NodeDef{
			Name:    exit.CollectorNode,
			Type:    NodeTypeGroupExit,
			Version: GroupPackageVersion,
		})
	}

	// Build internal connections: only edges where both endpoints are members,
	// plus edges from boundary-output src to collector.
	conns := buildPackageConnections(g, memberSet, exits)

	// Workflow-level Vars/Config (stripped of secrets).
	var ctx *types.WorkflowContext
	if g.vars != nil || g.config != nil {
		ctx = &types.WorkflowContext{
			Vars:   cloneStringAnyMap(g.vars),
			Config: cloneStringAnyMap(g.config),
		}
	}

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

	pkg := &GroupPackage{
		Version:         GroupPackageVersion,
		GroupName:       gm.Name,
		EntryNode:       g.nodes[gm.EntryIdx].Name,
		Def:             def,
		Exits:           exits,
		Requirements:    reqs,
		VisibleSupplies: visibleSupplies,
	}

	hash, err := ComputePackageHash(pkg)
	if err != nil {
		return nil, "", fmt.Errorf("compute package hash: %w", err)
	}

	return pkg, hash, nil
}

func buildGroupExits(g *Graph, gm *GroupMeta, _ map[int]bool) []GroupPackageExit {
	type exitKey struct {
		srcNode string
		port    string
	}
	seen := map[exitKey]bool{}
	var exits []GroupPackageExit

	for _, be := range gm.BoundaryOutputs {
		srcName := g.nodes[be.Src.NodeIdx].Name
		k := exitKey{srcNode: srcName, port: be.Src.Port}
		if seen[k] {
			continue
		}
		seen[k] = true
		collectorName := fmt.Sprintf("__collector_%s_%s", srcName, be.Src.Port)
		exits = append(exits, GroupPackageExit{
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

func buildPackageConnections(g *Graph, memberSet map[int]bool, exits []GroupPackageExit) types.Connections {
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

// ComputePackageHash computes the canonical SHA-256 hash of a GroupPackage.
func ComputePackageHash(pkg *GroupPackage) (string, error) {
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
func CompileProjectedPackage(pkg *GroupPackage) (*Graph, error) {
	if pkg == nil {
		return nil, fmt.Errorf("nil group package")
	}
	if pkg.Def == nil {
		return nil, fmt.Errorf("group package has nil Def")
	}
	return compileTrusted(pkg.Def, pkg.VisibleSupplies)
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
func compileTrusted(def *types.WorkflowDef, visibleSupplies []string) (*Graph, error) {
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
			Name:       nd.Name,
			Type:       nd.Type,
			Kind:       nd.Kind,
			Version:    nd.Version,
			OnError:    nd.OnError,
			Parameters: cloneStringAnyMap(nd.Parameters),
			GroupIdx:   -1,
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
	if err := buildDependencyEdges(def, depPorts, g, visibleSupplies); err != nil {
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
