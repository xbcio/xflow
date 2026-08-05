package graph

import (
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

// MapBodyPackage is the projection of one xflow.map node's body into a
// self-contained SubgraphPackage, plus its canonical hash.
//
// A body and a node group are the same structure — a set of member nodes with a
// unique dominating entry and boundary outputs — so a body reuses
// SubgraphPackage rather than getting a parallel type. What differs is where the
// members come from: a group's live in the compiled Graph as a GroupMeta, a
// body's live only in its map node's "body" parameter.
type MapBodyPackage struct {
	Package *SubgraphPackage
	Hash    string
}

// bodyExitPort is the port a body member's output is collected from. A body has
// no boundary-output declarations to project — the whole body is one item's
// computation, and its result is whatever its terminal nodes emit on "main".
const bodyExitPort = "main"

// ProjectMapBodyPackage projects the body declared on one map node.
//
// It is a separate entry point from ProjectSubgraphPackage rather than a widened
// version of it: that function reads members, boundary outputs, and entry index
// out of a GroupMeta stored in the compiled Graph, none of which a body has. The
// two converge on the same output type, which is what lets the executor stay
// agnostic about who asked.
//
// Called at compile time, so a malformed body is a compile error rather than a
// runtime surprise — the same reason the body's shape rules are enforced in
// validateMapBody.
func ProjectMapBodyPackage(mapNodeName string, params map[string]any) (*MapBodyPackage, error) {
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

	bg, entryIdx, err := compileBodyMembers(mapNodeName, nodes, conns)
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
		Version:      SubgraphPackageVersion,
		GroupName:    mapNodeName,
		EntryNode:    nodes[entryIdx].Name,
		Def:          &types.WorkflowDef{Name: mapNodeName, Nodes: defNodes, Connections: bodyConnections(conns, exits)},
		Exits:        exits,
		Requirements: buildBodyRequirements(defNodes),
	}
	hash, err := ComputePackageHash(pkg)
	if err != nil {
		return nil, fmt.Errorf("node %q: compute body package hash: %w", mapNodeName, err)
	}
	return &MapBodyPackage{Package: pkg, Hash: hash}, nil
}

// compileBodyMembers builds the minimal two-pass graph the entry rules need and
// returns it with the resolved entry index. Shared with validateMapBody so the
// validation and the projection cannot disagree about what a valid body is.
func compileBodyMembers(mapNodeName string, nodes []types.NodeDef, conns types.Connections) (*Graph, int, error) {
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
		return nil, 0, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	if _, err := buildEdges(bodyDef, bg); err != nil {
		return nil, 0, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	members := make(map[int]bool, n)
	for i := 0; i < n; i++ {
		members[i] = true
	}
	entry, _, err := resolveGroupEntry(bg, members)
	if err != nil {
		return nil, 0, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	if err := assertEntryDominates(bg, entry, members); err != nil {
		return nil, 0, fmt.Errorf("node %q: body: %w", mapNodeName, err)
	}
	return bg, entry, nil
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
