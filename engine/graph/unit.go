package graph

import (
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/types"
)

// UnitKind distinguishes node-level units from group-level units in the
// two-layer durable scheduling IR.
type UnitKind uint8

const (
	UnitNode  UnitKind = iota // standalone (ungrouped) node
	UnitGroup                 // co-located group treated as single scheduling unit
)

// UnitMeta is a vertex in the durable scheduling topology: either an ungrouped
// node or an entire co-location group.
type UnitMeta struct {
	Kind               UnitKind
	Name               string
	NodeIdx            int // valid for UnitNode; -1 otherwise
	GroupIdx           int // valid for UnitGroup; -1 otherwise
	RunnerSelector     *types.RunnerSelector
	Retry              *types.RetrySettings
	Timeout            time.Duration
	ActivationReplicas uint32 `json:",omitempty"`
}

// BoundaryEndpoint identifies a specific port on a node within (or outside) a
// group, used to describe cross-boundary edges.
type BoundaryEndpoint struct {
	NodeIdx int
	Port    string
}

// UnitEdge is a scheduling edge between two units; it preserves the original
// endpoint information so downstream code can read data by (member, port).
type UnitEdge struct {
	SrcUnit, DstUnit int
	Src, Dst         BoundaryEndpoint
}

// BoundaryEdge describes a single original node-level edge that crosses a group
// boundary (either entering or leaving the group).
type BoundaryEdge struct {
	Src, Dst         BoundaryEndpoint
	SrcUnit, DstUnit int
}

// GroupMeta is the compiled artifact for a co-location group.
type GroupMeta struct {
	Name               string
	Members            []int
	EntryIdx           int
	UnitIdx            int
	Trigger            bool
	BoundaryInputs     []BoundaryEdge
	BoundaryOutputs    []BoundaryEdge
	RunnerSelector     *types.RunnerSelector
	OnError            string
	Retry              *types.RetrySettings
	Timeout            time.Duration
	Mode               string
	PackageHash        string
	ActivationReplicas uint32 `json:",omitempty"`
}

// buildUnits constructs the durable unit graph. Ungrouped nodes become UnitNode
// units (preserving original node index order); each group collapses into a
// single UnitGroup unit (preserving group definition order). Cross-unit edges
// are derived from boundary edges. Non-cyclic graphs must have a DAG unit graph.
func buildUnits(g *Graph) error {
	g.nodeUnit = make([]int, len(g.nodes))
	for i := range g.nodeUnit {
		g.nodeUnit[i] = -1
	}
	g.units = g.units[:0]
	groupUnit := make([]int, len(g.groups))
	for i := range groupUnit {
		groupUnit[i] = -1
	}
	// Pass 1: ungrouped nodes first, preserving original node index order.
	for i := range g.nodes {
		if g.nodes[i].GroupIdx != -1 {
			continue
		}
		// A supply node maintains long-lived shared data and never advances an
		// execution. Keeping it out of the unit layer is what makes the
		// remaining-unit denominator (UnitCount) unchanged: if it became a unit,
		// nothing would ever complete it and every execution would hang forever.
		// nodeUnit[i] stays -1; buildDependencyEdges has already rejected any
		// dataflow edge touching it, so no unit edge can dereference that -1.
		if g.nodes[i].Kind == types.NodeKindSupply {
			continue
		}
		g.nodeUnit[i] = len(g.units)
		g.units = append(g.units, UnitMeta{Kind: UnitNode, Name: g.nodes[i].Name,
			NodeIdx: i, GroupIdx: -1, RunnerSelector: g.nodes[i].RunnerSelector, Retry: g.nodes[i].Retry,
			ActivationReplicas: g.nodes[i].ActivationReplicas})
	}
	// Pass 2: group units, preserving group definition order.
	for gi := range g.groups {
		gm := &g.groups[gi]
		groupUnit[gi] = len(g.units)
		gm.UnitIdx = len(g.units)
		g.units = append(g.units, UnitMeta{Kind: UnitGroup, Name: gm.Name, NodeIdx: -1,
			GroupIdx: gi, RunnerSelector: gm.RunnerSelector, Retry: gm.Retry, Timeout: gm.Timeout,
			ActivationReplicas: gm.ActivationReplicas})
		for _, memberIdx := range gm.Members {
			g.nodeUnit[memberIdx] = groupUnit[gi]
		}
	}
	buildUnitEdges(g)
	if !g.allowCycles {
		if err := detectUnitCycle(g); err != nil {
			return err
		}
	}
	return nil
}

// buildUnitEdges traverses original node edges, skips intra-unit edges, and
// records cross-unit edges with stable sorting.
func buildUnitEdges(g *Graph) {
	// BoundaryInputs/BoundaryOutputs are appended to below. buildUnits can run
	// more than once on the same Graph (snapshot UnmarshalJSON rebuilds the unit
	// IR), so reset them here or every rebuild doubles the boundary edge set.
	for gi := range g.groups {
		g.groups[gi].BoundaryInputs = g.groups[gi].BoundaryInputs[:0]
		g.groups[gi].BoundaryOutputs = g.groups[gi].BoundaryOutputs[:0]
	}
	g.unitOutEdges = make([][]UnitEdge, len(g.units))
	g.unitInEdges = make([][]UnitEdge, len(g.units))
	g.unitInDegree = make([]int, len(g.units))
	for src := range g.outEdges {
		for _, e := range g.outEdges[src] {
			su, du := g.nodeUnit[e.SrcIdx], g.nodeUnit[e.DstIdx]
			if su == du {
				continue // intra-unit edge: only for runner-local member graph
			}
			ue := UnitEdge{SrcUnit: su, DstUnit: du,
				Src: BoundaryEndpoint{NodeIdx: e.SrcIdx, Port: e.SrcPort},
				Dst: BoundaryEndpoint{NodeIdx: e.DstIdx, Port: e.DstPort}}
			g.unitOutEdges[su] = append(g.unitOutEdges[su], ue)
			g.unitInEdges[du] = append(g.unitInEdges[du], ue)
			g.unitInDegree[du]++
			be := BoundaryEdge{Src: ue.Src, Dst: ue.Dst, SrcUnit: su, DstUnit: du}
			if g.units[su].Kind == UnitGroup {
				gm := &g.groups[g.units[su].GroupIdx]
				gm.BoundaryOutputs = append(gm.BoundaryOutputs, be)
			}
			if g.units[du].Kind == UnitGroup {
				gm := &g.groups[g.units[du].GroupIdx]
				gm.BoundaryInputs = append(gm.BoundaryInputs, be)
			}
		}
	}
	for i := range g.unitOutEdges {
		sort.Slice(g.unitOutEdges[i], func(a, b int) bool { return unitEdgeLess(g.unitOutEdges[i][a], g.unitOutEdges[i][b]) })
	}
	for i := range g.unitInEdges {
		sort.Slice(g.unitInEdges[i], func(a, b int) bool { return unitEdgeLess(g.unitInEdges[i][a], g.unitInEdges[i][b]) })
	}
	for gi := range g.groups {
		sortBoundary(g.groups[gi].BoundaryOutputs)
		sortBoundary(g.groups[gi].BoundaryInputs)
	}
}

func sortBoundary(bs []BoundaryEdge) {
	sort.Slice(bs, func(a, b int) bool {
		return boundaryEdgeLess(bs[a], bs[b])
	})
}

// boundaryEdgeLess orders boundary edges by their node-level endpoint fields.
// Since all boundary edges in one group share the same SrcUnit (for outputs) or
// DstUnit (for inputs), the sort is driven by node index and port name.
func boundaryEdgeLess(a, b BoundaryEdge) bool {
	if a.Src.NodeIdx != b.Src.NodeIdx {
		return a.Src.NodeIdx < b.Src.NodeIdx
	}
	if a.Src.Port != b.Src.Port {
		return a.Src.Port < b.Src.Port
	}
	if a.Dst.NodeIdx != b.Dst.NodeIdx {
		return a.Dst.NodeIdx < b.Dst.NodeIdx
	}
	return a.Dst.Port < b.Dst.Port
}

func unitEdgeLess(a, b UnitEdge) bool {
	if a.SrcUnit != b.SrcUnit {
		return a.SrcUnit < b.SrcUnit
	}
	if a.DstUnit != b.DstUnit {
		return a.DstUnit < b.DstUnit
	}
	if a.Src.NodeIdx != b.Src.NodeIdx {
		return a.Src.NodeIdx < b.Src.NodeIdx
	}
	if a.Src.Port != b.Src.Port {
		return a.Src.Port < b.Src.Port
	}
	if a.Dst.NodeIdx != b.Dst.NodeIdx {
		return a.Dst.NodeIdx < b.Dst.NodeIdx
	}
	return a.Dst.Port < b.Dst.Port
}

// detectUnitCycle runs Kahn's algorithm on the unit graph to verify it is a DAG.
// Group collapsing may introduce cycles at the unit level even if the original
// node graph is acyclic (spec section 3.4).
func detectUnitCycle(g *Graph) error {
	indeg := make([]int, len(g.units))
	copy(indeg, g.unitInDegree)
	queue := make([]int, 0, len(g.units))
	for i, d := range indeg {
		if d == 0 {
			queue = append(queue, i)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, e := range g.unitOutEdges[cur] {
			indeg[e.DstUnit]--
			if indeg[e.DstUnit] == 0 {
				queue = append(queue, e.DstUnit)
			}
		}
	}
	if visited != len(g.units) {
		return fmt.Errorf("group boundary edges form a cycle across units")
	}
	return nil
}
