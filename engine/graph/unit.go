package graph

import (
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
	Kind           UnitKind
	Name           string
	NodeIdx        int // valid for UnitNode; -1 otherwise
	GroupIdx       int // valid for UnitGroup; -1 otherwise
	RunnerSelector *types.RunnerSelector
	Retry          *types.RetrySettings
	Timeout        time.Duration
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
	Name            string
	Members         []int
	EntryIdx        int
	UnitIdx         int
	Trigger         bool
	BoundaryInputs  []BoundaryEdge
	BoundaryOutputs []BoundaryEdge
	RunnerSelector  *types.RunnerSelector
	OnError         string
	Retry           *types.RetrySettings
	Timeout         time.Duration
	Mode            string
	PackageHash     string
}

// buildUnits constructs the durable unit graph. This implementation handles the
// no-group degenerate case: every node maps 1:1 to a UnitNode, unit indices
// equal node indices, and unit edges mirror node edges.
// Task 5/7 will extend this to merge grouped nodes into UnitGroup vertices and
// generate boundary edges.
func buildUnits(g *Graph) error {
	n := len(g.nodes)
	g.units = make([]UnitMeta, n)
	g.unitOutEdges = make([][]UnitEdge, n)
	g.unitInEdges = make([][]UnitEdge, n)
	g.unitInDegree = make([]int, n)
	for i := range g.nodes {
		g.units[i] = UnitMeta{Kind: UnitNode, Name: g.nodes[i].Name, NodeIdx: i, GroupIdx: -1}
	}
	for src := range g.outEdges {
		for _, e := range g.outEdges[src] {
			ue := UnitEdge{
				SrcUnit: src, DstUnit: e.DstIdx,
				Src: BoundaryEndpoint{NodeIdx: e.SrcIdx, Port: e.SrcPort},
				Dst: BoundaryEndpoint{NodeIdx: e.DstIdx, Port: e.DstPort},
			}
			g.unitOutEdges[src] = append(g.unitOutEdges[src], ue)
			g.unitInEdges[e.DstIdx] = append(g.unitInEdges[e.DstIdx], ue)
			g.unitInDegree[e.DstIdx]++
		}
	}
	return nil
}
