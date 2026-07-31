package control

import (
	"fmt"
	"sort"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
)

// deriveEntrySeedTopology resolves the entry unit index and the downstream unit
// arrivals its boundary outputs feed, server-side, from the compiled graph. It
// is what turns an admitted remote seed into actual downstream tasks: without a
// resolved graph + downstream, a seed is admitted but produces no fan-out.
//
// It faithfully mirrors the engine's normal unit-graph downstream derivation
// (engine.(*Engine).downstreamUnitArrivals, engine/group_exec.go:164): it builds
// the set of fired boundary ports from exits, walks the entry unit's out-edges,
// aggregates them per destination unit, and sets ActiveCount only for edges
// whose source (member,port) actually fired. Edges from ports that did NOT fire
// are counted in ArrivalCount but not ActiveCount, so an unfired branch is
// skip-propagated downstream rather than executed — exactly as the mid-graph
// commit path does.
//
// entryUnitID is the entry-unit ID (a standalone node name or a group name).
// exits are the fired boundary outputs from the seed request (req.Exits); they
// select which downstream edges are active. It fails CLOSED: a nil graph, or an
// entry unit that cannot be resolved to a unit, returns an error so the caller
// rejects the seed rather than admitting it with no downstream.
func deriveEntrySeedTopology(g *graph.Graph, entryUnitID string, exits []engine.BoundaryExit) (entryUnitIdx int, downstream []engine.DownstreamArrival, err error) {
	if g == nil {
		return 0, nil, fmt.Errorf("%w: nil graph", ErrEntrySeedWorkflowUnknown)
	}

	entryUnitIdx, ok := entryUnitIndex(g, entryUnitID)
	if !ok {
		return 0, nil, fmt.Errorf("%w: entry unit %q not found", ErrEntrySeedWorkflowUnknown, entryUnitID)
	}

	// The set of fired boundary ports, keyed by (source node index, port).
	// Mirrors downstreamUnitArrivals' `active` map: an edge is active only when
	// its source (member,port) is present here.
	active := make(map[string]bool, len(exits))
	for _, ex := range exits {
		nodeIdx, ok := g.NodeIndex(ex.NodeName)
		if !ok {
			continue // exit references a node not in the graph; ignore for activation
		}
		active[boundaryPortKey(nodeIdx, ex.Port)] = true
	}

	// Aggregate the entry unit's out-edges per destination unit. Mirrors
	// downstreamUnitArrivals (engine/group_exec.go): one DownstreamArrival per
	// downstream unit, with the destination's representative node/name resolved
	// through the same UnitNode/UnitGroup branch.
	byDst := make(map[int]engine.DownstreamArrival)
	for _, ue := range g.UnitOutEdges(entryUnitIdx) {
		a, seen := byDst[ue.DstUnit]
		if !seen {
			execType := engine.TaskTypeNodeExec
			target := g.UnitNodeIndex(ue.DstUnit)
			name := g.NodeAt(target).Name
			if g.UnitKindAt(ue.DstUnit) == graph.UnitGroup {
				gm := g.GroupMetaAt(ue.DstUnit)
				execType, target, name = engine.TaskTypeGroupExec, gm.EntryIdx, gm.Name
			}
			a = engine.DownstreamArrival{
				NodeName:     name,
				NodeIdx:      target,
				UnitIdx:      ue.DstUnit,
				MergeMode:    g.UnitMergeMode(ue.DstUnit),
				ExecTaskType: execType,
			}
		}
		a.ArrivalCount++
		if active[boundaryPortKey(ue.Src.NodeIdx, ue.Src.Port)] {
			a.ActiveCount++
		}
		byDst[ue.DstUnit] = a
	}

	// Deterministic order by destination unit index.
	dsts := make([]int, 0, len(byDst))
	for d := range byDst {
		dsts = append(dsts, d)
	}
	sort.Ints(dsts)
	downstream = make([]engine.DownstreamArrival, 0, len(dsts))
	for _, d := range dsts {
		downstream = append(downstream, byDst[d])
	}
	return entryUnitIdx, downstream, nil
}

// boundaryPortKey builds a lookup key from a node index and port name for
// matching fired boundary exits against unit out-edges. It mirrors the engine's
// boundaryKey (engine/group_exec.go:203) byte-for-byte.
func boundaryPortKey(nodeIdx int, port string) string {
	return fmt.Sprintf("%d\x00%s", nodeIdx, port)
}

// entryUnitIndex resolves an entry-unit ID to its scheduling unit index. The
// entry-unit ID is a group name for a group node, or a standalone node name for
// a single-node trigger (spec §11.5). Group names are checked first so a group
// and a member node with a colliding name cannot be confused.
func entryUnitIndex(g *graph.Graph, entryUnitID string) (int, bool) {
	for _, gm := range g.Groups() {
		if gm.Name == entryUnitID {
			return gm.UnitIdx, true
		}
	}
	if nodeIdx, ok := g.NodeIndex(entryUnitID); ok {
		unitIdx := g.UnitIndexForNode(nodeIdx)
		if unitIdx < 0 {
			// A supply node is registered in the node layer but deliberately absent
			// from the unit layer, so UnitIndexForNode yields -1. Returning that as
			// a valid index would seed an entry topology against unit -1.
			return -1, false
		}
		return unitIdx, true
	}
	return 0, false
}
