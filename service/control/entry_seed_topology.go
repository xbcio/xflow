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
// It mirrors the engine's normal unit-graph downstream derivation
// (engine.(*Engine).downstreamUnitArrivals, engine/group_exec.go:164): it walks
// the entry unit's out-edges, aggregates them per destination unit, and fills
// NodeName/NodeIdx/UnitIdx/MergeMode/ExecTaskType exactly as that path does.
//
// The one semantic difference from the mid-graph path: the entry unit is the
// seed source that just succeeded, so every boundary-output edge is ACTIVE
// (ActiveCount == ArrivalCount). The mid-graph path selects active edges from
// the fired exit ports because a completed unit may fire only a subset of its
// ports; an accepted entry seed carries a success outcome for the whole entry
// unit, so all its outgoing edges light up.
//
// entryUnitID is the entry-unit ID (a standalone node name or a group name).
// It fails CLOSED: a nil graph, or an entry unit that cannot be resolved to a
// unit, returns an error so the caller rejects the seed rather than admitting
// it with no downstream.
func deriveEntrySeedTopology(g *graph.Graph, entryUnitID string) (entryUnitIdx int, downstream []engine.DownstreamArrival, err error) {
	if g == nil {
		return 0, nil, fmt.Errorf("%w: nil graph", ErrEntrySeedWorkflowUnknown)
	}

	entryUnitIdx, ok := entryUnitIndex(g, entryUnitID)
	if !ok {
		return 0, nil, fmt.Errorf("%w: entry unit %q not found", ErrEntrySeedWorkflowUnknown, entryUnitID)
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
		// Entry seed = the source unit succeeded, so every edge is active.
		a.ActiveCount++
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
		return g.UnitIndexForNode(nodeIdx), true
	}
	return 0, false
}
