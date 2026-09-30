package engine

import (
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// UnselectedRootSkips returns the durable skip intents that resolve every root
// unit an entry-scoped start did not select.
//
// A workflow may declare several explicit entries (start plus triggers, or
// several triggers). An execution begins from exactly one of them — the entry
// Invoke names, or the entry unit a trigger admission seeds — and the other
// roots must not block it (DSL-SPECIFICATION.md, trigger nodes). The acyclic
// completion counter is seeded with every unit in the graph, so a root nothing
// ever schedules keeps that counter above zero and the execution can never
// finish; a wait_all fan-in shared with such a root never becomes ready either.
//
// Resolving the unselected roots through the ordinary skip cascade fixes both
// without a second completion protocol: each skip commits the root as skipped,
// decrements the counter, and propagates an inactive arrival downstream, which
// is exactly what an untaken branch already does. The backend that writes these
// intents must also write the matching "skip" scheduling marker for each unit
// in the same transition, because the skip commit is fenced on that marker.
//
// selectedUnit is the unit index of the chosen entry. Cyclic graphs keep their
// activation-based protocol and get no skips. Group roots are left alone: a
// skip commit resolves a single node, and a group's boundary is not one.
func UnselectedRootSkips(id types.ExecutionID, g *graph.Graph, selectedUnit int) []Task {
	if g == nil || g.AllowCycles() {
		return nil
	}
	var out []Task
	for unitIdx := 0; unitIdx < g.UnitCount(); unitIdx++ {
		if unitIdx == selectedUnit || g.UnitInDegreeAt(unitIdx) != 0 || g.UnitKindAt(unitIdx) != graph.UnitNode {
			continue
		}
		nodeIdx := g.UnitNodeIndex(unitIdx)
		out = append(out, Task{
			ExecutionID: id,
			NodeName:    g.NodeName(nodeIdx),
			NodeIdx:     nodeIdx,
			UnitIdx:     unitIdx,
			Type:        TaskTypeNodeSkip,
		})
	}
	return out
}
