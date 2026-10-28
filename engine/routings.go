package engine

import (
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// RoutableTaskType reports whether a task of this type is dispatched to a
// runner through the claim path. It is the same partition TaskRouting refuses
// with types.ErrPermanent: TaskTypeNodeAdvance, TaskTypeNodeSkip and
// TaskTypeSubWorkflow are consumed by the engine itself and never reach a
// runner, so no runner-availability question can involve them.
func RoutableTaskType(t TaskType) bool {
	switch t {
	case TaskTypeNodeAdvance, TaskTypeNodeSkip, TaskTypeSubWorkflow:
		return false
	}
	return true
}

// DispatchableUnit is one scheduling unit of a compiled graph whose work is
// dispatched to a runner when the workflow runs: an ordinary node executes as
// TaskTypeNodeExec, a co-location group as its TaskTypeGroupExec unit. Routing
// is the unit's placement metadata — built by the same graph-only constructors
// Engine.TaskRouting uses for a live dispatch, so the two cannot disagree.
type DispatchableUnit struct {
	UnitIdx  int
	Name     string // node name, or the group's name for a group unit
	TaskType TaskType
	Routing  TaskRouting
}

// DispatchableUnits enumerates, in unit order, the scheduling units of a
// compiled graph that produce runner tasks.
//
// The exclusions are each a place where the engine or the control plane owns
// the work instead of the claim path:
//
//   - Trigger units (a trigger node, or a group whose entry is a trigger) are
//     hosted activations: the entry-activation reconciler places them with
//     chooseRunner, and no assignment ever enters a claim queue.
//   - Supply nodes are not scheduling units at all (buildUnits keeps them out
//     of the unit layer), so they cannot appear here.
//   - xflow.subworkflow nodes run as engine-owned TaskTypeSubWorkflow.
//   - Definition-disabled nodes are committed as skipped by the engine's
//     disabled hook (engine/disabled.go) before any lease is issued.
//   - Pinned nodes are served the mock output by the engine's pin hook
//     (engine/pin_data.go) when pin_data_mode is "always" and the graph is
//     acyclic — exactly the conditions under which that hook intercepts.
//     Under "test_only" the decision depends on the execution's test flag,
//     which a static graph check cannot see, so those nodes stay counted: a
//     production execution dispatches them.
//
// A group member is covered through its group unit; a map body's inner nodes
// run inside their parent's batch task and share the parent's routing. An
// error from a group unit means its package failed to project; a caller that
// cannot answer the availability question should fail rather than guess.
func DispatchableUnits(g *graph.Graph) ([]DispatchableUnit, error) {
	if g == nil {
		return nil, nil
	}
	pinAlways := g.PinDataMode() == types.PinDataModeAlways && !g.AllowCycles()
	out := make([]DispatchableUnit, 0, g.UnitCount())
	for i := 0; i < g.UnitCount(); i++ {
		switch g.UnitKindAt(i) {
		case graph.UnitGroup:
			gm := g.GroupMetaAt(i)
			if gm.Trigger {
				continue
			}
			routing, err := groupTaskRouting(g, i)
			if err != nil {
				return nil, err
			}
			out = append(out, DispatchableUnit{
				UnitIdx:  i,
				Name:     gm.Name,
				TaskType: TaskTypeGroupExec,
				Routing:  routing,
			})
		case graph.UnitNode:
			meta := g.NodeAt(g.UnitNodeIndex(i))
			if meta.Kind == types.NodeKindTrigger ||
				meta.Type == types.SubWorkflowNodeType ||
				meta.Disabled ||
				(pinAlways && meta.PinOutput != nil) {
				continue
			}
			out = append(out, DispatchableUnit{
				UnitIdx:  i,
				Name:     meta.Name,
				TaskType: execTaskTypeForNode(meta),
				Routing:  nodeTaskRouting(meta),
			})
		}
	}
	return out, nil
}
