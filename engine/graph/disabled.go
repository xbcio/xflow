package graph

import (
	"fmt"

	"github.com/xbcio/xflow/types"
)

// assignDisabledNodes marks NodeMeta.Disabled on the compiled graph for every
// node whose definition carries disabled: true, and rejects every shape the
// engine's disabled-node hook cannot intercept (DSL-SPECIFICATION §3.1).
//
// The hook intercepts exactly one thing: a node-level exec task
// (TaskTypeNodeExec) whose scheduling unit is a plain node unit. For any other
// shape the flag could not be honoured, and quietly ignoring it would run the
// node for real -- the silent failure this pass exists to make impossible. So
// an unsupported shape is a COMPILE ERROR, not a warning. (Contrast
// assignPinData, which warns and ignores: a pin that cannot apply degrades to
// running the real handler, which is still a valid execution. A disabled node
// that runs for real is not.)
//
// It runs after compileGroups because co-location membership decides
// interceptability: a group member executes inside its group's
// TaskTypeGroupExec unit, which the hook never sees. It must run before
// assignGraphHash, since NodeMeta.Disabled is hashed inside Nodes.
func assignDisabledNodes(def *types.WorkflowDef, g *Graph) error {
	disabled := 0
	for i := range def.Nodes {
		nd := &def.Nodes[i]
		if !nd.Disabled {
			continue
		}
		idx, ok := g.index[nd.Name]
		if !ok {
			// registerNodes indexes every definition node, so a miss can only be
			// an internal inconsistency -- fail closed rather than guess.
			return fmt.Errorf("node %q: disabled node missing from compiled graph", nd.Name)
		}
		meta := &g.nodes[idx]
		switch {
		case g.allowCycles:
			return fmt.Errorf("node %q: disabled is not supported in allow_cycles workflows", nd.Name)
		case g.faf:
			return fmt.Errorf("node %q: disabled is not supported in faf workflows (they dispatch directly, without the scheduling path the disabled hook intercepts)", nd.Name)
		case meta.Kind == types.NodeKindTrigger:
			return fmt.Errorf("node %q: disabled is not supported on trigger nodes: a trigger's activation lifecycle is owned by the control plane, so the flag would leave activations running anyway -- remove the trigger's activation instead", nd.Name)
		case meta.Kind == types.NodeKindSupply:
			return fmt.Errorf("node %q: disabled is not supported on supply nodes, which never execute -- remove disabled", nd.Name)
		case meta.GroupIdx >= 0:
			return fmt.Errorf("node %q: disabled is not supported for co-location group members (they execute inside the group's unit) -- remove the node from its group or drop disabled", nd.Name)
		}
		meta.Disabled = true
		disabled++
	}
	if disabled == 0 {
		return nil
	}
	g.hasDisabled = true
	return nil
}
