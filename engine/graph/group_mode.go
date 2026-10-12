package graph

import (
	"fmt"

	"github.com/xbcio/xflow/types"
)

// suspendingNodeTypes lists the built-in node types whose handler implements
// types.SuspendingHandler, i.e. the types that can park a sub-execution and
// wait for an external signal or timer.
//
// It lives here because engine/graph cannot import node/ (every built-in
// handler lives there, and the package would then pull the handler registry
// into the compiler). Keeping the list in the compiler is the same trade
// evaluableParams makes, and TestSuspendingNodeTypesCoversRegisteredHandlers
// in the node graph tests pins it against the registry so a new suspending
// handler cannot be added without an entry.
//
// The list is a compile-time approximation of a runtime question, so a
// user-registered handler that implements SuspendingHandler outside this list
// is not caught here. The runtime still rejects it: a group's members run
// under the runner's embedded engine with suspend disabled (see
// service/runner/group_runtime.go's WithSuspendDisabled), and engine/commit.go
// fails such a task instead of parking it.
var suspendingNodeTypes = map[string]bool{
	"xflow.wait":     true,
	"xflow.approval": true,
}

// SuspendingNodeTypes returns the node types the compiler treats as
// suspending. It exists so a test outside this package can pin the list
// against node/registry's actual SuspendHandler implementations.
func SuspendingNodeTypes() map[string]bool {
	out := make(map[string]bool, len(suspendingNodeTypes))
	for t := range suspendingNodeTypes {
		out[t] = true
	}
	return out
}

// validateGroupMode enforces the compile-time contract for GroupDef.Mode.
//
// Transient groups are the short-TTL co-location mode: the group commits like
// a durable one, but its artifacts (exit outputs, unit status, lease keys) get
// a completion TTL once the execution ends rather than the execution TTL. Two
// shapes are rejected because the mode cannot deliver what they imply:
//
//  1. Unknown mode values. Mode was a free-form string that nothing validated
//     and nothing read (compileOneGroup copied it into GroupMeta verbatim), so
//     `mode: "Transient"` or `mode: "fire_and_forget"` compiled clean and ran
//     as durable. A typo that silently keeps the durable retention policy is
//     exactly the failure the transient mode exists to avoid.
//
//  2. Suspending members. A suspended member parks a sub-execution the group's
//     own lease cannot resume, which is why the runtime already disables
//     suspend inside every group (service/runner/group_runtime.go,
//     WithSuspendDisabled). Durable groups get that rejection at runtime; a
//     transient group must not compile at all, because "short TTL, crash
//     replay from the entry" is incompatible with a node whose resume point is
//     external state. The check walks nested bodies too — a member of a member
//     is still a member.
//
//  3. Trigger-entry groups with ActivationReplicas > 1. Sibling replicas each
//     consume from the same source-level consumer group and admit their own
//     executions, so the transient "one short-TTL execution per admission"
//     accounting would be spread across replicas the group cannot see.
//
// A workflow-level Options.Transient alongside a transient group is accepted:
// the group is then redundant with the workflow, not contradictory. The
// backend applies the shorter of the two retentions, so `group.Transient()`
// under a transient workflow is a no-op rather than an error.
func validateGroupMode(g *Graph, gd types.GroupDef) error {
	switch gd.Mode {
	case types.GroupModeDurable, types.GroupModeTransient:
	default:
		return fmt.Errorf("group %q: mode=%q is not a known group mode (only %q, %q are supported)",
			gd.Name, gd.Mode, types.GroupModeDurable, types.GroupModeTransient)
	}
	if gd.Mode != types.GroupModeTransient {
		return nil
	}
	// A transient group's members must not suspend, and neither may anything
	// nested inside a member's own body.
	for _, member := range gd.Members {
		idx, ok := g.index[member]
		if !ok {
			// Unknown members are compileOneGroup's error to report.
			continue
		}
		if suspendingNodeTypes[g.nodes[idx].Type] {
			return fmt.Errorf("group %q: transient group member %q is a suspending node (%s); "+
				"a suspended member parks a sub-execution the group lease cannot resume, "+
				"and a transient group is replayed from its entry instead",
				gd.Name, member, g.nodes[idx].Type)
		}
		if name, typ, ok := nestedSuspendingMember(g.nodes[idx].Parameters); ok {
			return fmt.Errorf("group %q: transient group member %q contains suspending node %q (%s); "+
				"suspend is disabled inside a group, and a transient group is replayed from its entry instead",
				gd.Name, member, name, typ)
		}
	}
	if gd.ActivationReplicas > 1 {
		memberSet := memberSetOf(g, gd.Members)
		_, trigger, err := resolveGroupEntry(g, memberSet)
		if err != nil {
			// A malformed group is compileOneGroup's error to report.
			return nil
		}
		if trigger {
			return fmt.Errorf("group %q: transient groups cannot use activation_replicas=%d: "+
				"sibling replicas admit independent executions from the same consumer group, "+
				"so the transient retention window would not cover all of them",
				gd.Name, gd.ActivationReplicas)
		}
	}
	return nil
}

// memberSetOf resolves member names to node indices, skipping names the graph
// does not know (compileOneGroup reports those).
func memberSetOf(g *Graph, members []string) map[int]bool {
	set := make(map[int]bool, len(members))
	for _, name := range members {
		if idx, ok := g.index[name]; ok {
			set[idx] = true
		}
	}
	return set
}

// nestedSuspendingMember reports the first suspending node inside a member's
// sub-graph body, walking bodies of bodies: today's compiler forbids nesting
// (validateNodeBody rejects a body member that declares its own body), but the
// walk is written to survive that rule being relaxed, and "the compiler will
// reject it elsewhere" is not a reason for a mode validator to look the other
// way.
func nestedSuspendingMember(params map[string]any) (name, typ string, found bool) {
	if len(params) == 0 {
		return "", "", false
	}
	raw, ok := params[subgraphBodyKey]
	if !ok {
		return "", "", false
	}
	bodyDef, err := decodeSubgraphBody(raw)
	if err != nil {
		// A malformed body is validateNodeBody's error to report.
		return "", "", false
	}
	nodes, _, err := decodeSubgraphMembers(bodyDef.Parameters)
	if err != nil {
		return "", "", false
	}
	for _, nd := range nodes {
		if suspendingNodeTypes[nd.Type] {
			return nd.Name, nd.Type, true
		}
		if name, typ, ok := nestedSuspendingMember(nd.Parameters); ok {
			return name, typ, true
		}
	}
	return "", "", false
}
