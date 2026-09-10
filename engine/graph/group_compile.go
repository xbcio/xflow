package graph

import (
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

// compileGroups validates and compiles def.Groups into g.groups.
// Must be called after buildEdges and before buildUnits.
func compileGroups(g *Graph, def *types.WorkflowDef) error {
	if len(def.Groups) == 0 {
		return nil
	}
	if err := validateGroupsAllowCyclesExclusion(g, true); err != nil {
		return err
	}
	g.groups = make([]GroupMeta, 0, len(def.Groups))
	seen := map[string]bool{}
	for i, gd := range def.Groups {
		if gd.Name == "" {
			return fmt.Errorf("group #%d: empty name", i)
		}
		if seen[gd.Name] {
			return fmt.Errorf("group %q: duplicate name", gd.Name)
		}
		if err := validateGroupNameNotReserved(gd.Name); err != nil {
			return err
		}
		seen[gd.Name] = true
		if err := validateGroupOnError(gd.Name, gd.OnError); err != nil {
			return err
		}
		if err := validateGroupRetry(gd.Name, gd.Retry); err != nil {
			return err
		}
		meta, err := compileOneGroup(g, gd, len(g.groups))
		if err != nil {
			return fmt.Errorf("group %q: %w", gd.Name, err)
		}
		if err := validateGroupPortability(g, &meta); err != nil {
			return err
		}
		if err := validateNoSecretLiterals(g, &meta); err != nil {
			return err
		}
		g.groups = append(g.groups, meta)
	}
	return nil
}

// validateGroupOnError restricts a group's on_error to the policies the group
// executor actually implements.
//
// groupOnErrorFatal (engine/group_exec.go) maps continue => non-fatal and
// everything else => fatal. So error_output and main_output are *accepted* by
// the type but run as stop: the author asks for the failure to be routed to a
// downstream branch and gets the whole execution failed instead, with no
// diagnostic on the path least likely to be exercised before production.
//
// Routing them is a new mechanism, not a wiring gap. GroupMeta.BoundaryOutputs
// is derived purely from member edges that cross the boundary, compileOneGroup
// never synthesizes one from OnError, and CommitGroupResult rejects any exit
// whose (nodeIdx, port) is absent from BoundaryOutputs — so a fabricated
// "group failed" exit is rejected today. See NODE-GROUP-COLOCATION.md §12.2.
//
// Unknown values are rejected for the same reason: OnError was a plain string
// with no validation, so `fail` (which types/group.go's own doc comment warns
// does not exist) or `error-output` also compiled clean and ran as fatal.
func validateGroupOnError(name, onErr string) error {
	switch onErr {
	case "", string(types.OnErrorStop), string(types.OnErrorContinue):
		return nil
	case string(types.OnErrorOutput), string(types.OnErrorMainOutput):
		return fmt.Errorf("group %q: on_error=%q is not supported on a group: "+
			"a group has no error output port to route to (only %q, %q, %q are supported)",
			name, onErr, "", types.OnErrorStop, types.OnErrorContinue)
	default:
		return fmt.Errorf("group %q: on_error=%q is not a known policy "+
			"(only %q, %q, %q are supported on a group)",
			name, onErr, "", types.OnErrorStop, types.OnErrorContinue)
	}
}

// validateGroupRetry rejects GroupDef.Retry outright: no runtime code
// enforces it.
//
// GroupDef.Retry's doc comment (types/group.go) promises "组级 retry = 从入口
// 整组重跑" (group-level retry means re-running the whole group from its
// entry), but compileOneGroup only copies the value from GroupDef into
// GroupMeta (this file, a few lines below) and nothing ever reads
// GroupMeta.Retry back out. There is no loop that compares the group's
// attempt count against Retry.MaxAttempts: engine/group_exec.go's own comment
// on executeGroup says enforcement "belongs to a future milestone" — the
// group's Attempt is only ever used as a lease fencing token
// (engine/group_lease.go), never checked against a limit, unlike the
// node-level path where atomic_commit.go compares attempt against
// settings.MaxAttempts before allowing another try.
//
// Accepting the field and silently ignoring it is the failure mode this
// rejects, for the same reason validateGroupOnError rejects unimplemented
// on_error values: an operator who writes `retry: {max_attempts: 3}` on a
// group gets a clean compile and zero behavior change, and will only
// discover the group never retries when it fails in production and nobody
// reran it. A compile error is the cheaper time to find that out.
func validateGroupRetry(name string, retry *types.RetrySettings) error {
	if retry == nil {
		return nil
	}
	return fmt.Errorf("group %q: retry is not supported on a group: "+
		"group-level retry (re-running the whole group from its entry) has no "+
		"runtime enforcement yet, so this setting would compile but never take "+
		"effect; configure retry on the individual member node(s) instead",
		name)
}

func compileOneGroup(g *Graph, gd types.GroupDef, groupIdx int) (GroupMeta, error) {
	if len(gd.Members) == 0 {
		return GroupMeta{}, fmt.Errorf("no members")
	}
	members := make([]int, 0, len(gd.Members))
	set := map[int]bool{}
	for _, name := range gd.Members {
		idx, ok := g.index[name]
		if !ok {
			return GroupMeta{}, fmt.Errorf("unknown member %q", name)
		}
		if set[idx] {
			return GroupMeta{}, fmt.Errorf("duplicate member %q", name)
		}
		if g.nodes[idx].GroupIdx != -1 {
			return GroupMeta{}, fmt.Errorf("member %q already belongs to another group", name)
		}
		if g.nodes[idx].RunnerSelector != nil {
			return GroupMeta{}, fmt.Errorf("member %q must not set RunnerSelector (placement belongs to the group)", name)
		}
		if g.nodes[idx].ActivationReplicas != 0 {
			return GroupMeta{}, fmt.Errorf("member %q must not set ActivationReplicas (activation cardinality belongs to the group)", name)
		}
		if g.nodes[idx].Kind == types.NodeKindSupply {
			return GroupMeta{}, fmt.Errorf("member %q is a supply node; supply node may not be a group member", name)
		}
		set[idx] = true
		members = append(members, idx)
	}
	// Sort members by node index for determinism.
	sort.Ints(members)
	for _, idx := range members {
		g.nodes[idx].GroupIdx = groupIdx
	}
	entry, trigger, err := resolveGroupEntry(g, set)
	if err != nil {
		return GroupMeta{}, err
	}
	if err := assertEntryDominates(g, entry, set); err != nil {
		return GroupMeta{}, err
	}
	return GroupMeta{
		Name:               gd.Name,
		Members:            members,
		EntryIdx:           entry,
		UnitIdx:            -1,
		Trigger:            trigger,
		RunnerSelector:     gd.RunnerSelector,
		OnError:            gd.OnError,
		Retry:              gd.Retry,
		Timeout:            gd.Timeout,
		Mode:               gd.Mode,
		ActivationReplicas: gd.ActivationReplicas,
	}, nil
}

// resolveGroupEntry determines the unique entry node for a group.
// A trigger member takes priority; otherwise the unique member with external
// incoming edges; otherwise the unique member with no intra-group predecessors.
func resolveGroupEntry(g *Graph, members map[int]bool) (entry int, trigger bool, err error) {
	var triggers, external, roots []int
	for idx := range members {
		if g.nodes[idx].Kind == types.NodeKindTrigger {
			triggers = append(triggers, idx)
		}
		externalIn, internalIn := false, false
		for _, e := range g.inEdges[idx] {
			if members[e.SrcIdx] {
				internalIn = true
			} else {
				externalIn = true
			}
		}
		if externalIn {
			external = append(external, idx)
		}
		if !internalIn {
			roots = append(roots, idx)
		}
	}
	switch {
	case len(triggers) > 1:
		return 0, false, fmt.Errorf("group has %d triggers, want at most 1", len(triggers))
	case len(triggers) == 1:
		return triggers[0], true, nil
	case len(external) > 1:
		return 0, false, fmt.Errorf("group has %d external entry points, want exactly 1", len(external))
	case len(external) == 1:
		return external[0], false, nil
	case len(roots) != 1:
		return 0, false, fmt.Errorf("group must have exactly one entry (member with no intra-group predecessor), found %d", len(roots))
	default:
		return roots[0], false, nil
	}
}

// assertEntryDominates verifies that every member is reachable from the entry
// node via directed paths that stay within the group (spec section 11.1).
func assertEntryDominates(g *Graph, entry int, members map[int]bool) error {
	seen := map[int]bool{entry: true}
	queue := []int{entry}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.outEdges[cur] {
			if members[e.DstIdx] && !seen[e.DstIdx] {
				seen[e.DstIdx] = true
				queue = append(queue, e.DstIdx)
			}
		}
	}
	if len(seen) != len(members) {
		return fmt.Errorf("entry %q reaches %d of %d members; entry must dominate all members",
			g.nodes[entry].Name, len(seen), len(members))
	}
	return nil
}
