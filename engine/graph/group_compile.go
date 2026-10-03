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
		if err := validateGroupErrorOutputs(gd.Name, gd.OnError, gd.ErrorOutputs); err != nil {
			return err
		}
		if err := validateGroupRetry(gd.Name, gd.Retry); err != nil {
			return err
		}
		meta, err := compileOneGroup(g, def, gd, len(g.groups))
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
// error_output is implemented: a failing group routes to its declared
// ErrorOutputs targets (see compileOneGroup / buildUnitEdges's synthetic
// ErrorEdge) instead of failing the whole execution. main_output stays
// rejected — unlike a node, which has its own successful output to merge the
// error into on main_output, a group that failed before committing has no
// single member output to stand in for "the group's main result", so there is
// no principled payload main_output could carry. See
// NODE-GROUP-COLOCATION.md §12.2 for the mechanism this implements.
//
// Unknown values are rejected because OnError was a plain string with no
// validation, so `fail` (which types/group.go's own doc comment warns does
// not exist) or `error-output` also compiled clean and ran as fatal.
func validateGroupOnError(name, onErr string) error {
	switch onErr {
	case "", string(types.OnErrorStop), string(types.OnErrorContinue), string(types.OnErrorOutput):
		return nil
	case string(types.OnErrorMainOutput):
		return fmt.Errorf("group %q: on_error=%q is not supported on a group: "+
			"a group has no single member output to stand in for the group's main "+
			"result when it fails before committing (only %q, %q, %q, %q are supported)",
			name, onErr, "", types.OnErrorStop, types.OnErrorContinue, types.OnErrorOutput)
	default:
		return fmt.Errorf("group %q: on_error=%q is not a known policy "+
			"(only %q, %q, %q, %q are supported on a group)",
			name, onErr, "", types.OnErrorStop, types.OnErrorContinue, types.OnErrorOutput)
	}
}

// validateGroupErrorOutputs cross-validates GroupDef.OnError against
// GroupDef.ErrorOutputs: the two must agree, or the configuration either
// routes nowhere (error_output with no targets) or declares a route that is
// never taken (targets declared under any other policy) — both silent
// no-ops that a compile error catches instead of a production incident.
func validateGroupErrorOutputs(name string, onErr string, errorOutputs []types.Connection) error {
	if onErr == string(types.OnErrorOutput) {
		if len(errorOutputs) == 0 {
			return fmt.Errorf("group %q: on_error=%q requires at least one error_outputs target",
				name, types.OnErrorOutput)
		}
		return nil
	}
	if len(errorOutputs) > 0 {
		return fmt.Errorf("group %q: error_outputs is set but on_error=%q does not route to it; "+
			"set on_error: %q or remove error_outputs", name, onErr, types.OnErrorOutput)
	}
	return nil
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

func compileOneGroup(g *Graph, def *types.WorkflowDef, gd types.GroupDef, groupIdx int) (GroupMeta, error) {
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
	errorOutputs, err := resolveGroupErrorOutputs(g, def, gd, set)
	if err != nil {
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
		ErrorOutputs:       errorOutputs,
		Retry:              gd.Retry,
		Timeout:            gd.Timeout,
		Mode:               gd.Mode,
		ActivationReplicas: gd.ActivationReplicas,
	}, nil
}

// resolveGroupErrorOutputs resolves GroupDef.ErrorOutputs targets to
// BoundaryEndpoint entries. Each target must name a real node outside this
// group (a target inside the group would be a member the group itself is
// about to abandon mid-execution — there is no running engine instance left
// to deliver to once the group fails) and must respect the target's own
// declared input ports, exactly like an ordinary connection
// (buildEdges/declaredInputPorts). Duplicate (node, port) targets are
// rejected: a routed failure fires each target edge's arrival exactly once,
// so a duplicate could not express anything a single entry does not already.
func resolveGroupErrorOutputs(g *Graph, def *types.WorkflowDef, gd types.GroupDef, members map[int]bool) ([]BoundaryEndpoint, error) {
	if len(gd.ErrorOutputs) == 0 {
		return nil, nil
	}
	out := make([]BoundaryEndpoint, 0, len(gd.ErrorOutputs))
	dup := map[string]bool{}
	for _, target := range gd.ErrorOutputs {
		idx, ok := g.index[target.Node]
		if !ok {
			return nil, fmt.Errorf("group %q: error_outputs references unknown node %q", gd.Name, target.Node)
		}
		if members[idx] {
			return nil, fmt.Errorf("group %q: error_outputs target %q is a member of this group", gd.Name, target.Node)
		}
		if g.nodes[idx].Kind == types.NodeKindSupply {
			return nil, fmt.Errorf("group %q: error_outputs target %q is a supply node", gd.Name, target.Node)
		}
		port := types.DefaultInputPort
		if target.Input != "" {
			port = target.Input
		}
		key := fmt.Sprintf("%d\x00%s", idx, port)
		if dup[key] {
			return nil, fmt.Errorf("group %q: error_outputs targets %s:%s more than once", gd.Name, target.Node, port)
		}
		dup[key] = true
		var dstDef *types.NodeDef
		for i := range def.Nodes {
			if def.Nodes[i].Name == target.Node {
				dstDef = &def.Nodes[i]
				break
			}
		}
		if err := declaredInputPorts(dstDef).validateEdgeTarget(gd.Name, target.Node, target.Input); err != nil {
			return nil, fmt.Errorf("group %q: %w", gd.Name, err)
		}
		out = append(out, BoundaryEndpoint{NodeIdx: idx, Port: port})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeIdx != out[j].NodeIdx {
			return out[i].NodeIdx < out[j].NodeIdx
		}
		return out[i].Port < out[j].Port
	})
	return out, nil
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
