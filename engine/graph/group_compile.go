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
		Name:           gd.Name,
		Members:        members,
		EntryIdx:       entry,
		UnitIdx:        -1,
		Trigger:        trigger,
		RunnerSelector: gd.RunnerSelector,
		OnError:        gd.OnError,
		Retry:          gd.Retry,
		Timeout:        gd.Timeout,
		Mode:           gd.Mode,
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
