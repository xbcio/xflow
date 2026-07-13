package graph

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xbcio/xflow/types"
)

// experimentalExpandTypes lists node types that depend on body sub-graph
// execution, which is not yet implemented (see engine/expand.go and
// .claude/specs/expand-gate.md). The compiler rejects workflows that
// reference any of these types unless WorkflowOptions.ExperimentalExpand is
// true. Capability tags on individual node Descriptors (carrying
// types.CapBodySubgraphRequired) describe the same requirement but cannot be
// inspected here without adding a graph → node runtime dependency.
var experimentalExpandTypes = map[string]struct{}{
	"xflow.loop":  {},
	"xflow.split": {},
}

// ErrExperimentalExpandRequired is returned by Compile when a workflow uses a
// node type that depends on body sub-graph execution but has not opted in via
// WorkflowOptions.ExperimentalExpand. It carries the offending node identifiers
// so callers can surface them in editor diagnostics.
type ErrExperimentalExpandRequired struct {
	Nodes []string // formatted as "name (type)"
}

func (e *ErrExperimentalExpandRequired) Error() string {
	return fmt.Sprintf(
		"loop/split nodes are experimental and not yet implemented; "+
			"set options.experimental_expand=true to opt in: %s",
		strings.Join(e.Nodes, ", "),
	)
}

// Compile validates a WorkflowDef and builds an immutable Graph IR.
// It returns an error if the definition is nil, has no nodes, contains
// duplicate node names, references unknown nodes, or contains a cycle.
func Compile(def *types.WorkflowDef) (*Graph, error) {
	if def == nil {
		return nil, errors.New("workflow definition is nil")
	}
	n := len(def.Nodes)
	if n == 0 {
		return nil, errors.New("workflow has no nodes")
	}
	if err := validateWorkflowRunnerSelector(def.RunnerSelector); err != nil {
		return nil, err
	}

	g := &Graph{
		Name:         def.Name,
		Nodes:        make([]NodeMeta, n),
		Index:        make(map[string]int, n),
		EntryIndexes: make(map[string]int),
		OutEdges:     make([][]Edge, n),
		InEdges:      make([][]Edge, n),
		InDegree:     make([]int, n),
		StartIdx:     -1,
	}
	var experimentalExpand bool
	if def.Options != nil {
		g.AllowCycles = def.Options.AllowCycles
		g.MaxAutoDepth = def.Options.MaxAutoDepth
		experimentalExpand = def.Options.ExperimentalExpand
	}
	if g.AllowCycles && g.MaxAutoDepth <= 0 {
		g.MaxAutoDepth = DefaultMaxAutoDepth
	}

	// Compile-time gate: block xflow.loop / xflow.split unless the workflow
	// opts into the unfinished body sub-graph implementation. See
	// .claude/specs/expand-gate.md.
	if !experimentalExpand {
		var blocked []string
		for _, nd := range def.Nodes {
			if _, gated := experimentalExpandTypes[nd.Type]; gated {
				blocked = append(blocked, fmt.Sprintf("%s (%s)", nd.Name, nd.Type))
			}
		}
		if len(blocked) > 0 {
			sort.Strings(blocked)
			return nil, &ErrExperimentalExpandRequired{Nodes: blocked}
		}
	}

	if def.Context != nil {
		g.Vars = def.Context.Vars
		g.Config = def.Context.Config
	}

	// First pass: register all nodes.
	startCount := 0
	for i, nd := range def.Nodes {
		if _, dup := g.Index[nd.Name]; dup {
			return nil, fmt.Errorf("duplicate node name: %s", nd.Name)
		}
		runnerSelector, err := resolveRunnerSelector(def.RunnerSelector, nd.RunnerSelector)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", nd.Name, err)
		}
		g.Index[nd.Name] = i
		g.Nodes[i] = NodeMeta{
			Name:           nd.Name,
			Type:           nd.Type,
			Kind:           nd.Kind,
			Version:        nd.Version,
			OnError:        nd.OnError,
			RunnerSelector: runnerSelector,
			MergeMode:      extractMergeMode(nd),
			Parameters:     nd.Parameters,
			Retry:          resolveRetry(nd.Retry, def.Settings),
		}
		if nd.Type == "xflow.start" || nd.Kind == types.NodeKindTrigger {
			g.EntryIndexes[nd.Name] = i
		}
		if g.AllowCycles {
			if nd.Type == "xflow.start" {
				startCount++
				g.StartIdx = i
			}
			if nd.Type == "xflow.merge" && extractMergeMode(nd) == "wait_all" {
				return nil, errors.New("xflow.merge wait_all is not supported in cyclic workflows")
			}
		}
	}

	// Second pass: build edges from Connections.
	for srcName, ports := range def.Connections {
		srcIdx, ok := g.Index[srcName]
		if !ok {
			return nil, fmt.Errorf("connection references unknown source node: %s", srcName)
		}
		for port, conns := range ports {
			for _, c := range conns {
				dstIdx, ok := g.Index[c.Node]
				if !ok {
					return nil, fmt.Errorf("connection references unknown destination node: %s", c.Node)
				}
				edge := Edge{
					SrcIdx:  srcIdx,
					DstIdx:  dstIdx,
					SrcPort: port,
					DstPort: c.Input,
				}
				g.OutEdges[srcIdx] = append(g.OutEdges[srcIdx], edge)
				g.InEdges[dstIdx] = append(g.InEdges[dstIdx], edge)
				g.InDegree[dstIdx]++
			}
		}

		// Collect distinct output port names for this source node.
		portSet := make(map[string]struct{})
		for _, e := range g.OutEdges[srcIdx] {
			portSet[e.SrcPort] = struct{}{}
		}
		portOuts := make([]string, 0, len(portSet))
		for p := range portSet {
			portOuts = append(portOuts, p)
		}
		g.Nodes[srcIdx].PortOuts = portOuts
	}

	if g.AllowCycles {
		if startCount != 1 {
			return nil, fmt.Errorf("cyclic workflow requires exactly one xflow.start node, got %d", startCount)
		}
	} else {
		if err := detectCycle(g); err != nil {
			return nil, err
		}
	}

	return g, nil
}

// resolveRetry chooses the effective retry settings for a node: per-node
// overrides win; otherwise the workflow-level WorkflowSettings.Retry applies;
// otherwise no retry. Returns nil when retries are disabled.
func resolveRetry(node *types.RetrySettings, settings *types.WorkflowSettings) *types.RetrySettings {
	if node != nil && node.MaxAttempts > 0 {
		cp := *node
		return &cp
	}
	if settings != nil && settings.Retry != nil && settings.Retry.MaxAttempts > 0 {
		cp := *settings.Retry
		return &cp
	}
	return nil
}

func validateWorkflowRunnerSelector(selector *types.RunnerSelector) error {
	if selector == nil {
		return nil
	}
	switch selector.Mode {
	case "", types.RunnerSelectorModeDefault, types.RunnerSelectorModeRequired:
	default:
		return fmt.Errorf("workflow runnerSelector.mode must be %q or %q", types.RunnerSelectorModeDefault, types.RunnerSelectorModeRequired)
	}
	if err := validateRunnerSelectorLabels(selector); err != nil {
		return fmt.Errorf("workflow runnerSelector: %w", err)
	}
	return nil
}

func validateNodeRunnerSelector(selector *types.RunnerSelector) error {
	if selector == nil {
		return nil
	}
	if selector.Mode != "" {
		return errors.New("runnerSelector.mode is only valid at workflow level")
	}
	if err := validateRunnerSelectorLabels(selector); err != nil {
		return fmt.Errorf("runnerSelector: %w", err)
	}
	return nil
}

func validateRunnerSelectorLabels(selector *types.RunnerSelector) error {
	for key, value := range selector.MatchLabels {
		if key == "" {
			return errors.New("matchLabels contains an empty key")
		}
		if value == "" {
			return fmt.Errorf("matchLabels[%q] is empty", key)
		}
	}
	return nil
}

func resolveRunnerSelector(workflowSelector, nodeSelector *types.RunnerSelector) (*types.RunnerSelector, error) {
	if err := validateNodeRunnerSelector(nodeSelector); err != nil {
		return nil, err
	}
	mode := types.RunnerSelectorModeDefault
	if workflowSelector != nil && workflowSelector.Mode != "" {
		mode = workflowSelector.Mode
	}
	switch mode {
	case types.RunnerSelectorModeRequired:
		return andRunnerSelectors(workflowSelector, nodeSelector)
	default:
		if nodeSelector != nil && (len(nodeSelector.MatchLabels) > 0 || nodeSelector.Mode != "") {
			return cloneRunnerSelector(nodeSelector), nil
		}
		return cloneRunnerSelector(workflowSelector), nil
	}
}

func andRunnerSelectors(workflowSelector, nodeSelector *types.RunnerSelector) (*types.RunnerSelector, error) {
	out := &types.RunnerSelector{}
	if workflowSelector != nil {
		out.Mode = workflowSelector.Mode
		out.MatchLabels = cloneStringMap(workflowSelector.MatchLabels)
	}
	if nodeSelector != nil {
		if out.MatchLabels == nil && len(nodeSelector.MatchLabels) > 0 {
			out.MatchLabels = make(map[string]string, len(nodeSelector.MatchLabels))
		}
		for key, value := range nodeSelector.MatchLabels {
			if existing, ok := out.MatchLabels[key]; ok && existing != value {
				return nil, fmt.Errorf("runnerSelector matchLabels[%q] conflicts with required workflow selector", key)
			}
			out.MatchLabels[key] = value
		}
	}
	if len(out.MatchLabels) == 0 && out.Mode == "" {
		return nil, nil
	}
	return out, nil
}

func cloneRunnerSelector(selector *types.RunnerSelector) *types.RunnerSelector {
	if selector == nil {
		return nil
	}
	return &types.RunnerSelector{
		Mode:        selector.Mode,
		MatchLabels: cloneStringMap(selector.MatchLabels),
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

// extractMergeMode returns the merge mode from a node's parameters if it's a merge node.
func extractMergeMode(nd types.NodeDef) string {
	if nd.Type != "xflow.merge" {
		return ""
	}
	if nd.Parameters == nil {
		return ""
	}
	if mode, ok := nd.Parameters["mode"].(string); ok {
		return mode
	}
	return ""
}

// detectCycle uses Kahn's algorithm (topological sort) to detect cycles.
func detectCycle(g *Graph) error {
	n := len(g.Nodes)
	inDeg := make([]int, n)
	copy(inDeg, g.InDegree)

	queue := make([]int, 0, n)
	for i, d := range inDeg {
		if d == 0 {
			queue = append(queue, i)
		}
	}

	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, e := range g.OutEdges[cur] {
			inDeg[e.DstIdx]--
			if inDeg[e.DstIdx] == 0 {
				queue = append(queue, e.DstIdx)
			}
		}
	}

	if visited != n {
		return errors.New("workflow contains a cycle")
	}
	return nil
}
