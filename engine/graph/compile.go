package graph

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xbcio/xflow/types"
)

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
		name:            def.Name,
		workflowVersion: def.Version,
		compilerVersion: compilerVersion,
		nodes:           make([]NodeMeta, n),
		index:           make(map[string]int, n),
		entryIndexes:    make(map[string]int),
		outEdges:        make([][]Edge, n),
		inEdges:         make([][]Edge, n),
		inDegree:        make([]int, n),
		startIdx:        -1,
	}
	if def.Options != nil {
		g.allowCycles = def.Options.AllowCycles
		g.maxAutoDepth = def.Options.MaxAutoDepth
	}
	if g.allowCycles && g.maxAutoDepth <= 0 {
		g.maxAutoDepth = DefaultMaxAutoDepth
	}

	// Reject reserved "xflow.group_*" types in user-authored workflows. These
	// types are only valid inside projected packages compiled via
	// CompileProjectedPackage (the trusted path).
	{
		var reserved []string
		for _, nd := range def.Nodes {
			if strings.HasPrefix(nd.Type, ReservedNodeTypePrefix) {
				reserved = append(reserved, fmt.Sprintf("%s (%s)", nd.Name, nd.Type))
			}
		}
		if len(reserved) > 0 {
			sort.Strings(reserved)
			return nil, fmt.Errorf("reserved node type %q is not allowed in user workflows: %s",
				ReservedNodeTypePrefix+"*", strings.Join(reserved, ", "))
		}
	}

	if def.Context != nil {
		g.vars = cloneStringAnyMap(def.Context.Vars)
		g.config = cloneStringAnyMap(def.Context.Config)
	}

	startCount, err := registerNodes(def, g)
	if err != nil {
		return nil, err
	}
	if err := validateGraphValueDomain(g); err != nil {
		return nil, err
	}
	depPorts, err := buildEdges(def, g)
	if err != nil {
		return nil, err
	}
	if err := buildDependencyEdges(def, depPorts, g, nil); err != nil {
		return nil, err
	}
	if err := compileGroups(g, def); err != nil {
		return nil, err
	}

	if g.allowCycles {
		if startCount != 1 {
			return nil, fmt.Errorf("cyclic workflow requires exactly one xflow.start node, got %d", startCount)
		}
	} else {
		if err := detectCycle(g); err != nil {
			return nil, err
		}
	}
	if err := buildUnits(g); err != nil {
		return nil, fmt.Errorf("build units: %w", err)
	}
	if err := assignPackageHashes(g); err != nil {
		return nil, fmt.Errorf("package hashes: %w", err)
	}
	if err := assignGraphHash(g); err != nil {
		return nil, fmt.Errorf("hash graph: %w", err)
	}

	return g, nil
}

// registerNodes performs the first compile pass: it populates g.index/g.nodes,
// records entry/start nodes, and returns the count of xflow.start nodes for
// cyclic-workflow validation.
func registerNodes(def *types.WorkflowDef, g *Graph) (int, error) {
	startCount := 0
	for i, nd := range def.Nodes {
		if _, dup := g.index[nd.Name]; dup {
			return 0, fmt.Errorf("duplicate node name: %s", nd.Name)
		}
		runnerSelector, err := resolveRunnerSelector(def.RunnerSelector, nd.RunnerSelector)
		if err != nil {
			return 0, fmt.Errorf("node %q: %w", nd.Name, err)
		}
		g.index[nd.Name] = i
		g.nodes[i] = NodeMeta{
			Name:           nd.Name,
			Type:           nd.Type,
			Kind:           nd.Kind,
			Version:        nd.Version,
			OnError:        nd.OnError,
			RunnerSelector: runnerSelector,
			MergeMode:      extractMergeMode(nd),
			Parameters:     cloneStringAnyMap(nd.Parameters),
			Retry:          resolveRetry(nd.Retry, def.Settings),
			GroupIdx:       -1,
		}
		if nd.Type == "xflow.start" || nd.Kind == types.NodeKindTrigger {
			g.entryIndexes[nd.Name] = i
		}
		if g.allowCycles {
			if nd.Type == "xflow.start" {
				startCount++
				g.startIdx = i
			}
			if nd.Type == "xflow.merge" && extractMergeMode(nd) == "wait_all" {
				return 0, errors.New("xflow.merge wait_all is not supported in cyclic workflows")
			}
		}
	}
	return startCount, nil
}

// buildEdges performs the second compile pass: it materializes Connections into
// g.outEdges/g.inEdges/g.inDegree and records the distinct output port names
// per source node on g.nodes[i].PortOuts.
//
// Dependency-typed ports (PortConnections.Type == ConnectionTypeDependency) are
// routed away from the dataflow topology entirely: they never enter outEdges,
// inEdges, inDegree, or PortOuts. Mixing them in would corrupt detectCycle's
// topological sort and buildUnits' in-degree accounting, since a dependency
// edge carries no data and its source (a supply node) is deliberately excluded
// from the unit layer (nodeUnit stays -1; see unit.go). They are instead
// collected into the returned []dependencyPort for buildDependencyEdges to
// consume.
//
// A supply node may never be the source or destination of a data edge: this
// is what used to be checked by inspecting outEdges/inEdges for supply nodes
// after compilation, but that check silently stops catching anything once
// dependency routing (this function) empties those sets for supply nodes.
// The check is enforced here instead, against the declared type while walking
// def.Connections, before the dataflow structures are populated.
func buildEdges(def *types.WorkflowDef, g *Graph) ([]dependencyPort, error) {
	sources := make([]string, 0, len(def.Connections))
	for srcName := range def.Connections {
		sources = append(sources, srcName)
	}
	sort.Strings(sources)

	var depPorts []dependencyPort

	for _, srcName := range sources {
		ports := def.Connections[srcName]
		srcIdx, ok := g.index[srcName]
		if !ok {
			return nil, fmt.Errorf("connection references unknown source node: %s", srcName)
		}
		portNames := make([]string, 0, len(ports))
		for port := range ports {
			portNames = append(portNames, port)
		}
		sort.Strings(portNames)

		portOuts := make([]string, 0, len(portNames))
		for _, port := range portNames {
			pc := ports[port]

			if pc.Type == types.ConnectionTypeDependency {
				// Declared type and node Kind cross-validate each other:
				// neither side wins silently over the other.
				if g.nodes[srcIdx].Kind != types.NodeKindSupply {
					return nil, fmt.Errorf("node %q port %q declares type dependency but the node is not a supply node",
						srcName, port)
				}
				for _, c := range pc.Targets {
					if c.Input != "" {
						return nil, fmt.Errorf("dependency edge %s -> %s must not declare an input: "+
							"the consumer reads $supplies.%s and has no matching input port",
							srcName, c.Node, srcName)
					}
				}
				depPorts = append(depPorts, dependencyPort{srcName: srcName, targets: pc.Targets})
				continue
			}

			// Data edge (Type == ConnectionTypeData, or unset which decodes/
			// defaults to data): neither endpoint may be a supply node.
			if g.nodes[srcIdx].Kind == types.NodeKindSupply {
				return nil, fmt.Errorf("%w: supply node %q emits a data edge from port %q",
					ErrSupplyInDataflow, srcName, port)
			}

			conns := pc.Targets
			for _, c := range conns {
				dstIdx, ok := g.index[c.Node]
				if !ok {
					return nil, fmt.Errorf("connection references unknown destination node: %s", c.Node)
				}
				if g.nodes[dstIdx].Kind == types.NodeKindSupply {
					return nil, fmt.Errorf("%w: supply node %q is the destination of a data edge from %s:%s",
						ErrSupplyInDataflow, c.Node, srcName, port)
				}
				edge := Edge{
					SrcIdx:  srcIdx,
					DstIdx:  dstIdx,
					SrcPort: port,
					DstPort: c.Input,
				}
				g.outEdges[srcIdx] = append(g.outEdges[srcIdx], edge)
				g.inEdges[dstIdx] = append(g.inEdges[dstIdx], edge)
				g.inDegree[dstIdx]++
			}
			if len(conns) > 0 {
				portOuts = append(portOuts, port)
			}
		}
		g.nodes[srcIdx].PortOuts = portOuts
	}
	return depPorts, nil
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
	n := len(g.nodes)
	inDeg := make([]int, n)
	copy(inDeg, g.inDegree)

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
		for _, e := range g.outEdges[cur] {
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
