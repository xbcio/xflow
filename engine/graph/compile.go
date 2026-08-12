package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xbcio/xflow/types"
)

// subgraphNodeType is the body-only node type: a NodeDef with this Type
// carries a self-contained {nodes, connections} sub-graph in its Parameters.
// It has no unit-layer semantics of its own — see the top-level rejection
// below and validateNodeBody's nesting check.
const subgraphNodeType = "xflow.subgraph"

// transformNodeTypes are the transform-style node types: the ones that compute
// a value per item and therefore take exactly one of an "expression" (inline,
// deterministic, no IO, no sub-execution) or a "body" (a sub-graph executed per
// item, with its own durable sub-executions). types.TransformSpec is the
// declared shape of that choice, and it names the family that belongs here: map
// today, filter / reduce's accumulator / sort's key as they land.
//
// This set does NOT decide what gets projected as a sub-graph — declaresSubgraphBody
// does, from the value. It scopes exactly two rules that are the transform
// contract and nothing else:
//
//   - expression and body are mutually exclusive, exactly one present;
//   - a declared body MUST be a sub-graph, so a malformed one is an error here
//     rather than an opaque parameter carried to the handler.
//
// Both are wrong for a wrapper-style body node — a retry, timeout, or try/catch
// whose body is the thing being guarded rather than a value computed per item.
// Such a node has no inline alternative to be exclusive with, so applying the
// XOR rule would reject every valid instance. It needs no entry here: its body
// is projected on the strength of its own shape.
var transformNodeTypes = map[string]bool{
	"xflow.map": true,
}

// fanOutNodeTypes are the node types that fan out — whose successful output is
// a set of batches the engine expands into sub-executions rather than a value to
// commit — WHEN they are written in their body form.
//
// This set is what makes that runtime decision compile-time knowable. The engine
// no longer sniffs an output key to decide whether to expand; it reads the
// node's projected body (graph.BodyAt). For that to be sound, a node of one of
// these types must never compile in a shape where its handler would emit a
// fan-out descriptor with no body projected — the engine would treat the
// descriptor as an ordinary value and commit {items, batches, ...} as the node's
// result, with the body never run. validateNodeBody rule 3 is what forbids that
// shape.
//
// The expression form is the other side of the same contract: it projects no
// body, so the engine commits its output verbatim — which is why its handler
// must return the finished {results, count} rather than a descriptor.
//
// It is a literal list rather than a value check because the property is about
// what the HANDLER does, which no parameter can show.
//
// xflow.split is not here. It is rejected outright in registerNodes: it fans
// out through downstream connections rather than a body, so there is nothing
// for the engine to expand and every batch fails.
var fanOutNodeTypes = map[string]bool{
	"xflow.map": true,
}

// bannedBodyMemberTypes are the node types a body sub-graph may never contain,
// named literally because each is banned for a reason a value cannot show:
//
//   - xflow.subgraph is the body container itself. It has no unit-layer
//     semantics and is already rejected at the top level.
//   - xflow.split is rejected everywhere (see registerNodes).
//   - xflow.map is a fan-out node (see fanOutNodeTypes). In its body form it
//     expands, so nesting one inside a body is exactly the recursion this ban
//     exists for. Its expression form does not expand and would be harmless,
//     but the ban stays type-level rather than form-level: a member's form is
//     one edit away from changing, and a ban that a parameter tweak can lift
//     silently is not a ban.
//
// The recursion ban that must stay extensible — a member that itself carries a
// sub-graph body, making the sub-execution tree unbounded — is NOT expressed
// here, because a type list cannot see it. validateNodeBody checks each
// member's own parameters with declaresSubgraphBody as well, so a node type
// that grows a body tomorrow is banned from nesting the day it lands, with no
// list to update.
var bannedBodyMemberTypes = map[string]bool{
	"xflow.split":    true,
	"xflow.map":      true,
	subgraphNodeType: true,
}

// declaresSubgraphBody reports whether a node's parameters carry a "body" that
// is a sub-graph — the criterion the compiler projects on, the snapshot guard
// fails closed on, and the nesting ban recurses on.
//
// The criterion is the VALUE's shape, not the node's type. A type whitelist was
// the obvious alternative and is the wrong one twice over: it makes every new
// body-bearing node type an edit to this package, and it drifts against the
// other places that must agree on the same question. That drift was real, not
// hypothetical — the snapshot guard (snapshot.go) has always keyed off the
// parameter's presence while the compiler keyed off the node's type, so an
// xflow.http node with a JSON request body compiled fine and then failed to
// decode, taking down every execution that reached LoadGraph.
//
// Sniffing the parameter NAME is what cannot work: xflow.http's "body" is a
// request payload. Sniffing the value can, because a sub-graph body is a
// NodeDef whose type is the reserved xflow.subgraph — a shape no request
// payload takes by accident, and one an author cannot write except deliberately
// (xflow.subgraph is rejected at the top level, so its only legal home is a
// body). Measured against every body value in this repo: an http object body
// decodes to an empty Type, an http string or array body fails to decode at
// all, and only a real body reaches "xflow.subgraph".
func declaresSubgraphBody(params map[string]any) bool {
	raw, ok := params["body"]
	if !ok {
		return false
	}
	bodyDef, err := decodeSubgraphBody(raw)
	if err != nil {
		return false
	}
	return bodyDef.Type == subgraphNodeType
}

// DeclaresSubgraphBody reports whether a node's parameters carry a sub-graph
// body. Exported for the same reason as EvaluableParams: a consumer outside
// this package must ask this question and there must be no second copy of the
// answer.
//
// That consumer is execution/params.go's boundary evaluation layer, which must
// NOT evaluate a body's contents. A body is the inner execution's source text,
// evaluated later against the per-item environment the map adapter injects
// ($item/$index/$items). Evaluating it here fails the OUTER node with "unknown
// name $item" -- and because a boundary failure is classified transient, the
// outer task retries forever and the execution hangs with its handler never
// called once.
func DeclaresSubgraphBody(params map[string]any) bool {
	return declaresSubgraphBody(params)
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

	// Reject xflow.subgraph at the top level, the same way reserved
	// "xflow.group_*" types are rejected above. xflow.subgraph is a body-only
	// node type: it has no unit-layer semantics, so if it appeared in the
	// top-level nodes list it would be scheduled as an in-degree-0 root unit
	// like any other node — a meaningless and unsupported shape. It is only
	// valid nested inside a transform node's "body" parameter (see
	// validateNodeBody).
	{
		var loose []string
		for _, nd := range def.Nodes {
			if nd.Type == subgraphNodeType {
				loose = append(loose, fmt.Sprintf("%s (%s)", nd.Name, nd.Type))
			}
		}
		if len(loose) > 0 {
			sort.Strings(loose)
			return nil, fmt.Errorf("node type %q is not allowed at the top level: %s; "+
				"it has no unit-layer semantics and would be scheduled as an in-degree-0 "+
				"root unit — use it only inside a transform node's body parameter",
				subgraphNodeType, strings.Join(loose, ", "))
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
	if err := validateTemplateForm(g); err != nil {
		return nil, err
	}
	depPorts, err := buildEdges(def, g)
	if err != nil {
		return nil, err
	}
	if err := buildDependencyEdges(def, depPorts, g, nil); err != nil {
		return nil, err
	}
	if err := projectNodeBodies(def, g); err != nil {
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
	// buildNodesRefs extracts $nodes references from parameters and validates
	// them. Placed after detectCycle so the DAG guarantee is already established
	// in non-cyclic mode. The pass itself carries a visited-set, so cyclic
	// graphs (allowCycles=true) are also safe.
	if err := buildNodesRefs(g, false); err != nil {
		return nil, err
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
		if nd.Type == "xflow.split" {
			return 0, fmt.Errorf("node %q: xflow.split is not implemented and cannot run: "+
				"its handler emits a fan-out shape the engine expands into batch tasks, but a batch "+
				"has no body to run (xflow.split declares no body parameter, so projectNodeBodies "+
				"never projects one), so every batch fails and the execution never completes. "+
				"Use xflow.map with a body instead", nd.Name)
		}
		if err := validateNodeBody(nd); err != nil {
			return 0, err
		}
		// Projection itself is deferred to projectNodeBodies, which runs after
		// buildDependencyEdges: a body needs the parent node's visible-supply
		// names (g.SupplyRefsFor(i)), and g.supplyRefs is not populated until that
		// later pass runs. validateNodeBody's shape checks stay here, in the first
		// pass, so a malformed body is still rejected at the same point it always
		// was -- only the projection itself moved, not the point of failure for a
		// bad shape.
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

// projectNodeBodies is the compile pass that projects every declared sub-graph
// body into a self-contained SubgraphPackage, once per node so N batches of the
// same node share one package and one hash. Which nodes have one is decided by
// declaresSubgraphBody — the value's shape, not the node's type — so this pass
// needs no edit when a new body-bearing node type lands, and it agrees by
// construction with the snapshot guard that fails closed on the same criterion.
//
// It runs AFTER buildDependencyEdges (not inside registerNodes, where the
// body's shape is merely validated) because it needs the parent node's own
// visible-supply names -- g.SupplyRefsFor(i) -- to widen the body's compile-time
// supply-usage check. g.supplyRefs does not exist yet during registerNodes;
// buildDependencyEdges is what populates it. A body member has no dependency
// edge of its own (it is never a top-level node in the outer graph def.Nodes),
// so without the parent's list threaded in, CompileProjectedPackage would
// reject any body member reading $supplies.<name> even when the parent node
// itself declares exactly that dependency.
//
// Indexing g.nodes by def.Nodes' index is sound because registerNodes appends
// one NodeMeta per def.Nodes entry in order and rejects duplicate names, so the
// two stay 1:1 — the same identity g.SupplyRefsFor(i) already relies on.
func projectNodeBodies(def *types.WorkflowDef, g *Graph) error {
	for i, nd := range def.Nodes {
		if !declaresSubgraphBody(nd.Parameters) {
			continue
		}
		body, err := ProjectNodeBodyPackage(nd.Name, nd.Parameters, g.SupplyRefsFor(i), projectedWorkflowContext(g))
		if err != nil {
			return err
		}
		g.nodes[i].Body = body
	}
	return assertFanOutNodesResolved(def, g)
}

// assertFanOutNodesResolved is projectNodeBodies' post-condition: every fan-out
// node has come out of the pass either with a projected body (it expands) or
// with an expression (it computes inline). Nothing else is runnable.
//
// This is the same rule validateNodeBody rule 3 states, checked again at a
// different place, and the duplication is the point. Rule 3 runs in
// registerNodes, which compileTrusted does not call — compileTrusted is a
// hand-written parallel of Compile's pass list and has drifted from it before,
// once omitting this very pass so that every group-member map compiled cleanly
// with a nil body. Before the expansion criterion moved to the graph, that
// drift surfaced loudly at run time as ErrNoMapBody, once per batch. It no
// longer can: a node with no body does not expand, so the same drift would now
// commit the handler's fan-out descriptor as the node's ordinary output and run
// the body zero times, silently. A post-condition on the pass itself is the
// only check both compilation paths are forced through.
func assertFanOutNodesResolved(def *types.WorkflowDef, g *Graph) error {
	for i, nd := range def.Nodes {
		if !fanOutNodeTypes[nd.Type] || g.nodes[i].Body != nil {
			continue
		}
		if expr, _ := nd.Parameters["expression"].(string); expr != "" {
			continue
		}
		return fmt.Errorf("node %q: %s compiled with neither a projected body nor an "+
			"expression, so it would neither expand nor compute; its handler's output "+
			"would be committed verbatim and its body never run", nd.Name, nd.Type)
	}
	return nil
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

// validateNodeBody enforces the compile-time rules for one node's parameters.
// It runs for EVERY node, and splits into two groups by what each rule is
// actually about:
//
// Transform-only (transformNodeTypes):
//
//  1. expression and body are mutually exclusive: exactly one must be present.
//     This is the transform contract — types.TransformSpec declares the same
//     {expression | body} shape — and it applies to every transform node, not
//     just xflow.map. It is checked here, at compile time, rather than sniffed
//     from the node's runtime output shape: this repo has already been burned
//     once by that pattern (a ScriptNode that sniffed a "messages" key and
//     built a parallel fan-out path in secret).
//
//     A wrapper-style body node — retry, timeout, try/catch, whose body is the
//     thing being guarded rather than a value computed per item — has no
//     inline alternative to be exclusive with. Applying this rule to it would
//     reject every valid instance, so it is scoped to the transform subset.
//
//  2. A declared body must BE a sub-graph. For a transform node the body is the
//     per-item computation, so a body that is not a sub-graph is a typo, not a
//     payload — and left unrejected it would compile into a node the engine
//     later fails on with ErrNoMapBody, once per batch, at run time.
//
//     This too is transform-scoped, and deliberately: a non-transform node's
//     "body" belongs to that node (xflow.http's is a request payload). The
//     compiler must not have an opinion about its shape.
//
// Fan-out-only (fanOutNodeTypes):
//
//  3. A fan-out node MUST declare exactly one of a sub-graph body or an
//     expression. See fanOutNodeTypes for why this one is load-bearing rather
//     than tidy: the engine decides "is this output a fan-out descriptor or a
//     value?" from the compiled body, so a fan-out node that compiles with
//     neither gets its descriptor committed as an ordinary output and its body
//     never runs. For a transform node this overlaps rule 1 — the difference is
//     that rule 1 lets a parameterless node through and this one does not.
//
// Every node:
//
//  4. A parameterless node (nd.Parameters is nil/empty) is left untouched by
//     the remaining rules: they only engage once the node declares parameters.
//     Rule 3 is deliberately checked BEFORE this, because a bare
//     `{Name: "m", Type: "xflow.map"}` is exactly one of the shapes it exists
//     to reject.
//  5. A node whose body IS a sub-graph (declaresSubgraphBody — the value's
//     shape, not the node's type) has that sub-graph validated: its members may
//     not be xflow.split / xflow.subgraph / xflow.map, and may not themselves
//     declare a sub-graph body. v1 forbids nesting because recursive fan-out
//     makes the sub-execution tree unbounded, and checking each member's own
//     parameters is what makes that ban hold for a node type that grows a body
//     after this code was written.
//  6. The body's members must have a unique, dominating entry — reusing
//     resolveGroupEntry/assertEntryDominates exactly as group compilation
//     does, since a body and a node group are the same structure.
//
// Nothing here is xflow.map-specific, and a new body-bearing node type needs no
// entry anywhere: rules 4-6 recognize its body by shape. Only a new TRANSFORM
// needs a line in transformNodeTypes (rules 1-2), and only a new FAN-OUT needs
// one in fanOutNodeTypes (rule 3).
func validateNodeBody(nd types.NodeDef) error {
	if fanOutNodeTypes[nd.Type] && !declaresSubgraphBody(nd.Parameters) {
		if expr, _ := nd.Parameters["expression"].(string); expr == "" {
			return fmt.Errorf("node %q: %s requires exactly one of a body sub-graph "+
				`(parameters.body with type %q) or parameters.expression`,
				nd.Name, nd.Type, subgraphNodeType)
		}
	}
	if len(nd.Parameters) == 0 {
		return nil
	}
	bodyRaw, hasBody := nd.Parameters["body"]
	if transformNodeTypes[nd.Type] {
		_, hasExpr := nd.Parameters["expression"]
		switch {
		case hasExpr && hasBody:
			return fmt.Errorf("node %q: expression and body are mutually exclusive", nd.Name)
		case !hasExpr && !hasBody:
			return fmt.Errorf("node %q: requires exactly one of expression or body", nd.Name)
		}
		if hasBody && !declaresSubgraphBody(nd.Parameters) {
			// Name the type we found where possible: a body that decodes but has
			// the wrong type is the common typo, and the value is the author's own.
			got := ""
			if bodyDef, err := decodeSubgraphBody(bodyRaw); err == nil {
				got = bodyDef.Type
			}
			return fmt.Errorf("node %q: body.type must be %q, got %q", nd.Name, subgraphNodeType, got)
		}
	}
	if !declaresSubgraphBody(nd.Parameters) {
		return nil
	}

	bodyDef, err := decodeSubgraphBody(bodyRaw)
	if err != nil {
		return fmt.Errorf("node %q: body: %w", nd.Name, err)
	}
	innerNodes, innerConns, err := decodeSubgraphMembers(bodyDef.Parameters)
	if err != nil {
		return fmt.Errorf("node %q: body: %w", nd.Name, err)
	}
	if len(innerNodes) == 0 {
		return fmt.Errorf("node %q: body has no nodes", nd.Name)
	}
	for _, inner := range innerNodes {
		if bannedBodyMemberTypes[inner.Type] {
			return fmt.Errorf("node %q: body member %q has type %q, which is not allowed "+
				"inside a body in v1 (nesting is rejected: recursive fan-out makes the "+
				"sub-execution tree unbounded)", nd.Name, inner.Name, inner.Type)
		}
		if declaresSubgraphBody(inner.Parameters) {
			return fmt.Errorf("node %q: body member %q declares a sub-graph body of its "+
				"own, which is not allowed inside a body in v1 (nesting is rejected: "+
				"recursive fan-out makes the sub-execution tree unbounded)", nd.Name, inner.Name)
		}
	}
	return validateBodyEntry(nd.Name, innerNodes, innerConns)
}

// decodeSubgraphBody decodes a map node's "body" parameter value (an
// opaque map[string]any as authored in JSON/YAML params) into a NodeDef.
func decodeSubgraphBody(raw any) (*types.NodeDef, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	var nd types.NodeDef
	if err := json.Unmarshal(data, &nd); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &nd, nil
}

// decodeSubgraphMembers decodes an xflow.subgraph node's own Parameters
// (its {nodes, connections} payload) into the shapes the rest of the
// compiler already understands. Connections goes through
// types.Connections' own UnmarshalJSON so the array-shorthand/object-form
// duality (types/connections.go) is handled identically to a top-level
// workflow definition.
func decodeSubgraphMembers(params map[string]any) ([]types.NodeDef, types.Connections, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, nil, fmt.Errorf("encode parameters: %w", err)
	}
	var shape struct {
		Nodes       []types.NodeDef   `json:"nodes"`
		Connections types.Connections `json:"connections,omitempty"`
	}
	if err := json.Unmarshal(data, &shape); err != nil {
		return nil, nil, fmt.Errorf("decode parameters: %w", err)
	}
	return shape.Nodes, shape.Connections, nil
}

// validateBodyEntry checks that a body's members have a unique, dominating
// entry. It builds the same minimal two-pass graph (registerNodes +
// buildEdges) that Compile itself builds, treating every body member as
// part of one implicit group, then reuses resolveGroupEntry and
// assertEntryDominates unchanged — a body and a node group are the same
// structure (group_compile.go:99-159), so no new validator is written here.
func validateBodyEntry(mapNodeName string, nodes []types.NodeDef, conns types.Connections) error {
	_, _, err := compileBodyMembers(mapNodeName, nodes, conns)
	return err
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
