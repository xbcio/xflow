package graph

import (
	"errors"
	"fmt"

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

	g := &Graph{
		Name:     def.Name,
		Nodes:    make([]NodeMeta, n),
		Index:    make(map[string]int, n),
		OutEdges: make([][]Edge, n),
		InEdges:  make([][]Edge, n),
		InDegree: make([]int, n),
	}

	if def.Context != nil {
		g.Vars = def.Context.Vars
		g.Config = def.Context.Config
	}

	// First pass: register all nodes.
	for i, nd := range def.Nodes {
		if _, dup := g.Index[nd.Name]; dup {
			return nil, fmt.Errorf("duplicate node name: %s", nd.Name)
		}
		g.Index[nd.Name] = i
		g.Nodes[i] = NodeMeta{
			Name:       nd.Name,
			Type:       nd.Type,
			OnError:    nd.OnError,
			Parameters: nd.Parameters,
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

	if err := detectCycle(g); err != nil {
		return nil, err
	}

	return g, nil
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
