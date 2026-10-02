package graph

import (
	"fmt"
	"sort"
	"strings"

	"github.com/xbcio/xflow/types"
)

// The declared-input contract.
//
// A node MAY declare its input ports:
//
//	inputs:
//	  - name: inventory
//	  - name: price
//	    required: true
//
// When it does, the declaration is authoritative: every incoming data edge must
// target one of the declared ports, and a port marked required must be
// connected. When it declares none, the node keeps the implicit main port and
// accepts an edge under any label -- the shape every graph authored before this
// contract existed relies on -- and a fan-in over several edges is reported as
// a warning rather than rejected, because those upstreams all share one key and
// only one of their payloads can be read.

// nodeInputPorts is the declared-port view of one node.
type nodeInputPorts struct {
	declared map[string]bool
	required []string
}

// declaredInputPorts returns the ports a node declares, or nil when it declares
// none (which is what makes a node unconstrained).
func declaredInputPorts(node *types.NodeDef) *nodeInputPorts {
	if node == nil || len(node.Inputs) == 0 {
		return nil
	}
	p := &nodeInputPorts{declared: make(map[string]bool, len(node.Inputs))}
	for _, d := range node.Inputs {
		// A declaration that names no port is the implicit one, matching how a
		// connection that names no port targets it.
		name := d.Name
		if name == "" {
			name = types.DefaultInputPort
		}
		if p.declared[name] {
			continue
		}
		p.declared[name] = true
		if d.Required {
			p.required = append(p.required, name)
		}
	}
	return p
}

// targetPort is the port an edge delivers to: the connection's `input:` label,
// or the implicit main port when it carries none.
func targetPort(label string) string {
	if label == "" {
		return types.DefaultInputPort
	}
	return label
}

func (p *nodeInputPorts) names() string {
	out := make([]string, 0, len(p.declared))
	for name := range p.declared {
		out = append(out, name)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// validateEdgeTarget rejects an edge that targets a port its destination does
// not declare. A node that declares nothing accepts any label -- only a
// declaration constrains.
func (p *nodeInputPorts) validateEdgeTarget(srcName, dstName, label string) error {
	if p == nil {
		return nil
	}
	port := targetPort(label)
	if p.declared[port] {
		return nil
	}
	return fmt.Errorf("connection %s -> %s targets input port %q, which node %q does not declare; node %q declares: %s",
		srcName, dstName, port, dstName, dstName, p.names())
}

// validateInputPorts is the node-level half of the contract: a required port
// must be connected, and a fan-in over a node that declares no ports is warned
// about.
//
// It runs on the authored graph (Compile) and deliberately not on
// compileTrusted's projected group/body packages: projection drops the boundary
// edges, so a member whose only edge arrived from outside its package would
// look unconnected and be rejected over a port that IS connected in the graph
// the author wrote.
func validateInputPorts(def *types.WorkflowDef, g *Graph) error {
	for i := range def.Nodes {
		node := &def.Nodes[i]
		idx, ok := g.index[node.Name]
		if !ok {
			continue
		}
		in := g.inEdges[idx]

		ports := declaredInputPorts(node)
		if ports == nil {
			if len(in) > 1 {
				g.addWarning(fmt.Sprintf(
					"node %q: %d 条入边且未声明 inputs，上游数据将全部落在隐含的 %q 端口上，只有最后一个非空上游可读；"+
						"请声明 inputs: 并给连线指定 input:（若这些入边互斥——同一 if/switch 的不同分支——可忽略本警告）",
					node.Name, len(in), types.DefaultInputPort))
			}
			continue
		}

		connected := make(map[string]bool, len(in))
		for _, e := range in {
			connected[targetPort(e.DstPort)] = true
		}
		for _, req := range ports.required {
			if !connected[req] {
				return fmt.Errorf("node %q declares input port %q as required but no connection targets it",
					node.Name, req)
			}
		}
	}
	return nil
}
