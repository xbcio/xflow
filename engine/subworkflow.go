package engine

import (
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
)

// ErrSubWorkflowUnsupported is returned when a workflow contains an
// xflow.subworkflow node but the wired engine or StateStore cannot run one:
// the engine was built without WithSubWorkflowResolver, or the StateStore does
// not implement SubWorkflowLinkStore.
//
// It fails closed by design, in the same shape as ErrConcurrencyUnsupported:
// running the node as an ordinary no-op would report success for work that
// never happened, and running it without a resolver would leave the child
// execution unresolvable. The checks are live type/field tests at submission
// time — not registration-time flags — so a deployment that gains the resolver
// or a backend that gains the capability stops failing without a re-register.
var ErrSubWorkflowUnsupported = errors.New("engine does not support sub-workflow nodes")

// ErrSubWorkflowUnresolved is returned for a graph whose xflow.subworkflow
// nodes were compiled without a resolver, so no target revision was pinned onto
// them.
//
// An unresolved node cannot be dispatched: the pin (version + definition hash)
// is what keeps the child from silently following the target's latest release,
// and it is recorded only by the compile-time resolution pass. Compiling
// without a resolver is therefore a legitimate intermediate state — a definition
// loaded before the service layer injects one — but it is not runnable, and the
// refusal names the un-resolved reference instead of failing at the first
// dispatch of the node.
var ErrSubWorkflowUnresolved = errors.New("sub-workflow reference was not resolved at compile time")

// admitSubWorkflows refuses a submission whose graph contains xflow.subworkflow
// nodes that this engine cannot run end to end. It returns nil for a graph
// without any, which is every graph that existed before this feature.
//
// It runs before anything is persisted, so a refusal leaves no execution, no
// graph cache entry and no task: the submitter gets the refusal itself rather
// than an execution that dies on its first sub-workflow node. The rules are
// deliberately structural (is the capability wired, is the reference pinned) —
// everything about the target workflow was already checked at registration by
// the compile-time resolution pass, and re-checking it here would put a registry
// read on the submission path.
func (e *Engine) admitSubWorkflows(g *graph.Graph) error {
	if g == nil {
		return nil
	}
	indexes := g.SubWorkflowNodeIndexes()
	if len(indexes) == 0 {
		return nil
	}
	if e.subWorkflowResolver == nil {
		return fmt.Errorf("workflow %q: %w: the engine was built without WithSubWorkflowResolver",
			g.Name(), ErrSubWorkflowUnsupported)
	}
	if _, ok := e.state.(SubWorkflowLinkStore); !ok {
		return fmt.Errorf("workflow %q: %w: the wired state store cannot park a node on a child execution",
			g.Name(), ErrSubWorkflowUnsupported)
	}
	for _, idx := range indexes {
		if g.NodeAt(idx).SubWorkflow == nil {
			return fmt.Errorf("workflow %q: node %q: %w: compile and register the workflow with a sub-workflow resolver",
				g.Name(), g.NodeName(idx), ErrSubWorkflowUnresolved)
		}
	}
	return nil
}
