package graph

import (
	"fmt"
	"strings"

	"github.com/xbcio/xflow/types"
)

// validateSubWorkflowParams checks the author-facing parameter shape of an
// xflow.subworkflow node. It runs on every compile, resolver or not, because
// every rule here is decidable from the node itself.
//
// The rules that need the target definition (does it exist, is the entry in its
// entry table, is the pinned hash still current, is the reference graph cyclic)
// do NOT live here: they are the resolution pass's, and a compile without a
// resolver deliberately compiles an unresolved node instead of guessing. See
// compileSubWorkflowRefs.
func validateSubWorkflowParams(nd types.NodeDef) error {
	if nd.Type != types.SubWorkflowNodeType {
		return nil
	}
	raw, present := nd.Parameters["workflow"]
	name, isString := raw.(string)
	if !present || !isString || strings.TrimSpace(name) == "" {
		return fmt.Errorf("node %q: %s requires parameters.workflow naming a workflow in the same namespace",
			nd.Name, types.SubWorkflowNodeType)
	}
	// version and entry are optional, but a present one must be a string: a
	// number or map here would reach the resolver as an untyped value and be
	// silently dropped, compiling "the version the author wrote" as "latest".
	for _, key := range []string{"version", "entry"} {
		if raw, ok := nd.Parameters[key]; ok && raw != nil {
			if _, ok := raw.(string); !ok {
				return fmt.Errorf("node %q: parameters.%s must be a string, got %T",
					nd.Name, key, raw)
			}
		}
	}
	// input and output are expression maps. A non-map (a bare template string
	// is the tempting mistake) would be traversed by the engine's evaluation
	// pass as an opaque value and land in the child's params, or in the node's
	// output, as a literal template.
	for _, key := range []string{"input", "output"} {
		if raw, ok := nd.Parameters[key]; ok && raw != nil {
			if _, ok := raw.(map[string]any); !ok {
				return fmt.Errorf("node %q: parameters.%s must be a map of expressions, got %T",
					nd.Name, key, raw)
			}
		}
	}
	return nil
}
