package execution

import (
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
)

// perItemExemptParams lists parameters that use per-item environment roots
// (item, index, $item, $index, $items) which only exist inside a handler's
// iteration loop — NOT at boundary time. Evaluating them here would crash with
// "unknown name item" or cause double evaluation when the handler re-evaluates
// the same expression with its own item-scoped env.
//
// This is separate from the evaluableParams-derived exemption because the
// REASON is different: evaluableParams says "the handler evaluates it";
// perItemExemptParams says "the boundary CANNOT evaluate it even if it wanted
// to, because the environment roots don't exist yet". The distinction matters
// for future maintainers: xflow.if's condition IS evaluable at the boundary
// (it uses $params/$input which exist), whereas xflow.transform.filter's
// condition is NOT (it uses bare "item"/"index" injected per-iteration).
//
// DO NOT evaluate these at the boundary AND let the handler evaluate them --
// that causes double evaluation. For example, xflow.map's "items" might
// resolve to a real array at the boundary, then the handler's evalItemsInline
// (map.go:139) calls cast.ToString on the array (expecting a string expression)
// and feeds the result to expr, producing an incomprehensible error.
var perItemExemptParams = map[string]map[string]bool{
	"xflow.transform.filter": {"condition": true},
	"xflow.map":              {"expression": true},
}

// evaluateParams renders all ${{ }} and {{ }} templates in lease.Input.Params
// for parameters that are NOT exempt. Exempt parameters are those that:
//   1. appear in graph.EvaluableParams() for this nodeType (the handler
//      evaluates them itself — code, condition, expression, items, etc.)
//   2. appear in perItemExemptParams (per-item env roots don't exist here)
//
// The exemption set is derived from graph.EvaluableParams() — the same data
// the compiler uses to reject unreachable templates. There is deliberately NO
// second copy of that table here; two tables would inevitably drift, and drift
// is silent: a parameter missing from both is neither compile-rejected nor
// runtime-evaluated, so the template ships verbatim with zero diagnostics.
//
// On error the function returns a wrapped error naming the node and parameter
// (for diagnostics) but NEVER the parameter value or evaluation result — the
// value may be an expanded credential (§7 absolute blacklist).
func evaluateParams(input *types.Input, nodeType string) error {
	if input == nil || len(input.Params) == 0 {
		return nil
	}

	// Build the exemption set for this node type by merging:
	// (a) evaluableParams from the compiler table (handler evaluates these)
	// (b) perItemExemptParams (boundary cannot evaluate these)
	exempt := buildExemptSet(nodeType)

	// Build the expression environment once for all parameters.
	env := exprx.BuildExprEnv(input, nil)

	for param, value := range input.Params {
		if exempt[param] {
			continue
		}
		evaluated, err := evaluateParamValue(value, env)
		if err != nil {
			// Error message: node name, node type, parameter name, expression
			// source (authored config). NEVER the value or the result.
			return fmt.Errorf("node %q (%s): template evaluation failed for parameter %q: %w",
				input.NodeName, nodeType, param, err)
		}
		input.Params[param] = evaluated
	}
	return nil
}

// buildExemptSet returns the set of parameter names that should NOT be
// evaluated at the boundary for the given node type.
func buildExemptSet(nodeType string) map[string]bool {
	evaluable := graph.EvaluableParams()
	result := make(map[string]bool)

	// (a) All parameters the handler evaluates itself.
	if handlerEvals, ok := evaluable[nodeType]; ok {
		for param := range handlerEvals {
			result[param] = true
		}
	}

	// (b) Per-item parameters whose env roots don't exist at boundary time.
	if perItem, ok := perItemExemptParams[nodeType]; ok {
		for param := range perItem {
			result[param] = true
		}
	}

	return result
}

// evaluateParamValue recursively renders templates in a parameter value tree.
// String leaves are passed through RenderTemplate; maps and slices are
// traversed so nested templates (e.g. headers map values) are also evaluated.
func evaluateParamValue(value any, env map[string]any) (any, error) {
	switch v := value.(type) {
	case string:
		return exprx.RenderTemplate(v, env)
	case map[string]any:
		result := make(map[string]any, len(v))
		for k, child := range v {
			evaluated, err := evaluateParamValue(child, env)
			if err != nil {
				return nil, err
			}
			result[k] = evaluated
		}
		return result, nil
	case []any:
		result := make([]any, len(v))
		for i, child := range v {
			evaluated, err := evaluateParamValue(child, env)
			if err != nil {
				return nil, err
			}
			result[i] = evaluated
		}
		return result, nil
	default:
		// Non-string, non-collection leaves (numbers, bools, nil) pass through.
		return value, nil
	}
}
