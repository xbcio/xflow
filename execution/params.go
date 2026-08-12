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
//   3. appear as sub-field paths in graph.EvaluableSubFields() — the boundary
//      evaluates the parameter's OTHER fields but preserves these sub-fields
//      verbatim for the handler (e.g. xflow.switch rules[].condition)
//
// The exemption set is derived from graph.EvaluableParams() and
// graph.EvaluableSubFields() — the same data the compiler uses. There is
// deliberately NO second copy of either table here; two tables would inevitably
// drift, and drift is silent: a parameter missing from both is neither
// compile-rejected nor runtime-evaluated, so the template ships verbatim with
// zero diagnostics.
//
// In-place mutation safety: evaluateParams mutates input.Params[param] in place.
// This is safe because each call to runner.Execute receives a freshly built
// TaskLease from engine.BuildTaskLease (which calls buildInput, constructing a
// new types.Input from stored state). The same lease is never passed to Execute
// twice. Within a single Execute call, evaluateParams runs exactly once (before
// the SuspendingHandler branch); executeSuspending's OnResume/PrepareSuspend
// consume the same already-evaluated lease.Input without re-entering Execute.
// Retries produce a new TaskLease via a new BuildTaskLease call.
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

	// (c) Sub-field exemptions: parameters where only SOME sub-fields are
	// handler-evaluated. These parameters are NOT in the exempt set (the
	// boundary must still evaluate their non-exempt sub-fields), but need
	// special traversal that skips the named paths.
	subFieldExempt := graph.EvaluableSubFields()[nodeType]

	// Build the expression environment once for all parameters.
	env := exprx.BuildExprEnv(input, nil)

	for param, value := range input.Params {
		if exempt[param] {
			continue
		}
		var (
			evaluated any
			err       error
		)
		if exemptKeys, hasSubFields := subFieldExempt[param]; hasSubFields {
			// This parameter has sub-field exemptions: traverse the array
			// elements, skipping the named keys in each element map.
			evaluated, err = evaluateParamWithSubFieldExemptions(value, env, exemptKeys)
		} else {
			evaluated, err = evaluateParamValue(value, env)
		}
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

// evaluateParamWithSubFieldExemptions handles parameters where only specific
// sub-fields within each array element are handler-evaluated (e.g.
// xflow.switch's "rules" parameter: rules[].condition is evaluated by the
// handler, but rules[].output is a literal port name). The parameter is
// expected to be []any of map[string]any; exempt keys within each element map
// are preserved verbatim while other keys are recursively evaluated.
func evaluateParamWithSubFieldExemptions(value any, env map[string]any, exemptKeys []string) (any, error) {
	elems, ok := value.([]any)
	if !ok {
		// Not the expected array shape — fall back to full evaluation.
		// The compile-time validator (checkSubFields) already rejects templates
		// in unexpected shapes, so this path is a no-op in practice.
		return evaluateParamValue(value, env)
	}

	exempt := make(map[string]bool, len(exemptKeys))
	for _, k := range exemptKeys {
		exempt[k] = true
	}

	result := make([]any, len(elems))
	for i, elem := range elems {
		m, ok := elem.(map[string]any)
		if !ok {
			// Non-map element — evaluate normally.
			evaluated, err := evaluateParamValue(elem, env)
			if err != nil {
				return nil, err
			}
			result[i] = evaluated
			continue
		}
		newMap := make(map[string]any, len(m))
		for k, child := range m {
			if exempt[k] {
				// Preserve verbatim — the handler evaluates this sub-field.
				newMap[k] = child
			} else {
				evaluated, err := evaluateParamValue(child, env)
				if err != nil {
					return nil, err
				}
				newMap[k] = evaluated
			}
		}
		result[i] = newMap
	}
	return result, nil
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
