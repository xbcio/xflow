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
//  1. appear in graph.EvaluableParams() for this nodeType (the handler
//     evaluates them itself — code, condition, expression, items, etc.)
//  2. appear in perItemExemptParams (per-item env roots don't exist here)
//  3. appear as sub-field paths in graph.EvaluableSubFields() — the boundary
//     evaluates the parameter's OTHER fields but preserves these sub-fields
//     verbatim for the handler (e.g. xflow.switch rules[].condition)
//  4. hold a sub-graph body — the inner execution's source text, which must
//     reach the inner compile verbatim (see skipSubgraphBody)
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

	// (d) A sub-graph body is the INNER execution's source text. It must reach
	// the inner compile verbatim, so the whole sub-tree is exempt. See
	// skipSubgraphBody for why this is keyed on the value's shape rather than on
	// the node's type or the parameter's name.
	skipBody := skipSubgraphBody(input.Params)

	// Build the expression environment once for all parameters.
	env := exprx.BuildExprEnv(input, nil)

	for param, value := range input.Params {
		if exempt[param] {
			continue
		}
		if skipBody && param == subgraphBodyParam {
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

// subgraphBodyParam is the parameter name a sub-graph body lives under. It is
// only ever consulted together with skipSubgraphBody -- the name alone means
// nothing (xflow.http's "body" is a request payload).
const subgraphBodyParam = "body"

// skipSubgraphBody reports whether this node's "body" parameter holds a
// sub-graph body, which the boundary must leave completely untouched.
//
// A body is the INNER execution's source text, not a value for this node. Its
// members' parameters are evaluated later, by the inner execution, against the
// per-item environment the map adapter injects (execution/subgraph/map_body.go's
// bodyItemInput supplies $item/$index/$items) and against the inner graph's own
// $nodes state (engine/graph/nodes_refs.go's deriveNodesRefs skips the body for
// exactly this reason). None of those roots exist in the OUTER node's env.
//
// Evaluating a body here breaks in both directions:
//
//   - A body member using its promised roots fails the outer node with "unknown
//     name $item". Because a boundary failure is a system error and the engine
//     classifies those as retriable transient failures, the outer xflow.map task
//     retries forever: the execution parks at status=running and the map handler
//     is never called once. Measured on backend/providers/local.
//   - A body member template that HAPPENS to resolve in the outer env is
//     silently rewritten, handing an inner node a value from a scope it never
//     declared a dependency on.
//
// The question is delegated to graph.DeclaresSubgraphBody so there is exactly
// one definition of "is this a sub-graph body" in the tree. It keys on the
// VALUE's shape, never on the parameter name or the node type -- xflow.http's
// "body" is a request payload whose templates MUST still be evaluated, which is
// the shape this whole layer exists to support.
//
// Malformed templates inside a body are not lost by skipping: they are already a
// compile error, because engine/graph's validateTemplateForm walks the entire
// parameter tree including the body.
func skipSubgraphBody(params map[string]any) bool {
	return graph.DeclaresSubgraphBody(params)
}

// buildExemptSet returns the set of parameter names that should NOT be
// evaluated at the boundary for the given node type.
//
// Note this is keyed on node type alone, so it cannot express the sub-graph
// body exemption, which depends on the parameter VALUE -- see skipSubgraphBody.
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
		// Not the expected array shape. Nothing rejects this earlier -- measured:
		// graph.Compile accepts xflow.switch with "rules" given as a single map
		// rather than an array. So this branch is reachable, and the sub-field
		// exemption cannot be honored on a shape that has no elements to look
		// inside: full evaluation here also evaluates what would have been the
		// exempt key (measured: a map-shaped rules has its "condition"
		// evaluated, which the array shape would have preserved verbatim).
		//
		// That is acceptable only because the shape is already broken for the
		// handler -- xflow.switch's executeRules does
		// `input.Params["rules"].([]any)` with the comma-ok discarded, so a map
		// yields nil, no rule is ever considered, and the node takes its default
		// output no matter what the boundary did to the value. Do not extend
		// this branch to a shape a handler can actually consume without giving
		// it real sub-field handling.
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
