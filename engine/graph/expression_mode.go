package graph

import (
	"strings"

	"github.com/xbcio/xflow/types"
)

// ExpressionModeValue classifies how the platform treats an expression written
// into one parameter (or sub-field) of a node type. It is the single
// editor-facing answer to "may the author write ${{ }} here, and what happens
// if they do": a consumer reads it instead of composing IsEvaluableParam,
// IsEvaluableSubField, IsHostSourceParam and the sub-graph body rule itself.
type ExpressionModeValue string

const (
	// ExpressionModeNone: the value is never evaluated by anyone. A template
	// written here ships as literal text. Today this is a sub-graph body
	// (xflow.map's body): it is the inner execution's source, whose members'
	// parameters get their own classification.
	ExpressionModeNone ExpressionModeValue = "none"
	// ExpressionModePure: the whole value IS an expression, evaluated by the
	// handler itself (xflow.if's condition, xflow.switch's rules[].condition).
	// No outer layer renders it.
	ExpressionModePure ExpressionModeValue = "pure"
	// ExpressionModeTemplate: the value is a literal that may embed ${{ }} /
	// {{ }} templates, rendered before the handler sees it -- at the task
	// boundary for an action (execution/params.go), at activation for a
	// trigger (EvaluateActivationParams, which only offers $config and $vars).
	ExpressionModeTemplate ExpressionModeValue = "template"
	// ExpressionModeLiteral: the value is source code in a host language
	// (xflow.script's code). It is never rendered and "${{" in it is that
	// language's own syntax.
	ExpressionModeLiteral ExpressionModeValue = "literal"
)

// expressionPathSep separates the segments of an ExpressionMode path.
const expressionPathSep = "/"

// ExpressionMode classifies the parameter at path of a node of the given kind
// and type. The precedence is fixed here so no consumer re-derives it:
//
//  1. sub-graph body -> none. The body parameter of a transform node type
//     (transformNodeTypes: a declared body MUST be a sub-graph there, so the
//     type alone decides it) is the inner execution's source text; the
//     boundary skips it (execution/params.go skipSubgraphBody).
//  2. host source (IsHostSourceParam) -> literal.
//  3. handler-evaluated parameter (IsEvaluableParam) or sub-field
//     (IsEvaluableSubField) -> pure.
//  4. everything else -> template.
//
// Trigger kind has no rule of its own: a trigger's parameters are not
// boundary-evaluated (a trigger is never scheduled as a task), but they ARE
// rendered at activation by EvaluateActivationParams -- for both the cluster
// entry-activation path and the SDK trigger runtime -- against $config and
// $vars, honouring the same evaluableParams exemptions. A trigger parameter
// therefore classifies exactly like an action parameter (every builtin trigger
// has an empty evaluableParams entry, so all of them are template). kind is
// accepted so the classification can depend on it without an API change, and
// so a caller can tell from it that a trigger template's roots are limited to
// $config and $vars.
//
// path is relative to the node's parameters, "/"-separated, with no leading
// slash and no array index segments:
//
//	"condition"        top-level parameter
//	"rules/condition"  sub-field of every element of an array-of-objects
//	                   parameter -- the shape evaluableSubFields is keyed by
//
// The classification of a path is that of its longest classified prefix: any
// path under a none/literal/pure parameter inherits its mode ("expressions/x"
// of xflow.transform.set is pure), "rules/condition/x" inherits pure from
// "rules/condition", and anything else is template. An empty path is template.
func ExpressionMode(kind types.NodeKind, nodeType, path string) ExpressionModeValue {
	param, rest, _ := strings.Cut(path, expressionPathSep)
	if param == "" {
		return ExpressionModeTemplate
	}
	if param == subgraphBodyKey && transformNodeTypes[nodeType] {
		return ExpressionModeNone
	}
	if IsHostSourceParam(nodeType, param) {
		return ExpressionModeLiteral
	}
	if IsEvaluableParam(nodeType, param) {
		return ExpressionModePure
	}
	if rest != "" {
		sub, _, _ := strings.Cut(rest, expressionPathSep)
		if IsEvaluableSubField(nodeType, param, sub) {
			return ExpressionModePure
		}
	}
	return ExpressionModeTemplate
}
