package graph

import (
	"fmt"
	"strings"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
)

// A trigger node's parameters never reach execution/params.go's boundary
// evaluation layer, because a trigger is never executed as a task: it is an
// entry index, not a node the engine schedules. The control plane copies its
// parameters onto the EntryActivation, the reconciler ships them in the
// directive, and the runner hands them straight to handler.Activate. Without
// this layer a template in a trigger parameter reaches the Kafka consumer as
// the literal string "${{ $config.topic }}" and it subscribes to a topic by
// that name -- a silent wrong answer, not a failure.
//
// Evaluation happens here, in the compiler package, rather than in the control
// plane, because the group path forces it: a group's parameters travel inside
// the projected package, and the package hash is computed at compile time by
// assignPackageHashes and RE-computed at directive-build time by the
// reconciler's projectPackageForGroup, which refuses to ship the package when
// the two disagree. Evaluating in only one of the two callers would make every
// group activation fail closed on hash drift.

// activationEnvRoots is the complete set of expression roots a trigger's
// parameters may reference. Activation happens before any execution exists:
// there is no $input, no $nodes, no $execution, and no per-item scope. Only the
// two workflow-level roots -- which travel with the definition version -- have
// a meaning.
//
// $supplies is deliberately absent even though exprx.BuildExprEnv would supply
// it: supply content is fetched by the runner's gate AFTER the directive is
// built, so a value rendered here would be whatever the control-plane process
// happened to hold, frozen into the activation params and never refreshed.
// A trigger that must react to supply content consumes it at execution time.
var activationEnvRoots = map[string]bool{
	"$config": true,
	"$vars":   true,
}

// EvaluateActivationParams renders the ${{ }} and {{ }} templates in a trigger
// node's parameters against the workflow-level $config and $vars, returning a
// NEW map. It never mutates params.
//
// Not mutating is load-bearing, not hygiene: the compiled *Graph is shared
// process-wide (workflowreg hands the same *Graph to every reader) and is the
// source both the package projection and the node-exec lease read from.
// Evaluating in place would burn the rendered value into it after the first
// derivation, so a later $config change could never re-render.
//
// Parameters the handler evaluates itself are exempt, read from the same
// evaluableParams table execution/params.go negates. There is deliberately no
// second copy: a parameter missing from both layers is neither compile-rejected
// nor evaluated anywhere, and the template ships verbatim with no diagnostics.
//
// On error the returned message carries the node name, node type, parameter
// name, and the expression SOURCE (authored configuration). It never carries a
// parameter VALUE or an evaluation result -- a trigger's parameters routinely
// hold broker addresses and, through $config, credentials.
func EvaluateActivationParams(g *Graph, nodeName, nodeType string, params map[string]any) (map[string]any, error) {
	if len(params) == 0 {
		return params, nil
	}
	var env map[string]any
	if g != nil {
		env = map[string]any{"$config": g.Config(), "$vars": g.Vars()}
	} else {
		env = map[string]any{"$config": map[string]any(nil), "$vars": map[string]any(nil)}
	}

	exempt := evaluableParams[nodeType]
	out := make(map[string]any, len(params))
	for name, value := range params {
		if exempt[name] {
			out[name] = value
			continue
		}
		evaluated, err := evaluateActivationValue(value, env)
		if err != nil {
			return nil, fmt.Errorf("node %q (%s): activation-time template evaluation failed for parameter %q: %w",
				nodeName, nodeType, name, err)
		}
		out[name] = evaluated
	}
	return out, nil
}

// evaluateActivationValue mirrors execution/params.go's evaluateParamValue:
// string leaves render, maps and slices recurse so a nested template (a broker
// list entry, a headers map value) is also rendered, and every other leaf
// passes through untouched.
func evaluateActivationValue(value any, env map[string]any) (any, error) {
	switch v := value.(type) {
	case string:
		if err := checkActivationRoots(v); err != nil {
			return nil, err
		}
		rendered, err := exprx.RenderTemplate(v, env)
		if err != nil {
			return nil, err
		}
		if strings.Contains(v, "{{") {
			if err := checkRenderedForDeferredMarkers(rendered, v); err != nil {
				return nil, err
			}
		}
		return rendered, nil
	case map[string]any:
		result := make(map[string]any, len(v))
		for k, child := range v {
			evaluated, err := evaluateActivationValue(child, env)
			if err != nil {
				return nil, err
			}
			result[k] = evaluated
		}
		return result, nil
	case []any:
		result := make([]any, len(v))
		for i, child := range v {
			evaluated, err := evaluateActivationValue(child, env)
			if err != nil {
				return nil, err
			}
			result[i] = evaluated
		}
		return result, nil
	default:
		return value, nil
	}
}

// checkActivationRoots rejects a template that references a root the activation
// environment does not hold, BEFORE handing it to expr.
//
// The check cannot be delegated to expr, because expr's verdict is not a
// function of (code, env) alone: exprx.CompileExpr caches programs by
// (code, asBool) and ignores env on a hit. Measured -- the same
// "${{ $input.a }}" reports "compile expression: unknown name $input" on a cold
// cache and "evaluate expression: cannot fetch a from <nil>" once some earlier
// node execution compiled that identical source under the full env. A bare
// "{{ $input }}" on a warm cache does not fail at all: it renders "<nil>" into
// the parameter, which is exactly the silent wrong answer this layer exists to
// remove.
//
// Scanning is confined to the inside of each {{ }} segment so a literal prefix
// containing a "$" (a shell-style default in a URL, say) is not mistaken for an
// expression root and rejected.
func checkActivationRoots(s string) error {
	for _, seg := range templateSegments(s) {
		for _, root := range exprRootsIn(seg) {
			if !activationEnvRoots[root] {
				return fmt.Errorf("expression %q references %s, which does not exist when a trigger activates: "+
					"only $config and $vars are available (a trigger's parameters are rendered before any execution runs)",
					strings.TrimSpace(seg), root)
			}
		}
	}
	return nil
}

// checkRenderedForDeferredMarkers rejects a rendered value that carries syntax a
// LATER layer would interpret. Rendering is one-pass, so whatever a $config or
// $vars value holds is substituted verbatim -- and both of the following escape
// every check that runs on the authored source, where the only root is the
// permitted $config:
//
//	$config.x = "{{ $input.y }}"      -> ships the literal "{{ $input.y }}"
//	$config.x = "$supplies.rules"     -> ships a supply reference nothing declared
//
// The first is the same silent wrong answer this whole layer exists to remove,
// one indirection deeper: the consumer subscribes to a topic literally named
// "{{ $input.y }}". The second is a fail-closed-at-the-wrong-layer bug --
// authoring "$supplies.rules" directly makes validateSupplyUsage reject the
// workflow at submit, but arriving at the same string through $config passes
// submit and fails later in CompileProjectedPackage, on the runner, at
// activation. Rejecting here puts both diagnostics back at submit time.
//
// The check runs only when the authored value contained a template, so a
// parameter whose literal text legitimately holds braces or a "$" (never
// rendered, never re-fed to expr) is untouched.
//
// The error names the parameter's authored source and the offending marker, not
// the surrounding rendered value: a rendered trigger parameter routinely holds
// broker addresses and, through $config, credentials.
func checkRenderedForDeferredMarkers(rendered any, source string) error {
	switch v := rendered.(type) {
	case string:
		if marker := deferredMarkerIn(v); marker != "" {
			return fmt.Errorf("expression %q rendered to a value containing %s: "+
				"$config and $vars values are substituted verbatim, never re-evaluated, so the "+
				"reference would reach the handler as literal text", source, marker)
		}
	case map[string]any:
		for _, child := range v {
			if err := checkRenderedForDeferredMarkers(child, source); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := checkRenderedForDeferredMarkers(child, source); err != nil {
				return err
			}
		}
	}
	return nil
}

// deferredMarkerIn names the first deferred-evaluation marker in s, or "" if
// there is none. The reference patterns are the same ones Compile scans the
// authored parameters with, so a value that arrives through $config is judged by
// exactly the rule a directly authored value is judged by.
func deferredMarkerIn(s string) string {
	if strings.Contains(s, "{{") {
		return `a "{{" template`
	}
	if suppliesRefPattern.MatchString(s) || suppliesDynamicPattern.MatchString(s) {
		return "a $supplies reference"
	}
	if nodesRefPattern.MatchString(s) {
		return "a $nodes reference"
	}
	return ""
}

// templateSegments returns the expression source of each {{ }} segment in s,
// including the one a "${{" opens. An unterminated segment yields nothing --
// RenderTemplate reports that case with a position and no content.
func templateSegments(s string) []string {
	var out []string
	rest := s
	for {
		start := strings.Index(rest, "{{")
		if start < 0 {
			return out
		}
		rest = rest[start+len("{{"):]
		end := strings.Index(rest, "}}")
		if end < 0 {
			return out
		}
		out = append(out, rest[:end])
		rest = rest[end+len("}}"):]
	}
}

// exprRootsIn extracts the "$name" identifiers in an expression source.
func exprRootsIn(code string) []string {
	var out []string
	for i := 0; i < len(code); i++ {
		if code[i] != '$' {
			continue
		}
		j := i + 1
		for j < len(code) && isIdentByte(code[j]) {
			j++
		}
		if j > i+1 {
			out = append(out, code[i:j])
		}
		i = j - 1
	}
	return out
}

func isIdentByte(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// evaluateEntryTriggerParams renders the parameters of a group's entry member
// when that member is a trigger, and returns everything else untouched.
//
// Only the entry trigger is rendered. Every other member of a projected package
// IS executed as a task by the inner engine, so its parameters pass through
// execution/params.go against an environment that holds $input, $nodes, and the
// per-item roots. Rendering them here would both double-evaluate and fail:
// those roots do not exist at activation time, so a member reading $input would
// abort the whole projection -- and with it, since assignPackageHashes runs
// inside Compile, the whole workflow.
func evaluateEntryTriggerParams(g *Graph, gm *GroupMeta, nodeIdx int, n NodeMeta) (map[string]any, error) {
	if !gm.Trigger || nodeIdx != gm.EntryIdx || n.Kind != types.NodeKindTrigger {
		return cloneStringAnyMap(n.Parameters), nil
	}
	return EvaluateActivationParams(g, n.Name, n.Type, cloneStringAnyMap(n.Parameters))
}
