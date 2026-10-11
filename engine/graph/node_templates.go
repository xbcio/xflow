package graph

import (
	"fmt"

	"github.com/xbcio/xflow/types"
)

// ExpandNodeTemplates resolves every NodeDef.Template reference against
// def.NodeTemplates and returns an expanded deep copy; def is never mutated.
// A templated node takes the template's type, and its parameters are the
// template's parameters merged under the node's own (DSL-SPECIFICATION.md
// node_templates): maps deep-merge with the node winning, lists and scalars
// are replaced, and a node key set to null deletes that key from the result.
//
// A def with no templated node is returned as is.
func ExpandNodeTemplates(def *types.WorkflowDef) (*types.WorkflowDef, error) {
	if def == nil {
		return nil, nil
	}
	templated := false
	for i := range def.Nodes {
		if def.Nodes[i].Template != "" {
			templated = true
			break
		}
	}
	if !templated {
		return def, nil
	}
	// Only Nodes is rewritten, and each templated node gets freshly built
	// parameters, so a shallow def copy with its own Nodes slice keeps the
	// caller's def untouched while leaving untemplated values (and their Go
	// types, hence the graph hash) exactly as given.
	cp := *def
	cp.Nodes = append([]types.NodeDef(nil), def.Nodes...)
	out := &cp
	for i := range out.Nodes {
		nd := &out.Nodes[i]
		if nd.Template == "" {
			continue
		}
		if nd.Type != "" {
			return nil, fmt.Errorf("node %q: template and type are mutually exclusive (template %q provides the type)", nd.Name, nd.Template)
		}
		tpl, ok := out.NodeTemplates[nd.Template]
		if !ok {
			return nil, fmt.Errorf("node %q: unknown node template %q", nd.Name, nd.Template)
		}
		if tpl.Type == "" {
			return nil, fmt.Errorf("node template %q has no type", nd.Template)
		}
		nd.Type = tpl.Type
		nd.Parameters = mergeTemplateParams(cloneStringAnyMap(tpl.Parameters), nd.Parameters)
		nd.Template = ""
	}
	return out, nil
}

// mergeTemplateParams merges node over base in place on base and returns it.
func mergeTemplateParams(base, node map[string]any) map[string]any {
	if base == nil && node == nil {
		return nil
	}
	if base == nil {
		base = make(map[string]any, len(node))
	}
	for k, v := range node {
		if v == nil {
			delete(base, k)
			continue
		}
		nodeMap, nodeIsMap := v.(map[string]any)
		baseMap, baseIsMap := base[k].(map[string]any)
		if nodeIsMap && baseIsMap {
			base[k] = mergeTemplateParams(baseMap, nodeMap)
			continue
		}
		if nodeIsMap {
			// Still strip nested nulls: a deletion marker must never survive
			// into the compiled parameters.
			base[k] = mergeTemplateParams(nil, nodeMap)
			continue
		}
		base[k] = cloneAny(v)
	}
	if len(base) == 0 {
		return nil
	}
	return base
}
