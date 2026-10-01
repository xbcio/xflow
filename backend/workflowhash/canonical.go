package workflowhash

import (
	"bytes"
	"encoding/json"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ParamSpecLookup resolves the ParamSpecs of a node type, kind-aware, by
// (kind, type, version). ok is false for a type it does not know, whose params
// Canonical then leaves alone. node.BuiltinParamSpecs is the production
// lookup: it covers builtin types only, never the live registry, so the hash
// cannot depend on which runners or handlers a process happens to see.
type ParamSpecLookup func(kind types.NodeKind, nodeType string, version int) ([]types.ParamSpec, bool)

// subgraphBodyParam is the param key a body-bearing node keeps its sub-graph
// under; engine/graph and execution use the same key.
const subgraphBodyParam = "body"

// Canonical returns def with every known node's missing or nil params filled
// from ParamSpec.Default, so a definition that omits a param hashes like one
// that spells out its Default. It mirrors what the SDK builder writes
// (validateParams in sdk/xflow), which makes it the identity on an SDK build:
//   - a param that is absent or explicitly nil is filled; any other value,
//     including "", is kept;
//   - only top-level ParamSpecs are filled, never nested Fields or Items;
//   - a body-bearing node (params.body is an xflow.subgraph node) is not
//     filled itself, but its body members are, recursively;
//   - disabled nodes are filled like any other;
//   - a node specs does not know (custom, direct, or unknown types) is left
//     alone.
//
// def is never mutated. Canonical copies only what it fills, and returns def
// itself when nothing needed filling or specs is nil.
func Canonical(def *types.WorkflowDef, specs ParamSpecLookup) *types.WorkflowDef {
	if def == nil || specs == nil {
		return def
	}
	var nodes []types.NodeDef
	for i := range def.Nodes {
		n := &def.Nodes[i]
		params, changed := canonicalParams(n.Kind, n.Type, n.Version, n.Parameters, specs)
		if !changed {
			continue
		}
		if nodes == nil {
			nodes = append([]types.NodeDef(nil), def.Nodes...)
		}
		nodes[i].Parameters = params
	}
	if nodes == nil {
		return def
	}
	out := *def
	out.Nodes = nodes
	return &out
}

// canonicalParams returns the canonical params of one node and whether they
// differ from params. params is never mutated.
func canonicalParams(kind types.NodeKind, nodeType string, version int, params map[string]any, specs ParamSpecLookup) (map[string]any, bool) {
	// The same check the compiler and execution use, keyed on the value's
	// shape (params.body decodes as an xflow.subgraph node), never on node
	// type, so a request-payload "body" (xflow.http) is never mistaken for one.
	if graph.DeclaresSubgraphBody(params) {
		body, changed := canonicalBody(params[subgraphBodyParam], specs)
		if !changed {
			return params, false
		}
		out := cloneParams(params)
		out[subgraphBodyParam] = body
		return out, true
	}
	paramSpecs, ok := specs(kind, nodeType, version)
	if !ok {
		return params, false
	}
	var out map[string]any
	for _, spec := range paramSpecs {
		if spec.Default == nil {
			continue
		}
		if val, exists := params[spec.Name]; exists && val != nil {
			continue
		}
		if out == nil {
			out = cloneParams(params)
		}
		out[spec.Name] = spec.Default
	}
	if out == nil {
		return params, false
	}
	return out, true
}

// canonicalBody fills the members of an xflow.subgraph body and reports
// whether anything changed. The SDK and the JSON decoder both produce the
// generic shape (map[string]any / []any), which is walked copy-on-write. Any
// other Go shape is first normalized to the generic one through JSON, and the
// normalized form is adopted only when a member was actually filled.
func canonicalBody(raw any, specs ParamSpecLookup) (any, bool) {
	if body, ok := raw.(map[string]any); ok {
		if out, changed, ok := canonicalGenericBody(body, specs); ok {
			return out, changed
		}
	}
	generic, ok := toGeneric(raw)
	if !ok {
		return raw, false
	}
	body, ok := generic.(map[string]any)
	if !ok {
		return raw, false
	}
	out, changed, ok := canonicalGenericBody(body, specs)
	if !ok || !changed {
		return raw, false
	}
	return out, true
}

// canonicalGenericBody walks body.parameters.nodes. ok is false when body is
// not in the generic shape, so the caller can normalize it and retry.
func canonicalGenericBody(body map[string]any, specs ParamSpecLookup) (out map[string]any, changed bool, ok bool) {
	rawParams, present := body["parameters"]
	if !present || rawParams == nil {
		return body, false, true
	}
	bodyParams, isMap := rawParams.(map[string]any)
	if !isMap {
		return nil, false, false
	}
	rawNodes, present := bodyParams["nodes"]
	if !present || rawNodes == nil {
		return body, false, true
	}
	members, isSlice := rawNodes.([]any)
	if !isSlice {
		return nil, false, false
	}
	var outMembers []any
	for i, rawMember := range members {
		member, isMap := rawMember.(map[string]any)
		if !isMap {
			return nil, false, false
		}
		memberParams, ok := memberParameters(member)
		if !ok {
			return nil, false, false
		}
		version, ok := memberVersion(member["version"])
		if !ok {
			// The compiler cannot decode this member either; leave it alone.
			continue
		}
		kind, _ := member["kind"].(string)
		nodeType, _ := member["type"].(string)
		filled, memberChanged := canonicalParams(types.NodeKind(kind), nodeType, version, memberParams, specs)
		if !memberChanged {
			continue
		}
		if outMembers == nil {
			outMembers = append([]any(nil), members...)
		}
		m := cloneParams(member)
		m["parameters"] = filled
		outMembers[i] = m
	}
	if outMembers == nil {
		return body, false, true
	}
	p := cloneParams(bodyParams)
	p["nodes"] = outMembers
	b := cloneParams(body)
	b["parameters"] = p
	return b, true, true
}

// memberParameters returns a member's parameters map; an absent or null one
// is an empty map, as the decoder would produce. ok is false for any other
// non-generic shape.
func memberParameters(member map[string]any) (map[string]any, bool) {
	raw, present := member["parameters"]
	if !present || raw == nil {
		return nil, true
	}
	params, ok := raw.(map[string]any)
	return params, ok
}

// memberVersion reads a member's version from the numeric shapes the SDK,
// the JSON decoder (float64) and a UseNumber decoder produce. An absent
// version is 0, which resolves the latest version.
func memberVersion(raw any) (int, bool) {
	switch v := raw.(type) {
	case nil:
		return 0, true
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		if v != float64(int(v)) {
			return 0, false
		}
		return int(v), true
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	default:
		return 0, false
	}
}

// toGeneric re-decodes v through JSON into map[string]any / []any, keeping
// numbers exact.
func toGeneric(v any) (any, bool) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, false
	}
	return out, true
}

// cloneParams returns a shallow copy of m, allocating for a nil m.
func cloneParams(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}
