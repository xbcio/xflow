package types

import (
	"encoding/json"
	"errors"
	"fmt"
)

// TransformSpec is the shared shape for "compute this per item" node
// parameters: exactly one of Expression or Body is set. It is not map-private:
// the same {expression | body} choice applies to any transform-style node
// (xflow.filter, xflow.sort's key, xflow.reduce's accumulator, ...), so the
// type lives here rather than being duplicated per node.
//
// Expression is a deterministic, no-IO computation evaluated inline by the
// node itself (no sub-execution, no durability). Body is a sub-graph node
// definition (Type == "xflow.subgraph") executed once per item/batch via the
// engine's expansion path, with its own durable sub-executions. The two carry
// different failure semantics and are mutually exclusive at compile time —
// never sniffed from runtime output shape.
//
// ParseTransformSpec is how both sides read it: the compiler
// (validateNodeBody rules 1-2) and the node handler. Neither reaches into
// Parameters for "expression"/"body" itself.
type TransformSpec struct {
	Expression string   `json:"expression,omitempty"`
	Body       *NodeDef `json:"body,omitempty"`
}

// SubgraphNodeType is the body-only node type: a NodeDef with this Type carries
// a self-contained {nodes, connections} sub-graph in its Parameters. It lives
// here because ParseTransformSpec must recognize it; the compiler keeps its own
// unexported alias so its rules read locally.
const SubgraphNodeType = "xflow.subgraph"

// ParseTransformSpec reads a transform node's parameters into the declared
// shape, and is the single place the {expression | body} choice is decided.
//
// It exists because the two sides of that contract disagreed. The compiler
// asked whether the "expression" KEY was present; xflow.map's handler asked
// whether its VALUE was non-empty. For xflow.map the divergence was masked —
// it is also a fan-out type, and that rule rejects a body-less node on the
// value, so `expression: ""` never survived compilation. A transform that does
// not fan out (filter, reduce's accumulator, sort's key) has no such second
// net: it would compile with an empty expression and no body, and its handler
// would then take the body branch against a body that does not exist.
//
// The value decides. An empty expression is not a declared expression, which
// also makes `expression: "" ` beside a real body read as the body form rather
// than a conflict — the shape the handler has always accepted.
//
// It does not validate the body's contents. Membership rules, nesting bans and
// entry dominance are the compiler's, which owns the graph machinery to check
// them; this only settles which of the two forms was declared.
func ParseTransformSpec(params map[string]any) (TransformSpec, error) {
	var spec TransformSpec
	spec.Expression, _ = params["expression"].(string)
	bodyRaw, hasBodyKey := params["body"]

	var body *NodeDef
	if hasBodyKey {
		decoded, err := decodeNodeDef(bodyRaw)
		if err != nil {
			return TransformSpec{}, fmt.Errorf("body: %w", err)
		}
		if decoded.Type != SubgraphNodeType {
			// Name the type we found: a body that decodes but has the wrong type
			// is the common typo, and the value is the author's own.
			return TransformSpec{}, fmt.Errorf("body.type must be %q, got %q",
				SubgraphNodeType, decoded.Type)
		}
		body = decoded
	}

	switch {
	case spec.Expression != "" && body != nil:
		return TransformSpec{}, errors.New("expression and body are mutually exclusive")
	case spec.Expression == "" && body == nil:
		return TransformSpec{}, errors.New("requires exactly one of expression or body")
	}
	spec.Body = body
	return spec, nil
}

// decodeNodeDef re-decodes an untyped parameter value into a NodeDef, the way
// every other parameter of structured shape is read: marshal what the author
// wrote, unmarshal into the declared type. Going through JSON is what lets a
// body arrive either as a map (an in-process definition) or already decoded.
func decodeNodeDef(raw any) (*NodeDef, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	var nd NodeDef
	if err := json.Unmarshal(data, &nd); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &nd, nil
}
