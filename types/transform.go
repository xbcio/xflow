package types

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
type TransformSpec struct {
	Expression string   `json:"expression,omitempty"`
	Body       *NodeDef `json:"body,omitempty"`
}
