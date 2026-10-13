package types

// SubWorkflowNodeType is the node type of a sub-workflow node: a node that runs
// another registered workflow as a child execution and commits that child's
// conclusion as its own result.
//
// It lives in types, not in the node package that registers the stub handler,
// for the same reason SubgraphNodeType does: the compiler (engine/graph) and
// the engine both have to recognize the type by name, and neither may depend on
// which handlers happen to be linked into the current binary.
const SubWorkflowNodeType = "xflow.subworkflow"
