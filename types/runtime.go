package types

// Runtime holds per-execution context supplied when a workflow is submitted.
//
// Unlike WorkflowContext, Runtime is not part of the static workflow
// definition. It can differ for every execution of the same workflow.
type Runtime struct {
	Vars map[string]any `json:"vars,omitempty"`
}
