package xflow

import (
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// ParamIssue is one ParamSpec validation finding (see graph.ParamIssue).
type ParamIssue = graph.ParamIssue

// ParamValidationMode selects what registration does with ParamSpec issues:
// types.ParamValidationOff, types.ParamValidationWarn (the default), or
// types.ParamValidationEnforce.
type ParamValidationMode = types.ParamValidationMode

// ParamIssuesError is returned by AddWorkflow under
// types.ParamValidationEnforce when a node's params violate its Descriptor.
// IsRetryableRegistrationError reports it as not retryable.
type ParamIssuesError = execution.ParamIssuesError

// WithParamValidation sets how Engine.AddWorkflow treats ParamSpec validation
// issues (graph.ValidateParams). The default is types.ParamValidationWarn.
//
//   - off: the validator does not run.
//   - warn: issues are logged through WithLogger's logger; nothing is
//     rejected.
//   - enforce: issues are logged, and AddWorkflow fails with a
//     *ParamIssuesError when any has severity error.
//
// In every mode the builder's long-standing Required check still runs and
// still fails the build for a missing required param: it predates this switch
// and is not governed by it. Nodes carrying a Body are not validated (their
// params are not normalized either, which keeps their workflow hash stable);
// the body's own members are.
//
// An invalid mode is ignored and the default kept.
func WithParamValidation(mode types.ParamValidationMode) Option {
	return func(c *engineConfig) {
		if mode.Valid() {
			c.paramValidation = mode
		}
	}
}

// applyParamValidation logs issues and returns the rejection mode demands.
func applyParamValidation(mode types.ParamValidationMode, logger engine.Logger, workflow string, issues []graph.ParamIssue) error {
	mode = mode.OrDefault()
	if mode == types.ParamValidationOff {
		return nil
	}
	logParamIssues(logger, workflow, issues)
	return execution.EnforceParamIssues(mode, workflow, issues)
}

// logParamIssues writes one warning per issue. The message fields name the
// node, the parameter path, and the rule -- never the value.
func logParamIssues(logger engine.Logger, workflow string, issues []graph.ParamIssue) {
	if logger == nil {
		return
	}
	for _, is := range issues {
		logger.Warn("workflow_param_issue",
			"workflow", workflow,
			"node", is.Node,
			"path", is.Path,
			"code", is.Code,
			"severity", is.Severity,
			"message", is.Message,
		)
	}
}

// finalOneOfIssues re-checks Descriptor.OneOf after resolveArtifacts has
// rewritten Script.File()'s build-time __artifact_file_path into
// artifact_digest, so the rule is judged on the parameter set that actually
// runs. It returns only issues the build-time pass did not already report.
// Types the process registry does not know are skipped: resolveArtifacts only
// rewrites xflow.script, a builtin.
func finalOneOfIssues(def *types.WorkflowDef, buildIssues []graph.ParamIssue) []graph.ParamIssue {
	type key struct{ node, path, code string }
	seen := make(map[key]bool, len(buildIssues))
	for _, is := range buildIssues {
		seen[key{is.Node, is.Path, is.Code}] = true
	}
	var out []graph.ParamIssue
	scriptOneOf := func(desc types.Descriptor, params map[string]any) []graph.ParamIssue {
		if desc.Type != scriptNodeType {
			return nil // nothing else is rewritten; body-bearing nodes stay unvalidated
		}
		return graph.ValidateOneOf(desc, params)
	}
	for _, is := range execution.ValidateWorkflowParamsWithOptions(def, execution.RegistryDescriptorLookup,
		execution.ParamValidationOptions{Validate: scriptOneOf}) {
		if !seen[key{is.Node, is.Path, is.Code}] {
			out = append(out, is)
		}
	}
	return out
}

// scriptNodeType is the only node type resolveArtifacts rewrites.
const scriptNodeType = "xflow.script"
