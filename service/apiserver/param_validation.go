package apiserver

import (
	"context"
	"errors"
	"net/http"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// workflowParamInvalidCode is the failure code of a definition rejected by
// ParamSpec validation under types.ParamValidationEnforce. It is a 400, like
// workflow_compile_failed, and the registration counter records it as
// invalid_definition.
const workflowParamInvalidCode = "workflow_param_invalid"

// registrationDiagnostics is everything non-fatal a registration reports: the
// compiler's warnings and the ParamSpec validator's issues. The two stay
// separate all the way to the wire (warnings vs param_issues).
type registrationDiagnostics struct {
	Warnings    []string
	ParamIssues []graph.ParamIssue
}

// WorkflowRegistrationResult is what RegisterWorkflowReport and
// ReplaceWorkflowReport return: the registered id plus the same diagnostics
// the HTTP registration response carries.
type WorkflowRegistrationResult struct {
	ID types.WorkflowID
	// Warnings are graph.Compile's non-fatal diagnostics.
	Warnings []string
	// ParamIssues are the ParamSpec validator's findings. Under enforce a
	// definition with an error-severity issue is rejected with a
	// *execution.ParamIssuesError instead, so only warnings reach here.
	ParamIssues []graph.ParamIssue
}

// RegisterWorkflowReport is RegisterWorkflow, returning the ParamSpec issues
// alongside the compile warnings. Under types.ParamValidationEnforce a
// definition with an error-severity issue fails with a
// *execution.ParamIssuesError carrying every issue.
func (s *APIServer) RegisterWorkflowReport(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (WorkflowRegistrationResult, error) {
	return s.registerReport(ctx, ns, def, false)
}

// ReplaceWorkflowReport is ReplaceWorkflow with the diagnostics of
// RegisterWorkflowReport.
func (s *APIServer) ReplaceWorkflowReport(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (WorkflowRegistrationResult, error) {
	return s.registerReport(ctx, ns, def, true)
}

func (s *APIServer) registerReport(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef, replace bool) (WorkflowRegistrationResult, error) {
	if s.ctrl == nil {
		return WorkflowRegistrationResult{}, errors.New("apiserver: workflow control module not initialized")
	}
	if def == nil {
		return WorkflowRegistrationResult{}, errors.New("apiserver: workflow definition must not be nil")
	}
	if err := validateWorkflowRegistrationDefinition(def); err != nil {
		return WorkflowRegistrationResult{}, err
	}
	if ns == "" {
		ns = namespace.Default
	}
	register := s.ctrl.registerWorkflow
	if replace {
		register = s.ctrl.replaceWorkflow
	}
	id, diag, err := register(ctx, ns, def)
	if err != nil {
		return WorkflowRegistrationResult{}, err
	}
	return WorkflowRegistrationResult{ID: id, Warnings: diag.Warnings, ParamIssues: diag.ParamIssues}, nil
}

// checkWorkflowParams runs ParamSpec validation over def in the module's mode.
// It returns the issues to report, or a *execution.ParamIssuesError when the
// mode is enforce and an error-severity issue exists. off returns nothing.
//
// Node types this process has not registered are skipped -- the definition
// may target a custom type that only a runner carries -- and counted in
// xflow_param_validation_skipped_total{type}.
func (m *workflowControlModule) checkWorkflowParams(_ context.Context, def *types.WorkflowDef) ([]graph.ParamIssue, error) {
	mode := m.paramValidation.OrDefault()
	if mode == types.ParamValidationOff || def == nil {
		return nil, nil
	}
	lookup := m.descriptorLookup
	if lookup == nil {
		lookup = execution.RegistryDescriptorLookup
	}
	issues := execution.ValidateWorkflowParamsWithOptions(def, lookup, execution.ParamValidationOptions{
		OnUnknownType: func(nodeType string, version int, node string) {
			m.paramValidationMetrics.ObserveSkipped(nodeType)
			if m.log != nil {
				m.log.Debug("workflow_param_validation_skipped", "workflow", def.Name, "node", node, "type", nodeType, "version", version)
			}
		},
	})
	if m.log != nil {
		for _, is := range issues {
			m.log.Warn("workflow_param_issue",
				"workflow", def.Name,
				"node", is.Node,
				"path", is.Path,
				"code", is.Code,
				"severity", is.Severity,
				"message", is.Message,
			)
		}
	}
	if err := execution.EnforceParamIssues(mode, def.Name, issues); err != nil {
		return nil, err
	}
	return issues, nil
}

// paramInvalidData is the data payload of a workflow_param_invalid failure --
// the one failure envelope that carries data, so the editor can place every
// issue on its field.
type paramInvalidData struct {
	ParamIssues []graph.ParamIssue `json:"param_issues"`
}

// writeParamInvalid answers err with 400 workflow_param_invalid when it is a
// *execution.ParamIssuesError, and reports whether it did.
func writeParamInvalid(w http.ResponseWriter, r *http.Request, err error) bool {
	var paramErr *execution.ParamIssuesError
	if !errors.As(err, &paramErr) {
		return false
	}
	issues := paramErr.Issues
	if issues == nil {
		issues = []graph.ParamIssue{}
	}
	writeEnvelope(w, r, http.StatusBadRequest, envelope{
		Success: false,
		Code:    workflowParamInvalidCode,
		Message: paramErr.Error(),
		Data:    paramInvalidData{ParamIssues: issues},
	})
	return true
}
