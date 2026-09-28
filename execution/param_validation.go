package execution

import (
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	nodereg "github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// DescriptorLookup resolves the Descriptor of a node type. version 0 means the
// latest registered version.
type DescriptorLookup func(nodeType string, version int) (types.Descriptor, bool)

// ParamValidationOptions tunes ValidateWorkflowParamsWithOptions.
type ParamValidationOptions struct {
	// Validate checks one node; nil means graph.ValidateParams.
	Validate func(desc types.Descriptor, params map[string]any) []graph.ParamIssue
	// OnUnknownType is called once per node whose type the lookup does not
	// know. Such a node is skipped, never rejected: the registering process
	// is not necessarily the one that will run it (a custom type registered
	// only on a runner). node is the qualified node name ("parent/child"
	// inside a body).
	OnUnknownType func(nodeType string, version int, node string)
}

// ParamIssuesError is the rejection a registration path returns under
// types.ParamValidationEnforce when ValidateWorkflowParams found issues of
// severity error. It is a caller-fixable definition problem, never retryable:
// the HTTP control plane answers it with 400 workflow_param_invalid and the
// SDK returns it from AddWorkflow.
type ParamIssuesError struct {
	// Workflow names the rejected definition, for the message only.
	Workflow string
	// Issues holds every finding, warnings included, in node order.
	Issues []graph.ParamIssue
}

func (e *ParamIssuesError) Error() string {
	n := 0
	first := ""
	for _, is := range e.Issues {
		if is.Severity != graph.ParamIssueError {
			continue
		}
		if n == 0 {
			first = fmt.Sprintf("node %q: %s", is.Node, is.Message)
		}
		n++
	}
	msg := fmt.Sprintf("workflow %q: %d invalid parameter(s)", e.Workflow, n)
	if first != "" {
		msg += ": " + first
		if n > 1 {
			msg += fmt.Sprintf(" (and %d more)", n-1)
		}
	}
	return msg
}

// EnforceParamIssues returns the error mode demands for issues: a
// *ParamIssuesError when mode is types.ParamValidationEnforce and at least one
// issue has severity error, nil otherwise. off and warn never reject.
func EnforceParamIssues(mode types.ParamValidationMode, workflow string, issues []graph.ParamIssue) error {
	if mode.OrDefault() != types.ParamValidationEnforce || !graph.HasParamErrors(issues) {
		return nil
	}
	return &ParamIssuesError{Workflow: workflow, Issues: issues}
}

// ValidateWorkflowParams validates every node of def against the Descriptor
// lookup returns for it, and returns the issues with ParamIssue.Node set.
//
//   - Disabled nodes are skipped.
//   - A node whose type lookup does not know is skipped.
//   - Sub-graph bodies are recursed into, detected by the value's shape
//     (graph.DeclaresSubgraphBody, the criterion skipSubgraphBody uses); a
//     member is named "parent/child".
//
// It never modifies def.
func ValidateWorkflowParams(def *types.WorkflowDef, lookup DescriptorLookup) []graph.ParamIssue {
	return ValidateWorkflowParamsWithOptions(def, lookup, ParamValidationOptions{})
}

// ValidateWorkflowParamsWithOptions is ValidateWorkflowParams with a custom
// per-node validator and an unknown-type callback (for skip metrics).
func ValidateWorkflowParamsWithOptions(def *types.WorkflowDef, lookup DescriptorLookup, opts ParamValidationOptions) []graph.ParamIssue {
	if def == nil || lookup == nil {
		return nil
	}
	validate := opts.Validate
	if validate == nil {
		validate = graph.ValidateParams
	}
	var out []graph.ParamIssue
	validateNodeList(def.Nodes, "", lookup, validate, opts.OnUnknownType, &out)
	return out
}

func validateNodeList(
	nodes []types.NodeDef,
	prefix string,
	lookup DescriptorLookup,
	validate func(types.Descriptor, map[string]any) []graph.ParamIssue,
	onUnknown func(string, int, string),
	out *[]graph.ParamIssue,
) {
	for i := range nodes {
		nd := &nodes[i]
		if nd.Disabled {
			continue
		}
		name := prefix + nd.Name
		if desc, ok := lookup(nd.Type, nd.Version); ok {
			for _, is := range validate(desc, nd.Parameters) {
				is.Node = name
				*out = append(*out, is)
			}
		} else if onUnknown != nil {
			onUnknown(nd.Type, nd.Version, name)
		}
		if skipSubgraphBody(nd.Parameters) {
			if members, ok := graph.SubgraphBodyMembers(nd.Parameters); ok {
				validateNodeList(members, name+"/", lookup, validate, onUnknown, out)
			}
		}
	}
}

// RegistryDescriptorLookup resolves Descriptors from the process-global node
// registry (node/registry), action handlers first, then triggers. version 0
// resolves the latest registered version.
func RegistryDescriptorLookup(nodeType string, version int) (types.Descriptor, bool) {
	type describer interface{ Descriptor() types.Descriptor }
	var h describer
	if version == 0 {
		if a, ok := nodereg.Lookup(nodeType); ok {
			h = a
		} else if tr, ok := nodereg.LookupTrigger(nodeType); ok {
			h = tr
		}
	} else {
		if a, ok := nodereg.LookupVersion(nodeType, version); ok {
			h = a
		} else if tr, ok := nodereg.LookupTriggerVersion(nodeType, version); ok {
			h = tr
		}
	}
	if h == nil {
		return types.Descriptor{}, false
	}
	return h.Descriptor(), true
}
