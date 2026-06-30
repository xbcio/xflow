package xflow

import (
	"context"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// HandlerLocator identifies a single (type, version) pair the registry must
// satisfy.
type HandlerLocator struct {
	Type    string
	Version int
	Kind    types.NodeKind
}

// ErrMissingHandlerVersions is returned by AddWorkflow when the workflow
// references node types/versions that no handler currently satisfies. The
// pre-check is always strict regardless of WithVersionPolicy: at registration
// time all handlers should be known. See
// .claude/docs/specs/handler-version.md.
type ErrMissingHandlerVersions struct {
	Missing []HandlerLocator
}

func (e *ErrMissingHandlerVersions) Error() string {
	parts := make([]string, 0, len(e.Missing))
	for _, m := range e.Missing {
		kind := string(m.Kind)
		if kind == "" {
			kind = string(types.NodeKindAction)
		}
		if m.Version > 0 {
			parts = append(parts, fmt.Sprintf("%s %s@v%d", kind, m.Type, m.Version))
		} else {
			parts = append(parts, fmt.Sprintf("%s %s", kind, m.Type))
		}
	}
	return "handler not registered: " + strings.Join(parts, ", ")
}

func (e *Engine) AddWorkflow(ctx context.Context, wf *WorkflowBuilder) (types.WorkflowID, error) {
	def, err := wf.build()
	if err != nil {
		return "", err
	}
	if err := e.registerWorkflowHandlers(wf); err != nil {
		return "", err
	}
	if err := e.registerDirectHandlers(wf); err != nil {
		return "", err
	}
	if err := preCheckHandlerVersions(def, wf); err != nil {
		return "", err
	}
	g, err := graph.Compile(def)
	if err != nil {
		return "", err
	}
	hash, err := definitionHash(def)
	if err != nil {
		return "", err
	}
	rec, err := e.workflowRegistry.AddWorkflow(ctx, backend.WorkflowRecord{
		Key:            workflowKey(def),
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: hash,
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		return "", err
	}
	if e.triggerRuntime != nil {
		if err := e.triggerRuntime.ReconcileWorkflow(ctx, rec); err != nil {
			return "", err
		}
	}
	return rec.ID, nil
}

// preCheckHandlerVersions verifies every NodeDef's (type, version) is
// resolvable. Workflow-bundled handlers (collected by the builder) are paired
// with their NodeDef.Version by construction, so they pass automatically.
// Built-in xflow.* types (xflow.if, xflow.merge, etc.) registered at package
// init time are also considered satisfied. Anything else must resolve through
// the global node registry's versioned lookups. Direct handlers
// (__direct__/<node-name>) are workflow-local and always satisfied.
func preCheckHandlerVersions(def *types.WorkflowDef, wf *WorkflowBuilder) error {
	if def == nil {
		return nil
	}
	bundledActions := wf.workflowHandlers()
	bundledTriggers := wf.workflowTriggerHandlers()

	var missing []HandlerLocator
	for _, nd := range def.Nodes {
		if strings.HasPrefix(nd.Type, "__direct__/") {
			continue // local-only direct handler, name-scoped
		}
		switch nd.Kind {
		case types.NodeKindTrigger:
			if _, ok := bundledTriggers[nd.Type]; ok {
				continue
			}
			if nd.Version > 0 {
				if _, ok := node.LookupTriggerVersion(nd.Type, nd.Version); ok {
					continue
				}
			} else if _, ok := node.LookupTrigger(nd.Type); ok {
				continue
			}
			missing = append(missing, HandlerLocator{Type: nd.Type, Version: nd.Version, Kind: types.NodeKindTrigger})
		default:
			if _, ok := bundledActions[nd.Type]; ok {
				continue
			}
			if nd.Version > 0 {
				if _, ok := node.LookupVersion(nd.Type, nd.Version); ok {
					continue
				}
			} else if _, ok := node.Lookup(nd.Type); ok {
				continue
			}
			missing = append(missing, HandlerLocator{Type: nd.Type, Version: nd.Version, Kind: types.NodeKindAction})
		}
	}
	if len(missing) > 0 {
		return &ErrMissingHandlerVersions{Missing: missing}
	}
	return nil
}

func (e *Engine) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	if e.triggerRuntime != nil {
		if err := e.triggerRuntime.RemoveWorkflow(ctx, workflowID); err != nil {
			return err
		}
	}
	return e.workflowRegistry.RemoveWorkflow(ctx, workflowID)
}
