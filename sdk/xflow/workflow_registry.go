package xflow

import (
	"context"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

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
	return rec.ID, nil
}

func (e *Engine) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	return e.workflowRegistry.RemoveWorkflow(ctx, workflowID)
}
