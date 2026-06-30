package backend

import (
	"context"
	"errors"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

var ErrWorkflowConflict = errors.New("workflow key already exists with different definition hash")
var ErrWorkflowNotFound = errors.New("workflow not found")

type WorkflowRecord struct {
	ID             types.WorkflowID
	Key            string
	Namespace      string
	Name           string
	Version        string
	DefinitionHash string
	Definition     *types.WorkflowDef
	Graph          *graph.Graph
}

type WorkflowRegistry interface {
	AddWorkflow(ctx context.Context, rec WorkflowRecord) (WorkflowRecord, error)
	GetWorkflow(ctx context.Context, id types.WorkflowID) (WorkflowRecord, error)
	RemoveWorkflow(ctx context.Context, id types.WorkflowID) error
}
