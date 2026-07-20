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
	ID               types.WorkflowID
	Key              string
	Namespace        string
	Name             string
	Version          string
	DefinitionHash   string
	AuditFingerprint string
	Definition       *types.WorkflowDef
	Graph            *graph.Graph
}

type WorkflowRegistry interface {
	AddWorkflow(ctx context.Context, rec WorkflowRecord) (WorkflowRecord, error)
	GetWorkflow(ctx context.Context, id types.WorkflowID) (WorkflowRecord, error)
	// GetWorkflowByKey returns the record currently registered under key, or
	// ErrWorkflowNotFound if no record exists. It is used by Engine.AddWorkflow
	// to inspect a conflicting existing record for legacy-hash reconciliation.
	GetWorkflowByKey(ctx context.Context, key string) (WorkflowRecord, error)
	// UpdateDefinitionHash atomically updates the DefinitionHash of the record
	// with id, but only if the currently-stored hash equals expectedOldHash.
	// Returns ErrWorkflowNotFound if the id is unknown and
	// ErrWorkflowConflict if the stored hash no longer matches expectedOldHash
	// (e.g. another registrar upgraded it concurrently). It is used by
	// Engine.AddWorkflow to upgrade legacy-format hashes when semantics match.
	UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error
	RemoveWorkflow(ctx context.Context, id types.WorkflowID) error
}
