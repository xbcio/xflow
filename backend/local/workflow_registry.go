package local

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/types"
)

type workflowRegistry struct {
	mu    sync.Mutex
	byID  map[types.WorkflowID]backend.WorkflowRecord
	byKey map[string]types.WorkflowID
}

func newWorkflowRegistry() *workflowRegistry {
	return &workflowRegistry{
		byID:  make(map[types.WorkflowID]backend.WorkflowRecord),
		byKey: make(map[string]types.WorkflowID),
	}
}

func (r *workflowRegistry) AddWorkflow(_ context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if id, ok := r.byKey[rec.Key]; ok {
		existing := r.byID[id]
		if existing.DefinitionHash != rec.DefinitionHash {
			return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
		}
		return existing, nil
	}
	if rec.ID == "" {
		rec.ID = types.WorkflowID(uuid.NewString())
	}
	r.byKey[rec.Key] = rec.ID
	r.byID[rec.ID] = rec
	return rec, nil
}

func (r *workflowRegistry) GetWorkflow(_ context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.byID[id]
	if !ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return rec, nil
}

func (r *workflowRegistry) RemoveWorkflow(_ context.Context, id types.WorkflowID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.byID[id]
	if !ok {
		return backend.ErrWorkflowNotFound
	}
	delete(r.byID, id)
	delete(r.byKey, rec.Key)
	return nil
}
