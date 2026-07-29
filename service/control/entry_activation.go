package control

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// Compile-time interface check.
var _ engine.EntryActivationStore = (*MemoryEntryActivationStore)(nil)

// MemoryEntryActivationStore is an in-memory implementation of
// engine.EntryActivationStore, suitable for single-node deployments and tests.
// A single mutex makes Assign/Fence atomic first-writer-wins operations.
type MemoryEntryActivationStore struct {
	mu      sync.Mutex
	records map[engine.EntryActivationKey]*engine.EntryActivation
}

// NewMemoryEntryActivationStore returns a ready-to-use store.
func NewMemoryEntryActivationStore() *MemoryEntryActivationStore {
	return &MemoryEntryActivationStore{
		records: make(map[engine.EntryActivationKey]*engine.EntryActivation),
	}
}

func entryActivationKeyOf(a engine.EntryActivation) engine.EntryActivationKey {
	return engine.EntryActivationKey{
		Namespace:       a.Namespace,
		WorkflowID:      a.WorkflowID,
		WorkflowVersion: a.WorkflowVersion,
		EntryUnitID:     a.EntryUnitID,
	}
}

// Upsert creates or updates the desired-state fields of an activation. It never
// clobbers the assignment fields of an existing record.
func (s *MemoryEntryActivationStore) Upsert(_ context.Context, act engine.EntryActivation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := entryActivationKeyOf(act)
	existing, ok := s.records[key]
	if !ok {
		rec := act
		s.records[key] = &rec
		return nil
	}
	// Preserve assignment state; only refresh desired-state fields.
	existing.NodeType = act.NodeType
	existing.Params = act.Params
	existing.PackageHash = act.PackageHash
	existing.Selector = act.Selector
	existing.Requirements = act.Requirements
	existing.Desired = act.Desired
	return nil
}

// Get returns a copy of the activation for key.
func (s *MemoryEntryActivationStore) Get(_ context.Context, key engine.EntryActivationKey) (engine.EntryActivation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return engine.EntryActivation{}, false, nil
	}
	return *rec, true, nil
}

// List returns copies of all activations in the namespace.
func (s *MemoryEntryActivationStore) List(_ context.Context, ns namespace.Namespace) ([]engine.EntryActivation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []engine.EntryActivation
	for k, rec := range s.records {
		if k.Namespace == ns {
			out = append(out, *rec)
		}
	}
	return out, nil
}

// Assign claims the activation for runnerID/sessionID at gen. First-writer-wins
// and monotonic: it succeeds only when gen strictly exceeds the stored
// generation.
func (s *MemoryEntryActivationStore) Assign(_ context.Context, key engine.EntryActivationKey, runnerID, sessionID string, gen uint64, deadline time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return false, nil
	}
	if gen <= rec.Generation {
		return false, nil
	}
	rec.RunnerID = runnerID
	rec.SessionID = sessionID
	rec.Generation = gen
	rec.LeaseDeadline = deadline
	// Snapshot the desired PackageHash in effect at assignment time so the
	// reconciler can detect a later within-version content change.
	rec.AssignedPackageHash = rec.PackageHash
	return true, nil
}

// Renew extends the lease deadline of the current owner without advancing the
// generation. Generation-gated: succeeds only when gen equals the stored
// generation. No-op (false) when absent or the generation does not match.
func (s *MemoryEntryActivationStore) Renew(_ context.Context, key engine.EntryActivationKey, gen uint64, deadline time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return false, nil
	}
	if rec.Generation != gen || rec.RunnerID == "" {
		return false, nil
	}
	rec.LeaseDeadline = deadline
	return true, nil
}

// Fence invalidates the current owner and raises the generation floor to at
// least gen. No-op when the activation does not exist.
func (s *MemoryEntryActivationStore) Fence(_ context.Context, key engine.EntryActivationKey, gen uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return nil
	}
	if gen > rec.Generation {
		rec.Generation = gen
	}
	rec.RunnerID = ""
	rec.SessionID = ""
	rec.LeaseDeadline = time.Time{}
	rec.AssignedPackageHash = ""
	return nil
}
