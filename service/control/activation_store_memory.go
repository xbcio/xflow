package control

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
)

// Compile-time interface check.
var _ engine.TriggerActivationStore = (*MemoryActivationStore)(nil)

// MemoryActivationStore is an in-memory implementation of
// engine.TriggerActivationStore, suitable for single-node deployments and tests.
type MemoryActivationStore struct {
	mu      sync.Mutex
	records map[engine.ActivationKey]*engine.TriggerActivation
}

// NewMemoryActivationStore returns a ready-to-use MemoryActivationStore.
func NewMemoryActivationStore() *MemoryActivationStore {
	return &MemoryActivationStore{
		records: make(map[engine.ActivationKey]*engine.TriggerActivation),
	}
}

func keyOf(act engine.TriggerActivation) engine.ActivationKey {
	return engine.ActivationKey{
		Namespace:  act.Namespace,
		WorkflowID: act.WorkflowID,
		GroupID:    act.GroupID,
	}
}

// SetDesired creates or updates a trigger activation record, marking it as desired.
// Generation is bumped when the record is new, or when packageHash/workflowVersion
// changed, or when transitioning from desired=false to desired=true.
func (s *MemoryActivationStore) SetDesired(_ context.Context, act engine.TriggerActivation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := keyOf(act)
	existing, ok := s.records[key]
	if !ok {
		act.Desired = true
		act.Generation = 1
		s.records[key] = &act
		return nil
	}

	bump := false
	if existing.PackageHash != act.PackageHash || existing.WorkflowVersion != act.WorkflowVersion {
		bump = true
	}
	if !existing.Desired { // transitioning from desired=false to desired=true
		bump = true
	}

	existing.PackageHash = act.PackageHash
	existing.WorkflowVersion = act.WorkflowVersion
	existing.Desired = true
	if bump {
		existing.Generation++
	}
	return nil
}

// ClearDesired marks the activation as no longer desired. No-op if not found or
// already desired=false.
func (s *MemoryActivationStore) ClearDesired(_ context.Context, key engine.ActivationKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok || !rec.Desired {
		return nil
	}
	rec.Desired = false
	rec.Generation++
	return nil
}

// ListActive returns all activations where Desired=true.
func (s *MemoryActivationStore) ListActive(_ context.Context) ([]engine.TriggerActivation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []engine.TriggerActivation
	for _, rec := range s.records {
		if rec.Desired {
			result = append(result, *rec)
		}
	}
	return result, nil
}

// ListByRunner returns all activations assigned to the given runner.
func (s *MemoryActivationStore) ListByRunner(_ context.Context, runnerID string) ([]engine.TriggerActivation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []engine.TriggerActivation
	for _, rec := range s.records {
		if rec.RunnerID == runnerID {
			result = append(result, *rec)
		}
	}
	return result, nil
}

// AssignRunner assigns a runner/session to the activation. Fails if the record
// does not exist or is not desired.
func (s *MemoryActivationStore) AssignRunner(_ context.Context, key engine.ActivationKey, runnerID, sessionID string, leaseDeadline time.Time) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return 0, fmt.Errorf("activation not found: %v", key)
	}
	if !rec.Desired {
		return 0, fmt.Errorf("activation not desired: %v", key)
	}

	rec.RunnerID = runnerID
	rec.SessionID = sessionID
	rec.LeaseDeadline = leaseDeadline
	rec.Generation++
	return rec.Generation, nil
}

// RenewActivationLease extends the lease deadline if the generation matches.
func (s *MemoryActivationStore) RenewActivationLease(_ context.Context, key engine.ActivationKey, generation uint64, deadline time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return false, nil
	}
	if rec.Generation != generation {
		return false, nil
	}
	rec.LeaseDeadline = deadline
	return true, nil
}

// RevokeAssignment clears the runner assignment if the generation matches.
func (s *MemoryActivationStore) RevokeAssignment(_ context.Context, key engine.ActivationKey, generation uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if !ok {
		return false, nil
	}
	if rec.Generation != generation {
		return false, nil
	}
	rec.RunnerID = ""
	rec.SessionID = ""
	rec.LeaseDeadline = time.Time{}
	return true, nil
}

// GetActivation returns a copy of the activation record, or nil if not found.
// When key.Namespace is empty, the store performs a partial-key lookup by
// (WorkflowID, GroupID) to support flows like HandleAck where the caller does
// not know the namespace up front.
func (s *MemoryActivationStore) GetActivation(_ context.Context, key engine.ActivationKey) (*engine.TriggerActivation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if ok {
		copy := *rec
		return &copy, nil
	}

	// Partial-key fallback: match by WorkflowID+GroupID when Namespace is empty.
	if key.Namespace == "" {
		for k, r := range s.records {
			if k.WorkflowID == key.WorkflowID && k.GroupID == key.GroupID {
				copy := *r
				return &copy, nil
			}
		}
	}
	return nil, nil
}
