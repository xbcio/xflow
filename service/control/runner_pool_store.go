package control

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/store"
)

type runnerInstanceKey struct {
	poolID   string
	systemID string
}

// MemoryRunnerPoolStore is the in-process RunnerPoolStore implementation.
// One lock covers pools, instance idempotency keys, runner IDs, and active
// counts so EnrollInstance is one atomic operation.
type MemoryRunnerPoolStore struct {
	mu         sync.RWMutex
	pools      map[string]RunnerPool
	instances  map[runnerInstanceKey]RunnerInstance
	byRunnerID map[string]runnerInstanceKey
}

var _ RunnerPoolStore = (*MemoryRunnerPoolStore)(nil)

// NewMemoryRunnerPoolStore returns an empty in-memory runner-pool store.
func NewMemoryRunnerPoolStore() *MemoryRunnerPoolStore {
	return &MemoryRunnerPoolStore{
		pools:      make(map[string]RunnerPool),
		instances:  make(map[runnerInstanceKey]RunnerInstance),
		byRunnerID: make(map[string]runnerInstanceKey),
	}
}

func (s *MemoryRunnerPoolStore) CreatePool(_ context.Context, pool RunnerPool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pools[pool.ID]; exists {
		return fmt.Errorf("control: runner pool %q already exists", pool.ID)
	}
	s.pools[pool.ID] = pool.Clone()
	return nil
}

func (s *MemoryRunnerPoolStore) GetPool(_ context.Context, id string, scope OwnerScope) (RunnerPool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pool, ok := s.pools[id]
	if !ok || !pool.VisibleTo(scope) {
		return RunnerPool{}, ErrRunnerPoolNotFound
	}
	return pool.Clone(), nil
}

func (s *MemoryRunnerPoolStore) ListPools(_ context.Context, scope OwnerScope) ([]RunnerPool, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RunnerPool, 0, len(s.pools))
	for _, pool := range s.pools {
		if pool.VisibleTo(scope) {
			out = append(out, pool.Clone())
		}
	}
	return out, nil
}

func (s *MemoryRunnerPoolStore) UpdatePool(_ context.Context, replacement RunnerPool, scope OwnerScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool, ok := s.pools[replacement.ID]
	if !ok || !pool.VisibleTo(scope) {
		return ErrRunnerPoolNotFound
	}
	pool.Name = replacement.Name
	pool.AllowedNamespaces = append([]string(nil), replacement.AllowedNamespaces...)
	pool.AllowedNodeTypes = append([]string(nil), replacement.AllowedNodeTypes...)
	if replacement.Labels == nil {
		pool.Labels = nil
	} else {
		pool.Labels = make(map[string]string, len(replacement.Labels))
		for key, value := range replacement.Labels {
			pool.Labels[key] = value
		}
	}
	pool.MaxInstances = replacement.MaxInstances
	pool.InheritNamespaces = replacement.InheritNamespaces
	pool.Paused = replacement.Paused
	s.pools[pool.ID] = pool
	return nil
}

func (s *MemoryRunnerPoolStore) DeletePool(_ context.Context, id string, scope OwnerScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool, ok := s.pools[id]
	if !ok || !pool.VisibleTo(scope) {
		return ErrRunnerPoolNotFound
	}
	pool.DeletedAt = time.Now().UTC()
	s.pools[id] = pool
	return nil
}

func (s *MemoryRunnerPoolStore) EnrollInstance(_ context.Context, req store.EnrollInstanceRequest) (store.EnrollInstanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool, ok := s.pools[req.PoolID]
	if !ok || !pool.VisibleTo(OwnerScope{All: true}) {
		return store.EnrollInstanceResult{}, ErrRunnerPoolNotFound
	}

	key := runnerInstanceKey{poolID: req.PoolID, systemID: req.SystemID}
	if instance, exists := s.instances[key]; exists {
		switch instance.State {
		case store.InstanceActive:
			instance.InstanceUID = req.InstanceUID
			instance.LastEnrolledAt = req.Now
			s.instances[key] = instance
			return store.EnrollInstanceResult{Instance: instance, Created: false}, nil
		case store.InstanceDraining:
			return store.EnrollInstanceResult{}, ErrRunnerInstanceNotActive
		case store.InstancePruned:
			if req.CandidateRunnerID == instance.RunnerID {
				return store.EnrollInstanceResult{}, fmt.Errorf("control: pruned runner ID %q cannot be reused", req.CandidateRunnerID)
			}
			if err := s.checkInstanceLimitLocked(pool); err != nil {
				return store.EnrollInstanceResult{}, err
			}
			if _, exists := s.byRunnerID[req.CandidateRunnerID]; exists {
				return store.EnrollInstanceResult{}, fmt.Errorf("control: runner ID %q already exists", req.CandidateRunnerID)
			}
			delete(s.byRunnerID, instance.RunnerID)
			instance.RunnerID = req.CandidateRunnerID
			instance.InstanceUID = req.InstanceUID
			instance.State = store.InstanceActive
			instance.CreatedAt = req.Now
			instance.LastEnrolledAt = req.Now
			instance.StateChangedAt = req.Now
			s.instances[key] = instance
			s.byRunnerID[instance.RunnerID] = key
			return store.EnrollInstanceResult{Instance: instance, Created: true}, nil
		default:
			return store.EnrollInstanceResult{}, ErrRunnerInstanceNotActive
		}
	}

	if err := s.checkInstanceLimitLocked(pool); err != nil {
		return store.EnrollInstanceResult{}, err
	}
	if existing, exists := s.byRunnerID[req.CandidateRunnerID]; exists {
		return store.EnrollInstanceResult{}, fmt.Errorf(
			"control: runner ID %q already belongs to pool %q system %q",
			req.CandidateRunnerID, existing.poolID, existing.systemID,
		)
	}
	instance := RunnerInstance{
		PoolID:         req.PoolID,
		SystemID:       req.SystemID,
		RunnerID:       req.CandidateRunnerID,
		InstanceUID:    req.InstanceUID,
		State:          store.InstanceActive,
		CreatedAt:      req.Now,
		LastEnrolledAt: req.Now,
		StateChangedAt: req.Now,
	}
	s.instances[key] = instance
	s.byRunnerID[instance.RunnerID] = key
	return store.EnrollInstanceResult{Instance: instance, Created: true}, nil
}

func (s *MemoryRunnerPoolStore) checkInstanceLimitLocked(pool RunnerPool) error {
	if pool.MaxInstances <= 0 {
		return nil
	}
	active := 0
	for _, instance := range s.instances {
		if instance.PoolID == pool.ID && instance.State == store.InstanceActive {
			active++
		}
	}
	if active >= pool.MaxInstances {
		return ErrRunnerPoolInstanceLimit
	}
	return nil
}

func (s *MemoryRunnerPoolStore) ListInstances(_ context.Context, poolID string, scope OwnerScope) ([]RunnerInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pool, ok := s.pools[poolID]
	if !ok || !pool.VisibleTo(scope) {
		return nil, ErrRunnerPoolNotFound
	}
	out := make([]RunnerInstance, 0)
	for _, instance := range s.instances {
		if instance.PoolID == poolID {
			out = append(out, instance)
		}
	}
	return out, nil
}

// ListInstancesByState implements store.RunnerPoolStore.
func (s *MemoryRunnerPoolStore) ListInstancesByState(_ context.Context, state store.InstanceState) ([]store.RunnerInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]store.RunnerInstance, 0)
	for _, instance := range s.instances {
		if instance.State == state {
			out = append(out, instance)
		}
	}
	return out, nil
}

// TransitionInstance implements store.RunnerPoolStore.
func (s *MemoryRunnerPoolStore) TransitionInstance(_ context.Context, runnerID string, from, to store.InstanceState, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.byRunnerID[runnerID]
	if !ok {
		return ErrRunnerInstanceNotFound
	}
	instance, ok := s.instances[key]
	if !ok {
		return ErrRunnerInstanceNotFound
	}
	if instance.State != from {
		return ErrRunnerInstanceStateConflict
	}
	instance.State = to
	instance.StateChangedAt = now
	s.instances[key] = instance
	return nil
}
