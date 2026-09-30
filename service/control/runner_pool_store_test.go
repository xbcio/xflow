package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/storecontract"
)

func TestMemoryRunnerPoolStoreSatisfiesContract(t *testing.T) {
	storecontract.RunRunnerPoolStoreContract(t, func(t *testing.T) store.RunnerPoolStore {
		return NewMemoryRunnerPoolStore()
	})
}

func (s *MemoryRunnerPoolStore) SetRunnerInstanceStateForContract(_ context.Context, poolID, systemID string, state store.InstanceState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := runnerInstanceKey{poolID: poolID, systemID: systemID}
	instance, ok := s.instances[key]
	if !ok {
		return errors.New("runner instance contract fixture not found")
	}
	instance.State = state
	s.instances[key] = instance
	return nil
}

func TestMemoryRunnerPoolStoreRejectsNonActiveInstance(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryRunnerPoolStore()
	pool := RunnerPool{
		ID: "pool-1", Name: "pool", OwnerKind: store.PoolOwnerTenant,
		OwnerNamespace: "team-a", CreatedAt: time.Now().UTC(),
	}
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	now := time.Now().UTC()
	req := store.EnrollInstanceRequest{
		PoolID: "pool-1", SystemID: "host-1", InstanceUID: "uid-1",
		CandidateRunnerID: "runner-1", Now: now,
	}
	if _, err := st.EnrollInstance(ctx, req); err != nil {
		t.Fatalf("first EnrollInstance: %v", err)
	}

	key := runnerInstanceKey{poolID: req.PoolID, systemID: req.SystemID}
	st.mu.Lock()
	instance := st.instances[key]
	instance.State = store.InstanceDraining
	st.instances[key] = instance
	st.mu.Unlock()

	req.InstanceUID = "uid-2"
	req.Now = now.Add(time.Minute)
	if _, err := st.EnrollInstance(ctx, req); !errors.Is(err, ErrRunnerInstanceNotActive) {
		t.Fatalf("EnrollInstance(draining) = %v, want ErrRunnerInstanceNotActive", err)
	}
	instances, err := st.ListInstances(ctx, pool.ID, OwnerScope{Namespace: "team-a"})
	if err != nil || len(instances) != 1 {
		t.Fatalf("ListInstances = (%+v, %v), want one instance", instances, err)
	}
	if instances[0].InstanceUID != "uid-1" || !instances[0].LastEnrolledAt.Equal(now) {
		t.Fatalf("rejected reenroll mutated instance: %+v", instances[0])
	}
}
