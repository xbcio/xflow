package store

import (
	"context"
	"errors"
	"time"
)

// Runner pools (design: runner-config spec §6.5, stage 3.5).
//
// A RunnerPool is the server-side entity behind a fleet of runner replicas
// that share one configuration: it owns the scope ceiling, the labels the
// server hands down, and the instance limit. Registration codes bound to a
// pool (RegistrationCode.PoolID) are its reusable registration tokens; several
// can be live at once so a token rotates without downtime. Each replica is a
// RunnerInstance keyed by (PoolID, SystemID), which maps to exactly one
// server-generated runner ID for the instance's whole life.

// PoolOwnerKind separates tenant-owned pools from platform-owned shared ones.
type PoolOwnerKind string

const (
	// PoolOwnerTenant pools belong to OwnerNamespace and are managed by that
	// namespace's principals (or a *_global principal).
	PoolOwnerTenant PoolOwnerKind = "tenant"
	// PoolOwnerPlatform pools are shared infrastructure. Only a *_global
	// principal (OwnerScope{All: true}) may see or manage them, regardless of
	// the namespace the creating principal happened to belong to. This closes
	// the hole where a global principal's code was stamped with its own
	// namespace and became manageable by every ordinary tenant in it.
	PoolOwnerPlatform PoolOwnerKind = "platform"
)

// RunnerPool is one fleet of interchangeable runner instances.
type RunnerPool struct {
	ID        string
	Name      string
	OwnerKind PoolOwnerKind
	// OwnerNamespace is the owning namespace for PoolOwnerTenant and "" for
	// PoolOwnerPlatform.
	OwnerNamespace string
	// AllowedNamespaces / AllowedNodeTypes are the ceiling for every instance,
	// with RunnerPolicy's conventions ("*" = unrestricted).
	AllowedNamespaces []string
	AllowedNodeTypes  []string
	// Labels are server-owned labels merged into every instance's
	// registration; a runner-reported label with the same key is a conflict.
	Labels map[string]string
	// MaxInstances bounds the number of instances in state active. 0 means
	// unlimited. It does not count history: an instance that was pruned frees
	// its slot.
	MaxInstances int
	// InheritNamespaces lets an instance that declares no namespaces register
	// for every concrete namespace in AllowedNamespaces (capped at
	// MaxInheritedNamespaces). Off by default: a shared pool's token holder
	// must not silently pick up every tenant's work.
	InheritNamespaces bool
	// Paused refuses new enrolls (existing identities keep working).
	Paused    bool
	CreatedAt time.Time
	// DeletedAt marks a soft-deleted pool. Deleted pools are invisible to
	// every read and refuse enroll.
	DeletedAt time.Time
}

// MaxInheritedNamespaces caps RunnerPool.InheritNamespaces expansion.
const MaxInheritedNamespaces = 16

// VisibleTo reports whether scope may see the pool. Platform pools are visible
// only under OwnerScope{All: true}; tenant pools follow OwnerScope.Matches.
// An invalid scope sees nothing.
func (p RunnerPool) VisibleTo(scope OwnerScope) bool {
	if scope.Validate() != nil || !p.DeletedAt.IsZero() {
		return false
	}
	if scope.All {
		return true
	}
	return p.OwnerKind == PoolOwnerTenant && scope.Matches(p.OwnerNamespace)
}

// Policy projects the pool ceiling onto RunnerPolicy, mirroring
// RegistrationCode.Policy, so scope matching has one implementation.
func (p RunnerPool) Policy() RunnerPolicy {
	return RunnerPolicy{
		Name:              "runner-pool:" + p.ID,
		AllowedNodeTypes:  p.AllowedNodeTypes,
		AllowedNamespaces: p.AllowedNamespaces,
	}
}

// Clone returns a copy whose slices and map do not alias p's.
func (p RunnerPool) Clone() RunnerPool {
	p.AllowedNamespaces = append([]string(nil), p.AllowedNamespaces...)
	p.AllowedNodeTypes = append([]string(nil), p.AllowedNodeTypes...)
	if p.Labels != nil {
		labels := make(map[string]string, len(p.Labels))
		for k, v := range p.Labels {
			labels[k] = v
		}
		p.Labels = labels
	}
	return p
}

// InstanceState is a RunnerInstance lifecycle state. Stage 3.6 (prune saga)
// adds the transitions out of active; 3.5 only ever writes active.
type InstanceState string

const (
	InstanceActive   InstanceState = "active"
	InstanceDraining InstanceState = "draining"
	InstancePruned   InstanceState = "pruned"
)

// RunnerInstance is one replica of a pool. (PoolID, SystemID) and RunnerID are
// each unique across the store.
type RunnerInstance struct {
	PoolID string
	// SystemID is runner-reported (pod name, hostname, or a persisted random
	// value). It is an idempotency key and display name, never an identity.
	SystemID string
	// RunnerID is server-generated at first enroll and never changes.
	RunnerID string
	// InstanceUID is the process instance of the latest enroll (pod UID).
	InstanceUID    string
	State          InstanceState
	CreatedAt      time.Time
	LastEnrolledAt time.Time
	// StateChangedAt is when State last changed (CreatedAt for a new row).
	StateChangedAt time.Time
}

// EnrollInstanceRequest asks the store to find or create the instance for
// (PoolID, SystemID). CandidateRunnerID is used only when the instance is
// created.
type EnrollInstanceRequest struct {
	PoolID            string
	SystemID          string
	InstanceUID       string
	CandidateRunnerID string
	Now               time.Time
}

// EnrollInstanceResult reports the instance and whether this call created it.
type EnrollInstanceResult struct {
	Instance RunnerInstance
	Created  bool
}

var (
	// ErrRunnerPoolNotFound is returned for a pool that does not exist, is
	// soft-deleted, or is outside the caller's scope — the three are
	// deliberately indistinguishable.
	ErrRunnerPoolNotFound = errors.New("store: runner pool not found")
	// ErrRunnerPoolInstanceLimit is returned by EnrollInstance when creating
	// the instance would exceed MaxInstances.
	ErrRunnerPoolInstanceLimit = errors.New("store: runner pool instance limit reached")
	// ErrRunnerInstanceNotActive is returned by EnrollInstance when the
	// (PoolID, SystemID) instance exists and is draining: the prune saga owns
	// it until it reaches pruned.
	ErrRunnerInstanceNotActive = errors.New("store: runner instance not active")
	// ErrRunnerInstanceStateConflict is returned by TransitionInstance when the
	// instance is not in the expected from-state.
	ErrRunnerInstanceStateConflict = errors.New("store: runner instance state conflict")
	// ErrRunnerInstanceNotFound is returned by TransitionInstance for an
	// unknown runner ID.
	ErrRunnerInstanceNotFound = errors.New("store: runner instance not found")
)

// RunnerPoolStore persists pools and their instances. Implementations:
// control.MemoryRunnerPoolStore and store/sqlstore's runnerPoolRepo, both held
// to storecontract.RunRunnerPoolStoreContract.
type RunnerPoolStore interface {
	CreatePool(ctx context.Context, pool RunnerPool) error
	// GetPool returns the pool visible under scope, or ErrRunnerPoolNotFound.
	GetPool(ctx context.Context, id string, scope OwnerScope) (RunnerPool, error)
	ListPools(ctx context.Context, scope OwnerScope) ([]RunnerPool, error)
	// UpdatePool replaces the mutable fields (Name, AllowedNamespaces,
	// AllowedNodeTypes, Labels, MaxInstances, InheritNamespaces, Paused) of the
	// pool visible under scope. ID, OwnerKind,
	// OwnerNamespace, CreatedAt and DeletedAt are never changed by it. Whether
	// a scope change is a permitted narrowing is the caller's decision.
	UpdatePool(ctx context.Context, pool RunnerPool, scope OwnerScope) error
	// DeletePool soft-deletes the pool visible under scope.
	DeletePool(ctx context.Context, id string, scope OwnerScope) error
	// EnrollInstance atomically finds or creates the (PoolID, SystemID)
	// instance. Two concurrent calls for the same key must return the same
	// RunnerID with exactly one Created == true. On an existing active
	// instance it updates InstanceUID and LastEnrolledAt. On an existing
	// draining instance it returns ErrRunnerInstanceNotActive. On an existing
	// pruned instance it recycles the row as a brand-new instance: RunnerID =
	// CandidateRunnerID, State = active, CreatedAt = StateChangedAt = Now,
	// Created == true — the pruned runner ID is never reused, so a revoked
	// identity cannot come back. Creation (fresh or recycled) checks
	// MaxInstances against the active count inside the same atomic step. It
	// does not check scope: the caller has already resolved the pool.
	EnrollInstance(ctx context.Context, req EnrollInstanceRequest) (EnrollInstanceResult, error)
	// ListInstancesByState returns every instance in state across all pools.
	// It is unscoped: only the control plane's prune worker calls it.
	ListInstancesByState(ctx context.Context, state InstanceState) ([]RunnerInstance, error)
	// TransitionInstance moves the instance with runnerID from `from` to `to`
	// by compare-and-swap, stamping StateChangedAt = now. Wrong current state
	// returns ErrRunnerInstanceStateConflict; unknown runnerID returns
	// ErrRunnerInstanceNotFound.
	TransitionInstance(ctx context.Context, runnerID string, from, to InstanceState, now time.Time) error
	// ListInstances returns the instances of the pool visible under scope.
	ListInstances(ctx context.Context, poolID string, scope OwnerScope) ([]RunnerInstance, error)
}
