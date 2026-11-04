package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

const (
	DefaultRunnerInstanceIdleTTL       = 24 * time.Hour
	DefaultRunnerInstancePruneInterval = time.Minute
)

// ErrRunnerInstanceScopeUnsupported is returned by Sweep when the pruner has an
// owned namespace set but its pool store cannot list instances by scope. The
// pass is refused rather than run unscoped: an unscoped pass would drain and
// revoke instances that belong to another deployment.
var ErrRunnerInstanceScopeUnsupported = errors.New("control: runner pool store does not implement store.ScopedRunnerInstanceLister; scoped pruning is disabled")

// RunnerInstancePrunerConfig supplies the durable stores and directory used by
// the instance-pruning saga.
type RunnerInstancePrunerConfig struct {
	Pools            RunnerPoolStore
	IssuedIdentities IssuedIdentityStore
	Directory        RunnerDirectory
	Leader           LeaderGate
	Logger           engine.Logger
	IdleTTL          time.Duration
	Clock            func() time.Time
	// Namespaces restricts pruning to the instances of pools this set owns
	// (tenant pools by owner namespace; platform pools only when it contains
	// namespace.Default). nil, or only empty entries, is the legacy pass over
	// every pool.
	Namespaces []namespace.Namespace
}

// RunnerInstancePruner transitions idle instances through active -> draining
// -> pruned, revoking credentials before it removes directory state.
//
// Invariant: a missing directory record means "unknown", never "idle". The
// directory returns no error, so absence cannot distinguish a never-registered
// runner from an empty or replaced state store, a transient backend failure, or
// a deleted record. Only a present record with a stale heartbeat is evidence
// that the instance is idle.
type RunnerInstancePruner struct {
	pools      RunnerPoolStore
	identities IssuedIdentityStore
	directory  RunnerDirectory
	remover    RunnerRemover
	leader     LeaderGate
	logger     engine.Logger
	idleTTL    time.Duration
	clock      func() time.Time
	// owned is nil for the legacy pass; scoped is the store's scoped lister,
	// nil when owned is set but the store lacks it.
	owned  []namespace.Namespace
	scoped store.ScopedRunnerInstanceLister
}

func NewRunnerInstancePruner(cfg RunnerInstancePrunerConfig) *RunnerInstancePruner {
	idleTTL := cfg.IdleTTL
	if idleTTL <= 0 {
		idleTTL = DefaultRunnerInstanceIdleTTL
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	remover, _ := cfg.Directory.(RunnerRemover)
	var owned []namespace.Namespace
	for _, ns := range cfg.Namespaces {
		if ns != "" {
			owned = append(owned, ns)
		}
	}
	var scoped store.ScopedRunnerInstanceLister
	if owned != nil {
		scoped, _ = cfg.Pools.(store.ScopedRunnerInstanceLister)
	}
	return &RunnerInstancePruner{
		pools: cfg.Pools, identities: cfg.IssuedIdentities, directory: cfg.Directory,
		remover: remover, leader: cfg.Leader, logger: cfg.Logger,
		idleTTL: idleTTL, clock: clock,
		owned: owned, scoped: scoped,
	}
}

// listByState is the one place the pruner reads instances, so the scope
// decision cannot differ between the active and draining passes. An unscoped
// pruner reports every instance with an empty PoolOwnerKind: the legacy store
// call does not know it.
func (p *RunnerInstancePruner) listByState(ctx context.Context, state store.InstanceState) ([]store.ScopedRunnerInstance, error) {
	if p.owned == nil {
		instances, err := p.pools.ListInstancesByState(ctx, state)
		if err != nil {
			return nil, err
		}
		out := make([]store.ScopedRunnerInstance, len(instances))
		for i, instance := range instances {
			out[i] = store.ScopedRunnerInstance{RunnerInstance: instance}
		}
		return out, nil
	}
	if p.scoped == nil {
		return nil, ErrRunnerInstanceScopeUnsupported
	}
	return p.scoped.ListInstancesByStateIn(ctx, p.owned, state)
}

// Sweep performs one idempotent prune pass. Non-leaders do no store or
// directory I/O.
func (p *RunnerInstancePruner) Sweep(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if p.leader == nil || !p.leader.IsLeader() {
		return nil
	}
	if p.pools == nil || p.identities == nil || p.directory == nil || p.remover == nil {
		return errors.New("control: runner instance pruner dependencies are incomplete")
	}

	now := p.clock().UTC()
	cutoff := now.Add(-p.idleTTL)
	var errs []error
	active, err := p.listByState(ctx, store.InstanceActive)
	if err != nil {
		return fmt.Errorf("list active runner instances: %w", err)
	}
	var skippedAbsent []string
	for _, instance := range active {
		snapshot, found := p.directory.Runner(ctx, instance.RunnerID)
		if !found {
			// Absence from the directory is not evidence of idleness. Runner
			// returns no error, so a missing record is indistinguishable from an
			// empty or replaced state store, a transient backend failure, or a
			// deleted record. Draining on that ambiguity would revoke the
			// identities of a live fleet during a store cutover. Normal reaping
			// is unaffected: directory records carry no TTL, so a runner that
			// registered and then died still has a record with a stale
			// heartbeat.
			skippedAbsent = append(skippedAbsent, instance.RunnerID)
			continue
		}
		if !snapshot.LastHeartbeat.Before(cutoff) {
			continue
		}
		if err := p.pools.TransitionInstance(ctx, instance.RunnerID, store.InstanceActive, store.InstanceDraining, now); err != nil {
			if errors.Is(err, store.ErrRunnerInstanceStateConflict) || errors.Is(err, store.ErrRunnerInstanceNotFound) {
				continue
			}
			errs = append(errs, fmt.Errorf("drain runner instance %q: %w", instance.RunnerID, err))
			continue
		}
		if p.logger != nil {
			p.logger.Info("runner instance entered draining", "runner_id", instance.RunnerID)
		}
	}
	if len(skippedAbsent) > 0 && p.logger != nil {
		sample := skippedAbsent
		if len(sample) > 5 {
			sample = sample[:5]
		}
		p.logger.Debug("runner instance prune skipped instances absent from this deployment's directory",
			"count", len(skippedAbsent), "sample_runner_ids", strings.Join(sample, ","))
	}

	draining, err := p.listByState(ctx, store.InstanceDraining)
	if err != nil {
		errs = append(errs, fmt.Errorf("list draining runner instances: %w", err))
		return errors.Join(errs...)
	}
	for _, instance := range draining {
		if err := p.identities.Revoke(ctx, instance.RunnerID, OwnerScope{All: true}); err != nil && !errors.Is(err, ErrIssuedIdentityNotFound) {
			errs = append(errs, fmt.Errorf("revoke runner identity %q: %w", instance.RunnerID, err))
			continue
		}
		if err := p.remover.RemoveRunner(ctx, instance.RunnerID); err != nil {
			if errors.Is(err, ErrRunnerHasOutstandingWork) {
				if p.logger != nil {
					p.logger.Info("runner instance prune waiting for outstanding work", "runner_id", instance.RunnerID)
				}
				continue
			}
			errs = append(errs, fmt.Errorf("remove runner directory state %q: %w", instance.RunnerID, err))
			continue
		}
		if err := p.pools.TransitionInstance(ctx, instance.RunnerID, store.InstanceDraining, store.InstancePruned, now); err != nil {
			if errors.Is(err, store.ErrRunnerInstanceStateConflict) || errors.Is(err, store.ErrRunnerInstanceNotFound) {
				continue
			}
			errs = append(errs, fmt.Errorf("prune runner instance %q: %w", instance.RunnerID, err))
			continue
		}
		if p.logger != nil {
			p.logger.Info("runner instance pruned", "runner_id", instance.RunnerID)
		}
	}
	return errors.Join(errs...)
}
