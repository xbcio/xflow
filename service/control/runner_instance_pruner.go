package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
)

const (
	DefaultRunnerInstanceIdleTTL       = 24 * time.Hour
	DefaultRunnerInstancePruneInterval = time.Minute
)

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
}

// RunnerInstancePruner transitions idle instances through active -> draining
// -> pruned, revoking credentials before it removes directory state.
type RunnerInstancePruner struct {
	pools      RunnerPoolStore
	identities IssuedIdentityStore
	directory  RunnerDirectory
	remover    RunnerRemover
	leader     LeaderGate
	logger     engine.Logger
	idleTTL    time.Duration
	clock      func() time.Time
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
	return &RunnerInstancePruner{
		pools: cfg.Pools, identities: cfg.IssuedIdentities, directory: cfg.Directory,
		remover: remover, leader: cfg.Leader, logger: cfg.Logger,
		idleTTL: idleTTL, clock: clock,
	}
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
	active, err := p.pools.ListInstancesByState(ctx, store.InstanceActive)
	if err != nil {
		return fmt.Errorf("list active runner instances: %w", err)
	}
	for _, instance := range active {
		snapshot, found := p.directory.Runner(ctx, instance.RunnerID)
		idle := found && snapshot.LastHeartbeat.Before(cutoff)
		if !found {
			idle = instance.LastEnrolledAt.Before(cutoff)
		}
		if !idle {
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

	draining, err := p.pools.ListInstancesByState(ctx, store.InstanceDraining)
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
