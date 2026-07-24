package control

import "context"

// RunnerClaimObserver receives durable runner-directory recovery events.
// Implementations must be non-blocking and must not use runner, claim, lease,
// or execution IDs as metric labels.
type RunnerClaimObserver interface {
	OnRunnerClaimReclaimed(ctx context.Context, count int)
	OnRunnerLeaseReplayed(ctx context.Context)
}

func (d *RedisRunnerDirectory) observeClaimReclaimed(ctx context.Context, count int) {
	if d.observer == nil || count <= 0 {
		return
	}
	defer func() { _ = recover() }()
	d.observer.OnRunnerClaimReclaimed(ctx, count)
}

func (d *RedisRunnerDirectory) observeLeaseReplay(ctx context.Context) {
	if d.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	d.observer.OnRunnerLeaseReplayed(ctx)
}
