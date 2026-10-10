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

// RunnerClaimPollObserver is the optional RunnerClaimObserver extension that
// receives the cost of one claim poll's queue walk: how many queue targets
// were actually walked (non-empty after the depth probe) and how many Redis
// round trips the walk issued, the probe included. Each target walks under
// its own page/scan/attempt budget, so the worst-case poll cost grows with
// the number of non-empty targets; this is the observation that shows
// whether that worst case is ever approached before a poll-wide budget is
// introduced. A poll that failed part-way is not observed.
type RunnerClaimPollObserver interface {
	OnRunnerClaimPoll(ctx context.Context, targets, roundTrips int)
}

func (d *RedisRunnerDirectory) observeClaimPoll(ctx context.Context, targets, roundTrips int) {
	observer, ok := d.observer.(RunnerClaimPollObserver)
	if !ok {
		return
	}
	defer func() { _ = recover() }()
	observer.OnRunnerClaimPoll(ctx, targets, roundTrips)
}
