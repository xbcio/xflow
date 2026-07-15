package control

// RunnerClaimObserver receives durable runner-directory recovery events.
// Implementations must be non-blocking and must not use runner, claim, lease,
// or execution IDs as metric labels.
type RunnerClaimObserver interface {
	OnRunnerClaimReclaimed(count int)
	OnRunnerLeaseReplayed()
}

func (d *RedisRunnerDirectory) observeClaimReclaimed(count int) {
	if d.observer == nil || count <= 0 {
		return
	}
	defer func() { _ = recover() }()
	d.observer.OnRunnerClaimReclaimed(count)
}

func (d *RedisRunnerDirectory) observeLeaseReplay() {
	if d.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	d.observer.OnRunnerLeaseReplayed()
}
