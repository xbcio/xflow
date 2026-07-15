package asynq

import "time"

// LeaseObserver receives Redis lease lifecycle observations. Implementations
// must be non-blocking and must not retain execution, lease, or runner IDs as
// metric labels.
type LeaseObserver interface {
	OnLeaseAcquire(result string, elapsed time.Duration)
	OnLeaseExpiryScan(candidates int, elapsed time.Duration, err error)
	OnLeaseRepair(reconciled int, elapsed time.Duration, err error)
}

func (s *redisState) observeLeaseAcquire(result string, elapsed time.Duration) {
	if s.leaseObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.leaseObserver.OnLeaseAcquire(result, elapsed)
}

func (s *redisState) observeLeaseExpiryScan(candidates int, elapsed time.Duration, err error) {
	if s.leaseObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.leaseObserver.OnLeaseExpiryScan(candidates, elapsed, err)
}

func (s *redisState) observeLeaseRepair(reconciled int, elapsed time.Duration, err error) {
	if s.leaseObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.leaseObserver.OnLeaseRepair(reconciled, elapsed, err)
}
