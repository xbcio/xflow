package rstate

import (
	"context"
	"time"
)

// LeaseObserver receives Redis lease lifecycle observations. Implementations
// must be non-blocking and must not retain execution, lease, or runner IDs as
// metric labels.
type LeaseObserver interface {
	OnLeaseAcquire(ctx context.Context, result string, elapsed time.Duration)
	OnLeaseExpiryScan(ctx context.Context, candidates int, elapsed time.Duration, err error)
	OnLeaseRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error)
}

func (s *Store) observeLeaseAcquire(ctx context.Context, result string, elapsed time.Duration) {
	if s.leaseObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.leaseObserver.OnLeaseAcquire(ctx, result, elapsed)
}

func (s *Store) observeLeaseExpiryScan(ctx context.Context, candidates int, elapsed time.Duration, err error) {
	if s.leaseObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.leaseObserver.OnLeaseExpiryScan(ctx, candidates, elapsed, err)
}

func (s *Store) observeLeaseRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error) {
	if s.leaseObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.leaseObserver.OnLeaseRepair(ctx, reconciled, elapsed, err)
}
