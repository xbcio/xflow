package local

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// idempotencyClaim is one keyed submission's claim on its execution ID.
type idempotencyClaim struct {
	fingerprint engine.IdempotencyFingerprint
	token       string
	confirmed   bool
	// leaseUntil bounds how long a pending claim is presumed to belong to a
	// request that is still creating its execution.
	leaseUntil time.Time
	// expiresAt is when the claim stops answering for its execution. Expiry is
	// lazy: a claim past it is treated as absent by the next request.
	expiresAt time.Time
}

var _ engine.IdempotentStateStore = (*memoryState)(nil)

// ClaimIdempotency decides one keyed request under the store's single mutex, so
// the claim and the read of the execution it names cannot interleave with a
// creation.
func (s *memoryState) ClaimIdempotency(_ context.Context, req engine.IdempotencyClaimRequest) (engine.IdempotencyClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	existing := s.idempotency[req.ExecutionID]
	if existing != nil && !existing.expiresAt.IsZero() && !now.Before(existing.expiresAt) {
		delete(s.idempotency, req.ExecutionID)
		existing = nil
	}
	exec, created := s.executions[req.ExecutionID]
	status := types.ExecutionStatus("")
	if created {
		status = exec.snap.Status
	}

	if existing == nil {
		if created {
			// The execution outlived its claim. Creating it again would write
			// over it in place, so it is the answer, whatever this request is.
			return engine.IdempotencyClaim{State: engine.IdempotencyClaimDuplicate, Status: status}, nil
		}
		s.putIdempotencyClaimLocked(req, now)
		return engine.IdempotencyClaim{State: engine.IdempotencyClaimAccepted}, nil
	}
	if existing.fingerprint != req.Fingerprint {
		return engine.IdempotencyClaim{State: engine.IdempotencyClaimConflict}, nil
	}
	if existing.confirmed || created {
		return engine.IdempotencyClaim{State: engine.IdempotencyClaimDuplicate, Status: status}, nil
	}
	if now.Before(existing.leaseUntil) {
		return engine.IdempotencyClaim{State: engine.IdempotencyClaimInProgress}, nil
	}
	s.putIdempotencyClaimLocked(req, now)
	return engine.IdempotencyClaim{State: engine.IdempotencyClaimTakeover}, nil
}

func (s *memoryState) putIdempotencyClaimLocked(req engine.IdempotencyClaimRequest, now time.Time) {
	if s.idempotency == nil {
		s.idempotency = make(map[types.ExecutionID]*idempotencyClaim)
	}
	claim := &idempotencyClaim{
		fingerprint: req.Fingerprint,
		token:       req.Token,
		leaseUntil:  now.Add(req.Lease),
	}
	if req.TTL > 0 {
		claim.expiresAt = now.Add(req.TTL)
	}
	s.idempotency[req.ExecutionID] = claim
}

// ConfirmIdempotency marks the claim held under token as having created its
// execution.
func (s *memoryState) ConfirmIdempotency(_ context.Context, id types.ExecutionID, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if claim := s.idempotency[id]; claim != nil && claim.token == token {
		claim.confirmed = true
	}
	return nil
}

// ReleaseIdempotency frees a pending claim held under token.
func (s *memoryState) ReleaseIdempotency(_ context.Context, id types.ExecutionID, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if claim := s.idempotency[id]; claim != nil && claim.token == token && !claim.confirmed {
		delete(s.idempotency, id)
	}
	return nil
}
