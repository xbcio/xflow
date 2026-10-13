package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var _ engine.IdempotentStateStore = (*Store)(nil)

// idempotencyKey holds a keyed submission's claim. It is an execution key, so
// it shares the {execID} hash tag with the execution's status key and one
// script can decide the claim and read the execution atomically.
func idempotencyKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "idem")
}

// idempotencyClaimLua decides one keyed request.
//
// KEYS[1] = claim hash, KEYS[2] = execution status key
// ARGV[1] = fingerprint, ARGV[2] = token, ARGV[3] = now (unix ms),
// ARGV[4] = lease (ms), ARGV[5] = claim TTL (ms, 0 = no expiry)
// Returns {state, status}: state is one of the engine's IdempotencyClaimState
// values, and status is the existing execution's status on a duplicate.
//
// The status key is read in the same script as the claim, which is what makes
// "the key is free" and "the key's pending claim is abandoned" safe answers:
// neither can be given while the execution exists. An execution that outlived
// its claim (a queue wait or a suspension extended it) is a duplicate whatever
// the request is, because creating it again would overwrite it in place.
var idempotencyClaimLua = redis.NewScript(`
local held = redis.call('HMGET', KEYS[1], 'fp', 'token', 'confirmed', 'lease')
local status = redis.call('GET', KEYS[2])
local now = tonumber(ARGV[3])
local function claim()
    redis.call('DEL', KEYS[1])
    redis.call('HSET', KEYS[1], 'fp', ARGV[1], 'token', ARGV[2], 'confirmed', '0',
        'lease', now + tonumber(ARGV[4]))
    local ttl = tonumber(ARGV[5])
    if ttl > 0 then
        redis.call('PEXPIRE', KEYS[1], ttl)
    end
end
if not held[1] then
    if status then
        return {'duplicate', status}
    end
    claim()
    return {'accepted', ''}
end
if held[1] ~= ARGV[1] then
    return {'conflict', ''}
end
if held[3] == '1' or status then
    return {'duplicate', status or ''}
end
if now < tonumber(held[4] or '0') then
    return {'in_progress', ''}
end
claim()
return {'claimed_pending', ''}
`)

// idempotencyConfirmLua marks the claim held under ARGV[1] as confirmed.
// KEYS[1] = claim hash.
var idempotencyConfirmLua = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'token') == ARGV[1] then
    redis.call('HSET', KEYS[1], 'confirmed', '1')
    return 1
end
return 0
`)

// idempotencyReleaseLua deletes the pending claim held under ARGV[1]. A
// confirmed claim is never released: its execution exists.
// KEYS[1] = claim hash.
var idempotencyReleaseLua = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'token') == ARGV[1] and redis.call('HGET', KEYS[1], 'confirmed') ~= '1' then
    return redis.call('DEL', KEYS[1])
end
return 0
`)

// ClaimIdempotency decides one keyed request atomically with the read of the
// execution it names.
func (s *Store) ClaimIdempotency(ctx context.Context, req engine.IdempotencyClaimRequest) (engine.IdempotencyClaim, error) {
	t := namespace.FromContext(ctx)
	reply, err := idempotencyClaimLua.Run(ctx, s.rdb,
		[]string{idempotencyKey(t, req.ExecutionID), execKey(t, req.ExecutionID, "status")},
		string(req.Fingerprint), req.Token, time.Now().UnixMilli(),
		req.Lease.Milliseconds(), s.idempotencyClaimTTL(req.TTL).Milliseconds(),
	).StringSlice()
	if err != nil {
		return engine.IdempotencyClaim{}, fmt.Errorf("claim idempotency for %q: %w", req.ExecutionID, err)
	}
	if len(reply) != 2 {
		return engine.IdempotencyClaim{}, fmt.Errorf("claim idempotency for %q: unexpected reply %v", req.ExecutionID, reply)
	}
	return engine.IdempotencyClaim{
		State:  engine.IdempotencyClaimState(reply[0]),
		Status: types.ExecutionStatus(reply[1]),
	}, nil
}

// idempotencyClaimTTL caps the requested window at the retention a new
// execution gets in this store, so a claim never answers for an execution that
// has already expired. The engine has already capped it at what it knows (a
// per-execution TTL override, a transient graph's TTL); the store-wide
// retention is the part only the store knows. A zero retention is unbounded.
func (s *Store) idempotencyClaimTTL(requested time.Duration) time.Duration {
	retention := s.execTTL
	if s.transient && s.transientTTL > 0 {
		retention = s.transientTTL
	}
	if requested <= 0 || (retention > 0 && retention < requested) {
		return retention
	}
	return requested
}

// ConfirmIdempotency marks the claim held under token as having created its
// execution.
func (s *Store) ConfirmIdempotency(ctx context.Context, id types.ExecutionID, token string) error {
	t := namespace.FromContext(ctx)
	if err := idempotencyConfirmLua.Run(ctx, s.rdb, []string{idempotencyKey(t, id)}, token).Err(); err != nil {
		return fmt.Errorf("confirm idempotency for %q: %w", id, err)
	}
	return nil
}

// ReleaseIdempotency frees a pending claim held under token.
func (s *Store) ReleaseIdempotency(ctx context.Context, id types.ExecutionID, token string) error {
	t := namespace.FromContext(ctx)
	if err := idempotencyReleaseLua.Run(ctx, s.rdb, []string{idempotencyKey(t, id)}, token).Err(); err != nil {
		return fmt.Errorf("release idempotency for %q: %w", id, err)
	}
	return nil
}
