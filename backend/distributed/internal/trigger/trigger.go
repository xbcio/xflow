package trigger

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/types"
)

type Primitives struct {
	rdb redis.UniversalClient
}

func New(rdb redis.UniversalClient) *Primitives {
	return &Primitives{rdb: rdb}
}

// Key schema (Redis Cluster-safe, G2 Phase 2 / Task 2.2).
//
// All Redis operations in this package are single-key. There is no Lua script
// that touches more than one key, so no hash tag is required for cluster
// safety. Each primitive owns a distinct key prefix:
//
//	xflow:trigger:dedup:<key>       -> dedup marker (SetNX, single-key)
//	xflow:trigger:lock:<key>        -> distributed lock value (SetNX + Lua)
//	xflow:trigger:state:<scope>:<key> -> scoped state payload (Get/Set/Del)
//
// The two Lua scripts (release/renew trigger lock) only reference KEYS[1],
// which is the lock key for the same <key>. They are therefore cluster-safe
// without hash tags.
//
// Tenant prefix is reserved for Task 7.2 (Phase 6/7). When a tenant prefix is
// added, the expected shape is `xflow:{tenant}:trigger:dedup:<key>` etc.,
// keeping the per-key operation model unchanged. The hash tag for any future
// multi-key Lua must be anchored on a scope+key that all KEYS share, e.g.
// `{<scope>:<key>}` or `{<key>}`.
func triggerDedupKey(key string) string { return "xflow:trigger:dedup:" + key }

func triggerLockKey(key string) string { return "xflow:trigger:lock:" + key }

func triggerStateKey(scope, key string) string {
	return "xflow:trigger:state:" + scope + ":" + key
}

func (p *Primitives) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return p.rdb.SetNX(ctx, triggerDedupKey(key), "1", ttl).Result()
}

func (p *Primitives) TryLock(ctx context.Context, key string, ttl time.Duration) (types.TriggerLock, bool, error) {
	if ttl <= 0 {
		ttl = time.Minute
	}
	token := uuid.NewString()
	ok, err := p.rdb.SetNX(ctx, triggerLockKey(key), token, ttl).Result()
	if err != nil || !ok {
		return nil, ok, err
	}
	return &triggerLock{rdb: p.rdb, key: triggerLockKey(key), token: token}, true, nil
}

func (p *Primitives) State(_ context.Context, scope string) types.TriggerState {
	return &triggerState{rdb: p.rdb, scope: scope}
}

type triggerLock struct {
	rdb   redis.UniversalClient
	key   string
	token string
}

const (
	releaseTriggerLockScriptSrc = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`
	renewTriggerLockScriptSrc = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`
)

var releaseTriggerLockScript = redis.NewScript(releaseTriggerLockScriptSrc)

var renewTriggerLockScript = redis.NewScript(renewTriggerLockScriptSrc)

func (l *triggerLock) Renew(ctx context.Context, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = time.Minute
	}
	ttlMillis := ttl.Milliseconds()
	if ttl > 0 && ttlMillis == 0 {
		ttlMillis = 1
	}
	renewed, err := renewTriggerLockScript.Run(
		ctx,
		l.rdb,
		[]string{l.key},
		l.token,
		ttlMillis,
	).Int64()
	if err != nil {
		return false, err
	}
	return renewed == 1, nil
}

func (l *triggerLock) Release(ctx context.Context) error {
	return releaseTriggerLockScript.Run(ctx, l.rdb, []string{l.key}, l.token).Err()
}

type triggerState struct {
	rdb   redis.UniversalClient
	scope string
}

func (s *triggerState) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := s.rdb.Get(ctx, triggerStateKey(s.scope, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}

func (s *triggerState) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl > 0 {
		return s.rdb.Set(ctx, triggerStateKey(s.scope, key), value, ttl).Err()
	}
	return s.rdb.Set(ctx, triggerStateKey(s.scope, key), value, 0).Err()
}

func (s *triggerState) Delete(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, triggerStateKey(s.scope, key)).Err()
}
