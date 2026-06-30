package asynq

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/types"
)

type triggerPrimitives struct {
	rdb *redis.Client
}

func newTriggerPrimitives(rdb *redis.Client) *triggerPrimitives {
	return &triggerPrimitives{rdb: rdb}
}

func triggerDedupKey(key string) string { return "xflow:trigger:dedup:" + key }

func triggerLockKey(key string) string { return "xflow:trigger:lock:" + key }

func triggerStateKey(scope, key string) string {
	return "xflow:trigger:state:" + scope + ":" + key
}

func (p *triggerPrimitives) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return p.rdb.SetNX(ctx, triggerDedupKey(key), "1", ttl).Result()
}

func (p *triggerPrimitives) TryLock(ctx context.Context, key string, ttl time.Duration) (types.TriggerLock, bool, error) {
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

func (p *triggerPrimitives) State(_ context.Context, scope string) types.TriggerState {
	return &triggerState{rdb: p.rdb, scope: scope}
}

type triggerLock struct {
	rdb   *redis.Client
	key   string
	token string
}

var releaseTriggerLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

var renewTriggerLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

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
	rdb   *redis.Client
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
