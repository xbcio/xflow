package asynq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// timeoutZSetKey is the Redis ZSET key used to track node timeout deadlines.
// Members are "executionID\x00nodeName", scores are Unix timestamps.
const timeoutZSetKey = "xflow:timeouts"

// popExpiredLua atomically pops up to ARGV[2] members whose score <= ARGV[1].
var popExpiredLua = redis.NewScript(`
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #expired > 0 then
    redis.call('ZREM', KEYS[1], unpack(expired))
end
return expired
`)

// TimeoutMonitor polls a Redis ZSET for expired node timeouts and delivers
// timeout signals to the engine. This replaces the previous MySQL-polling approach.
type TimeoutMonitor struct {
	rdb      redis.Cmdable
	engine   *engine.Engine
	hooks    engine.Hooks
	logger   engine.Logger
	interval time.Duration
	stop     chan struct{}
	once     sync.Once
}

// NewTimeoutMonitor creates a ZSET-based timeout monitor.
// interval controls how often the ZSET is polled (default 5s if <= 0).
func NewTimeoutMonitor(rdb redis.Cmdable, engine *engine.Engine, hooks engine.Hooks, logger engine.Logger, interval time.Duration) *TimeoutMonitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &TimeoutMonitor{
		rdb:      rdb,
		engine:   engine,
		hooks:    hooks,
		logger:   logger,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Run starts the polling loop. It blocks until Stop is called.
func (m *TimeoutMonitor) Run() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.processTimeouts(now)
		}
	}
}

// Stop signals the monitor to exit its polling loop. Safe to call multiple times.
func (m *TimeoutMonitor) Stop() { m.once.Do(func() { close(m.stop) }) }

// processTimeouts pops expired entries from the ZSET and delivers timeout signals.
func (m *TimeoutMonitor) processTimeouts(now time.Time) {
	ctx := context.Background()
	results, err := popExpiredLua.Run(ctx, m.rdb,
		[]string{timeoutZSetKey},
		fmt.Sprintf("%d", now.Unix()), "100",
	).StringSlice()
	if err != nil && err != redis.Nil {
		if m.logger != nil {
			m.logger.Error("timeout monitor: popExpired failed", "error", err)
		}
		return
	}

	for _, member := range results {
		parts := strings.SplitN(member, "\x00", 2)
		if len(parts) != 2 {
			continue
		}
		execID := types.ExecutionID(parts[0])
		nodeName := parts[1]

		// Fire the OnNodeTimeout hook.
		if m.hooks != nil {
			engine.SafeHook(ctx, m.logger, func(ctx context.Context) {
				m.hooks.OnNodeTimeout(ctx, execID, nodeName)
			})
		}

		// Directly enqueue a timeout resume task, bypassing signal name matching.
		_ = m.engine.TimeoutNode(ctx, execID, nodeName)
	}
}

// timeoutMember builds the ZSET member string for a given execution + node.
func timeoutMember(id types.ExecutionID, nodeName string) string {
	return string(id) + "\x00" + nodeName
}
