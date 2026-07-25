package timeout

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/distributed/internal/rstate"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// timeoutKeyPattern is the SCAN glob for per-tenant, per-execution timeout
// ZSETs. Each execution owns a sharded key
// xflow:t<tenant>:exec:{<id>}:timeouts (sharing the execution hash tag) so the
// global timeout workload is distributed across Redis Cluster slots instead of
// funneling onto a single hot key, and one tenant's scan never crosses into
// another tenant's keys. The monitor iterates the tenant registry so a SCAN
// never crosses a tenant boundary.
const timeoutKeyPattern = "xflow:t%s:exec:{*}:timeouts"

// timeoutScanCount is the page size used when scanning for timeout keys.
const timeoutScanCount = int64(128)

// timeoutRetryBackoff is applied when delivering a timeout signal fails. The
// member is re-added to the ZSET with this delay so the next poll retries
// delivery instead of permanently dropping the timeout (which would leave the
// suspended node stranded until execution TTL expiry).
const timeoutRetryBackoff = 5 * time.Second

// peekExpiredLua returns up to ARGV[2] members whose score <= ARGV[1] WITHOUT
// removing them. Removal happens after confirmed delivery, giving at-least-once
// semantics: a crash between peek and delivery simply re-delivers on the next
// poll rather than permanently losing the timeout.
var peekExpiredLua = redis.NewScript(`
return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
`)

// Monitor polls a Redis ZSET for expired node timeouts and delivers
// timeout signals to the engine. This replaces the previous MySQL-polling approach.
type Monitor struct {
	rdb      redis.Cmdable
	engine   *engine.Engine
	hooks    engine.Hooks
	logger   engine.Logger
	interval time.Duration
	stop     chan struct{}
	cancel   context.CancelFunc // cancels the ctx used by processTimeouts (#8b)
	once     sync.Once
}

// New creates a ZSET-based timeout monitor.
// interval controls how often the ZSET is polled (default 5s if <= 0).
// If logger is nil a default slog-based logger is used so errors are never
// silently swallowed (#6).
func New(rdb redis.Cmdable, engine *engine.Engine, hooks engine.Hooks, logger engine.Logger, interval time.Duration) *Monitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if logger == nil {
		logger = slogAdapter{}
	}
	return &Monitor{
		rdb:      rdb,
		engine:   engine,
		hooks:    hooks,
		logger:   logger,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Run starts the polling loop. It blocks until Stop is called.
func (m *Monitor) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			cancel()
			return
		case now := <-ticker.C:
			m.processTimeouts(ctx, now)
		}
	}
}

// Stop signals the monitor to exit its polling loop and cancels any in-flight
// SCAN/pop operations (#8b). Safe to call multiple times.
func (m *Monitor) Stop() {
	m.once.Do(func() {
		close(m.stop)
		if m.cancel != nil {
			m.cancel()
		}
	})
}

// processTimeouts scans every per-execution timeout ZSET, pops expired entries,
// and delivers timeout signals. It iterates the tenant registry so a SCAN never
// crosses a tenant boundary; within a tenant it scans sharded keys
// (xflow:t<tenant>:exec:{id}:timeouts) instead of one global key to avoid a
// single Redis Cluster slot becoming a write hotspot for every
// suspend/deliver/resume in the system.
func (m *Monitor) processTimeouts(ctx context.Context, now time.Time) {
	nowUnix := fmt.Sprintf("%d", now.Unix())

	tenants, err := rstate.ListTenants(ctx, m.rdb)
	if err != nil {
		if m.logger != nil {
			m.logger.Error("timeout monitor: list tenants failed", "error", err)
		}
		return
	}
	for _, t := range tenants {
		m.processTimeoutsForTenant(ctx, t, now, nowUnix)
	}
}

func (m *Monitor) processTimeoutsForTenant(ctx context.Context, t tenant.TenantID, now time.Time, nowUnix string) {
	var cursor uint64
	seen := make(map[string]struct{})
	pattern := fmt.Sprintf(timeoutKeyPattern, t)
	for {
		keys, next, err := m.rdb.Scan(ctx, cursor, pattern, timeoutScanCount).Result()
		if err != nil && err != redis.Nil {
			if m.logger != nil {
				m.logger.Error("timeout monitor: scan timeout keys failed", "tenant", string(t), "error", err)
			}
			return
		}
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			m.processTimeoutKey(ctx, t, key, now, nowUnix)
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

// processTimeoutKey peeks expired members from a single execution's timeout
// ZSET and delivers them. Members are removed only after confirmed successful
// delivery, giving at-least-once semantics: a crash or lock-contention failure
// leaves the member in the ZSET for retry on the next poll (#8).
func (m *Monitor) processTimeoutKey(ctx context.Context, t tenant.TenantID, key string, now time.Time, nowUnix string) {
	results, err := peekExpiredLua.Run(ctx, m.rdb,
		[]string{key},
		nowUnix, "100",
	).StringSlice()
	if err != nil && err != redis.Nil {
		if m.logger != nil {
			m.logger.Error("timeout monitor: peekExpired failed", "key", key, "error", err)
		}
		return
	}

	for _, member := range results {
		parts := strings.SplitN(member, "\x00", 2)
		if len(parts) != 2 {
			// Malformed member — remove to avoid infinite retry loop.
			m.rdb.ZRem(ctx, key, member)
			continue
		}
		execID := types.ExecutionID(parts[0])
		nodeName := parts[1]

		// Inject the tenant that owns this timeout key into the context so the
		// engine processes the resume under the correct tenant scope: the
		// store's keys are tenant-prefixed and would not be found under a
		// different tenant.
		tenantCtx := tenant.WithTenant(ctx, t)

		// Fire the OnNodeTimeout hook.
		if m.hooks != nil {
			engine.SafeHook(tenantCtx, m.logger, func(ctx context.Context) {
				m.hooks.OnNodeTimeout(ctx, execID, nodeName)
			})
		}

		// Directly enqueue a timeout resume task, bypassing signal name matching.
		if err := m.engine.TimeoutNode(tenantCtx, execID, nodeName); err != nil {
			// Before requeuing, verify the execution/node is still in a state
			// worth retrying. A terminal or canceled execution/node must not
			// be re-added every 5s — that would wedge a dead member into the
			// ZSET forever. Only transient delivery failures (engine busy,
			// outbox write hiccup) justify a backoff retry.
			if m.isTimeoutTargetDead(tenantCtx, t, execID, nodeName) {
				if m.logger != nil {
					m.logger.Warnf("timeout monitor: delivery failed for terminal/canceled target, removing", "execution_id", execID, "node", nodeName, "error", err)
				}
				// Target is dead — remove from ZSET to prevent infinite retries.
				m.rdb.ZRem(ctx, key, member)
				continue
			}
			// Transient failure: bump the score to retry after backoff. The member
			// stays in the ZSET (never removed) so it is not lost.
			retryAt := now.Add(timeoutRetryBackoff).Unix()
			if requeueErr := m.rdb.ZAdd(ctx, key, redis.Z{
				Score:  float64(retryAt),
				Member: member,
			}).Err(); requeueErr != nil && m.logger != nil {
				m.logger.Error("timeout monitor: requeue failed", "execution_id", execID, "node", nodeName, "error", requeueErr)
			}
			if m.logger != nil {
				m.logger.Warnf("timeout monitor: delivery failed, will retry", "execution_id", execID, "node", nodeName, "retry_at", retryAt, "error", err)
			}
			continue
		}

		// Delivery confirmed successful — remove from ZSET. This is the only
		// path that removes a member, ensuring at-least-once delivery (#8).
		if remErr := m.rdb.ZRem(ctx, key, member).Err(); remErr != nil && m.logger != nil {
			m.logger.Error("timeout monitor: ZREM after delivery failed (will re-deliver next poll, idempotent)", "execution_id", execID, "node", nodeName, "error", remErr)
		}
	}
}

// isTimeoutTargetDead reports whether the execution or node is no longer in a
// state where a timeout retry is meaningful: a terminal/canceled execution, or
// a node that is no longer suspended (it has already been resumed, completed,
// canceled, or never parked). When true the monitor drops the member instead
// of re-adding it on every poll.
func (m *Monitor) isTimeoutTargetDead(ctx context.Context, t tenant.TenantID, execID types.ExecutionID, nodeName string) bool {
	execStatus, err := m.rdb.Get(ctx, rstate.ExecKey(t, execID, "status")).Result()
	if err == nil {
		switch types.ExecutionStatus(execStatus) {
		case types.ExecutionStatusSuccess, types.ExecutionStatusFailed,
			types.ExecutionStatusCanceled, types.ExecutionStatusTimeout:
			return true
		}
	}
	nodeStatus, err := m.rdb.Get(ctx, rstate.NodeStatusKey(t, execID, nodeName)).Result()
	if err == nil {
		switch types.NodeStatus(nodeStatus) {
		case types.NodeStatusSuccess, types.NodeStatusFailed,
			types.NodeStatusSkipped, types.NodeStatusCanceled,
			types.NodeStatusContinued, types.NodeStatusRunning,
			types.NodeStatusCommitting, types.NodeStatusWaiting,
			types.NodeStatusPending:
			return true
		}
	}
	return false
}

// slogAdapter is a minimal engine.Logger backed by slog.Default(), used as a
// fallback when no explicit logger is provided so errors are never silently
// swallowed (#6).
type slogAdapter struct{}

func (slogAdapter) Debug(msg string, args ...any)  { slog.Debug(msg, args...) }
func (slogAdapter) Debugf(format string, args ...any) {
	slog.Debug(fmt.Sprintf(format, args...))
}
func (slogAdapter) Info(msg string, args ...any) { slog.Info(msg, args...) }
func (slogAdapter) Infof(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}
func (slogAdapter) Warn(msg string, args ...any) { slog.Warn(msg, args...) }
func (slogAdapter) Warnf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args...))
}
func (slogAdapter) Error(msg string, args ...any) { slog.Error(msg, args...) }
func (slogAdapter) Errorf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args...))
}
func (slogAdapter) Panic(msg string, args ...any) { slog.Error(msg, args...) }
func (slogAdapter) Panicf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args...))
}
