package timeout

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/rstate"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// timeoutScanCount is the SCAN page size of the safety-net walk.
const timeoutScanCount = int64(128)

// timeoutDueBatch bounds one index read; timeoutDueMaxBatches bounds how many
// batches a single tick drains so one namespace cannot monopolise the loop.
const (
	timeoutDueBatch      = int64(500)
	timeoutDueMaxBatches = 10
)

// Safety-net walk. The timeout index (rstate/timeout_index.go) is written
// best-effort after each timeout write, so a registration lost to process
// death, or data written before the index existed, is only found by walking
// the keyspace for `exec:{*}:timeouts`. That walk used to run in full every
// tick on every replica (46% of Redis command time at 2.35M keys). It is now
// incremental with a persistent per-namespace cursor:
//
//   - Backfill: until the first full round after process start completes, every
//     tick advances up to timeoutBackfillPagesPerTick pages, so pre-existing
//     timeouts are registered quickly after a deploy.
//   - Steady state: one burst every timeoutWalkEvery, sized (from DBSIZE) so a
//     full round takes about timeoutWalkRound. A tick between bursts issues no
//     SCAN at all.
//
// Staleness bound for a lost registration: one round, about timeoutWalkRound
// (10 minutes) plus up to one burst interval; a timeout in that window fires
// late by at most that bound, never not at all. Keys the walk finds are
// registered with ZADD NX at score 0 (a valid lower bound, due immediately in
// this tick) so an existing exact score is not disturbed.
const (
	timeoutBackfillPagesPerTick = 2048
	timeoutWalkEvery            = 30 * time.Second
	timeoutWalkRound            = 10 * time.Minute
	timeoutWalkMaxBurstPages    = 4096
)

// walkState is one namespace's safety-net walk progress.
type walkState struct {
	cursors    redisx.Cursors
	backfilled bool
	lastBurst  time.Time
}

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
	mu       sync.Mutex
	once     sync.Once

	// ownedNamespaces scopes processTimeouts when non-nil: the monitor scans
	// exactly this set instead of the global namespace registry. nil preserves
	// legacy behavior (registry + default injection). Written once via
	// SetNamespaces before Run; read-only afterwards, hence no lock.
	ownedNamespaces []namespace.Namespace

	walkMu sync.Mutex
	walks  map[namespace.Namespace]*walkState
}

// SetNamespaces scopes the monitor to exactly the given set. It is the
// monitor-side counterpart of rstate's owned-namespace scope: a process that
// owns only its own namespaces must not scan or deliver timeouts for another
// deployment's namespaces even on a shared Redis. Pass a normalized set
// (namespace.NormalizeOwned); nil restores legacy behavior. Call it before
// Run starts polling. The set is copied, so later mutation of the caller's
// slice does not retroactively change the monitor's scope.
func (m *Monitor) SetNamespaces(owned []namespace.Namespace) {
	if owned == nil {
		m.ownedNamespaces = nil
		return
	}
	m.ownedNamespaces = make([]namespace.Namespace, len(owned))
	copy(m.ownedNamespaces, owned)
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
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
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
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.cancel != nil {
			m.cancel()
		}
	})
}

// processTimeouts scans every per-execution timeout ZSET, pops expired entries,
// and delivers timeout signals. It iterates the namespace registry so a SCAN never
// crosses a namespace boundary; within a namespace it scans sharded keys
// (xflow:ns:<namespace>:exec:{id}:timeouts) instead of one global key to avoid a
// single Redis Cluster slot becoming a write hotspot for every
// suspend/deliver/resume in the system.
func (m *Monitor) processTimeouts(ctx context.Context, now time.Time) {
	nowUnix := fmt.Sprintf("%d", now.Unix())

	namespaces, err := m.scopedNamespaces(ctx)
	if err != nil {
		if m.logger != nil {
			m.logger.Error("timeout monitor: list namespaces failed", "error", err)
		}
		return
	}
	for _, t := range namespaces {
		m.processTimeoutsForNamespace(ctx, t, now, nowUnix)
	}
}

// scopedNamespaces mirrors rstate's owned-namespace scope: a non-nil owned
// set is iterated directly (no registry read, no default injection); nil
// falls back to the shared registry exactly as before.
func (m *Monitor) scopedNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	if m.ownedNamespaces != nil {
		out := make([]namespace.Namespace, len(m.ownedNamespaces))
		copy(out, m.ownedNamespaces)
		return out, nil
	}
	return rstate.ListNamespaces(ctx, m.rdb)
}

func (m *Monitor) processTimeoutsForNamespace(ctx context.Context, t namespace.Namespace, now time.Time, nowUnix string) {
	m.walkSafetyNet(ctx, t, now)

	// Drain due executions from the namespace index. An execution processed
	// this tick is re-armed to its exact next timeout, so it is only seen again
	// within the tick when it still has due members beyond the peek limit; seen
	// stops that from spinning on one execution.
	seen := make(map[types.ExecutionID]struct{})
	for batch := 0; batch < timeoutDueMaxBatches; batch++ {
		ids, err := rstate.DueTimeoutExecutions(ctx, m.rdb, t, now, timeoutDueBatch)
		if err != nil {
			if m.logger != nil {
				m.logger.Error("timeout monitor: read timeout index failed", "namespace", string(t), "error", err)
			}
			return
		}
		fresh := 0
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			fresh++
			m.processTimeoutKey(ctx, t, rstate.TimeoutZSetKey(t, id), now, nowUnix)
			if err := rstate.RefreshTimeoutIndex(ctx, m.rdb, t, id); err != nil && m.logger != nil {
				m.logger.Error("timeout monitor: refresh timeout index failed", "namespace", string(t), "execution_id", string(id), "error", err)
			}
		}
		if fresh == 0 || int64(len(ids)) < timeoutDueBatch {
			return
		}
	}
}

// walkSafetyNet advances the incremental keyspace walk for one namespace and
// registers every timeout ZSET it meets into the index (see the constants
// above for cadence and staleness bound).
func (m *Monitor) walkSafetyNet(ctx context.Context, t namespace.Namespace, now time.Time) {
	m.walkMu.Lock()
	defer m.walkMu.Unlock()
	if m.walks == nil {
		m.walks = make(map[namespace.Namespace]*walkState)
	}
	st := m.walks[t]
	if st == nil {
		st = &walkState{}
		m.walks[t] = st
	}

	pages := timeoutBackfillPagesPerTick
	if st.backfilled {
		if now.Sub(st.lastBurst) < timeoutWalkEvery {
			return
		}
		pages = m.steadyBurstPages(ctx)
	}
	st.lastBurst = now

	pattern := rstate.TimeoutScanPattern(t)
	for i := 0; i < pages; i++ {
		keys, next, err := redisx.ScanPage(ctx, m.rdb, st.cursors, pattern, timeoutScanCount)
		if err != nil {
			if m.logger != nil {
				m.logger.Error("timeout monitor: scan timeout keys failed", "namespace", string(t), "error", err)
			}
			return
		}
		m.registerWalkedKeys(ctx, t, keys)
		st.cursors = next
		done := true
		for _, c := range next {
			if c != 0 {
				done = false
			}
		}
		if done {
			st.cursors = nil
			st.backfilled = true
			return
		}
	}
}

// steadyBurstPages sizes a steady-state burst so a round takes about
// timeoutWalkRound: total pages (DBSIZE / page size) spread over the bursts in
// a round. DBSIZE on a cluster client reaches one master only, which is a
// good-enough estimate here; on error one page still makes progress.
func (m *Monitor) steadyBurstPages(ctx context.Context) int {
	size, err := m.rdb.DBSize(ctx).Result()
	if err != nil || size <= 0 {
		return 1
	}
	totalPages := size/timeoutScanCount + 1
	bursts := int64(timeoutWalkRound / timeoutWalkEvery)
	pages := (totalPages + bursts - 1) / bursts
	if pages > timeoutWalkMaxBurstPages {
		pages = timeoutWalkMaxBurstPages
	}
	return int(pages)
}

func (m *Monitor) registerWalkedKeys(ctx context.Context, t namespace.Namespace, keys []string) {
	if len(keys) == 0 {
		return
	}
	pipe := m.rdb.Pipeline()
	n := 0
	for _, key := range keys {
		id, ok := rstate.TimeoutExecutionID(t, key)
		if !ok {
			continue
		}
		pipe.ZAddNX(ctx, rstate.TimeoutIndexKey(t), redis.Z{Score: 0, Member: string(id)})
		n++
	}
	if n == 0 {
		return
	}
	if _, err := pipe.Exec(ctx); err != nil && m.logger != nil {
		m.logger.Error("timeout monitor: register walked timeout keys failed", "namespace", string(t), "error", err)
	}
}

// processTimeoutKey peeks expired members from a single execution's timeout
// ZSET and delivers them. Members are removed only after confirmed successful
// delivery, giving at-least-once semantics: a crash or lock-contention failure
// leaves the member in the ZSET for retry on the next poll (#8).
func (m *Monitor) processTimeoutKey(ctx context.Context, t namespace.Namespace, key string, now time.Time, nowUnix string) {
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

		// Inject the namespace that owns this timeout key into the context so the
		// engine processes the resume under the correct namespace scope: the
		// store's keys are namespace-prefixed and would not be found under a
		// different namespace.
		namespaceCtx := namespace.WithNamespace(ctx, t)

		// Fire the OnNodeTimeout hook.
		if m.hooks != nil {
			engine.SafeHook(namespaceCtx, m.logger, func(ctx context.Context) {
				m.hooks.OnNodeTimeout(ctx, execID, nodeName)
			})
		}

		// Directly enqueue a timeout resume task, bypassing signal name matching.
		if err := m.engine.TimeoutNode(namespaceCtx, execID, nodeName); err != nil {
			// Before requeuing, verify the execution/node is still in a state
			// worth retrying. A terminal or canceled execution/node must not
			// be re-added every 5s — that would wedge a dead member into the
			// ZSET forever. Only transient delivery failures (engine busy,
			// outbox write hiccup) justify a backoff retry.
			if m.isTimeoutTargetDead(namespaceCtx, t, execID, nodeName) {
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
func (m *Monitor) isTimeoutTargetDead(ctx context.Context, t namespace.Namespace, execID types.ExecutionID, nodeName string) bool {
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
			types.NodeStatusContinued, types.NodeStatusPinned,
			types.NodeStatusRunning,
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

func (slogAdapter) Debug(msg string, args ...any) { slog.Debug(msg, args...) }
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
