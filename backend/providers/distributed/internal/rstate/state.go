package rstate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	rdb                    redis.UniversalClient
	db                     store.Store // may be nil
	execTTL                time.Duration
	transient              bool
	transientTTL           time.Duration
	transientCompletionTTL time.Duration

	// in-memory graph cache for CheckCompletion (avoids Redis round-trips per node)
	mu     sync.RWMutex
	graphs map[types.ExecutionID]*graph.Graph

	// per-execution TTL overrides (set via SubmitOption)
	ttlMu    sync.RWMutex
	execTTLs map[types.ExecutionID]time.Duration

	// per-execution transient overrides (set via per-workflow transient context)
	transientMu    sync.RWMutex
	execTransient  map[types.ExecutionID]bool
	execTransientTTL           map[types.ExecutionID]time.Duration
	execTransientCompletionTTL map[types.ExecutionID]time.Duration

	// leaseRepairCursor advances a bounded reconciliation scan across node
	// status keys, one cursor per namespace so a multi-namespace store never lets
	// one namespace's scan progress starve another. The mutex prevents
	// concurrent control-plane maintenance loops from repeatedly scanning the
	// same Redis page.
	leaseRepairMu     sync.Mutex
	leaseRepairCursor map[namespace.Namespace]uint64

	// Audit-trail observability — Redis is system-of-record; the store/sqlstore
	// audit trail is best-effort. auditWrite routes failures through these
	// instead of silently dropping them.
	audit         AuditObserver
	auditCounters *auditCounters
	leaseObserver LeaseObserver
	logger        engine.Logger

	// cursorKey is a process-local random key used to HMAC-sign opaque
	// ListDeadLetters pagination cursors. It is generated once at Store
	// construction and never persisted, so cursors are valid only for the
	// lifetime of the process that issued them: after a restart, decode fails
	// with a clear "cursor expired" error and the caller must restart the
	// listing from the first page. This needs no configuration and keeps the
	// signing secret out of logs/config. It is not a security boundary
	// (dead-letter listings are already operator-scoped); it only prevents
	// accidental cross-execution or stale-cursor replay.
	cursorKey []byte
}

func New(rdb redis.UniversalClient, db store.Store, execTTL time.Duration) *Store {
	s := &Store{
		rdb:                        rdb,
		db:                         db,
		execTTL:                    execTTL,
		graphs:                     make(map[types.ExecutionID]*graph.Graph),
		execTTLs:                   make(map[types.ExecutionID]time.Duration),
		execTransient:              make(map[types.ExecutionID]bool),
		execTransientTTL:           make(map[types.ExecutionID]time.Duration),
		execTransientCompletionTTL: make(map[types.ExecutionID]time.Duration),
		leaseRepairCursor:          make(map[namespace.Namespace]uint64),
		audit:                      noopAuditObserver{},
		auditCounters:              &auditCounters{},
		cursorKey:                  newCursorSigningKey(),
	}
	// The default namespace is registered lazily on the first durable execution
	// create, and listNamespaces also defensively includes the default namespace, so
	// single-namespace deployments work without any eager SADD. Transient
	// (fire-and-forget) mode skips the registry write entirely to preserve the
	// documented no-bookkeeping-on-mutation invariant; its keys are still
	// discoverable because the default namespace is always scanned.
	return s
}

func (s *Store) ttlSec() int {
	if s.transient && s.transientTTL > 0 {
		return int(s.transientTTL.Seconds())
	}
	return int(s.execTTL.Seconds())
}

// ttlSecForExec returns the TTL in seconds for a specific execution,
// considering per-execution transient overrides.
func (s *Store) ttlSecForExec(id types.ExecutionID) int {
	if s.isTransient(id) {
		if t := s.getTransientTTL(id); t > 0 {
			return int(t.Seconds())
		}
	}
	return int(s.execTTL.Seconds())
}

// getExecTTL returns the per-execution TTL override if set, otherwise the adapter default.
func (s *Store) getExecTTL(id types.ExecutionID) time.Duration {
	s.ttlMu.RLock()
	ttl := s.execTTLs[id]
	s.ttlMu.RUnlock()
	if ttl > 0 {
		return ttl
	}
	if s.isTransient(id) {
		if t := s.getTransientTTL(id); t > 0 {
			return t
		}
	}
	return s.execTTL
}

// isTransient reports whether the given execution should use transient mode.
// Per-execution override takes priority; falls back to the global setting.
func (s *Store) isTransient(id types.ExecutionID) bool {
	s.transientMu.RLock()
	perExec, ok := s.execTransient[id]
	s.transientMu.RUnlock()
	if ok {
		return perExec
	}
	return s.transient
}

// getTransientTTL returns the transient active TTL for the given execution.
// Per-execution override takes priority; falls back to the global setting.
func (s *Store) getTransientTTL(id types.ExecutionID) time.Duration {
	s.transientMu.RLock()
	ttl := s.execTransientTTL[id]
	s.transientMu.RUnlock()
	if ttl > 0 {
		return ttl
	}
	return s.transientTTL
}

// getTransientCompletionTTL returns the completion TTL for the given execution.
// Per-execution override takes priority; falls back to the global setting.
func (s *Store) getTransientCompletionTTL(id types.ExecutionID) time.Duration {
	s.transientMu.RLock()
	ttl := s.execTransientCompletionTTL[id]
	s.transientMu.RUnlock()
	if ttl > 0 {
		return ttl
	}
	return s.transientCompletionTTL
}

// MarkExecutionTransient marks a single execution as transient with explicit
// TTL overrides. Called by createExecution when the submission context carries
// a per-workflow transient hint.
func (s *Store) MarkExecutionTransient(id types.ExecutionID, ttl, completionTTL time.Duration) {
	s.transientMu.Lock()
	s.execTransient[id] = true
	if ttl > 0 {
		s.execTransientTTL[id] = ttl
	}
	if completionTTL > 0 {
		s.execTransientCompletionTTL[id] = completionTTL
	}
	s.transientMu.Unlock()
}

func executionKeySetKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "keys")
}

func (s *Store) refreshTransientTTL(_ context.Context, _ types.ExecutionID, _ ...string) error {
	// No-op. This used to slide the transient TTL on the structural keys and
	// SADD every mutated key into a per-execution :keys set on every state
	// mutation (SADD + N×EXPIRE + one pipeline round-trip per commit). That work
	// is redundant under the documented transient-mode constraint: transientTTL
	// must exceed the maximum single-execution wall-clock time, so
	//   - the structural keys (:status/:params/:runtime/:trace_id/:span_id/:graph)
	//     are written with transientTTL at CreateExecution and never expire before
	//     the run finishes, and
	//   - every per-node key already carries a fresh transientTTL from its own Lua
	//     write (the ttl arg to upsertNodeLua / commitNodeLua / acquireTaskLeaseLua
	//     / advanceNodeLua), which also re-EXPIREs :status/:error.
	// Completion-time TTL shortening no longer needs the :keys set either — it
	// enumerates keys deterministically from the graph (shortenTransientCompletionTTL).
	// Keeping the method (as a no-op) avoids churning its ~11 call sites; in
	// non-transient mode it was already a no-op.
	return nil
}

// transientExecutionKeys enumerates every Redis key an execution can own,
// derived deterministically from its graph (mirrors cleanupCreatedExecution).
// It replaces the per-mutation-maintained :keys set as the source of truth for
// completion-time TTL shortening.
func transientExecutionKeys(t namespace.Namespace, id types.ExecutionID, g *graph.Graph) []string {
	keys := []string{
		execKey(t, id, "status"),
		execKey(t, id, "graph"),
		execKey(t, id, "error"),
		execKey(t, id, "params"),
		execKey(t, id, "runtime"),
		execKey(t, id, "trace_id"),
		execKey(t, id, "span_id"),
		execKey(t, id, "trace_carrier"),
		remainingNodesKey(t, id),
		failedNodesKey(t, id),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		// The outbox dead-letter keys are enumerated here even though transient
		// mode does not produce suspend-related keys: an outbox entry that
		// exhausts its retries still lands on the attempts counter and the
		// dead-letter/body hashes regardless of mode, so completion-time TTL
		// shortening must cover them or they outlive the execution's shortened
		// completion TTL.
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		timeoutZSetKey(t, id),
	}
	if g != nil {
		for i := 0; i < g.NodeCount(); i++ {
			node := g.NodeAt(i)
			keys = append(keys,
				inDegreeKey(t, id, i),
				activeInputsKey(t, id, i),
				scheduleKey(t, id, i),
				nodeStatusKey(t, id, node.Name),
				nodeMetaKey(t, id, node.Name),
				outputKey(t, id, node.Name),
			)
		}
	}
	return keys
}

func (s *Store) shortenTransientCompletionTTL(ctx context.Context, id types.ExecutionID, newKeys ...string) error {
	if !s.isTransient(id) {
		return nil
	}
	ttl := s.getTransientCompletionTTL(id)
	if ttl <= 0 {
		return nil
	}
	// Completion is one-shot per execution. Enumerate the execution's keys from
	// its (cached) graph rather than from a per-mutation-maintained :keys set —
	// the SADD/SMEMBERS bookkeeping the hot path used to pay is gone. EXPIRE on a
	// missing key is a harmless no-op, so over-enumeration is safe.
	t := namespace.FromContext(ctx)
	g, _ := s.LoadGraph(ctx, id)
	keys := transientExecutionKeys(t, id, g)
	keys = append(keys, newKeys...)
	pipe := s.rdb.Pipeline()
	for _, key := range keys {
		if key != "" {
			pipe.Expire(ctx, key, ttl)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("shorten transient completion ttl %q: %w", id, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

// redisResultInt coerces a Redis Lua result element into an int64, tolerating
// both the int64 and string encodings the client may surface.
func redisResultInt(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case string:
		var parsed int64
		_, _ = fmt.Sscanf(typed, "%d", &parsed)
		return parsed
	default:
		return 0
	}
}

// redisResultString coerces a Redis Lua result element into a string.
func redisResultString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

// evictExecutionCaches drops the in-memory graph and per-execution TTL entries
// once an execution reaches a terminal state, so completed executions do not
// pin memory.
func (s *Store) evictExecutionCaches(id types.ExecutionID) {
	s.mu.Lock()
	delete(s.graphs, id)
	s.mu.Unlock()
	s.ttlMu.Lock()
	delete(s.execTTLs, id)
	s.ttlMu.Unlock()
	s.transientMu.Lock()
	delete(s.execTransient, id)
	delete(s.execTransientTTL, id)
	delete(s.execTransientCompletionTTL, id)
	s.transientMu.Unlock()
}
