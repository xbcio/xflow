package rstate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	rdb                    *redis.Client
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

	// leaseRepairCursor advances a bounded reconciliation scan across node
	// status keys. The mutex prevents concurrent control-plane maintenance
	// loops from repeatedly scanning the same Redis page.
	leaseRepairMu     sync.Mutex
	leaseRepairCursor uint64

	// Audit-trail observability — Redis is system-of-record; the store/sqlstore
	// audit trail is best-effort. auditWrite routes failures through these
	// instead of silently dropping them.
	audit         AuditObserver
	auditCounters *auditCounters
	leaseObserver LeaseObserver
	logger        engine.Logger
}

func New(rdb *redis.Client, db store.Store, execTTL time.Duration) *Store {
	return &Store{
		rdb:           rdb,
		db:            db,
		execTTL:       execTTL,
		graphs:        make(map[types.ExecutionID]*graph.Graph),
		execTTLs:      make(map[types.ExecutionID]time.Duration),
		audit:         noopAuditObserver{},
		auditCounters: &auditCounters{},
	}
}

func (s *Store) ttlSec() int {
	if s.transient && s.transientTTL > 0 {
		return int(s.transientTTL.Seconds())
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
	if s.transient && s.transientTTL > 0 {
		return s.transientTTL
	}
	return s.execTTL
}

func executionKeySetKey(id types.ExecutionID) string {
	return execKey(id, "keys")
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
func transientExecutionKeys(id types.ExecutionID, g *graph.Graph) []string {
	keys := []string{
		execKey(id, "status"),
		execKey(id, "graph"),
		execKey(id, "error"),
		execKey(id, "params"),
		execKey(id, "runtime"),
		execKey(id, "trace_id"),
		execKey(id, "span_id"),
		remainingNodesKey(id),
		failedNodesKey(id),
		leaseExpiryZSetKey(id),
		outboxReadyKey(id),
		outboxBodyKey(id),
		// The outbox dead-letter keys are enumerated here even though transient
		// mode does not produce suspend-related keys: an outbox entry that
		// exhausts its retries still lands on the attempts counter and the
		// dead-letter/body hashes regardless of mode, so completion-time TTL
		// shortening must cover them or they outlive the execution's shortened
		// completion TTL.
		outboxAttemptsKey(id),
		outboxDeadKey(id),
		outboxDeadBodyKey(id),
	}
	if g != nil {
		for i := 0; i < g.NodeCount(); i++ {
			node := g.NodeAt(i)
			keys = append(keys,
				inDegreeKey(id, i),
				activeInputsKey(id, i),
				scheduleKey(id, i),
				nodeStatusKey(id, node.Name),
				nodeMetaKey(id, node.Name),
				outputKey(id, node.Name),
			)
		}
	}
	return keys
}

func (s *Store) shortenTransientCompletionTTL(ctx context.Context, id types.ExecutionID, newKeys ...string) error {
	if !s.transient {
		return nil
	}
	ttl := s.transientCompletionTTL
	if ttl <= 0 {
		return nil
	}
	// Completion is one-shot per execution. Enumerate the execution's keys from
	// its (cached) graph rather than from a per-mutation-maintained :keys set —
	// the SADD/SMEMBERS bookkeeping the hot path used to pay is gone. EXPIRE on a
	// missing key is a harmless no-op, so over-enumeration is safe.
	g, _ := s.LoadGraph(ctx, id)
	keys := transientExecutionKeys(id, g)
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
