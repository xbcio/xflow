package rstate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/internal/graphcache"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

type Store struct {
	rdb                    redis.UniversalClient
	db                     store.Store // may be nil
	execTTL                time.Duration
	transient              bool
	transientTTL           time.Duration
	transientCompletionTTL time.Duration

	// ownedNamespaces scopes every maintenance sweep (lease expiry, lease
	// repair, outbox discovery/metrics, concurrency reconciliation, dead-letter
	// receipts) to exactly this set when non-nil. It is the embedder's declared
	// namespace scope — a process must never touch another deployment's
	// namespaces even when they share one Redis. nil preserves the legacy
	// behavior of iterating the append-only xflow:namespaces registry (always
	// including the default namespace). Written once via ConfigureOwnedNamespaces
	// before the store starts serving; read-only afterwards, hence no lock.
	ownedNamespaces []namespace.Namespace

	// in-memory graph cache: LoadGraph decodes into it and CheckCompletion reads
	// it. Bounded because every replica that executes one node of an execution
	// populates it while only the terminalizing replica evicts it — see
	// internal/graphcache.
	graphs             *graphcache.Cache
	graphCacheMaxBytes int64
	graphCacheIdleTTL  time.Duration

	// per-execution TTL overrides (set via SubmitOption)
	ttlMu    sync.RWMutex
	execTTLs map[types.ExecutionID]time.Duration
	// execDeadlines memoizes each execution's persisted deadline (zero = no time
	// budget) so getExecTTL can keep every status write past it without a Redis
	// read per call. Guarded by ttlMu like execTTLs. The deadline is immutable
	// after creation, so a memoized value never goes stale; the map is only
	// bounded, see rememberExecDeadline.
	execDeadlines map[types.ExecutionID]time.Time

	// per-execution transient overrides (set via per-workflow transient context).
	// This is a read-through cache over the Redis marker, not the source of
	// truth -- see lookupTransient. Only verdicts backed by durable evidence are
	// cached: an explicit marker or a confirmed SQL row. A "no evidence" answer
	// is deliberately re-derived.
	transientMu   sync.RWMutex
	execTransient map[types.ExecutionID]transientMark

	// leaseRepairCursors advance a bounded reconciliation scan across node
	// status keys, with one node-local cursor per Redis master and namespace.
	// This prevents both cross-master cursor reuse and one namespace's progress
	// from starving another. The mutex prevents concurrent maintenance loops
	// from repeatedly scanning the same Redis pages.
	leaseRepairMu      sync.Mutex
	leaseRepairCursors map[namespace.Namespace]redisx.Cursors

	// concurrencySweepMu keeps two maintenance loops from sweeping the same
	// workflow slot sets at the same time. The sweep is idempotent, so this is
	// about not multiplying the index reads, not correctness.
	concurrencySweepMu sync.Mutex

	// outboxDiscoveryCursors advance the bounded outbox discovery scan the same
	// way leaseRepairCursors do: one node-local cursor per Redis master and
	// namespace, so a resumed scan neither reuses a cursor across masters nor
	// lets one namespace's backlog starve another's discovery. The mutex keeps
	// two dispatchers sharing a Store from re-walking the same Redis pages; it
	// also guards outboxDiscoveryCalls below.
	outboxDiscoveryMu      sync.Mutex
	outboxDiscoveryCursors map[namespace.Namespace]redisx.Cursors
	// outboxDiscoveryCalls counts discovery calls, which is how the keyspace
	// sweep is throttled once the readiness index has proven it carries work.
	outboxDiscoveryCalls uint64

	// outboxIndexOn switches the best-effort readiness index off entirely.
	// outboxIndexProven records that the index has been observed carrying work
	// in this process, which is the only state in which the sweep may be
	// throttled. It is set by a discovery read that returned work and cleared
	// by every index failure, so an index that goes quiet stops throttling the
	// sweep that is its only fallback. Both are written from producer
	// goroutines, hence atomics. See state_outbox_index.go for the contract they
	// belong to.
	outboxIndexOn     atomic.Bool
	outboxIndexProven atomic.Bool
	// outboxIndexLogMu guards outboxIndexLastLog, which rate-limits the
	// best-effort index-failure log. The refresh runs on the outbox hot path, so
	// a Redis outage would otherwise turn one failure per transition into a log
	// storm on top of the outage.
	outboxIndexLogMu   sync.Mutex
	outboxIndexLastLog time.Time

	// outputCompression switches storing node outputs compressed. Written once at
	// construction and read from every node commit, hence an atomic. The READ
	// path deliberately does not consult it — see output_codec.go for why the two
	// halves are asymmetric.
	outputCompression atomic.Bool

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

	// nsRegisteredAt maps a namespace to the instant this process last SADDed it
	// into the registry from the entry-seed path (see registerNamespaceThrottled).
	// nsClock is that throttle's clock; nil means time.Now, tests replace it.
	nsRegisteredAt sync.Map // namespace.Namespace -> time.Time
	nsClock        func() time.Time
}

// Option configures a Store at construction time.
type Option func(*Store)

// WithGraphCache bounds the store's in-memory decoded-graph cache.
//
// The cache is populated by every replica that loads an execution's graph and
// evicted only when that execution reaches a terminal state on the same
// replica, so without a bound a non-finalizing replica pins every graph it ever
// touched. maxBytes <= 0 and idleTTL <= 0 disable the respective bound;
// omitting the option keeps graphcache.DefaultMaxBytes / DefaultIdleTTL.
func WithGraphCache(maxBytes int64, idleTTL time.Duration) Option {
	return func(s *Store) {
		s.graphCacheMaxBytes = maxBytes
		s.graphCacheIdleTTL = idleTTL
	}
}

func New(rdb redis.UniversalClient, db store.Store, execTTL time.Duration, opts ...Option) *Store {
	s := &Store{
		rdb:                    rdb,
		db:                     db,
		execTTL:                execTTL,
		graphCacheMaxBytes:     graphcache.DefaultMaxBytes,
		graphCacheIdleTTL:      graphcache.DefaultIdleTTL,
		execTTLs:               make(map[types.ExecutionID]time.Duration),
		execDeadlines:          make(map[types.ExecutionID]time.Time),
		execTransient:          make(map[types.ExecutionID]transientMark),
		leaseRepairCursors:     make(map[namespace.Namespace]redisx.Cursors),
		outboxDiscoveryCursors: make(map[namespace.Namespace]redisx.Cursors),
		audit:                  noopAuditObserver{},
		auditCounters:          &auditCounters{},
		cursorKey:              newCursorSigningKey(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.graphs = graphcache.New(graphcache.Options{
		// The entry cap is a fixed secondary guard; the byte budget is the
		// configurable one.
		MaxEntries: graphcache.DefaultMaxEntries,
		MaxBytes:   s.graphCacheMaxBytes,
		IdleTTL:    s.graphCacheIdleTTL,
		// The backend installs the logger right after New, so it is read at call
		// time rather than captured. Same line and keys as the engine's cache,
		// distinguished by scope.
		OnSweep: func(st graphcache.Stats) {
			if s.logger == nil {
				return
			}
			s.logger.Info("graph_cache_stats", "scope", "store",
				"entries", st.Entries, "bytes", st.Bytes,
				"evicted_idle", st.EvictedIdle,
				"evicted_capacity", st.EvictedCapacity,
				"evicted_explicit", st.EvictedExplicit)
		},
	})
	// The readiness index is on by default because it is a pure accelerator:
	// with it the drain discovers ready work in time proportional to the
	// backlog, without it every discovery call sweeps the keyspace exactly as
	// it did before. See state_outbox_index.go.
	s.outboxIndexOn.Store(true)
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
func (s *Store) ttlSecForExec(ctx context.Context, id types.ExecutionID) int {
	if mark := s.lookupTransient(ctx, id); mark.transient {
		if t := s.transientTTLOr(mark); t > 0 {
			return int(t.Seconds())
		}
	}
	return int(s.execTTL.Seconds())
}

// getExecTTL returns the retention every write to the execution's keys must use:
// the retention baseExecTTL resolves, lengthened so the keys outlive the
// execution's deadline by deadlineGrace.
//
// The deadline term is derived here, at the one point every writer funnels
// through, rather than at each call site. A replica that did not create the
// execution holds no in-memory override, so a per-call-site extension would be
// missed by every writer running there and the status key would be shrunk back
// to the default retention while its deadline is still ahead: the execution
// would vanish and never time out. The deadline is recomputed against the clock
// on every call; only the deadline itself is memoized, never the duration.
func (s *Store) getExecTTL(ctx context.Context, id types.ExecutionID) time.Duration {
	base := s.baseExecTTL(ctx, id)
	if base <= 0 {
		// Unbounded in this store: there is nothing to lengthen.
		return base
	}
	return retentionPastDeadline(base, s.execDeadline(ctx, id))
}

// execDeadline resolves an execution's deadline for retention purposes: the
// memo when this replica has seen the execution, otherwise one transactional read
// of the persisted state (executionDeadline). "No time budget" is memoized as a
// zero deadline, so an execution without a timeout costs one read per replica
// rather than one per write.
//
// Only a settled answer is memoized. An answer for an execution that does not
// exist yet, and a failed read, are returned for this call alone: memoizing
// either would pin the execution to "no budget" on this replica for as long as
// it runs, and every later write would shrink the status back to the default
// retention (see executionDeadline).
//
// The memo is filled with set-if-absent here. A creation path on this replica
// may have recorded the authoritative value between the read and this write, and
// a reader must never overwrite it; the value that ends up in the memo is the one
// returned.
func (s *Store) execDeadline(ctx context.Context, id types.ExecutionID) time.Time {
	s.ttlMu.RLock()
	deadline, ok := s.execDeadlines[id]
	s.ttlMu.RUnlock()
	if ok {
		return deadline
	}
	deadline, settled := s.executionDeadline(ctx, id)
	if !settled {
		return deadline
	}
	return s.learnExecDeadline(id, deadline)
}

// execDeadlineMemoCap bounds execDeadlines. Terminalization evicts an entry only
// on the replica that writes the terminal status, while every replica that
// touches the execution memoizes it, so without a bound a replica that never
// finalizes would keep one entry per execution it ever worked on for as long as
// it runs (the same shape internal/graphcache bounds). The memo is only an
// optimization, so overflowing it drops it whole and the live executions refill
// it at one read each.
const execDeadlineMemoCap = 1 << 16

// rememberExecDeadline records an execution's deadline, zero for "no time
// budget", overwriting whatever the memo held. It is for the creation paths,
// which hold the authoritative value they just wrote and spare the first
// getExecTTL its read. A reader that learned something from Redis must use
// learnExecDeadline instead, which never overwrites.
func (s *Store) rememberExecDeadline(id types.ExecutionID, deadline time.Time) {
	s.ttlMu.Lock()
	s.storeExecDeadlineLocked(id, deadline)
	s.ttlMu.Unlock()
}

// learnExecDeadline records a deadline read from Redis unless the memo already
// holds one, and returns the deadline the memo holds afterwards. Set-if-absent
// under the write lock, so a reader racing a creation on this replica cannot
// replace the creator's value with what it saw before the creation landed.
func (s *Store) learnExecDeadline(id types.ExecutionID, deadline time.Time) time.Time {
	s.ttlMu.Lock()
	defer s.ttlMu.Unlock()
	if known, ok := s.execDeadlines[id]; ok {
		return known
	}
	s.storeExecDeadlineLocked(id, deadline)
	return deadline
}

// storeExecDeadlineLocked writes the memo entry, dropping the memo whole first
// when it is full. The caller holds ttlMu for writing.
func (s *Store) storeExecDeadlineLocked(id types.ExecutionID, deadline time.Time) {
	if _, known := s.execDeadlines[id]; !known && len(s.execDeadlines) >= execDeadlineMemoCap {
		clear(s.execDeadlines)
	}
	s.execDeadlines[id] = deadline
}

// baseExecTTL returns the per-execution TTL override if set, otherwise the
// per-workflow transient TTL, otherwise the adapter default. It knows nothing of
// the execution's deadline: the creation paths size their own retention from the
// deadline they carry and start from this, so they must not see the lengthening
// getExecTTL adds.
func (s *Store) baseExecTTL(ctx context.Context, id types.ExecutionID) time.Duration {
	s.ttlMu.RLock()
	ttl := s.execTTLs[id]
	s.ttlMu.RUnlock()
	if ttl > 0 {
		return ttl
	}
	if mark := s.lookupTransient(ctx, id); mark.transient {
		if t := s.transientTTLOr(mark); t > 0 {
			return t
		}
	}
	return s.execTTL
}

// isTransient reports whether the given execution should use transient mode.
// Per-execution override takes priority; falls back to the global setting.
func (s *Store) isTransient(ctx context.Context, id types.ExecutionID) bool {
	return s.lookupTransient(ctx, id).transient
}

// getTransientTTL returns the transient active TTL for the given execution.
// Per-execution override takes priority; falls back to the global setting.
func (s *Store) getTransientTTL(ctx context.Context, id types.ExecutionID) time.Duration {
	return s.transientTTLOr(s.lookupTransient(ctx, id))
}

// getTransientCompletionTTL returns the completion TTL for the given execution.
// Per-execution override takes priority; falls back to the global setting.
func (s *Store) getTransientCompletionTTL(ctx context.Context, id types.ExecutionID) time.Duration {
	if ttl := s.lookupTransient(ctx, id).completionTTL; ttl > 0 {
		return ttl
	}
	return s.transientCompletionTTL
}

func (s *Store) transientTTLOr(mark transientMark) time.Duration {
	if mark.ttl > 0 {
		return mark.ttl
	}
	return s.transientTTL
}

// transientMark is one execution's transient decision, whether it came from the
// per-execution marker or from the store-wide setting.
type transientMark struct {
	transient     bool
	ttl           time.Duration
	completionTTL time.Duration
	// durableConfirmed records that the "durable" verdict rests on evidence that
	// cannot turn into its opposite: the explicit durable marker written in the
	// execution's create transaction, or -- for an execution created before that
	// marker existed -- an execution row in SQL. See lookupTransient.
	durableConfirmed bool
}

// lookupTransient resolves an execution's transient mode, consulting the
// process-local cache first and Redis second.
//
// The Redis read is what makes this correct across control-plane replicas. The
// replica that admits a per-workflow transient execution is not the one that
// runs every later mutation: the outbox dispatcher, lease sweeper, and timeout
// monitor claim work by scanning Redis from whichever replica holds the loop.
// A marker kept only on the admitting replica's heap would leave every other
// replica reading the store-wide flag -- OFF on a control plane that also hosts
// durable workflows -- and projecting the node payload into SQL. Redis is the
// only place all replicas can see.
//
// The authoritative evidence is the per-execution marker written while the
// execution is created: field "transient" names an execution that opted out of
// SQL projection, field "durable" an execution whose row is expected there. The
// durable marker exists so transience is never inferred from a bare SQL miss.
// The projection is a best-effort follow-up that lands after the Redis commit,
// so during that lag the row's absence says nothing about the execution's mode;
// inferring "transient" from it -- and caching that verdict -- made a
// cross-replica execution skip its audit projection permanently once the row
// did land.
//
// Marker absence is the only case that still asks SQL. Executions created
// before the durable marker existed, and transient executions whose marker
// lapsed, both look that way. A row confirms durable and is cached. No row
// keeps the fail-closed transient answer for THIS call but is deliberately not
// cached, because it becomes wrong as soon as the projection lands.
//
// The cache holds only verdicts backed by durable evidence, so a durable
// execution pays the Redis round-trip once per replica rather than once per
// state mutation. Entries are dropped by evictExecutionCaches when the
// execution reaches a terminal state, giving them the same lifetime as the
// graph and TTL caches beside them.
//
// A Redis read failure resolves to transient. Losing an audit row is a
// best-effort miss the storage contract already permits; persisting the payload
// of an execution that asked not to be persisted is the leak this mode exists
// to prevent, and it is not recoverable after the fact.
func (s *Store) lookupTransient(ctx context.Context, id types.ExecutionID) transientMark {
	s.transientMu.RLock()
	mark, ok := s.execTransient[id]
	s.transientMu.RUnlock()
	// Only a verdict backed by durable evidence is reusable: an explicit marker
	// (either mode) or a confirmed SQL row. "Nothing says transient" is not
	// evidence -- the marker can lapse and the SQL row can lag the create -- so
	// that answer is re-derived instead of cached.
	if ok && (mark.transient || mark.durableConfirmed) {
		return mark
	}
	if s.transient {
		// Store-wide transient mode never writes a per-execution marker, so
		// there is nothing in Redis to find and the answer cannot change.
		return transientMark{transient: true, ttl: s.transientTTL, completionTTL: s.transientCompletionTTL}
	}

	t := namespace.FromContext(ctx)
	fields, err := s.rdb.HGetAll(ctx, transientMarkKey(t, id)).Result()
	if err != nil && err != redis.Nil {
		if s.logger != nil {
			s.logger.Error("transient_marker_read_failed", "execution_id", string(id), "err", err)
		}
		// Fail closed; do not cache, so a transient Redis fault does not pin
		// every later read of this execution to the pessimistic answer.
		return transientMark{transient: true, ttl: s.transientTTL, completionTTL: s.transientCompletionTTL}
	}
	resolved := transientMark{}
	switch {
	case fieldPresent(fields, "transient"):
		// The explicit transient marker. Its presence is the signal; the TTL
		// fields only carry per-workflow overrides.
		resolved.transient = true
		resolved.ttl = parseDurationMillis(fields["ttl_ms"])
		resolved.completionTTL = parseDurationMillis(fields["completion_ttl_ms"])
	case fieldPresent(fields, "durable"):
		// The explicit durable marker. No SQL round-trip: it was written by the
		// same creation that made the execution visible, so it is already on
		// every replica -- and it cannot be contradicted by a slow projection.
		resolved.durableConfirmed = true
	case len(fields) > 0:
		// A marker this code does not write. Fail closed rather than guess.
		resolved.transient = true
		resolved.ttl = s.transientTTL
		resolved.completionTTL = s.transientCompletionTTL
	}

	// No marker at all: the execution was created before the durable marker
	// existed, or its transient marker lapsed. SQL is a fallback for exactly
	// this markerless case, never the authority.
	if len(fields) == 0 && s.db != nil {
		switch _, err := s.db.GetExecution(ctx, id); {
		case err == nil:
			resolved.durableConfirmed = true
		case errors.Is(err, store.ErrNotFound):
			// The row's absence is NOT evidence of transience: the projection is
			// a best-effort follow-up that may not have landed yet (or ever, for
			// a genuinely transient execution). Keep the fail-closed answer for
			// this call and do NOT cache it; the next call re-asks, so the
			// verdict repairs itself as soon as the row appears.
			resolved.transient = true
			resolved.ttl = s.transientTTL
			resolved.completionTTL = s.transientCompletionTTL
			return resolved
		default:
			// The store is unreadable, which is not evidence either way. Keep the
			// durable verdict and proceed: the projection this unlocks writes to
			// the same store, so an unreadable store cannot silently persist a
			// payload -- it fails that write and the failure is counted. Marking
			// transient here would instead stop projecting every durable
			// execution for the duration of the outage, silently, which is the
			// worse failure. Do not cache, so recovery is immediate.
			if s.logger != nil {
				s.logger.Error("transient_row_check_failed", "execution_id", string(id), "err", err)
			}
			return resolved
		}
	}
	if resolved.transient || resolved.durableConfirmed {
		s.transientMu.Lock()
		s.execTransient[id] = resolved
		s.transientMu.Unlock()
	}
	return resolved
}

// fieldPresent reports whether an HGETALL reply carries name. An empty hash
// arrives as an empty map, so presence -- not truthiness -- is the marker's
// signal.
func fieldPresent(fields map[string]string, name string) bool {
	_, ok := fields[name]
	return ok
}

// rememberTransient records a verdict this replica established itself, so later
// reads do not have to re-derive it. Only callers that know the answer
// authoritatively (execution creation) should use it.
func (s *Store) rememberTransient(id types.ExecutionID, mark transientMark) {
	s.transientMu.Lock()
	s.execTransient[id] = mark
	s.transientMu.Unlock()
}

func parseDurationMillis(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	var ms int64
	if _, err := fmt.Sscanf(raw, "%d", &ms); err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// markExecutionTransient records the per-execution transient marker in Redis so
// every replica resolves the same answer, and primes the local cache. The write
// is issued on the caller's pipeline: the marker must land in the same
// transaction as the execution's structural keys, or a replica could observe an
// execution that exists but is not yet known to be transient and project its
// first node output.
// transientMarkerTTLFactor is how much longer the transient marker lives than
// the keyTTL it qualifies.
//
// The invariant that makes absence of the marker mean "not transient" is that
// the marker outlives EVERY execution-scoped key it speaks for. Reaching that
// invariant by refreshing the marker on each write is necessary but not
// sufficient on its own: it holds only for the paths that remember to refresh,
// and a single forgotten path re-EXPIREs the node keys past the marker. That is
// not hypothetical -- it is exactly how transient executions leaked node rows
// into SQL (xflow_nodes=187 with xflow_executions=0 on a real deployment): a
// path extended the node keys while the marker kept its original deadline, so
// isTransient read the lapsed marker as "durable" and the projection ran.
//
// The factor turns that class of mistake from "leaks a payload" into "defers
// cleanup": with a margin of one full keyTTL, a forgotten refresh still leaves
// the marker alive as long as some execution-scoped key is alive, because any
// key can only ever be extended to now+keyTTL. The marker's absence therefore
// keeps meaning what callers assume it means.
//
// Kept in sync with commitNodeLua's `EXPIRE KEYS[12]`, which applies the same
// factor inside the commit transaction.
const transientMarkerTTLFactor = 2

// markerTTL is the lifetime to give a transient marker qualifying keys that
// live for keyTTL. See transientMarkerTTLFactor for why it is longer.
func markerTTL(keyTTL time.Duration) time.Duration {
	if keyTTL <= 0 {
		return 0
	}
	return keyTTL * transientMarkerTTLFactor
}

func (s *Store) markExecutionTransient(ctx context.Context, pipe redis.Pipeliner, id types.ExecutionID, ttl, completionTTL, keyTTL time.Duration) {
	fields := map[string]any{}
	if ttl > 0 {
		fields["ttl_ms"] = ttl.Milliseconds()
	}
	if completionTTL > 0 {
		fields["completion_ttl_ms"] = completionTTL.Milliseconds()
	}
	// HSET with no fields is an error, and an empty hash does not exist in
	// Redis, so a marker carrying no TTL overrides still needs one field to be
	// findable. Its presence is the signal; the value is not read.
	fields["transient"] = 1

	key := transientMarkKey(namespace.FromContext(ctx), id)
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, markerTTL(keyTTL))

	s.transientMu.Lock()
	s.execTransient[id] = transientMark{transient: true, ttl: ttl, completionTTL: completionTTL}
	s.transientMu.Unlock()
}

// markExecutionDurable records the explicit durable verdict in Redis so every
// replica resolves the execution without consulting the SQL projection.
//
// The write belongs to creation, in the same transaction (or, on the seed
// paths, before the script) that makes the execution visible: otherwise a
// replica can observe the execution while SQL has no row for it and -- before
// this marker existed -- cache the missing row as "transient" forever. The
// projection is a best-effort follow-up by contract; it cannot be the witness
// that decides whether projection may run.
//
// Invalidation: the marker uses the same key as the transient marker, so the
// creation/rollback inventory (createdExecutionKeys) already covers it, and
// commitNodeLua's exists-guarded EXPIRE keeps it alive alongside the node keys.
// TTL follows markerTTL for the same reason the transient marker does: the
// marker must outlive every execution-scoped key whose mode it qualifies, so its
// absence never becomes evidence.
//
// The caller remains responsible for priming the local cache
// (rememberTransient); this helper is the cross-replica half only.
func (s *Store) markExecutionDurable(ctx context.Context, pipe redis.Pipeliner, id types.ExecutionID, keyTTL time.Duration) {
	pipe.HSet(ctx, transientMarkKey(namespace.FromContext(ctx), id), "durable", 1)
	if ttl := markerTTL(keyTTL); ttl > 0 {
		pipe.Expire(ctx, transientMarkKey(namespace.FromContext(ctx), id), ttl)
	}
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

// createdExecutionKeys enumerates every key createExecution writes for an
// execution, before any node/outbox transition runs. Creation used to enumerate
// its optional keys inline while cleanupCreatedExecution and completion-time TTL
// shortening each re-derived the set by hand; the three drifted, so scope/
// parent/parent_node/depth/rerun_from were written but never rolled back or
// shortened. Both paths now share this single inventory.
//
// The inventory is derived from the id and graph rather than from the full
// snapshot because rollback and TTL shortening only still hold those. Fields
// that creation writes conditionally (params, scope, trace ids, derived-run
// links, the transient marker) are therefore enumerated unconditionally, and
// per-unit keys cover every unit rather than only the skip entries that write a
// schedule key. That is a superset: DEL and EXPIRE on a missing key are no-ops,
// and every key here is scoped to this execution's {id} hash tag, so
// over-enumeration cannot touch another execution or a shared index.
//
// The transient marker and retention record ARE part of this inventory (creation
// writes them), but completion-time shortening deliberately skips them; their
// lifetime follows its own rules (see transientMarkKey and retentionRecordKey).
func createdExecutionKeys(t namespace.Namespace, id types.ExecutionID, g *graph.Graph) []string {
	keys := []string{
		execKey(t, id, "status"),
		execKey(t, id, "graph"),
		execKey(t, id, "namespace"),
		execKey(t, id, "params"),
		execKey(t, id, "runtime"),
		execKey(t, id, "scope"),
		execKey(t, id, "test_run"),
		execKey(t, id, "trace_id"),
		execKey(t, id, "span_id"),
		execKey(t, id, "trace_carrier"),
		// The execution's identity and clock are written once with the
		// execution itself. They belong to the creation inventory for the same
		// reason status does: rollback must remove them and completion-time
		// shortening must bound them with the payload keys they describe, or a
		// reader that still finds the execution cannot name or time what it ran.
		execKey(t, id, "workflow_id"),
		execKey(t, id, "workflow_version"),
		execKey(t, id, "created_at_ms"),
		execKey(t, id, "updated_at_ms"),
		// The absolute deadline of the execution's time budget; written once with
		// the execution, and only when it has a budget.
		execKey(t, id, "deadline_ms"),
		execKey(t, id, "parent"),
		execKey(t, id, "parent_node"),
		execKey(t, id, "depth"),
		execKey(t, id, "rerun_from"),
		transientMarkKey(t, id),
		retentionRecordKey(t, id),
		// Enumerated even when this execution was created without outbox entries:
		// DEL/EXPIRE on the then-absent keys is a no-op.
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	}
	if g == nil {
		return keys
	}
	// Creation writes the counters only for acyclic graphs, gating its SETs on
	// this same !AllowCycles() check (see createExecution). Mirroring that gate
	// here keeps the inventory exactly in step with what creation wrote: a
	// cyclic execution never has these keys, so listing them would describe a
	// write that does not happen. (EXPIRing or DELeting a missing key is a
	// no-op, so the gate is about accuracy, not safety.)
	if !g.AllowCycles() {
		keys = append(keys, remainingNodesKey(t, id), failedNodesKey(t, id))
	}
	// Creation seeds an in-degree key only for units with in-degree > 0 and a
	// schedule key only for skip intents, but both families are unit-scoped and
	// execution-owned, so covering every unit is a safe superset. Later
	// transitions write these same families. UnitCount bounds every index
	// creation writes (UnitCount <= NodeCount).
	for i := 0; i < g.UnitCount(); i++ {
		keys = append(keys, inDegreeKey(t, id, i), scheduleKey(t, id, i))
	}
	return keys
}

// transientExecutionKeys enumerates every Redis key an execution can own,
// derived deterministically from its graph (the creation inventory plus keys
// written by later transitions). It replaces the per-mutation-maintained :keys
// set as the source of truth for completion-time TTL shortening.
func transientExecutionKeys(t namespace.Namespace, id types.ExecutionID, g *graph.Graph) []string {
	// NOTE: the terminal marker (terminalMarkKey) is deliberately ABSENT from
	// this list. It is written with the execution's ACTIVE retention and exists
	// precisely to outlive the shortened keys: shortening it to the completion
	// TTL would re-create the exact gap it closes, where a backlogged task
	// consumed after the completion TTL reads an execution that finished
	// normally as lost work.
	//
	// The transient marker and the retention record are absent for their own
	// reasons (see below), and both ARE part of createdExecutionKeys -- creation
	// writes them and rollback must remove them -- so they are filtered out of
	// the creation inventory here. The terminal marker is not: creation does not
	// write it (setTerminalMark does, on terminalization), cleanupCreatedExecution
	// appends it separately, and it is never shortened here.
	transientKey, retentionKey := transientMarkKey(t, id), retentionRecordKey(t, id)
	keys := make([]string, 0, 32)
	for _, key := range createdExecutionKeys(t, id, g) {
		if key == transientKey || key == retentionKey {
			continue
		}
		keys = append(keys, key)
	}
	keys = append(keys,
		execKey(t, id, "error"),
		// The completion counters are execution-scoped and safe to expire: a
		// finished execution's remaining/failed counts are never consulted again.
		// They are listed unconditionally, unlike in the creation inventory, so a
		// completion whose cached graph is missing or unreadable cannot leave
		// them behind on the active TTL. EXPIRE on an absent key (the cyclic-graph
		// case) is a no-op; the duplicate EXPIRE for an acyclic graph is harmless.
		remainingNodesKey(t, id),
		failedNodesKey(t, id),
		// The outbox dead-letter keys are enumerated here even though transient
		// mode does not produce suspend-related keys: an outbox entry that
		// exhausts its retries still lands on the attempts counter and the
		// dead-letter/body hashes regardless of mode, so completion-time TTL
		// shortening must cover them or they outlive the execution's shortened
		// completion TTL.
		leaseExpiryZSetKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		timeoutZSetKey(t, id),
	)
	// NOTE: the transient marker is deliberately ABSENT from this list.
	//
	// It used to be included, so that completion shortened the marker along
	// with the data -- the intent being that a marker must not outlive its
	// execution and answer for a recycled ID. That intent is sound but the
	// cost was the opposite failure, and the opposite failure is worse:
	//
	//   shortening is a one-way door. Once the marker is dropped to the
	//   completion TTL it dies ahead of the node keys, because a late or
	//   duplicate CommitNode for a terminal execution still re-EXPIREs its
	//   node keys via getExecTTL -- and with the marker gone that call
	//   returns the DURABLE ttl, so the keys are extended to the long
	//   lifetime while isTransient now answers "durable". The projection
	//   then runs and persists output from an execution declared ephemeral.
	//   Measured on a real deployment: xflow_nodes rows with no matching
	//   xflow_executions row, one per completed transient execution.
	//
	// The marker is a handful of bytes with no payload, and its own TTL is
	// bounded by transientMarkerTTLFactor, so leaving it to expire on its
	// own keeps it alive at least as long as every key above and costs
	// nothing. The recycled-ID concern needs an ID to be reused within one
	// TTL while the original keys are still alive, which is not a scenario
	// this codebase's ID generation produces; the leak above is observed,
	// not theoretical.
	if g != nil {
		// activeInputsKey is unit-indexed while this loop walks node indices.
		// That is safe because UnitCount <= NodeCount — a supply node takes a
		// node index and no unit, and a group collapses many nodes into one unit
		// — so 0..NodeCount-1 is a superset of every unit key. The in-degree and
		// schedule keys are already covered by createdExecutionKeys.
		for i := 0; i < g.NodeCount(); i++ {
			node := g.NodeAt(i)
			keys = append(keys,
				activeInputsKey(t, id, i),
				nodeStatusKey(t, id, node.Name),
				nodeMetaKey(t, id, node.Name),
				outputKey(t, id, node.Name),
				subExecutionKey(t, id, node.Name),
			)
		}
	}
	return keys
}

func (s *Store) shortenTransientCompletionTTL(ctx context.Context, id types.ExecutionID, newKeys ...string) error {
	if !s.isTransient(ctx, id) {
		return nil
	}
	ttl := s.getTransientCompletionTTL(ctx, id)
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

// shortenTransientCompletionTTLBestEffort runs completion-time TTL shortening
// from a post-commit hook, where the authoritative Redis transition has already
// been accepted inside a Lua script.
//
// It cannot return an error. The commit is already durable, so failing the call
// would make the caller retry a transition that succeeded; the other post-commit
// work here (auditWrite, projectExecutionStatus) is best-effort for the same
// reason. It does not stay silent either: shortening that keeps failing leaves
// finished executions' output holding its full active TTL, which is precisely
// the keyspace growth this hook exists to bound, and a silent failure of it is
// indistinguishable from a healthy store.
func (s *Store) shortenTransientCompletionTTLBestEffort(ctx context.Context, id types.ExecutionID, newKeys ...string) {
	err := s.shortenTransientCompletionTTL(ctx, id, newKeys...)
	if err != nil && s.logger != nil {
		s.logger.Error("transient_completion_ttl_shorten_failed", "execution_id", string(id), "err", err)
	}
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

// redisResultBit strictly parses a Lua 0|1 result. Privacy is fail-closed:
// accepting an unknown representation as public could leak a projected output.
func redisResultBit(value any) (bool, error) {
	switch typed := value.(type) {
	case int64:
		switch typed {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
	case string:
		switch typed {
		case "0":
			return false, nil
		case "1":
			return true, nil
		}
	case []byte:
		switch string(typed) {
		case "0":
			return false, nil
		case "1":
			return true, nil
		}
	}
	return false, fmt.Errorf("want Redis bit 0 or 1, got %T(%v)", value, value)
}

// redisResultString coerces a Redis Lua result element into a string.
func redisResultString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

// redisResultStrings coerces a Redis Lua result element that is itself a table
// into a string slice. A script that returns an empty Lua table surfaces here as
// an empty (or absent) slice, so callers get nil rather than a phantom entry.
func redisResultStrings(value any) []string {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text := redisResultString(item); text != "" {
			out = append(out, text)
		}
	}
	return out
}

// evictExecutionCaches drops the in-memory graph and per-execution TTL entries
// once an execution reaches a terminal state, so completed executions do not
// pin memory.
func (s *Store) evictExecutionCaches(id types.ExecutionID) {
	s.graphs.Delete(id)
	s.ttlMu.Lock()
	delete(s.execTTLs, id)
	delete(s.execDeadlines, id)
	s.ttlMu.Unlock()
	s.transientMu.Lock()
	delete(s.execTransient, id)
	s.transientMu.Unlock()
}
