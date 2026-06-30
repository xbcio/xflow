package asynq

import (
	"context"
	"sync"
	"sync/atomic"
)

// AuditObserver receives audit-store write outcomes. Implementations must be
// safe for concurrent use and non-blocking — the redis state writer calls
// these on the hot path. A nil observer (the default) is permitted; the
// state store falls back to atomic counters exposed via AuditStats().
//
// AuditObserver is the contract documented in
// .claude/docs/storage-contract.md: Redis is the system of record; the
// store/sqlstore audit trail is best-effort and may fail without affecting
// scheduling correctness, so every site that previously swallowed errors now
// surfaces them here for ops to count and reconcile.
type AuditObserver interface {
	// OnAuditOK fires when an audit write succeeded for the named operation
	// (e.g. "upsert_node", "save_signal", "revoke_signal").
	OnAuditOK(op string)
	// OnAuditFailed fires when the audit-store write failed. The Redis side
	// of the dual-write already succeeded; the workflow is correct, but the
	// audit trail diverged for that record. err is the underlying database
	// error for logging.
	OnAuditFailed(op string, err error)
}

// AuditStats is an atomic counter view of audit outcomes — useful for tests
// and for /metrics surfaces that can read counters directly without wiring an
// observer.
type AuditStats struct {
	OK     map[string]uint64
	Failed map[string]uint64
}

// auditCounters is the default counters-only implementation that backs
// (*Backend).AuditStats(). External observers (e.g. a Prometheus adapter)
// compose with these counters rather than replacing them.
type auditCounters struct {
	ok     sync.Map // op -> *atomic.Uint64
	failed sync.Map
}

func (c *auditCounters) inc(m *sync.Map, op string) {
	v, _ := m.LoadOrStore(op, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

func (c *auditCounters) OnAuditOK(op string)            { c.inc(&c.ok, op) }
func (c *auditCounters) OnAuditFailed(op string, _ error) { c.inc(&c.failed, op) }

func (c *auditCounters) snapshot() AuditStats {
	out := AuditStats{OK: map[string]uint64{}, Failed: map[string]uint64{}}
	c.ok.Range(func(k, v any) bool {
		out.OK[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})
	c.failed.Range(func(k, v any) bool {
		out.Failed[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})
	return out
}

// auditWrite runs fn against the audit-store and routes the outcome through
// the configured observer (always) and any per-instance counters. It NEVER
// returns an error: by contract the audit trail is best-effort and Redis is
// authoritative. Callers that need transactional rollback (e.g.
// CreateExecution) must NOT use auditWrite — they keep their explicit
// error-path handling.
func (s *redisState) auditWrite(ctx context.Context, op string, fn func(context.Context) error) {
	if s.db == nil {
		return
	}
	if err := fn(ctx); err != nil {
		s.audit.OnAuditFailed(op, err)
		if s.auditCounters != nil {
			s.auditCounters.OnAuditFailed(op, err)
		}
		if s.logger != nil {
			s.logger.Error("audit_write_failed", "op", op, "err", err)
		}
		return
	}
	s.audit.OnAuditOK(op)
	if s.auditCounters != nil {
		s.auditCounters.OnAuditOK(op)
	}
}

// noopAuditObserver is the default observer. It performs no work; counters
// in auditCounters are the authoritative observable signal until an external
// observer is wired in.
type noopAuditObserver struct{}

func (noopAuditObserver) OnAuditOK(string)            {}
func (noopAuditObserver) OnAuditFailed(string, error) {}
