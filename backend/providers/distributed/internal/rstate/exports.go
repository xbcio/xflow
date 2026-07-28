package rstate

import (
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// SetAuditObserver installs an external audit-store observer. A nil observer is
// ignored, preserving the default no-op observer.
func (s *Store) SetAuditObserver(o AuditObserver) {
	if o != nil {
		s.audit = o
	}
}

// SetLeaseObserver installs an external lease-lifecycle observer. A nil
// observer is ignored.
func (s *Store) SetLeaseObserver(o LeaseObserver) {
	if o != nil {
		s.leaseObserver = o
	}
}

// SetLogger installs the logger used for best-effort audit-write failures.
func (s *Store) SetLogger(l engine.Logger) { s.logger = l }

// ConfigureTransient sets transient (fire-and-forget) retention: a sliding
// active TTL and a shorter completion TTL. enabled=false keeps durable mode.
func (s *Store) ConfigureTransient(enabled bool, activeTTL, completionTTL time.Duration) {
	s.transient = enabled
	s.transientTTL = activeTTL
	s.transientCompletionTTL = completionTTL
}

// AuditStats returns a point-in-time snapshot of audit-store dual-write
// outcomes (ok and failed counts keyed by op).
func (s *Store) AuditStats() AuditStats { return s.auditCounters.snapshot() }

// ExecKey returns the execution-scoped Redis key for the given suffix. It
// exposes the store's key schema to out-of-store readers (e.g. the timeout
// monitor) that must read the same keys the store writes. The namespace scopes
// the key to a namespace namespace; callers must pass the namespace from the
// request context (namespace.FromContext), never a client-supplied value.
func ExecKey(t namespace.Namespace, id types.ExecutionID, suffix string) string {
	return execKey(t, id, suffix)
}

// NodeStatusKey returns the Redis key holding a node's status string.
func NodeStatusKey(t namespace.Namespace, id types.ExecutionID, name string) string {
	return nodeStatusKey(t, id, name)
}
