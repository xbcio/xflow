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

// SetLogger installs the logger used to report background entry activation
// index-rebuild failures. The rebuild is best-effort by construction — List
// keeps serving the scan path while it is failing — so without a logger the
// failures would be silent. Safe to call at any time: the logger is held
// behind an atomic pointer, so a call after the store has started serving
// replaces the previous logger for all later readers (a nil logger disables
// logging).
func (s *EntryActivationStore) SetLogger(l engine.Logger) { s.logger.Store(&l) }

// ConfigureTransient sets transient (fire-and-forget) retention. The active TTL
// starts when an execution is created and ordinary state mutations do not renew
// it, so callers must set activeTTL above the maximum execution wall-clock
// duration. completionTTL applies after the execution becomes terminal. Supported
// suspension handling may explicitly extend affected keys while waiting; that is
// not general sliding retention. enabled=false keeps durable mode.
func (s *Store) ConfigureTransient(enabled bool, activeTTL, completionTTL time.Duration) {
	s.transient = enabled
	s.transientTTL = activeTTL
	s.transientCompletionTTL = completionTTL
}

// ConfigureOwnedNamespaces scopes every maintenance sweep on this store to
// exactly the given set. The input must already be normalized
// (namespace.NormalizeOwned): nil restores legacy behavior — iterating the
// append-only xflow:namespaces registry with the default namespace always
// included — while a non-nil set is iterated directly, without consulting the
// registry and without injecting the default namespace.
//
// This is the static scope half of deployment isolation: a process that owns
// only its own namespaces never reads or repairs another deployment's keys
// even when both deployments share one Redis. Call it once before the store
// starts serving; the field is read without a lock afterwards.
//
// The set is copied, so later mutation of the caller's slice does not
// retroactively change the store's scope.
func (s *Store) ConfigureOwnedNamespaces(owned []namespace.Namespace) {
	if owned == nil {
		s.ownedNamespaces = nil
		return
	}
	s.ownedNamespaces = make([]namespace.Namespace, len(owned))
	copy(s.ownedNamespaces, owned)
}

// ConfigureOutboxReadyIndex enables or disables the best-effort outbox
// readiness index.
//
// It is an accelerator, not part of the state machine: with it the dispatcher
// discovers ready work in time proportional to the ready backlog, without it
// every discovery call sweeps the keyspace for `...:outbox:ready` exactly as it
// did before the index existed. Disabling it therefore costs discovery
// throughput and nothing else — every entry stays discoverable, because the
// outbox body and its per-execution ready set are still written atomically and
// the keyspace sweep is still the fallback. See state_outbox_index.go.
//
// Defaults to enabled.
func (s *Store) ConfigureOutboxReadyIndex(enabled bool) {
	s.outboxIndexOn.Store(enabled)
}

// ConfigureOutputCompression enables storing node outputs compressed, and is the
// write-side half of the feature described in output_codec.go.
//
// It is a switch rather than always-on for one reason: a process running an older
// build cannot decode a zstd frame, so a mixed-version fleet that starts writing
// compressed values would hand those processes unreadable node outputs. Ship the
// code first — its read path already serves both forms — then enable this once
// every process can decode. Turning it back off is always safe, and no data
// migration is needed in either direction.
func (s *Store) ConfigureOutputCompression(enabled bool) {
	s.outputCompression.Store(enabled)
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
