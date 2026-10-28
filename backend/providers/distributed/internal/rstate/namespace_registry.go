package rstate

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
)

// namespaceSetKey is the global SET that records every namespace that has ever
// written an execution-scoped key. The leader-only maintenance loops (lease
// sweeper, lease repair, outbox dispatcher/metrics) iterate this set so a
// single SCAN never crosses a namespace boundary.
//
// Invariant (design §3.2): namespaces are append-only. A namespace is SADDed the
// first time it durably writes a namespace-scoped key and is never removed.
// Removing a namespace would let an in-flight execution/outbox/dead-letter key
// escape the sweeper and become an orphan that is never scanned again. The
// default namespace is always returned by listNamespaces even before it is SADDed,
// so single-namespace deployments keep working without any explicit
// registration.
const namespaceSetKey = "xflow:namespaces"

// registerNamespace records t in the namespace registry. It is idempotent and
// best-effort: the registry only drives maintenance SCAN fan-out, so a
// failed SADD is not fatal — the next durable write retries and the namespace
// is eventually discovered. The default namespace is always returned by
// listNamespaces even before it is SADDed.
func (s *Store) registerNamespace(ctx context.Context, t namespace.Namespace) error {
	if t == "" {
		t = namespace.Default
	}
	if err := namespace.Validate(t); err != nil {
		return fmt.Errorf("register namespace %q: %w", t, err)
	}
	if err := s.rdb.SAdd(ctx, namespaceSetKey, string(t)).Err(); err != nil {
		return fmt.Errorf("register namespace %q: %w", t, err)
	}
	return nil
}

// namespaceRegisterInterval is how long registerNamespaceThrottled trusts its own
// earlier SADD. It is deliberately finite: the registry is only a cache of
// "which namespaces have data", so a Redis that was flushed or failed over must
// be repopulated by the next seed rather than stay empty until a restart.
const namespaceRegisterInterval = time.Minute

// registerNamespaceThrottled is registerNamespace for the entry-seed path, which
// is the collection hot path: it issues the SADD at most once per
// namespaceRegisterInterval per namespace per Store, instead of once per seed.
//
// The window is only started by a SADD that succeeded, so a failed registration
// is retried by the very next seed. Two concurrent first calls may both issue
// the SADD; it is idempotent, so that costs one redundant command, not
// correctness. The record is process-local and expires, never a permanent
// "already registered" fact.
func (s *Store) registerNamespaceThrottled(ctx context.Context, t namespace.Namespace) error {
	if t == "" {
		t = namespace.Default
	}
	now := time.Now
	if s.nsClock != nil {
		now = s.nsClock
	}
	if last, ok := s.nsRegisteredAt.Load(t); ok {
		if elapsed := now().Sub(last.(time.Time)); elapsed >= 0 && elapsed < namespaceRegisterInterval {
			return nil
		}
	}
	if err := s.registerNamespace(ctx, t); err != nil {
		return err
	}
	s.nsRegisteredAt.Store(t, now())
	return nil
}

// ListNamespaces returns every namespace known to the registry, always including
// the default namespace even if it has not yet been SADDed. The result is sorted
// for stable SCAN ordering. It is exported so out-of-store maintenance loops
// (e.g. the timeout monitor) iterate the same namespace set the store writes.
func ListNamespaces(ctx context.Context, rdb redis.Cmdable) ([]namespace.Namespace, error) {
	members, err := rdb.SMembers(ctx, namespaceSetKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	set := make(map[namespace.Namespace]struct{}, len(members)+1)
	set[namespace.Default] = struct{}{}
	for _, m := range members {
		if m != "" {
			set[namespace.Namespace(m)] = struct{}{}
		}
	}
	out := make([]namespace.Namespace, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// listNamespaces is the store-bound convenience wrapper around ListNamespaces.
func (s *Store) listNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	return ListNamespaces(ctx, s.rdb)
}

// scopedNamespaces returns the set of namespaces every maintenance sweep on
// this store may touch. When ConfigureOwnedNamespaces installed a set, it is
// returned directly — no registry read, and deliberately no default-namespace
// injection, because a non-default deployment's scope is exactly the set its
// embedder declared. nil (legacy mode) falls back to the registry so that
// unscoped processes keep scanning every namespace as before.
func (s *Store) scopedNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	if s.ownedNamespaces != nil {
		out := make([]namespace.Namespace, len(s.ownedNamespaces))
		copy(out, s.ownedNamespaces)
		return out, nil
	}
	return s.listNamespaces(ctx)
}
