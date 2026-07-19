package rstate

import (
	"context"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/tenant"
)

// tenantSetKey is the global SET that records every tenant that has ever
// written an execution-scoped key. The leader-only maintenance loops (lease
// sweeper, lease repair, outbox dispatcher/metrics) iterate this set so a
// single SCAN never crosses a tenant boundary.
//
// Invariant (design §3.2): tenants are append-only. A tenant is SADDed the
// first time it durably writes a tenant-scoped key and is never removed.
// Removing a tenant would let an in-flight execution/outbox/dead-letter key
// escape the sweeper and become an orphan that is never scanned again. The
// default tenant is always returned by listTenants even before it is SADDed,
// so single-tenant deployments keep working without any explicit
// registration.
const tenantSetKey = "xflow:tenants"

// registerTenant records t in the tenant registry. It is idempotent and
// best-effort: the registry only drives maintenance SCAN fan-out, so a
// failed SADD is not fatal — the next durable write retries and the tenant
// is eventually discovered. The default tenant is always returned by
// listTenants even before it is SADDed.
func (s *Store) registerTenant(ctx context.Context, t tenant.TenantID) error {
	if t == "" {
		t = tenant.DefaultTenant
	}
	if err := s.rdb.SAdd(ctx, tenantSetKey, string(t)).Err(); err != nil {
		return fmt.Errorf("register tenant %q: %w", t, err)
	}
	return nil
}

// ListTenants returns every tenant known to the registry, always including
// the default tenant even if it has not yet been SADDed. The result is sorted
// for stable SCAN ordering. It is exported so out-of-store maintenance loops
// (e.g. the timeout monitor) iterate the same tenant set the store writes.
func ListTenants(ctx context.Context, rdb redis.Cmdable) ([]tenant.TenantID, error) {
	members, err := rdb.SMembers(ctx, tenantSetKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	set := make(map[tenant.TenantID]struct{}, len(members)+1)
	set[tenant.DefaultTenant] = struct{}{}
	for _, m := range members {
		if m != "" {
			set[tenant.TenantID(m)] = struct{}{}
		}
	}
	out := make([]tenant.TenantID, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// listTenants is the store-bound convenience wrapper around ListTenants.
func (s *Store) listTenants(ctx context.Context) ([]tenant.TenantID, error) {
	return ListTenants(ctx, s.rdb)
}
