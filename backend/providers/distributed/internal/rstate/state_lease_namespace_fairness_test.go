package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestListExpiredLeasesDoesNotStarveLaterNamespaces pins the fairness rule for
// lease reclamation. The batch bound is shared across namespaces, and it used
// to be consumed in sorted namespace order: once an early-sorting namespace
// filled it, the loop broke and every namespace after it was never scanned. Its
// leases went unreclaimed for as long as that backlog lasted — a task stuck
// behind a crashed runner, not merely a slow sweep.
//
// "acme" is seeded with a full batch so it sorts before "default" and saturated
// the old bound.
func TestListExpiredLeasesDoesNotStarveLaterNamespaces(t *testing.T) {
	state, _, _ := newTestRedisState(t)

	const acme namespace.Namespace = "acme"
	if err := state.registerNamespace(context.Background(), acme); err != nil {
		t.Fatalf("register namespace %q: %v", acme, err)
	}

	past := time.Now().Add(-time.Minute).UTC()
	for i := 0; i < leaseIndexBatchLimit; i++ {
		mustUpsertRunningInNamespace(t, state, acme,
			types.ExecutionID(fmt.Sprintf("acme-%03d", i)), "n",
			fmt.Sprintf("tok-acme-%03d", i), past, time.Second)
	}
	mustUpsertRunningInNamespace(t, state, namespace.Default,
		"default-1", "n", "tok-default-1", past, time.Second)

	expired, err := state.ListExpiredLeases(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}

	var gotDefault bool
	for _, lease := range expired {
		if lease.ExecutionID == "default-1" {
			gotDefault = true
			if lease.Namespace != namespace.Default {
				t.Errorf("default lease namespace = %q, want %q", lease.Namespace, namespace.Default)
			}
		}
	}
	if !gotDefault {
		t.Fatalf("the default namespace's expired lease was not reclaimed while %q had a full "+
			"batch; %d leases returned, all from %q — later namespaces must each keep a share "+
			"of the batch, or their leases are never reclaimed",
			acme, len(expired), expiredNamespace(expired))
	}
	if len(expired) > leaseIndexBatchLimit {
		t.Fatalf("ListExpiredLeases() returned %d leases, want at most %d -- the per-call "+
			"bound must survive the split across namespaces", len(expired), leaseIndexBatchLimit)
	}
}

func expiredNamespace(leases []engine.ExpiredLease) namespace.Namespace {
	seen := make(map[namespace.Namespace]struct{})
	for _, lease := range leases {
		seen[lease.Namespace] = struct{}{}
	}
	out := ""
	for ns := range seen {
		if out != "" {
			out += ","
		}
		out += string(ns)
	}
	return namespace.Namespace(out)
}

func mustUpsertRunningInNamespace(t *testing.T, s *Store, ns namespace.Namespace, execID types.ExecutionID, name, token string, issued time.Time, ttl time.Duration) {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), ns)
	if err := s.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:   execID,
		Name:          name,
		Status:        types.NodeStatusRunning,
		LeaseID:       engine.LeaseID(token + "-id"),
		LeaseToken:    engine.LeaseToken(token),
		LeaseIssuedAt: issued.UTC(),
		LeaseTTL:      ttl,
	}); err != nil {
		t.Fatalf("upsert %s/%s: %v", ns, execID, err)
	}
}
