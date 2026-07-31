// Package storetest holds cross-backend behavioural contracts that both the
// in-memory and SQL store implementations must satisfy. It is a normal package
// (not an external _test package) so both store/memstore's unit tests and
// test/integration's MySQL tests can import the same assertions.
package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/store"
)

// SupplyContract asserts the store.Supplies behavioural contract: revision
// monotonicity, If-Match CAS semantics, and namespace isolation.
//
// nsPrefix disambiguates rows between backends and between reruns against a
// persistent database — a MySQL run must not collide with a previous run's rows.
func SupplyContract(t *testing.T, s store.Supplies, nsPrefix string) {
	t.Helper()
	ctx := context.Background()
	ns1, ns2 := nsPrefix+"-a", nsPrefix+"-b"

	if _, err := s.GetSupply(ctx, ns1, "rules"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetSupply on missing = %v, want ErrNotFound", err)
	}

	// Create-if-absent: ifMatch == 0.
	zero := uint64(0)
	rec, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns1, Name: "rules",
		Content: []byte(`{"rules":[]}`), ContentType: "application/json",
		UpdatedBy: "svc-a",
	}, &zero)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rec.Revision != 1 {
		t.Fatalf("first revision = %d, want 1", rec.Revision)
	}
	if rec.ContentHash != store.ContentHash([]byte(`{"rules":[]}`)) {
		t.Fatalf("content hash = %q", rec.ContentHash)
	}

	// Create-if-absent against an existing row must conflict.
	if _, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns1, Name: "rules", Content: []byte(`x`),
	}, &zero); !errors.Is(err, store.ErrRevisionConflict) {
		t.Fatalf("duplicate create = %v, want ErrRevisionConflict", err)
	}

	// CAS on the right revision succeeds and bumps to 2.
	one := uint64(1)
	rec2, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns1, Name: "rules", Content: []byte(`{"rules":[1]}`),
		UpdatedBy: "svc-b",
	}, &one)
	if err != nil {
		t.Fatalf("cas update: %v", err)
	}
	if rec2.Revision != 2 {
		t.Fatalf("revision = %d, want 2", rec2.Revision)
	}

	// CAS on a stale revision conflicts and leaves the row untouched.
	if _, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns1, Name: "rules", Content: []byte(`bad`),
	}, &one); !errors.Is(err, store.ErrRevisionConflict) {
		t.Fatalf("stale cas = %v, want ErrRevisionConflict", err)
	}
	cur, err := s.GetSupply(ctx, ns1, "rules")
	if err != nil {
		t.Fatalf("get after conflict: %v", err)
	}
	if cur.Revision != 2 || string(cur.Content) != `{"rules":[1]}` {
		t.Fatalf("conflicted write leaked: rev=%d len=%d", cur.Revision, len(cur.Content))
	}

	// Unconditional write bumps revision even when content is identical.
	rec3, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns1, Name: "rules", Content: []byte(`{"rules":[1]}`),
	}, nil)
	if err != nil {
		t.Fatalf("blind write: %v", err)
	}
	if rec3.Revision != 3 {
		t.Fatalf("revision = %d, want 3 (revision bumps even for identical content)", rec3.Revision)
	}
	if rec3.ContentHash != cur.ContentHash {
		t.Fatal("identical content must keep the same hash")
	}

	// Namespace isolation: the same name in another namespace is a distinct row.
	if _, err := s.GetSupply(ctx, ns2, "rules"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-namespace read = %v, want ErrNotFound", err)
	}
	if _, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns2, Name: "rules", Content: []byte(`{"rules":[]}`),
	}, &zero); err != nil {
		t.Fatalf("ns2 create: %v", err)
	}
	back, err := s.GetSupply(ctx, ns1, "rules")
	if err != nil {
		t.Fatalf("ns1 get: %v", err)
	}
	if back.Revision != 3 {
		t.Fatalf("ns2 write clobbered ns1: rev=%d", back.Revision)
	}
}
