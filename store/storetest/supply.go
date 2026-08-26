// Package storetest holds cross-backend behavioural contracts that both the
// in-memory and SQL store implementations must satisfy. It is a normal package
// (not an external _test package) so both store/memstore's unit tests and
// test/integration's MySQL tests can import the same assertions.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
)

// SupplyNamespaceNormContract probes the empty-namespace normalisation contract:
// writing with namespace "" must be findable by reading with namespace "".
// Both write and read paths must normalise "" → "default" so that callers do
// not need to know the canonical name.
func SupplyNamespaceNormContract(t *testing.T, s store.Supplies) {
	t.Helper()
	ctx := context.Background()
	const name = "probe-ns-norm"

	content := []byte(`{"probe":true}`)
	if _, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace:   "",
		Name:        name,
		Content:     content,
		ContentType: "application/json",
	}, nil); err != nil {
		t.Fatalf("PutSupply with empty namespace: %v", err)
	}

	// Round-trip with "": should find the record stored as "default".
	rec, err := s.GetSupply(ctx, "", name)
	if err != nil {
		t.Fatalf("GetSupply with empty namespace returned error %v; want the record written via PutSupply(\"\", ...)", err)
	}
	if string(rec.Content) != string(content) {
		t.Fatalf("content mismatch: got %s, want %s", rec.Content, content)
	}

	// Explicit "default" must find the same record.
	rec2, err := s.GetSupply(ctx, "default", name)
	if err != nil {
		t.Fatalf("GetSupply with \"default\" namespace returned error %v", err)
	}
	if rec.Revision != rec2.Revision {
		t.Fatalf("\"\" and \"default\" returned different revisions: %d vs %d", rec.Revision, rec2.Revision)
	}
}

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
	// Pinned to a digest computed outside this program (shasum -a 256), not to
	// store.ContentHash. The write path calls store.ContentHash too, so
	// comparing against it is x == x: truncating the digest to sum[:8] weakens
	// every caller — including sqlstore's corruption guard, which exists to
	// catch a bad migration or a direct SQL edit — while both sides of this
	// comparison truncate identically and stay green.
	const wantHash = "sha256:da506c8a9c8a9f31aa00eaeef23d49764b9ace97158a1a0a7aa628e6d446b0fb"
	if rec.ContentHash != wantHash {
		t.Fatalf("content hash = %q, want %q (sha256 of the written content, "+
			"computed independently of the production hasher)", rec.ContentHash, wantHash)
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

	// LastFetchAt round-trip: zero value must remain zero (DB stores NULL).
	got, err := s.GetSupply(ctx, ns1, "rules")
	if err != nil {
		t.Fatalf("get for LastFetchAt zero check: %v", err)
	}
	if !got.LastFetchAt.IsZero() {
		t.Fatalf("LastFetchAt zero round-trip: got %v, want zero", got.LastFetchAt)
	}

	// LastFetchAt round-trip: a concrete time must survive (DATETIME(3) has ms
	// precision, so we truncate to millisecond before comparing).
	fetchTime := time.Date(2026, 7, 30, 12, 34, 56, 789000000, time.UTC)
	three := uint64(3)
	rec4, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns1, Name: "rules", Content: []byte(`{"rules":[2]}`),
		LastFetchAt: fetchTime,
	}, &three)
	if err != nil {
		t.Fatalf("put with LastFetchAt: %v", err)
	}
	gotFetch := rec4.LastFetchAt.Truncate(time.Millisecond)
	wantFetch := fetchTime.Truncate(time.Millisecond)
	if !gotFetch.Equal(wantFetch) {
		t.Fatalf("LastFetchAt concrete round-trip: got %v, want %v", gotFetch, wantFetch)
	}
	// Also verify via a fresh Get.
	rec4g, err := s.GetSupply(ctx, ns1, "rules")
	if err != nil {
		t.Fatalf("get for LastFetchAt concrete check: %v", err)
	}
	gotFetch2 := rec4g.LastFetchAt.Truncate(time.Millisecond)
	if !gotFetch2.Equal(wantFetch) {
		t.Fatalf("LastFetchAt concrete round-trip via Get: got %v, want %v", gotFetch2, wantFetch)
	}
}
