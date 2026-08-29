package xflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// TestRunnerArtifactResolverCachePartitionsByNamespace pins the WIRING of
// objectstore.FSStore.PartitionByNamespace at its single production call site,
// newRunnerArtifactResolver.
//
// Why this test has to exist separately from the integration test that already
// covers the same behaviour. That test
// (test/integration/artifact_namespace_declaration_real_test.go's
// TestArtifactRunnerCacheDoesNotCrossNamespaces) builds its own FSStore and
// sets PartitionByNamespace = true itself, so it proves the MECHANISM works
// while proving nothing about whether the runner turns it on. Deleting the
// `cache.PartitionByNamespace = true` line from newRunnerArtifactResolver left
// that test -- and the whole ./sdk/xflow and ./test/integration artifact
// suites -- entirely green. A field being set by the probe is not evidence the
// product sets it.
//
// The assertion therefore goes through newRunnerArtifactResolver itself and
// touches no objectstore field directly: fetch a digest declaring tenantA
// (the origin serves it), then fetch the SAME digest declaring tenantB (the
// origin 404s it). The second fetch must both fail AND reach the origin. An
// unpartitioned cache would serve tenantB the bytes tenantA pulled, without a
// single request -- the server's per-tenant check never running at all.
//
// Origin hit counts are asserted for exact equality rather than ">= 1": a
// second request is the whole claim, so an inequality that a cache hit would
// also satisfy would assert nothing.
//
// Mutation target: the `cache.PartitionByNamespace = true` line in
// sdk/xflow/runner.go's newRunnerArtifactResolver. Removing it must turn this
// test red.
func TestRunnerArtifactResolverCachePartitionsByNamespace(t *testing.T) {
	const (
		tenantA = "wiring-tenant-a"
		tenantB = "wiring-tenant-b"
	)
	content := []byte("runner artifact cache namespace wiring probe content")
	digest := store.ContentHash(content)

	var originHits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		// Stand in for the server's per-tenant authorization: only tenantA
		// references this digest. The point of the test is that this handler
		// is consulted a second time at all.
		if r.Header.Get(objectstore.NamespaceHeader) != tenantA {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(content)
	}))
	defer ts.Close()

	cfg := RunnerConfig{
		ServerURL:        ts.URL,
		Token:            "wiring-probe-token",
		RunnerID:         "wiring-probe-runner",
		ArtifactCacheDir: t.TempDir(),
	}
	resolve, err := newRunnerArtifactResolver(cfg)
	if err != nil {
		t.Fatalf("newRunnerArtifactResolver: %v", err)
	}

	ctxA := namespace.WithNamespace(context.Background(), tenantA)
	got, err := resolve(ctxA, digest)
	if err != nil {
		t.Fatalf("resolve declaring %s = %v, want success", tenantA, err)
	}
	if string(got) != string(content) {
		t.Fatalf("resolve declaring %s returned %q, want %q", tenantA, got, content)
	}
	if n := originHits.Load(); n != 1 {
		t.Fatalf("origin hits after first fetch = %d, want 1", n)
	}

	ctxB := namespace.WithNamespace(context.Background(), tenantB)
	if _, err := resolve(ctxB, digest); err == nil {
		t.Fatalf("resolve declaring %s succeeded, want ErrNotFound -- digest %s is only "+
			"served to %s", tenantB, digest, tenantA)
	} else if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("resolve declaring %s = %v, want objectstore.ErrNotFound", tenantB, err)
	}
	if n := originHits.Load(); n != 2 {
		t.Fatalf("origin hits after second fetch = %d, want 2 -- the %s fetch must reach the "+
			"origin rather than be served bytes the %s fetch cached on disk", n, tenantB, tenantA)
	}
}
