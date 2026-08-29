package xflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// TestRunnerArtifactResolverRespectsMaxBytes pins the WIRING of
// RunnerConfig.ArtifactCacheMaxBytes into FSStore.MaxBytes at its single
// production call site, newRunnerArtifactResolver.
//
// It deliberately does not touch objectstore.FSStore at all: it goes through
// newRunnerArtifactResolver itself, exactly like
// TestRunnerArtifactResolverCachePartitionsByNamespace in
// runner_artifact_cache_wiring_test.go, and for the identical reason recorded
// there -- a test that constructs its own FSStore and sets the field itself
// proves the mechanism works, never that the runner turns it on. Mutating
// away the `cache.MaxBytes = artifactCacheMaxBytes(cfg)` line in runner.go
// must turn this test red.
//
// The assertion fetches three distinct digests through the real resolver with
// a 2-entry budget, then re-fetches the oldest one and the second-oldest one.
// The oldest must have been evicted from disk by the third fetch's write and
// therefore reach the origin again; the second-oldest must still be served
// from disk and must NOT reach the origin again. Both directions are needed:
// asserting only "the origin was hit again" would also pass an
// implementation that evicts everything on every write, and asserting only
// "no second hit" would also pass an implementation that never enforces any
// cap at all.
func TestRunnerArtifactResolverRespectsMaxBytes(t *testing.T) {
	const entrySize = 512 * 1024 // 512 KiB: large enough to matter, small enough to be fast

	mkContent := func(b byte) []byte {
		c := make([]byte, entrySize)
		for i := range c {
			c[i] = b
		}
		return c
	}
	contentA := mkContent('A')
	contentB := mkContent('B')
	contentC := mkContent('C')
	digestA := store.ContentHash(contentA)
	digestB := store.ContentHash(contentB)
	digestC := store.ContentHash(contentC)
	byDigest := map[string][]byte{digestA: contentA, digestB: contentB, digestC: contentC}

	var mu sync.Mutex
	hits := map[string]int{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		digest, _ := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/v1/artifacts/"))
		mu.Lock()
		hits[digest]++
		mu.Unlock()
		content, ok := byDigest[digest]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(content)
	}))
	defer ts.Close()
	hitsOf := func(digest string) int {
		mu.Lock()
		defer mu.Unlock()
		return hits[digest]
	}

	cfg := RunnerConfig{
		ServerURL:             ts.URL,
		Token:                 "wiring-cap-probe-token",
		RunnerID:              "wiring-cap-probe-runner",
		ArtifactCacheDir:      t.TempDir(),
		ArtifactCacheMaxBytes: 2 * entrySize, // room for exactly two of the three artifacts below
	}
	resolve, err := newRunnerArtifactResolver(cfg)
	if err != nil {
		t.Fatalf("newRunnerArtifactResolver: %v", err)
	}
	ctx := namespace.WithNamespace(context.Background(), "wiring-cap-tenant")

	// Fill the cache: A (oldest), B, C (newest). Writing C pushes the total
	// to 3 * entrySize against a 2 * entrySize budget, so this must evict A.
	for _, d := range []string{digestA, digestB, digestC} {
		if _, err := resolve(ctx, d); err != nil {
			t.Fatalf("resolve(%s): %v", d, err)
		}
	}
	for _, d := range []string{digestA, digestB, digestC} {
		if got := hitsOf(d); got != 1 {
			t.Fatalf("origin hits for %s after first fetch = %d, want 1", d, got)
		}
	}

	// digestB is still cached at this point (only A should have been
	// evicted): re-fetching it must be served from disk, not the origin.
	if _, err := resolve(ctx, digestB); err != nil {
		t.Fatalf("re-resolve(digestB): %v", err)
	}
	if got := hitsOf(digestB); got != 1 {
		t.Fatalf("origin hits for digestB after re-fetch = %d, want 1 -- it should still be served "+
			"from the local disk cache", got)
	}

	// digestA was the oldest of the three and must have been evicted by
	// digestC's write once the total exceeded the 2*entrySize budget:
	// re-fetching it must reach the origin a second time.
	if _, err := resolve(ctx, digestA); err != nil {
		t.Fatalf("re-resolve(digestA): %v", err)
	}
	if got := hitsOf(digestA); got != 2 {
		t.Fatalf("origin hits for digestA after re-fetch = %d, want 2 -- the %d-byte cap "+
			"(RunnerConfig.ArtifactCacheMaxBytes) must have evicted it from the local disk cache; "+
			"if this is 1, ArtifactCacheMaxBytes is not reaching FSStore.MaxBytes", got, cfg.ArtifactCacheMaxBytes)
	}
}
