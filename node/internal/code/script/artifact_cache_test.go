package script_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// The cache is process-wide (like the engine caches below it), so every test in
// this file must use source text no other test uses. Reusing a payload makes a
// digest collide across tests and the resolver-call count reads zero — a test
// that "passes" by being served another test's entry proves nothing.
const (
	srcOncePerDigest = `({doubled: $input.x * 2, tag: "once"})`
	srcPerDigestA    = `({doubled: $input.x * 2, tag: "perdigest-a"})`
	srcPerDigestB    = `({doubled: $input.x * 3, tag: "perdigest-b"})`
	srcConcurrent    = `({doubled: $input.x * 2, tag: "concurrent"})`
	srcNoResolver    = `({doubled: $input.x * 2, tag: "no-resolver"})`
	srcNamespaced    = `({doubled: $input.x * 2, tag: "namespaced"})`
)

// TestScript_ArtifactCacheDoesNotMaskMissingResolver is the guard that keeps the
// memo from turning a wiring defect into a heisenbug.
//
// Several dispatch paths must call SetArtifactCodeResolver (the distributed
// backend, the group runtime, the subgraph runtime, the map body). When one of
// them forgets, the node must fail — every time, on every process. A cache
// consulted before that check would serve an entry some other execution
// populated, so the missing wiring would only show on a cold start, in whatever
// order the workflows happened to run. That is exactly the failure the existing
// TestGroupRuntime_MemberScriptWithoutResolver positive control exists to catch.
func TestScript_ArtifactCacheDoesNotMaskMissingResolver(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")

	src := []byte(srcNoResolver)
	sum := sha256.Sum256(src)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	params := map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"artifact_digest": digest,
	}

	// Warm the cache through a properly wired execution.
	warm := &types.Input{Params: params, Data: map[string]any{"x": 21.0}}
	warm.SetArtifactCodeResolver(func(_ context.Context, _ string) ([]byte, error) { return src, nil })
	if _, err := h.Execute(context.Background(), warm); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// Same digest, no resolver: must still fail.
	cold := &types.Input{Params: params, Data: map[string]any{"x": 21.0}}
	_, err := h.Execute(context.Background(), cold)
	if err == nil {
		t.Fatal("execution with no artifact resolver succeeded off a warm cache; " +
			"an unwired dispatch path would then fail only on a cold process")
	}
	if !strings.Contains(err.Error(), "no artifact resolver is configured") {
		t.Fatalf("error = %v, want the artifact_unavailable config error", err)
	}
}

// TestScript_ArtifactCacheIsNamespaceScoped pins the authorisation boundary. A
// digest names immutable bytes, but the right to read those bytes is per
// namespace and is enforced by the artifact store's reference check. A cache
// keyed on the digest alone would let a namespace that has no reference to an
// artifact run it, bypassing that check entirely.
func TestScript_ArtifactCacheIsNamespaceScoped(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")

	src := []byte(srcNamespaced)
	sum := sha256.Sum256(src)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	params := map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"artifact_digest": digest,
	}

	// Namespace "tenant-a" holds a reference and resolves fine.
	a := &types.Input{Params: params, Data: map[string]any{"x": 21.0}}
	a.SetNamespace(namespace.Namespace("tenant-a"))
	a.SetArtifactCodeResolver(func(_ context.Context, _ string) ([]byte, error) { return src, nil })
	if _, err := h.Execute(context.Background(), a); err != nil {
		t.Fatalf("tenant-a: %v", err)
	}

	// Namespace "tenant-b" has no reference; its store rejects the digest. The
	// rejection must reach the caller rather than being pre-empted by the entry
	// tenant-a just cached.
	var bCalled bool
	b := &types.Input{Params: params, Data: map[string]any{"x": 21.0}}
	b.SetNamespace(namespace.Namespace("tenant-b"))
	b.SetArtifactCodeResolver(func(_ context.Context, _ string) ([]byte, error) {
		bCalled = true
		return nil, errors.New("artifact not found in namespace")
	})
	if _, err := h.Execute(context.Background(), b); err == nil {
		t.Fatal("tenant-b ran an artifact it has no reference to, served from tenant-a's cache entry")
	}
	if !bCalled {
		t.Fatal("tenant-b's resolver was never consulted; the cache is not namespace-scoped")
	}
}

// TestScript_ArtifactCodeResolvedOncePerDigest pins the per-message cost of the
// artifact path.
//
// A digest is content-addressable: the bytes behind it cannot change. Resolving
// it on every Execute re-reads the module (6.8 MiB for the SAS decode guest) and
// — for wasm — re-base64-encodes it into a fresh 9.1 MiB string, every message,
// for every script node. A CPU profile of a 400-message run put 26.65% of all
// samples inside ScriptNode.Execute, of which base64 encoding was 9.25% and the
// artifact fetch 6.03%, against 10% for the wasm execution the node exists to
// do; the resulting garbage drove GC to 28%.
//
// The engine-level cache downstream (wasm's codeCache) is keyed by the encoded
// string, so it can only be reached after that cost has already been paid.
func TestScript_ArtifactCodeResolvedOncePerDigest(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")

	src := []byte(srcOncePerDigest)
	sum := sha256.Sum256(src)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	var calls int
	input := &types.Input{
		Params: map[string]any{
			"language":        "js",
			"runtime":         "goja",
			"artifact_digest": digest,
		},
		Data: map[string]any{"x": 21.0},
	}
	input.SetArtifactCodeResolver(func(_ context.Context, d string) ([]byte, error) {
		if d != digest {
			t.Errorf("resolver called with digest %q, want %q", d, digest)
		}
		calls++
		return src, nil
	})

	for i := 0; i < 5; i++ {
		out, err := h.Execute(context.Background(), input)
		if err != nil {
			t.Fatalf("execute %d: %v", i, err)
		}
		if got := asFloat(out.Data["doubled"]); got != 42.0 {
			t.Fatalf("execute %d: doubled = %v, want 42", i, out.Data["doubled"])
		}
	}

	if calls != 1 {
		t.Fatalf("artifact resolver called %d times for 5 executions of one digest; "+
			"the bytes behind a digest never change, so every call past the first is a "+
			"re-read (and, for wasm, a re-encode) on the per-message hot path", calls)
	}
}

// TestScript_ArtifactCacheIsPerDigest guards the obvious way to get the memo
// wrong: a single-entry cache that thrashes, or one that serves the first
// digest's bytes to every later digest. Two nodes with different artifacts run
// interleaved, as they do inside a map body.
func TestScript_ArtifactCacheIsPerDigest(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")

	type artifact struct {
		digest string
		src    []byte
		want   float64
	}
	arts := []artifact{
		{src: []byte(srcPerDigestA), want: 42},
		{src: []byte(srcPerDigestB), want: 63},
	}
	var calls [2]int
	for i := range arts {
		sum := sha256.Sum256(arts[i].src)
		arts[i].digest = "sha256:" + hex.EncodeToString(sum[:])
	}

	newInput := func(i int) *types.Input {
		in := &types.Input{
			Params: map[string]any{
				"language":        "js",
				"runtime":         "goja",
				"artifact_digest": arts[i].digest,
			},
			Data: map[string]any{"x": 21.0},
		}
		in.SetArtifactCodeResolver(func(_ context.Context, d string) ([]byte, error) {
			if d != arts[i].digest {
				t.Errorf("resolver %d called with digest %q", i, d)
			}
			calls[i]++
			return arts[i].src, nil
		})
		return in
	}

	for round := 0; round < 3; round++ {
		for i := range arts {
			out, err := h.Execute(context.Background(), newInput(i))
			if err != nil {
				t.Fatalf("artifact %d round %d: %v", i, round, err)
			}
			if got := asFloat(out.Data["doubled"]); got != arts[i].want {
				t.Fatalf("artifact %d round %d: doubled = %v, want %v", i, round, out.Data["doubled"], arts[i].want)
			}
		}
	}

	for i, n := range calls {
		if n != 1 {
			t.Fatalf("artifact %d resolver called %d times across 3 interleaved rounds, want 1", i, n)
		}
	}
}

// TestScript_ArtifactCacheConcurrent runs the artifact path from several
// goroutines at once, which is how it runs in production: a map node fans items
// out across workers and every one of them lands on the same digest.
func TestScript_ArtifactCacheConcurrent(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")

	src := []byte(srcConcurrent)
	sum := sha256.Sum256(src)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	var mu sync.Mutex
	calls := 0

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := &types.Input{
				Params: map[string]any{
					"language":        "js",
					"runtime":         "goja",
					"artifact_digest": digest,
				},
				Data: map[string]any{"x": 21.0},
			}
			in.SetArtifactCodeResolver(func(_ context.Context, _ string) ([]byte, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				return src, nil
			})
			out, err := h.Execute(context.Background(), in)
			if err != nil {
				t.Errorf("execute: %v", err)
				return
			}
			if got := asFloat(out.Data["doubled"]); got != 42.0 {
				t.Errorf("doubled = %v, want 42", out.Data["doubled"])
			}
		}()
	}
	wg.Wait()

	// Racing goroutines may each miss before the first store lands, so the bound
	// is "not once per call" rather than exactly one.
	mu.Lock()
	defer mu.Unlock()
	if calls > 4 {
		t.Fatalf("artifact resolver called %d times across 8 concurrent executions; the memo is not shared", calls)
	}
}
