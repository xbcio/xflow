package script

import (
	"context"
	"encoding/base64"

	"github.com/xbcio/xflow/namespace"
	lru "github.com/hashicorp/golang-lru/v2"
)

// DefaultArtifactCodeCacheSize bounds the resolved-artifact cache. Entries are
// the engine-ready code string, so a wasm entry is ~4/3 the module size (9.1 MiB
// for the 6.8 MiB SAS decode guest). A deployment normally runs a handful of
// distinct guests, so this bound exists to cap a pathological workflow
// population rather than to shape the common case.
const DefaultArtifactCodeCacheSize = 32

// artifactCodeCache memoises digest -> engine-ready code string.
//
// Without it, ScriptNode.Execute pays the full resolution cost on every message:
// the resolver reads the module (a disk read at best -- the runner's artifact
// store is a read-through FS cache with no memory tier), and the wasm path then
// base64-encodes it into a fresh multi-megabyte string. A CPU profile of a
// 400-message run attributed 26.65% of samples to ScriptNode.Execute -- 9.25%
// base64, 6.03% artifact fetch -- against 10% for the wasm execution itself,
// with GC at 28% clearing the wreckage.
//
// The engine layer already caches downstream (wasm/host.go's codeCache is keyed
// by this very string, deliberately: hashing a 9 MiB Go string key uses AES-NI
// and costs microseconds, where re-keying by module sha256 would add ~3 ms per
// message). That cache is at the right layer; the problem was that reaching it
// cost a full re-resolve. This memo is what makes its fast path reachable.
//
// Caching by digest is safe by construction -- a content-addressable digest
// names one immutable byte sequence -- but authorisation is NOT content
// addressable, so the key carries the namespace too. Entries also carry the
// language, since the same bytes encode differently for wasm (base64) and js
// (raw text).
type artifactCodeCache struct {
	c *lru.Cache[artifactCodeKey, string]
}

type artifactCodeKey struct {
	namespace namespace.Namespace
	digest    string
	language  string
}

func newArtifactCodeCache(size int) *artifactCodeCache {
	c, err := lru.New[artifactCodeKey, string](size)
	if err != nil {
		// Only reachable with a non-positive size, which is a programming error
		// in this package rather than a runtime condition.
		panic("script: invalid artifact code cache size: " + err.Error())
	}
	return &artifactCodeCache{c: c}
}

var sharedArtifactCode = newArtifactCodeCache(DefaultArtifactCodeCacheSize)

// get returns the engine-ready code for a digest, resolving and encoding it on
// a miss. The bool reports whether a resolver was configured at all; a zero-byte
// artifact returns ("", true, nil), which is a different failure from "the
// feature is unavailable" and must stay distinguishable at the call site.
//
// Concurrent misses on the same key may each call resolve; that costs a
// duplicated fetch under a cold burst, and is preferable to holding a lock
// across a network or disk read while every other worker waits behind it.
func (a *artifactCodeCache) get(
	ctx context.Context,
	ns namespace.Namespace,
	digest, language string,
	resolve func(context.Context, string) ([]byte, error),
) (string, bool, error) {
	key := artifactCodeKey{namespace: ns, digest: digest, language: language}
	if code, ok := a.c.Get(key); ok {
		return code, true, nil
	}

	raw, err := resolve(ctx, digest)
	if err != nil {
		return "", false, err
	}
	if raw == nil {
		// "No resolver configured" -- the caller distinguishes this from an error
		// and must keep doing so. Nothing is cached: a resolver arriving later
		// must be able to succeed.
		return "", false, nil
	}

	// The engine interface takes a code string. For wasm that is base64 of the
	// module bytes; for js the raw source text.
	code := string(raw)
	if language == "wasm" {
		code = base64.StdEncoding.EncodeToString(raw)
	}
	a.c.Add(key, code)
	return code, true, nil
}
