package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// ReadThrough composes a fast local cache with a slower origin. On cache miss it
// fetches from origin, writes through to cache, and returns the content. This is
// the runner-side composition: fsstore (cache) + httpstore (origin).
type ReadThrough struct {
	cache  Store
	origin Store
}

var _ Store = (*ReadThrough)(nil)

// NewReadThrough builds a read-through store. cache is typically an FSStore
// (local disk), origin is typically an HTTPStore (server endpoint).
func NewReadThrough(cache, origin Store) *ReadThrough {
	return &ReadThrough{cache: cache, origin: origin}
}

// PutObject is forwarded to the origin. In the current architecture no caller
// uses this (runners are read-only), but the contract requires an implementation.
// If the origin does not support writes (e.g. HTTPStore) it will return its own
// unsupported error.
func (rt *ReadThrough) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*Object, error) {
	return rt.origin.PutObject(ctx, key, body, size, opts)
}

// GetObject checks the cache first. On miss it fetches from origin, validates
// size, writes through to cache (fail-open on write errors), and returns the
// content.
//
// The full body is buffered in memory because io.ReadCloser can only be consumed
// once and we need to both write it to cache and return it to the caller. The
// 16 MiB ceiling (maxArtifactResponseBytes) makes this acceptable: runner memory
// is sized for at least one concurrent wasm instance (128 MiB default), and
// artifacts are fetched sequentially per execution.
func (rt *ReadThrough) GetObject(ctx context.Context, key string) (io.ReadCloser, *Object, error) {
	// A cache hit short-circuits. Any cache failure — missing key or a broken
	// cache directory alike — falls through to the origin: the cache is a cache,
	// so nothing it can do should be able to fail a fetch that the origin could
	// still serve. The two cases are deliberately NOT distinguished here.
	if rc, obj, err := rt.cache.GetObject(ctx, key); err == nil {
		return rc, obj, nil
	}

	originRC, originObj, err := rt.origin.GetObject(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = originRC.Close() }()

	// Read the full body with a defensive limit. We already trust origin to
	// enforce maxArtifactResponseBytes (HTTPStore does via limitedReadCloser),
	// but apply a second layer here in case origin is a different impl.
	data, err := io.ReadAll(io.LimitReader(originRC, maxArtifactResponseBytes+1))
	if err != nil {
		return nil, nil, fmt.Errorf("objectstore/readthrough: read origin: %w", err)
	}
	if int64(len(data)) > maxArtifactResponseBytes {
		return nil, nil, fmt.Errorf("objectstore/readthrough: origin content exceeds %d bytes", maxArtifactResponseBytes)
	}

	// Size validation: if origin reported a size, the actual bytes must match.
	// A mismatch implies corruption or a misbehaving server — refuse rather than
	// cache and serve garbage (design doc §7 step 5).
	if originObj != nil && originObj.Size >= 0 && originObj.Size != int64(len(data)) {
		return nil, nil, fmt.Errorf(
			"objectstore/readthrough: size mismatch: origin reported %d, got %d bytes",
			originObj.Size, len(data),
		)
	}

	// Write to cache. Failure is non-fatal: the artifact is still usable, the
	// only cost is a cache miss next time (fail-open, design doc §8.2).
	_, cacheErr := rt.cache.PutObject(ctx, key, bytes.NewReader(data), int64(len(data)), PutOptions{})
	_ = cacheErr // intentionally swallowed

	// Copy rather than mutate: originObj belongs to the origin backend, which may
	// hand out a pointer into its own state (a cached header set, say). Writing
	// Size through it would edit that state from here.
	obj := &Object{Key: key}
	if originObj != nil {
		copied := *originObj
		obj = &copied
	}
	obj.Size = int64(len(data))

	return io.NopCloser(bytes.NewReader(data)), obj, nil
}

// HeadObject checks the cache first (a local stat is nearly free). On miss — or
// on any cache failure, for the same reason as GetObject — it delegates to origin.
func (rt *ReadThrough) HeadObject(ctx context.Context, key string) (*Object, error) {
	if obj, err := rt.cache.HeadObject(ctx, key); err == nil {
		return obj, nil
	}
	return rt.origin.HeadObject(ctx, key)
}
