package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// FSStore implements Store by mapping keys directly to filesystem paths under a
// root directory. Keys produced by store.ObjectKeyForDigest are already two-level
// sharded (artifacts/sha256/ab/cd/<hex>), so no additional hashing is applied.
//
// Metadata is not persisted: the filesystem has no natural place for it without
// sidecar files, and the upstream refFromObject tolerates nil Metadata (Filename
// is purely descriptive). This trade-off means a round-trip through FSStore
// loses the Metadata map, which is acceptable for a cache that only needs to
// serve bytes back by digest.
type FSStore struct {
	root string

	// PartitionByNamespace, when true, prefixes every resolved path with
	// "ns/<namespace>/" where namespace comes from namespace.FromContext(ctx).
	// Default false: FSStore's fundamental contract is content-addressed
	// global dedup (design §4.2) — the SAME digest resolves to the SAME file
	// regardless of which namespace is asking, which is exactly what lets a
	// server-side backing store (and the many tests that use FSStore as a
	// stand-in for one, e.g. service/apiserver/artifact_endpoint_test.go)
	// Put once under one ambient namespace and Get/Stat under another and
	// still find the bytes.
	//
	// The ONE place that invariant is actively harmful is the runner-side
	// local read-through cache (sdk/xflow/runner.go's
	// newRunnerArtifactResolver): there, a cache HIT bypasses the server
	// entirely, so if namespace A's task fetches and caches digest D, a later
	// task in namespace B on the SAME runner would be served D from disk with
	// zero requests to origin — silently defeating the server's per-tenant
	// HasReference check (module_artifact.go) no matter how correct that
	// check is. See docs/superpowers/specs/2026-08-30-runner-artifact-namespace-authorization-design.md
	// §2(c)/§5.4. Only that call site sets this field; every other FSStore
	// construction in this repository leaves it false.
	PartitionByNamespace bool
}

var _ Store = (*FSStore)(nil)

// NewFSStore creates a store rooted at dir. The directory must already exist;
// subdirectories are created on demand during PutObject.
func NewFSStore(dir string) *FSStore {
	return &FSStore{root: filepath.Clean(dir)}
}

// PutObject writes body to disk atomically using a temp file + rename pattern.
// Concurrent puts to the same key are safe: both rename, last writer wins, both
// produce identical content (content-addressed). If opts.IfNoneMatch is set and
// the file already exists, ErrPreconditionFailed is returned without rewriting.
func (s *FSStore) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*Object, error) {
	path, err := s.resolve(ctx, key)
	if err != nil {
		return nil, err
	}

	if opts.IfNoneMatch {
		if _, statErr := os.Stat(path); statErr == nil {
			return nil, ErrPreconditionFailed
		}
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("objectstore/fs: mkdir %s: %w", dir, err)
	}

	// Write to a temp file in the same directory so rename is atomic (same
	// filesystem). Cleanup on any failure path.
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("objectstore/fs: create temp: %w", err)
	}
	tmpName := tmp.Name()
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	n, err := io.Copy(tmp, body)
	if err != nil {
		return nil, fmt.Errorf("objectstore/fs: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("objectstore/fs: close temp: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("objectstore/fs: size mismatch: wrote %d, expected %d", n, size)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return nil, fmt.Errorf("objectstore/fs: rename: %w", err)
	}
	success = true

	return &Object{
		Key:          key,
		Size:         n,
		ETag:         digestFromKey(key),
		LastModified: time.Now(),
	}, nil
}

// GetObject opens the file for streaming reads. The caller must close the
// returned ReadCloser. Returns ErrNotFound when the file does not exist.
func (s *FSStore) GetObject(ctx context.Context, key string) (io.ReadCloser, *Object, error) {
	path, err := s.resolve(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("objectstore/fs: get %s: %w", key, ErrNotFound)
		}
		return nil, nil, fmt.Errorf("objectstore/fs: open: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("objectstore/fs: stat after open: %w", err)
	}

	obj := &Object{
		Key:          key,
		Size:         info.Size(),
		ETag:         digestFromKey(key),
		LastModified: info.ModTime(),
	}
	return f, obj, nil
}

// HeadObject returns metadata (size, mtime) without reading content. Uses
// os.Stat only. Returns ErrNotFound when the file does not exist.
func (s *FSStore) HeadObject(ctx context.Context, key string) (*Object, error) {
	path, err := s.resolve(ctx, key)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("objectstore/fs: head %s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("objectstore/fs: stat: %w", err)
	}

	return &Object{
		Key:          key,
		Size:         info.Size(),
		ETag:         digestFromKey(key),
		LastModified: info.ModTime(),
	}, nil
}

// resolve maps key to an absolute path and validates that it stays within the
// permitted subtree. This is the path-traversal defence: even though keys
// from ObjectKeyForDigest are structurally safe, this layer must not assume
// the caller validated.
//
// When s.PartitionByNamespace is set, every path is additionally partitioned
// under root/ns/<namespace>/ so two namespaces sharing a runner never resolve
// to the same file for the same digest — see the field's doc comment for why
// that matters and why it defaults off.
func (s *FSStore) resolve(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("objectstore/fs: empty key")
	}
	base := s.root
	if s.PartitionByNamespace {
		ns := namespace.FromContext(ctx)
		if err := namespace.Validate(ns); err != nil {
			return "", fmt.Errorf("objectstore/fs: invalid namespace %q: %w", ns, err)
		}
		base = filepath.Join(s.root, "ns", string(ns))
	}
	joined := filepath.Join(base, filepath.FromSlash(key))
	cleaned := filepath.Clean(joined)
	// Ensure the resolved path is strictly within the permitted subtree — when
	// partitioned, this is stricter than "within root", so a crafted key
	// cannot traverse into a sibling namespace's cache directory either.
	if !strings.HasPrefix(cleaned, base+string(filepath.Separator)) {
		return "", fmt.Errorf("objectstore/fs: key %q resolves outside root", key)
	}
	return cleaned, nil
}

// digestFromKey recovers the "sha256:<hex>" digest from a key shaped like
// "artifacts/sha256/ab/cd/<hex>". If the key does not match, returns empty.
// Shared with HTTPStore (same package).
func digestFromKey(key string) string {
	// Expected: artifacts/<alg>/<xx>/<yy>/<hex>
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		return ""
	}
	alg := parts[1]
	hex := parts[4]
	return alg + ":" + hex
}
