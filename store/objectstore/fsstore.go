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
func (s *FSStore) PutObject(_ context.Context, key string, body io.Reader, size int64, opts PutOptions) (*Object, error) {
	path, err := s.resolve(key)
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
func (s *FSStore) GetObject(_ context.Context, key string) (io.ReadCloser, *Object, error) {
	path, err := s.resolve(key)
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
func (s *FSStore) HeadObject(_ context.Context, key string) (*Object, error) {
	path, err := s.resolve(key)
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

// resolve maps key to an absolute path and validates that it stays within root.
// This is the path-traversal defence: even though keys from ObjectKeyForDigest
// are structurally safe, this layer must not assume the caller validated.
func (s *FSStore) resolve(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("objectstore/fs: empty key")
	}
	joined := filepath.Join(s.root, filepath.FromSlash(key))
	cleaned := filepath.Clean(joined)
	// Ensure the resolved path is strictly within root.
	if !strings.HasPrefix(cleaned, s.root+string(filepath.Separator)) {
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
