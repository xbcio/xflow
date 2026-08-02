package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// testKey is a valid two-level-sharded key matching ObjectKeyForDigest output.
const testKey = "artifacts/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

func TestFSStore_PutGetHead(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()

	content := []byte("hello world")
	obj, err := fs.PutObject(ctx, testKey, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if obj.Size != int64(len(content)) {
		t.Fatalf("Size = %d, want %d", obj.Size, len(content))
	}
	if obj.ETag != "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" {
		t.Fatalf("ETag = %q, unexpected", obj.ETag)
	}

	// GetObject returns streaming content.
	rc, gObj, err := fs.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("GetObject content = %q, want %q", got, content)
	}
	if gObj.Size != int64(len(content)) {
		t.Fatalf("GetObject Size = %d, want %d", gObj.Size, len(content))
	}

	// HeadObject returns metadata without content.
	hObj, err := fs.HeadObject(ctx, testKey)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if hObj.Size != int64(len(content)) {
		t.Fatalf("HeadObject Size = %d, want %d", hObj.Size, len(content))
	}
}

func TestFSStore_IfNoneMatch(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()

	content := []byte("original")
	_, err := fs.PutObject(ctx, testKey, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{})
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	// Second put with IfNoneMatch should fail with ErrPreconditionFailed.
	_, err = fs.PutObject(ctx, testKey, bytes.NewReader([]byte("different")), 9, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("IfNoneMatch put = %v, want ErrPreconditionFailed", err)
	}

	// Original content should be unchanged.
	rc, _, err := fs.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("GetObject after IfNoneMatch: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("content changed after IfNoneMatch rejection: got %q", got)
	}
}

func TestFSStore_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()

	// Keys that escape the root directory — these MUST be rejected.
	escaping := []string{
		"../../etc/passwd",
		"../../../etc/shadow",
		"../sibling/file",
	}
	for _, key := range escaping {
		_, err := fs.PutObject(ctx, key, bytes.NewReader([]byte("x")), 1, objectstore.PutOptions{})
		if err == nil {
			t.Errorf("PutObject(%q) should have been rejected (escapes root)", key)
		}
		_, _, err = fs.GetObject(ctx, key)
		if err == nil {
			t.Errorf("GetObject(%q) should have been rejected (escapes root)", key)
		}
		_, err = fs.HeadObject(ctx, key)
		if err == nil {
			t.Errorf("HeadObject(%q) should have been rejected (escapes root)", key)
		}
	}

	// Keys that resolve within root after cleaning are allowed by the traversal
	// check (they cannot read outside the store). Verify they do NOT error on the
	// traversal check itself — they may error on other grounds (file not found).
	withinRoot := []string{
		"artifacts/sha256/../../etc/passwd", // resolves to <root>/etc/passwd
	}
	for _, key := range withinRoot {
		_, _, err := fs.GetObject(ctx, key)
		// Should get ErrNotFound (file doesn't exist), NOT a traversal error.
		if err != nil && strings.Contains(err.Error(), "resolves outside root") {
			t.Errorf("GetObject(%q) was incorrectly rejected as traversal", key)
		}
	}
}

func TestFSStore_GetNotFound(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()

	_, _, err := fs.GetObject(ctx, testKey)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("GetObject missing = %v, want ErrNotFound", err)
	}

	_, err = fs.HeadObject(ctx, testKey)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("HeadObject missing = %v, want ErrNotFound", err)
	}
}

func TestHTTPStore_GetObject_OK(t *testing.T) {
	content := []byte("wasm binary content here")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		// Verify token is in header, not URL.
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("Authorization = %q, want Bearer test-token", auth)
		}
		if strings.Contains(r.URL.RawQuery, "test-token") || strings.Contains(r.URL.Path, "test-token") {
			t.Error("token leaked into URL")
		}
		w.Header().Set("Content-Length", "24")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	hs := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "test-token", Client: srv.Client()}
	rc, obj, err := hs.GetObject(context.Background(), testKey)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("content = %q, want %q", got, content)
	}
	if obj.ETag != "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" {
		t.Fatalf("ETag = %q", obj.ETag)
	}
}

func TestHTTPStore_GetObject_404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	hs := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "t", Client: srv.Client()}
	_, _, err := hs.GetObject(context.Background(), testKey)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("404 mapped to %v, want ErrNotFound", err)
	}
}

func TestHTTPStore_HeadObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	hs := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "t", Client: srv.Client()}
	obj, err := hs.HeadObject(context.Background(), testKey)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if obj.Size != 1024 {
		t.Fatalf("Size = %d, want 1024", obj.Size)
	}
}

func TestHTTPStore_PutObject_Unsupported(t *testing.T) {
	hs := &objectstore.HTTPStore{BaseURL: "http://unused"}
	_, err := hs.PutObject(context.Background(), testKey, nil, 0, objectstore.PutOptions{})
	if err == nil {
		t.Fatal("PutObject should return error")
	}
}

func TestHTTPStore_TokenInHeader(t *testing.T) {
	var gotAuth string
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	hs := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "secret-token", Client: srv.Client()}
	rc, _, err := hs.GetObject(context.Background(), testKey)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	_ = rc.Close()

	if gotAuth != "Bearer secret-token" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if strings.Contains(gotURL, "secret-token") {
		t.Error("token appeared in URL")
	}
}

// countingStore wraps an objectstore.Store and counts GetObject calls.
type countingStore struct {
	objectstore.Store
	gets atomic.Int64
}

func (c *countingStore) GetObject(ctx context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	c.gets.Add(1)
	return c.Store.GetObject(ctx, key)
}

func (c *countingStore) HeadObject(ctx context.Context, key string) (*objectstore.Object, error) {
	return c.Store.HeadObject(ctx, key)
}

func (c *countingStore) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (*objectstore.Object, error) {
	return c.Store.PutObject(ctx, key, body, size, opts)
}

// TestReadThrough_CacheMissThenHit proves: cache miss -> fetch origin -> write
// cache -> second request hits cache without a second origin call.
func TestReadThrough_CacheMissThenHit(t *testing.T) {
	content := []byte("artifact bytes")
	originDir := t.TempDir()
	originFS := objectstore.NewFSStore(originDir)
	ctx := context.Background()

	// Seed the origin.
	_, err := originFS.PutObject(ctx, testKey, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{})
	if err != nil {
		t.Fatalf("seed origin: %v", err)
	}

	cacheDir := t.TempDir()
	cacheFS := objectstore.NewFSStore(cacheDir)

	origin := &countingStore{Store: originFS}
	rt := objectstore.NewReadThrough(cacheFS, origin)

	// First request: cache miss, hits origin.
	rc, obj, err := rt.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("first GetObject: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("first read = %q, want %q", got, content)
	}
	if obj.Size != int64(len(content)) {
		t.Fatalf("first Size = %d", obj.Size)
	}
	if origin.gets.Load() != 1 {
		t.Fatalf("origin gets after first = %d, want 1", origin.gets.Load())
	}

	// Second request: should hit cache, no additional origin call.
	rc, _, err = rt.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("second GetObject: %v", err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("second read = %q, want %q", got, content)
	}
	if origin.gets.Load() != 1 {
		t.Fatalf("origin gets after second = %d, want 1 (cache should hit)", origin.gets.Load())
	}
}

// TestReadThrough_SizeMismatch proves that a size discrepancy between origin's
// reported size and actual bytes causes an error (design doc §7 step 5).
func TestReadThrough_SizeMismatch(t *testing.T) {
	// Build an origin that lies about size: reports 100 but returns 5 bytes.
	origin := &fakeSizeOrigin{
		content:      []byte("short"),
		reportedSize: 100,
	}

	cacheDir := t.TempDir()
	cacheFS := objectstore.NewFSStore(cacheDir)
	rt := objectstore.NewReadThrough(cacheFS, origin)

	_, _, err := rt.GetObject(context.Background(), testKey)
	if err == nil {
		t.Fatal("expected size mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("error = %v, want size mismatch", err)
	}
}

// fakeSizeOrigin is a Store that lies about size for testing size validation.
type fakeSizeOrigin struct {
	content      []byte
	reportedSize int64
}

func (f *fakeSizeOrigin) PutObject(_ context.Context, _ string, _ io.Reader, _ int64, _ objectstore.PutOptions) (*objectstore.Object, error) {
	return nil, errors.New("not supported")
}

func (f *fakeSizeOrigin) GetObject(_ context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	obj := &objectstore.Object{
		Key:  key,
		Size: f.reportedSize,
	}
	return io.NopCloser(bytes.NewReader(f.content)), obj, nil
}

func (f *fakeSizeOrigin) HeadObject(_ context.Context, key string) (*objectstore.Object, error) {
	return &objectstore.Object{Key: key, Size: f.reportedSize}, nil
}

// TestReadThrough_ConcurrentCacheMiss proves that multiple goroutines racing on
// the same cache miss do not produce a corrupted file: all of them should see
// the correct content after the dust settles.
func TestReadThrough_ConcurrentCacheMiss(t *testing.T) {
	content := []byte("concurrent artifact content that must survive intact")
	originDir := t.TempDir()
	originFS := objectstore.NewFSStore(originDir)
	ctx := context.Background()

	_, err := originFS.PutObject(ctx, testKey, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}

	cacheDir := t.TempDir()
	cacheFS := objectstore.NewFSStore(cacheDir)
	rt := objectstore.NewReadThrough(cacheFS, originFS)

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			rc, _, e := rt.GetObject(ctx, testKey)
			if e != nil {
				errs[idx] = e
				return
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if !bytes.Equal(got, content) {
				errs[idx] = errors.New("content mismatch")
			}
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: %v", i, e)
		}
	}

	// Verify final cached file is correct.
	rc, _, err := cacheFS.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("final cache read: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("final cached content = %q, want %q", got, content)
	}
}

// TestReadThrough_CacheUnwritable_FailOpen proves that when the cache directory
// is not writable, ReadThrough falls back to origin every time without returning
// an error to the caller.
func TestReadThrough_CacheUnwritable_FailOpen(t *testing.T) {
	content := []byte("origin content")
	originDir := t.TempDir()
	originFS := objectstore.NewFSStore(originDir)
	ctx := context.Background()

	_, err := originFS.PutObject(ctx, testKey, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a file where the cache root directory should be — MkdirAll inside
	// PutObject will fail because a file occupies the path. This reliably
	// causes write failure regardless of running as root (where chmod 0 would
	// not actually restrict).
	cacheRoot := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(cacheRoot, []byte("I am a file"), 0o644); err != nil {
		t.Fatal(err)
	}
	cacheFS := objectstore.NewFSStore(cacheRoot)
	origin := &countingStore{Store: originFS}
	rt := objectstore.NewReadThrough(cacheFS, origin)

	// First call: origin hit, cache write fails silently.
	rc, _, err := rt.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("first GetObject with unwritable cache: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("content = %q, want %q", got, content)
	}

	// Second call: still hits origin (cache never got written).
	rc, _, err = rt.GetObject(ctx, testKey)
	if err != nil {
		t.Fatalf("second GetObject with unwritable cache: %v", err)
	}
	_ = rc.Close()
	if origin.gets.Load() != 2 {
		t.Fatalf("origin gets = %d, want 2 (cache should never succeed)", origin.gets.Load())
	}
}

// TestReadThrough_HeadCacheThenOrigin verifies HeadObject checks cache first.
func TestReadThrough_HeadCacheThenOrigin(t *testing.T) {
	content := []byte("head test")
	cacheDir := t.TempDir()
	cacheFS := objectstore.NewFSStore(cacheDir)
	ctx := context.Background()

	// Put in cache directly.
	_, err := cacheFS.PutObject(ctx, testKey, bytes.NewReader(content), int64(len(content)), objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Origin that would error if called.
	origin := &fakeSizeOrigin{content: nil, reportedSize: 999}
	rt := objectstore.NewReadThrough(cacheFS, origin)

	obj, err := rt.HeadObject(ctx, testKey)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	// Should reflect cache (9 bytes), not origin's lie (999).
	if obj.Size != int64(len(content)) {
		t.Fatalf("HeadObject Size = %d, want %d", obj.Size, len(content))
	}
}
