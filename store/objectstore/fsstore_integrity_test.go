package objectstore_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// TestFSStore_PutObject_SizeMismatchRejected proves that FSStore.PutObject
// itself refuses to accept a body whose actual byte count disagrees with the
// caller-supplied size, independent of ReadThrough's own size check.
//
// If the `n != size` guard inside PutObject were removed (or short-circuited
// to always pass), a truncated or overlong write would still be renamed into
// place and reported back as success with a lying Object.Size — silently
// planting a corrupt, under- or over-length artifact on disk that later
// consumers (e.g. a wasm loader) would trust and fail on far from the write
// site, with no error at the point of corruption.
func TestFSStore_PutObject_SizeMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()

	content := "actual content is eleven"
	claimedSize := int64(len(content)) + 1000 // lie about the size directly to FSStore.

	_, err := fs.PutObject(ctx, testKey, io.NopCloser(strings.NewReader(content)), claimedSize, objectstore.PutOptions{})
	if err == nil {
		t.Fatal("PutObject with mismatched size should have failed, got nil error")
	}
	if !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("error = %v, want size mismatch", err)
	}

	// No partial/renamed file should be readable at the key: the rename must
	// not have happened.
	_, _, getErr := fs.GetObject(ctx, testKey)
	if getErr == nil {
		t.Fatal("GetObject succeeded after a rejected PutObject; a corrupt file was committed")
	}
}
