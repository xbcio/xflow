package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/objectstore"
)

// Get implements Store. It checks the ref, reads at most a bounded number of
// bytes, decodes by the frame header, and verifies both the decoded length and
// the SHA-256 against ref before returning anything.
func (b *ObjectBacked) Get(ctx context.Context, ns namespace.Namespace, ref Ref) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkRef(ns, ref); err != nil {
		return nil, err
	}
	if ref.Size > b.maxBytes {
		return nil, fmt.Errorf("%w: ref size %d exceeds the %d byte limit", ErrTooLarge, ref.Size, b.maxBytes)
	}
	key, err := objectKey(ns, ref.Digest)
	if err != nil {
		return nil, err
	}
	frame, err := b.readObject(ctx, key, frameReadLimit(ref.Size))
	if err != nil {
		return nil, err
	}
	data, err := decodeFrame(frame, ref.Size)
	if err != nil {
		return nil, fmt.Errorf("%w (object %s)", err, key)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != ref.Digest {
		return nil, fmt.Errorf("%w: digest of object %s differs from ref", ErrCorrupt, key)
	}
	// A raw frame aliases the buffer readObject returned, which only this call
	// holds, so the caller owns the result as the Store contract requires.
	return data, nil
}

// readObject reads the whole object under key, failing with ErrCorrupt if it is
// longer than limit bytes. It never buffers more than limit+1 bytes, so an
// oversized or hostile object cannot exhaust memory.
func (b *ObjectBacked) readObject(ctx context.Context, key string, limit int64) ([]byte, error) {
	rc, _, err := b.objects.GetObject(ctx, key)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("blob: get %s: %w", key, err)
	}
	// The body has been fully consumed or abandoned by now, so a Close failure
	// cannot change the result and there is nothing useful to do with it.
	defer func() { _ = rc.Close() }()
	frame, err := io.ReadAll(io.LimitReader(rc, limit+1))
	if err != nil {
		return nil, fmt.Errorf("blob: read %s: %w", key, err)
	}
	if int64(len(frame)) > limit {
		return nil, fmt.Errorf("%w: object %s is longer than %d bytes", ErrCorrupt, key, limit)
	}
	return frame, nil
}

// Delete implements Store. It needs the underlying objectstore.Store to
// implement objectstore.Deleter and returns ErrDeleteUnsupported otherwise.
// Deleting a missing blob returns nil.
func (b *ObjectBacked) Delete(ctx context.Context, ns namespace.Namespace, ref Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkRef(ns, ref); err != nil {
		return err
	}
	deleter, ok := b.objects.(objectstore.Deleter)
	if !ok {
		return ErrDeleteUnsupported
	}
	key, err := objectKey(ns, ref.Digest)
	if err != nil {
		return err
	}
	if err := deleter.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("blob: delete %s: %w", key, err)
	}
	return nil
}
