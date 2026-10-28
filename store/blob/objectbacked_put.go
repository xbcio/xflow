package blob

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/xbcio/xflow/store/objectstore"
)

// store writes one framed object under key. Normally it does not overwrite: if
// the object already exists it runs a light plausibility check on the existing
// one instead (see checkExisting), and if that object has vanished in the
// meantime, because a concurrent garbage collection removed it, store writes once
// more. With overwrite set it writes unconditionally and once, for a caller that
// has just taken the object back from a collector and so cannot trust whatever
// is stored under key.
func (b *ObjectBacked) store(ctx context.Context, key string, open func() (io.Reader, int64), size int64, overwrite bool) error {
	if overwrite {
		body, bodyLen := open()
		if _, err := b.objects.PutObject(ctx, key, body, bodyLen, objectstore.PutOptions{}); err != nil {
			return fmt.Errorf("blob: put %s: %w", key, err)
		}
		return nil
	}
	opts := objectstore.PutOptions{IfNoneMatch: true}
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		body, bodyLen := open()
		if _, err = b.objects.PutObject(ctx, key, body, bodyLen, opts); err == nil {
			return nil
		}
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return fmt.Errorf("blob: put %s: %w", key, err)
		}
		switch err = b.checkExisting(ctx, key, size); {
		case err == nil:
			return nil
		case errors.Is(err, objectstore.ErrNotFound):
			continue // Deleted between the failed write and the check: write again.
		default:
			return err
		}
	}
	return fmt.Errorf("blob: put %s: object kept disappearing: %w", key, err)
}

// checkExisting inspects the object a duplicate Put found. It reads metadata
// only. An empty object, or one longer than any valid frame for size, cannot be
// something this package wrote and is reported as ErrCorrupt without being
// touched. A missing object is returned as an error wrapping
// objectstore.ErrNotFound so store can retry.
func (b *ObjectBacked) checkExisting(ctx context.Context, key string, size int64) error {
	info, err := b.objects.HeadObject(ctx, key)
	if err != nil {
		return fmt.Errorf("blob: head %s: %w", key, err)
	}
	if info.Size == 0 || info.Size > frameReadLimit(size) {
		return fmt.Errorf("%w: existing object %s has %d bytes, implausible for a %d byte blob", ErrCorrupt, key, info.Size, size)
	}
	return nil
}
