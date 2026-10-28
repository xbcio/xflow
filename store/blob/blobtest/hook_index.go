package blobtest

import (
	"context"
	"time"

	"github.com/xbcio/xflow/store/blob"
)

// HookIndex wraps an Index and runs a callback at the one point in a collection
// pass where the fence matters: after MarkDeleting has claimed an object and
// before the collector's Purge. The callback runs in the collector's own
// goroutine, so an interleaving ("a writer arrives exactly now") is reproduced
// by calling the writer from the hook, with no sleeps and no scheduler luck.
type HookIndex struct {
	blob.Index

	// AfterMark, when set, runs after a successful MarkDeleting and before it
	// returns to the caller, with the claim the collector now holds. It runs with
	// no index lock held (MarkDeleting has already returned inside the wrapped
	// Index), so it may call any Index or Store method.
	AfterMark func(fence blob.Fence)
}

// MarkDeleting implements blob.Index.
func (h *HookIndex) MarkDeleting(ctx context.Context, ref blob.Ref, grace time.Duration) (blob.Fence, error) {
	fence, err := h.Index.MarkDeleting(ctx, ref, grace)
	if err == nil && h.AfterMark != nil {
		h.AfterMark(fence)
	}
	return fence, err
}
