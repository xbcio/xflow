// Package emitonce is the deduplicate-then-emit path shared by the triggers
// that do not admit through entry seed. It lives in one package because the
// ordering it encodes is a delivery guarantee, not a convenience: a claim taken
// before an emit that then fails would make the redelivery look like a
// duplicate and be dropped, turning an at-least-once trigger into an
// at-most-once one.
package emitonce

import (
	"context"
	"log/slog"
	"time"

	"github.com/xbcio/xflow/types"
)

// dedupReleaser is the optional capability that undoes a Dedup claim. It is
// discovered rather than required: TriggerRuntime is a public contract, and a
// runtime that cannot release (the entry-seed runtime, a bare test stub) must
// still be usable — the claim is then left to expire.
type dedupReleaser interface {
	ReleaseDedup(ctx context.Context, key string) error
}

// Emit deduplicates event under key and emits it at most once, reporting
// whether it was emitted. A false result with a nil error is a duplicate: an
// earlier delivery already claimed the key, so there is nothing left to do.
//
// The claim is released when Emit fails. Without that release a redelivery of
// the same event would be answered as a duplicate for the whole TTL and
// silently dropped; releasing it is what lets the redelivery be processed
// instead. A release that itself fails is logged and not returned: the caller
// must act on the emit error it was given, and the event is never emitted a
// second time here.
func Emit(ctx context.Context, in *types.TriggerActivateInput, key string, ttl time.Duration, event *types.TriggerEvent) (bool, error) {
	fresh, err := in.Runtime.Dedup(ctx, key, ttl)
	if err != nil {
		return false, err
	}
	if !fresh {
		return false, nil
	}
	if _, err := in.Emit(ctx, event); err != nil {
		release(ctx, in, key)
		return false, err
	}
	return true, nil
}

// release undoes the claim one failed emit left behind.
//
// It runs on a context detached from cancellation. The emit may have failed
// BECAUSE its context was cancelled (shutdown, a departed HTTP caller), and a
// release that inherits that cancellation is guaranteed to fail — leaving the
// key claimed for the full TTL, which is exactly the loss this release exists
// to prevent. Values are preserved, so the namespace the primitives scope the
// key by is still there.
func release(ctx context.Context, in *types.TriggerActivateInput, key string) {
	releaser, ok := in.Runtime.(dedupReleaser)
	if !ok {
		slog.Warn("trigger dedup key was not released after emit failure: runtime has no dedup release capability",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"key", key)
		return
	}
	if err := releaser.ReleaseDedup(context.WithoutCancel(ctx), key); err != nil {
		slog.Warn("trigger dedup key release failed after emit failure",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"key", key,
			"err", err)
	}
}
