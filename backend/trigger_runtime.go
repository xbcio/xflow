package backend

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

type TriggerPrimitives interface {
	Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error)
	TryLock(ctx context.Context, key string, ttl time.Duration) (types.TriggerLock, bool, error)
	State(ctx context.Context, scope string) types.TriggerState
}

// DedupReleaser is the optional capability behind Dedup: it removes a claim
// whose emission failed, so a redelivery of the same event is not treated as a
// duplicate and dropped for the rest of the claim's TTL.
//
// It is not part of TriggerPrimitives because a provider that cannot release
// is still a usable provider — the claim expires on its own — and adding a
// required method to a public interface would break every external
// implementation to improve one failure path.
type DedupReleaser interface {
	ReleaseDedup(ctx context.Context, key string) error
}
