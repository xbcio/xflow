package rstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/types"
)

var _ engine.BatchItemsStore = (*Store)(nil)

// ErrBlobNotConfigured is returned by ResolveItems on a Store whose ConfigureBlob
// was never called: a reference to stored items reached a process that has no
// blob store to read them from.
var ErrBlobNotConfigured = errors.New("rstate: blob store is not configured")

// BlobObserver receives the Store-level blob events that blob.Policy cannot
// see. Policy reports puts and gets through blob.PolicyObserver, which
// BlobConfig carries separately, so a metrics adapter can implement both
// without this package depending on it. Methods run synchronously on the
// caller's goroutine, may be called from many goroutines at once, and must not
// block. A nil BlobObserver is valid and costs nothing.
//
// Nothing in this file emits OnOutboxExternalized: it is for the outbox write
// path, which stores a payload outside the outbox entry and is not part of the
// batch-items capability implemented here.
type BlobObserver interface {
	// OnOutboxExternalized reports one outbox payload kept in the blob store
	// instead of inline in the entry.
	OnOutboxExternalized()
}

// BlobConfig configures how a Store keeps large payloads outside Redis.
type BlobConfig struct {
	// Store holds the bytes and Index tracks their lifecycle. Index must be the
	// Index the Store registers its blobs with (see blob.ObjectBackedOptions.Index),
	// otherwise linking a freshly written blob is refused. Both are required.
	Store blob.Store
	Index blob.Index
	// Enabled switches on writing. While it is false the Store only reads
	// references that already exist, and ExternalizeItems never encodes, stores or
	// links anything.
	Enabled bool
	// InlineMax is the largest encoded payload, in bytes, that stays inline.
	// Zero externalizes every non-empty payload; it must not be negative.
	InlineMax int
	// Grace is how long after its last link expires a blob is still kept; it must
	// match the grace period of the collector that reclaims these blobs, and it
	// must be positive when Enabled is true.
	Grace time.Duration
	// PolicyObserver, when set, is told the outcome of each blob write and read
	// that reached the Store. Observer receives the events of this package.
	PolicyObserver blob.PolicyObserver
	Observer       BlobObserver

	// now is the clock link expiry is computed from; nil means time.Now. It is
	// unexported so only tests in this package can replace it, and they must give
	// the Index the same clock.
	now func() time.Time
}

// blobRuntime is the immutable state ConfigureBlob installs. A Store without one
// behaves exactly as it did before blob support existed.
type blobRuntime struct {
	policy    *blob.Policy
	enabled   bool
	inlineMax int
	grace     time.Duration
	now       func() time.Time
	observer  BlobObserver
}

// ConfigureBlob installs the blob store ExternalizeItems and ResolveItems use.
// Until it is called a Store behaves exactly as it did before blob support
// existed: ExternalizeItems reports "not externalized" without encoding or
// storing anything, and ResolveItems fails with ErrBlobNotConfigured.
//
// Like the other Configure methods it is meant to run before the Store starts
// serving; the installed state is immutable, and a later call replaces it whole.
func (s *Store) ConfigureBlob(cfg BlobConfig) error {
	if cfg.Enabled && cfg.Grace <= 0 {
		return fmt.Errorf("rstate: blob grace %s is not positive", cfg.Grace)
	}
	policy, err := blob.NewPolicy(cfg.Store, cfg.Index, blob.PolicyOptions{
		InlineMax: cfg.InlineMax,
		Observer:  cfg.PolicyObserver,
	})
	if err != nil {
		return err
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	s.blob.Store(&blobRuntime{
		policy:    policy,
		enabled:   cfg.Enabled,
		inlineMax: cfg.InlineMax,
		grace:     cfg.Grace,
		now:       now,
		observer:  cfg.Observer,
	})
	return nil
}

// blobLinkExpiry returns when the link that keeps a blob alive may lapse:
// now + 2*ttl + grace, where ttl is the owning execution's retention as
// ExternalizeItems resolves it: the larger of what this process knows (a
// per-execution override it created itself, else the process default) and the
// retention record persisted when the execution was created.
//
// The process-local value alone is not enough. An execution created by another
// replica, or before this process restarted, has no local override, and the
// process default can be shorter than the retention it was created with; a link
// sized from that would lapse while the execution is still alive and let the
// collector reclaim a blob its map node still needs.
//
// One ttl is not enough. A reference to the blob can outlive the execution's
// state keys: an outbox entry that keeps failing is moved to the dead-letter
// set, and that set is given a fresh ttl at the moment the entry arrives, which
// is after the retries that led there. So the last reader can come up to a full
// ttl after the first ttl has run out, and the blob has to be there for it. The
// factor 2 covers that second window; grace is the collector's own margin on top
// (see blob.Index.Link). A reference held longer than that is outside the
// retention the execution's own keys promise.
func blobLinkExpiry(now time.Time, ttl, grace time.Duration) time.Time {
	return now.Add(2*ttl + grace)
}

// ExternalizeItems implements engine.BatchItemsStore. It stores the items array
// of a map node as one blob, linked to execID, and returns the reference that
// stands in for it.
//
// externalized=false with a nil error means nothing was stored and the caller
// keeps the items inline. That is the answer when ConfigureBlob was never
// called, when writing is not enabled (in both cases the items are not even
// encoded), when the encoded array is no larger than InlineMax, when the blob
// layer gave up linking the blob and chose inline itself, and when the blob
// Store refused the array as larger than it accepts (blob.ErrTooLarge). The
// last case is not a failure: without blob support the same array would have
// stayed inline and the map would have run, so enabling the capability must not
// turn that into an error. Policy has already counted the refusal as too_large,
// so it is not counted again here. Any other failure of the blob layer is
// returned as is, because only the caller can tell a store that is down from a
// payload that is simply small.
//
// The link's lifetime is sized from the execution's retention, which is the
// larger of the process-local value and the persisted retention record (see
// blobLinkExpiry). That record is read before anything is written, so a failed
// read is returned with no blob and no link left behind; a record that is gone
// (engine.ExecutionRetentionUnknown) just leaves the process-local value in use.
func (s *Store) ExternalizeItems(ctx context.Context, execID string, items []any) (json.RawMessage, bool, error) {
	rt := s.blob.Load()
	if rt == nil || !rt.enabled {
		return nil, false, nil
	}
	data, err := json.Marshal(items)
	if err != nil {
		return nil, false, fmt.Errorf("rstate: encode batch items: %w", err)
	}
	id := types.ExecutionID(execID)
	// An array that stays inline needs no link, so it must not pay for a Redis
	// read, nor fail because that read did. The threshold is the one Policy
	// applies; checking it here only skips the read.
	if len(data) <= rt.inlineMax {
		return nil, false, nil
	}
	ttl := s.getExecTTL(ctx, id)
	retention, err := s.GetExecutionRetention(ctx, id)
	if err != nil {
		return nil, false, fmt.Errorf("rstate: read execution retention for batch items: %w", err)
	}
	// Taking the larger value can only keep a blob longer, never reclaim it
	// early, so a stale or default-sized local ttl is the only thing it corrects.
	if retention != engine.ExecutionRetentionUnknown && retention > ttl {
		ttl = retention
	}
	expireAt := blobLinkExpiry(rt.now(), ttl, rt.grace)
	ref, externalized, err := rt.policy.Externalize(ctx, namespace.FromContext(ctx), execID, data, expireAt)
	if errors.Is(err, blob.ErrTooLarge) {
		return nil, false, nil
	}
	if err != nil || !externalized {
		return nil, false, err
	}
	raw, err := json.Marshal(ref)
	if err != nil {
		return nil, false, fmt.Errorf("rstate: encode batch items ref: %w", err)
	}
	return raw, true, nil
}

// ResolveItems implements engine.BatchItemsStore. It returns the array a
// reference from ExternalizeItems was made from.
//
// Reading does not depend on BlobConfig.Enabled: a Store that no longer writes
// references still resolves the ones already in flight. Without ConfigureBlob
// there is nothing to read from and the error is ErrBlobNotConfigured.
//
// errors.Is tells the conditions apart. blob.ErrNotFound, blob.ErrCorrupt,
// blob.ErrNamespaceMismatch, blob.ErrInvalidRef and blob.ErrTooLarge are
// permanent: re-reading the same reference cannot change the answer. They are
// returned wrapping both their blob sentinel and types.ErrPermanent, so the
// engine, which must not import the blob layer, can tell from types.IsPermanent
// alone that a retry is pointless and settle the batch instead. The same goes
// for a reference that is not a blob reference and for stored bytes that are not
// an array.
//
// Two conditions deliberately stay unmarked. ErrBlobNotConfigured describes this
// process's wiring, not the data: the same batch may well resolve on a replica
// that has the blob store, so a retry elsewhere can succeed. blob.ErrTransient
// (everything the blob layer could not classify) is a store that may recover.
//
// The namespace comes from ctx, and a reference issued in another namespace is
// refused by the blob layer, not by this method.
//
// Numbers decode as float64, the same as the items would after a trip through
// the task queue.
//
// Known gap: the link that keeps the blob alive is written once, when the items
// are externalized, and expires at now + 2*ttl + grace. An execution whose
// retention is renewed (see extendExecTTL) can run longer than that, and
// extending the link would need the execution ID. Neither the reference nor the
// BatchItemsStore.ResolveItems signature carries one, so this method cannot call
// Index.Link and does not try. A batch that is retried after the link has
// expired and the blob has been collected fails with blob.ErrNotFound.
func (s *Store) ResolveItems(ctx context.Context, ref json.RawMessage) ([]any, error) {
	rt := s.blob.Load()
	if rt == nil {
		return nil, ErrBlobNotConfigured
	}
	var r blob.Ref
	if err := json.Unmarshal(ref, &r); err != nil {
		// The decode error quotes input; the reference came from state and is not
		// trusted, so none of it is repeated.
		return nil, markPermanent(fmt.Errorf("%w: items reference is not a blob reference", blob.ErrInvalidRef))
	}
	data, err := rt.policy.Resolve(ctx, namespace.FromContext(ctx), r)
	if err != nil {
		return nil, classifyResolveError(err)
	}
	var items []any
	if err := json.Unmarshal(data, &items); err != nil {
		// The bytes matched the reference's digest, so they are what was stored;
		// they are just not an array. Retrying cannot change that.
		return nil, markPermanent(fmt.Errorf("%w: stored items are not a JSON array", blob.ErrCorrupt))
	}
	return items, nil
}

// permanentBlobErrors are the blob conditions a re-read cannot change: the
// object is gone, damaged, someone else's, malformed as a reference, or refused
// for its size. blob.ErrTransient is deliberately absent.
var permanentBlobErrors = []error{
	blob.ErrNotFound,
	blob.ErrCorrupt,
	blob.ErrNamespaceMismatch,
	blob.ErrInvalidRef,
	blob.ErrTooLarge,
}

// markPermanent stamps err as one that must not be retried while keeping every
// sentinel already in its chain matchable, so a caller can still errors.Is for
// the blob condition behind it.
func markPermanent(err error) error {
	return fmt.Errorf("%w: %w", types.ErrPermanent, err)
}

// classifyResolveError marks the permanent blob conditions and returns every
// other error, transient ones included, untouched.
func classifyResolveError(err error) error {
	for _, permanent := range permanentBlobErrors {
		if errors.Is(err, permanent) {
			return markPermanent(err)
		}
	}
	return err
}
