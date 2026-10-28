package distributed

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/rstate"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/blob"
)

// defaultBlobInlineMax is the inline threshold used when BlobConfig.InlineMax is
// left at zero. Zero cannot mean "externalize everything" here: that is a valid
// but surprising setting for a payload threshold, so an unset field gets a
// conventional size instead.
const defaultBlobInlineMax = 64 << 10

// BlobObserver receives the blob layer's events: OnPut and OnGet report payload
// writes and reads, OnGC reports the outcome of each collection pass, and an
// observer that also implements OnGCFailed(failed int) is told about collection
// failures as well. Calls come from many goroutines at once and must not block.
type BlobObserver interface {
	blob.PolicyObserver
	OnGC(deleted, fenced int)
}

// BlobConfig configures the optional blob store behind WithBlobStore.
type BlobConfig struct {
	// Store holds the bytes and Index tracks their lifecycle. Index must be the
	// Index the Store registers its blobs with, otherwise linking a new blob is
	// refused. Both are required.
	Store blob.Store
	Index blob.Index
	// Enabled switches on writing. While it is false the backend only reads
	// references that already exist and never stores a payload. It is never
	// defaulted to true: turning writes on is always an explicit decision.
	Enabled bool
	// InlineMax is the largest encoded payload, in bytes, that stays inline. Zero
	// means 64 KiB; it must not be negative.
	InlineMax int
	// Grace is how long an unreferenced blob is kept before the collector may
	// delete it. It must be positive and is not defaulted: the writer's link
	// margin and the collector's grace have to agree, so the caller chooses it.
	Grace time.Duration
	// Batch bounds one collection pass. It must be positive and is not defaulted,
	// for the same reason as Grace.
	Batch int
	// GCInterval is the wait between collection passes. Zero means the loop's own
	// default; it must not be negative.
	GCInterval time.Duration
	// Observer, when set, is told about blob writes, reads and collection passes.
	Observer BlobObserver
}

// WithBlobStore turns on the blob layer: batch items that exceed
// BlobConfig.InlineMax are kept in the blob store instead of in Redis, and a
// background collector reclaims blobs nothing references any more. Without this
// option the backend behaves exactly as before.
//
// The collector runs only in processes that consume (the default). API-only
// instances (WithConsumer(false)) never start it, so one deployment needs at
// least one consuming process for unreferenced blobs to be reclaimed.
//
// New returns an error when db is nil (the blob layer is part of the durable
// mode) or when the configuration is invalid, before any connection is opened.
// Calling this option again replaces the earlier configuration.
func WithBlobStore(cfg BlobConfig) Option {
	return func(c *config) {
		v := cfg
		c.blob = &v
	}
}

// validateBlobSelection rejects an unusable blob selection before any
// connection is opened. Each failure names only the offending field, never a
// configured value, because a store or index may be built from credentials.
func validateBlobSelection(cfg *config, db store.Store) error {
	b := cfg.blob
	if b == nil {
		return nil
	}
	switch {
	case db == nil:
		return errors.New("distributed backend: WithBlobStore requires a non-nil store.Store")
	case b.Store == nil:
		return errors.New("distributed backend: blob config Store is required")
	case b.Index == nil:
		return errors.New("distributed backend: blob config Index is required")
	case b.InlineMax < 0:
		return errors.New("distributed backend: blob config InlineMax must not be negative")
	case b.Grace <= 0:
		return errors.New("distributed backend: blob config Grace must be positive")
	case b.Batch <= 0:
		return errors.New("distributed backend: blob config Batch must be positive")
	case b.GCInterval < 0:
		return errors.New("distributed backend: blob config GCInterval must not be negative")
	}
	return nil
}

// withDefaults fills the fields that have a default. Grace and Batch have none,
// and Enabled stays whatever the caller chose.
func (c BlobConfig) withDefaults() BlobConfig {
	if c.InlineMax == 0 {
		c.InlineMax = defaultBlobInlineMax
	}
	return c
}

// stateConfig maps the validated config onto the state store's. Observer is an
// interface, so a nil one converts to a nil blob.PolicyObserver rather than to a
// non-nil interface holding nil, and the store's nil check keeps working.
func (c BlobConfig) stateConfig() rstate.BlobConfig {
	return rstate.BlobConfig{
		Store:          c.Store,
		Index:          c.Index,
		Enabled:        c.Enabled,
		InlineMax:      c.InlineMax,
		Grace:          c.Grace,
		PolicyObserver: c.Observer,
	}
}

// gcOptions maps the config onto the collection loop's options. The nil-observer
// reasoning of stateConfig applies to blobGCOptions.Observer as well.
func (c BlobConfig) gcOptions(logger engine.Logger) blobGCOptions {
	return blobGCOptions{Tick: c.GCInterval, Batch: c.Batch, Observer: c.Observer, Logger: logger}
}

// buildBlobGC is newBlobGC behind a variable so tests can run the collection
// loop of bindHandler without a real Collector. Production code never
// reassigns it.
var buildBlobGC = newBlobGC

// newBlobGC builds the Collector whose Run is one collection pass.
func newBlobGC(cfg BlobConfig) (func(context.Context) (blob.CollectResult, error), error) {
	c, err := blob.NewCollector(cfg.Index, cfg.Store, blob.CollectorOptions{Grace: cfg.Grace, Batch: cfg.Batch})
	if err != nil {
		return nil, err
	}
	return c.Run, nil
}
