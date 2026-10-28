package blob

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// CollectorOptions configures a Collector. Both fields are required: a zero
// grace would make every unlinked object garbage the instant it is written, and
// a zero batch would make a pass do nothing without saying so.
type CollectorOptions struct {
	// Grace is how long an object must have gone without a write or a Link before
	// it can be collected. It must be longer than the slowest writer takes to get
	// from Index.Register (inside Put) to Index.Link, plus the clock skew between
	// the nodes sharing the Index.
	Grace time.Duration
	// Batch bounds one Run: it prunes at most Batch expired links and examines at
	// most Batch candidates. A Run that reports Candidates == Batch may have left
	// more behind; call it again.
	Batch int
}

// CollectResult says what one Collector.Run did. Candidates is the number of
// objects the Run examined; each ends in exactly one of Deleted, Fenced,
// Skipped, or Failed (or in the error Run returns, when it stops early).
type CollectResult struct {
	// PrunedLinks is how many expired links were removed.
	PrunedLinks int
	// Candidates is how many objects were listed as garbage.
	Candidates int
	// Deleted is how many objects had their bytes and metadata removed.
	Deleted int
	// Fenced is how many deletions were abandoned because a writer took the
	// object back between the claim and the delete. No bytes were removed.
	Fenced int
	// Skipped is how many candidates stopped being garbage, or were removed by
	// another collector, between being listed and being claimed.
	Skipped int
	// Failed is how many objects stayed claimed because the byte delete failed.
	// The next Run retries them.
	Failed int
}

// Collector deletes the blobs nothing references any more. It is the only
// supported way to delete from a Store that has an Index: it claims an object
// through the Index and deletes the bytes through the Index's Purge, so a writer
// that Puts the same digest at the same time either keeps its object or finds it
// cleanly gone, never a Ref to deleted bytes (see Index for the protocol).
//
// A Collector holds no state between runs and several may run at once, on one
// node or many: the Index arbitrates, and a candidate another collector took is
// counted as Skipped.
type Collector struct {
	index Index
	store Store
	grace time.Duration
	batch int
}

// NewCollector returns a Collector that deletes from store the objects index
// reports as garbage. store must be the Store whose Put registers with index.
func NewCollector(index Index, store Store, opts CollectorOptions) (*Collector, error) {
	if index == nil {
		return nil, errors.New("blob: collector needs an index")
	}
	if store == nil {
		return nil, errors.New("blob: collector needs a store")
	}
	if err := ValidateGCArgs(opts.Grace, opts.Batch); err != nil {
		return nil, err
	}
	return &Collector{index: index, store: store, grace: opts.Grace, batch: opts.Batch}, nil
}

// deleteError marks an error that came from Store.Delete inside Index.Purge, so
// the collector can tell a failed byte delete (retry the object next run) from a
// failed index operation (stop, the Index is unhealthy).
type deleteError struct{ err error }

func (e *deleteError) Error() string { return e.err.Error() }
func (e *deleteError) Unwrap() error { return e.err }

// Run performs one collection pass: it prunes expired links, lists candidates,
// and for each one claims it, deletes its bytes, and removes its metadata, with
// the Index confirming under its own lock that no writer revived the object in
// between.
//
// A failed byte delete leaves the object claimed and does not stop the pass: one
// object that cannot be deleted must not starve the ones behind it. Those errors
// are joined and returned after the pass, along with the counts. Any other
// error (the Index failing, ctx ending) stops the pass at once. The counts
// always describe the work done so far.
func (c *Collector) Run(ctx context.Context) (CollectResult, error) {
	var res CollectResult
	pruned, err := c.index.PruneLinks(ctx, c.batch)
	res.PrunedLinks = pruned
	if err != nil {
		return res, fmt.Errorf("blob: prune links: %w", err)
	}
	candidates, err := c.index.Candidates(ctx, c.grace, c.batch)
	if err != nil {
		return res, fmt.Errorf("blob: list candidates: %w", err)
	}
	res.Candidates = len(candidates)

	var failures []error
	// stop ends the pass with err, keeping any delete failures already seen.
	stop := func(err error) (CollectResult, error) {
		return res, errors.Join(append(failures, err)...)
	}
	for _, ref := range candidates {
		if err := ctx.Err(); err != nil {
			return stop(err)
		}
		fence, err := c.index.MarkDeleting(ctx, ref, c.grace)
		if errors.Is(err, ErrNotCandidate) || errors.Is(err, ErrNotFound) {
			res.Skipped++
			continue
		}
		if err != nil {
			return stop(fmt.Errorf("blob: claim %s/%s: %w", ref.Namespace, ref.Digest, err))
		}
		ns := namespace.Namespace(fence.Ref.Namespace)
		err = c.index.Purge(ctx, fence, func(ctx context.Context) error {
			if err := c.store.Delete(ctx, ns, fence.Ref); err != nil {
				return &deleteError{err: err}
			}
			return nil
		})
		var del *deleteError
		switch {
		case err == nil:
			res.Deleted++
		case errors.Is(err, ErrFenced):
			res.Fenced++
		case errors.Is(err, ErrDeleteUnsupported):
			// Permanent for the whole Store: every other candidate would fail the
			// same way.
			return stop(fmt.Errorf("blob: delete %s/%s: %w", ref.Namespace, ref.Digest, err))
		case errors.As(err, &del):
			res.Failed++
			failures = append(failures, fmt.Errorf("blob: delete %s/%s: %w", ref.Namespace, ref.Digest, err))
		default:
			return stop(fmt.Errorf("blob: purge %s/%s: %w", ref.Namespace, ref.Digest, err))
		}
	}
	return res, errors.Join(failures...)
}
