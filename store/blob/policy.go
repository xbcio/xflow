package blob

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// maxLinkAttempts bounds how many times Externalize writes a blob again after the
// Index refuses to link it. The refusal means a collector overtook the Put, which
// is rare, and a collector cannot overtake a Put that has just revived the
// object, so a few attempts are plenty. Past the bound the payload stays inline
// instead of looping while the Index is in an unexpected state.
const maxLinkAttempts = 3

// Results Policy reports to its PolicyObserver. They form a closed set, so a
// metrics adapter can use them as label values without risking unbounded
// cardinality; PutResults and GetResults return the full lists.
const (
	// PutResultOK: the payload is stored and linked, and Externalize returned a Ref.
	PutResultOK = "ok"
	// PutResultTooLarge: the Store rejected the payload with ErrTooLarge.
	PutResultTooLarge = "too_large"
	// PutResultError: any other failure to externalize, including a payload that
	// stays inline because every attempt to link it was fenced.
	PutResultError = "error"

	// GetResultOK: Resolve returned the bytes.
	GetResultOK = "ok"
	// GetResultMissing: the blob does not exist (ErrNotFound).
	GetResultMissing = "missing"
	// GetResultCorrupt: the stored bytes do not match the Ref (ErrCorrupt).
	GetResultCorrupt = "corrupt"
	// GetResultNamespace: the Ref belongs to another namespace (ErrNamespaceMismatch).
	GetResultNamespace = "namespace"
	// GetResultError: any other failure, transient or not.
	GetResultError = "error"
)

// PutResults returns every value Policy passes to PolicyObserver.OnPut. The
// slice is a fresh copy on each call.
func PutResults() []string {
	return []string{PutResultOK, PutResultTooLarge, PutResultError}
}

// GetResults returns every value Policy passes to PolicyObserver.OnGet. The
// slice is a fresh copy on each call.
func GetResults() []string {
	return []string{GetResultOK, GetResultMissing, GetResultCorrupt, GetResultNamespace, GetResultError}
}

// PolicyObserver receives one event per Externalize call that had to store a
// payload and one per Resolve call. It is how a metrics adapter counts blob
// traffic without this package depending on one. The strings are drawn from
// PutResults and GetResults.
//
// Methods are called synchronously on the caller's goroutine and may be called
// from many goroutines at once, so an implementation must be safe for concurrent
// use and must not block.
type PolicyObserver interface {
	// OnPut reports the outcome of an Externalize call that tried to store a payload.
	// A payload kept inline because it is small never reaches it.
	OnPut(result string)
	// OnGet reports the outcome of a Resolve call.
	OnGet(result string)
}

// PolicyOptions configures a Policy.
type PolicyOptions struct {
	// InlineMax is the largest payload, in bytes of the data passed to Externalize,
	// that stays inline. A payload of exactly InlineMax bytes is inline; one byte
	// more is externalized. The size is that of the uncompressed bytes, so
	// compression choices in the Store never move a payload across the threshold.
	// Zero externalizes every non-empty payload. It must not be negative.
	InlineMax int
	// Observer, when set, is told the result of each Externalize and Resolve call
	// that touched the Store. Nil costs nothing.
	Observer PolicyObserver
}

// Policy decides which payloads leave the state store and does the bookkeeping
// that makes leaving safe: it stores the bytes, links them to their execution so
// garbage collection keeps them, and only then hands back the Ref that goes into
// state. Resolve is the way back.
//
// A Policy is immutable after NewPolicy and holds no state of its own, so one
// value may be shared by any number of goroutines, provided its Store, Index, and
// Observer are themselves safe for concurrent use (Store and Index must be).
//
// Errors returned for a failure of the Store or the Index never repeat the
// underlying error's text, which for a file-backed Store contains the path of the
// file. Use errors.Is: ErrNotFound, ErrCorrupt, ErrNamespaceMismatch, ErrInvalidRef,
// ErrTooLarge, and ErrMetadataConflict each identify a permanent condition, and
// ErrTransient identifies everything else, which may succeed if retried.
type Policy struct {
	store     Store
	index     Index
	inlineMax int
	observer  PolicyObserver
}

// NewPolicy returns a Policy over store and index. store must be the Store whose
// Put registers with index (see NewObjectBacked and ObjectBackedOptions.Index), or
// Link will refuse its blobs.
func NewPolicy(store Store, index Index, opts PolicyOptions) (*Policy, error) {
	if store == nil {
		return nil, errors.New("blob: policy needs a store")
	}
	if index == nil {
		return nil, errors.New("blob: policy needs an index")
	}
	if opts.InlineMax < 0 {
		return nil, fmt.Errorf("blob: policy inline max %d is negative", opts.InlineMax)
	}
	return &Policy{store: store, index: index, inlineMax: opts.InlineMax, observer: opts.Observer}, nil
}

// Externalize stores data as a blob when it is larger than InlineMax and returns
// its Ref with externalized true. For data of InlineMax bytes or fewer it returns
// externalized false and does not touch the Store or the Index: the caller keeps
// the bytes inline, exactly as it would without a Policy.
//
// The order is fixed: Put (which registers the object with the Index), then
// Link(execID, ref, expireAt), and only then does the Ref reach the caller, so a
// Ref the caller writes into state always names an object a collector will leave
// alone until expireAt. expireAt must exceed the execution's retention plus the
// collector's grace period (see Index.Link). A failed Put or Link is returned as
// an error and the caller must fail its write: falling back to inline after a
// failed Put would hide a broken Store.
//
// There is one exception. When Link answers ErrFenced or ErrNotFound a collector
// overtook the Put, so Externalize writes the blob again and links again, up to
// three attempts in all. If the third also fails it returns externalized false
// and a nil error, and the caller keeps the data inline. That fallback is safe
// because nothing has been written to state yet. The orphaned bytes are the
// collector's to reclaim.
//
// The returned error satisfies errors.Is for the classes described on Policy.
func (p *Policy) Externalize(ctx context.Context, ns namespace.Namespace, execID string, data []byte, expireAt time.Time) (Ref, bool, error) {
	if len(data) <= p.inlineMax {
		return Ref{}, false, nil
	}
	hint := sizeHint(len(data))
	if err := ValidateExecID(execID); err != nil {
		p.observePut(PutResultError)
		return Ref{}, false, &policyError{op: "put", hint: hint, reason: "invalid execution id", class: ErrInvalidRef}
	}
	for attempt := 0; attempt < maxLinkAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			p.observePut(PutResultError)
			return Ref{}, false, policyFailure("put", hint, err)
		}
		ref, err := p.store.Put(ctx, ns, data)
		if err != nil {
			p.observePut(putResult(err))
			return Ref{}, false, policyFailure("put", hint, err)
		}
		err = p.index.Link(ctx, execID, ref, expireAt)
		if err == nil {
			p.observePut(PutResultOK)
			return ref, true, nil
		}
		if errors.Is(err, ErrFenced) || errors.Is(err, ErrNotFound) {
			continue
		}
		p.observePut(PutResultError)
		return Ref{}, false, policyFailure("link", hint, err)
	}
	p.observePut(PutResultError)
	return Ref{}, false, nil
}

// Resolve returns the bytes ref points at. ref comes from state and is not
// trusted: the Store checks it against ns and against the stored bytes. The
// result belongs to the caller.
//
// The error satisfies errors.Is(err, ErrNotFound), ErrCorrupt, or
// ErrNamespaceMismatch for those three conditions, which are permanent and which
// the caller should not retry; ErrInvalidRef and ErrTooLarge likewise. Any other
// failure satisfies errors.Is(err, ErrTransient) and is worth retrying. The error
// text never contains a file path.
func (p *Policy) Resolve(ctx context.Context, ns namespace.Namespace, ref Ref) ([]byte, error) {
	data, err := p.store.Get(ctx, ns, ref)
	if err != nil {
		p.observeGet(getResult(err))
		return nil, policyFailure("get", refHint(ref), err)
	}
	p.observeGet(GetResultOK)
	return data, nil
}

// putResult maps a failed Store.Put to the closed set PutResults.
func putResult(err error) string {
	if errors.Is(err, ErrTooLarge) {
		return PutResultTooLarge
	}
	return PutResultError
}

// getResult maps a failed Store.Get to the closed set GetResults.
func getResult(err error) string {
	switch {
	case errors.Is(err, ErrNamespaceMismatch):
		return GetResultNamespace
	case errors.Is(err, ErrNotFound):
		return GetResultMissing
	case errors.Is(err, ErrCorrupt):
		return GetResultCorrupt
	}
	return GetResultError
}

func (p *Policy) observePut(result string) {
	if p.observer != nil {
		p.observer.OnPut(result)
	}
}

func (p *Policy) observeGet(result string) {
	if p.observer != nil {
		p.observer.OnGet(result)
	}
}
