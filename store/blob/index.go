package blob

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Object states recorded by an Index.
const (
	// ObjectLive marks an object writers may reference.
	ObjectLive = "live"
	// ObjectDeleting marks an object a garbage collector has claimed. A writer
	// that meets it revives the object (see Index.Register).
	ObjectDeleting = "deleting"
)

// MaxExecIDLen is the longest execution ID Index.Link accepts. SQL backends keep
// it in a VARCHAR(64) column.
const MaxExecIDLen = 64

var (
	// ErrFenced reports that a garbage-collection claim is no longer valid: a
	// writer revived the object after the claim was taken, or the object is
	// gone. Index.Purge returns it without running the delete callback, and
	// Index.Link returns it for an object a collector has claimed. A caller of
	// Link treats it as "write the bytes again" and Puts the blob a second time.
	ErrFenced = errors.New("blob: garbage-collection fence lost")

	// ErrNotCandidate reports that an object is not (or no longer) eligible for
	// garbage collection: an unexpired link still names it, or it was
	// referenced within the grace period.
	ErrNotCandidate = errors.New("blob: object is not a garbage-collection candidate")

	// ErrMetadataConflict reports that Index.Register found the digest already
	// recorded with a different Size. The digest is a SHA-256 of the content, so
	// two sizes for one digest mean a bug or damaged metadata; Register fails
	// instead of choosing one.
	ErrMetadataConflict = errors.New("blob: recorded metadata conflicts with ref")
)

// Index is the lifecycle metadata of the blobs a Store holds: which objects
// exist, which executions still link to them, and the claim a garbage collector
// takes before it deletes bytes. It stores no bytes and takes no part in
// scheduling.
//
// Why it exists. A content-addressed blob has no owner and no reference count:
// there is no reliable "execution finished" event to decrement a counter on, so
// a counter would drift. Instead every link carries an expiry, an object is
// garbage when no unexpired link names it and nothing has touched it for a grace
// period, and the Index arbitrates between writers and the collector.
//
// The fence. Deleting bytes is the one step that cannot be undone, and a
// writer that dedups against bytes about to be deleted would be handed a Ref to
// nothing. The protocol that prevents it:
//
//   - A writer calls Register BEFORE it writes or dedups against any bytes. An
//     object the collector had claimed (ObjectDeleting) is revived to ObjectLive
//     and its generation is incremented, which invalidates every claim taken
//     earlier.
//   - The collector calls MarkDeleting, which re-checks eligibility and returns
//     the claim (a Fence), then Purge. Purge re-checks the claim and runs the
//     byte delete as one step under the object's lock, so a writer's Register
//     either lands before Purge (the claim fails and nothing is deleted) or
//     waits until Purge is done (and then writes its bytes into a fresh object).
//     Checking the claim and deleting in two separate steps would leave a window
//     in which a writer revives the object after the check and the delete still
//     lands.
//
// Implementations must be safe for concurrent use and must take every time they
// compare or store from one clock, truncated to milliseconds in UTC.
type Index interface {
	// Register records that a writer is about to store ref's bytes, and must
	// run before the writer touches the object store.
	//
	// A missing object is created live. A live object only has its last-reference
	// time refreshed, and the returned Registration.Ref carries the Size, Stored,
	// and Codec ALREADY RECORDED, not the writer's: two writers can encode the
	// same digest differently, and the index is the single source of truth for
	// statistics. An object in ObjectDeleting is revived: it becomes live, its
	// generation is incremented, the writer's Stored and Codec replace the
	// recorded ones, and Registration.Revived is set so the writer rewrites the
	// bytes instead of trusting whatever is stored.
	//
	// A recorded Size that differs from ref.Size for the same digest is an
	// error: the digest is a SHA-256 of the content, so it signals a bug or
	// damaged metadata and must not be papered over.
	Register(ctx context.Context, ref Ref) (Registration, error)

	// Link records that execution execID still needs ref until expireAt, and
	// refreshes the object's last-reference time. A link only ever moves later:
	// linking again with an earlier expireAt keeps the later one. expireAt must
	// be after the Index's current time and must exceed the execution's
	// retention plus the collector's grace period, otherwise the link protects
	// nothing.
	//
	// An object that was never registered returns ErrNotFound. One a collector
	// has claimed returns ErrFenced: its bytes may be deleted at any moment, so
	// the caller must Put the blob again and link the Ref that returns.
	Link(ctx context.Context, execID string, ref Ref, expireAt time.Time) error

	// PruneLinks deletes up to limit links whose expiry has passed and returns
	// how many it removed. An expired link no longer protects anything, so
	// pruning never changes which objects are candidates; it only bounds the
	// table. limit must be positive.
	PruneLinks(ctx context.Context, limit int) (int, error)

	// Candidates lists up to limit objects that look like garbage: no link with
	// expireAt after the current time names them, and their last reference is
	// older than grace. Objects already claimed (ObjectDeleting) are included,
	// so a collector that crashed after claiming does not strand them. The
	// result is ordered by last reference, oldest first, then by namespace and
	// digest. It is a hint: MarkDeleting re-checks. grace and limit must be
	// positive.
	Candidates(ctx context.Context, grace time.Duration, limit int) ([]Ref, error)

	// MarkDeleting claims the object ref names for deletion after re-checking,
	// under the object's lock, that it is still a candidate for grace. Only
	// ref.Namespace and ref.Digest identify the object; the rest of ref must be
	// well formed but is otherwise ignored. It returns ErrNotFound for an
	// unknown object and ErrNotCandidate for one that is not eligible any more.
	// Claiming an object that is already claimed succeeds and returns the same
	// Fence. The returned Fence carries the recorded Ref, which the collector
	// passes to Store.Delete.
	MarkDeleting(ctx context.Context, ref Ref, grace time.Duration) (Fence, error)

	// Purge deletes the object fence names. Under the object's lock it checks
	// that the object is still claimed with the same generation, returning
	// ErrFenced (and not calling del) otherwise; then it calls del to remove the
	// bytes, and only if del succeeds removes the object and its links.
	//
	// If del fails the object stays claimed and Purge returns del's error, so a
	// later run retries. del must be idempotent and must not call back into the
	// Index: the lock is held while it runs, so the delete is atomic with
	// respect to Register. An implementation may run it again when the database
	// aborts the transaction as a deadlock victim, which is one more reason it must
	// be idempotent.
	Purge(ctx context.Context, fence Fence, del func(context.Context) error) error
}

// Registration is the outcome of Index.Register.
type Registration struct {
	// Ref is the metadata the index now records for the object: the first
	// writer's Size, Stored, and Codec for an existing live object, the caller's
	// for a new or revived one. Writers return this Ref, not their own.
	Ref Ref
	// Revived reports that the object had been claimed by a collector and this
	// call took it back. The writer must rewrite the bytes.
	Revived bool
}

// Fence is a garbage collector's claim on an object. It is valid until a writer
// revives the object, which changes its generation. Callers treat it as opaque
// and pass it back to Index.Purge unchanged.
type Fence struct {
	// Ref is the metadata the index records for the claimed object.
	Ref Ref
	// Generation is the object's generation when the claim was taken.
	Generation int64
}

// ValidateExecID reports whether id can be stored as the execution ID of a link.
func ValidateExecID(id string) error {
	if id == "" {
		return errors.New("execution id is empty")
	}
	if len(id) > MaxExecIDLen {
		return fmt.Errorf("execution id is %d bytes, longer than %d", len(id), MaxExecIDLen)
	}
	if strings.ContainsFunc(id, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return errors.New("execution id contains whitespace or a control character")
	}
	return nil
}

// Validate reports whether f can be passed to Index.Purge: a well-formed Ref and
// a positive generation. A zero Fence is a caller bug rather than a lost race, so
// it is an invalid ref and not ErrFenced. Every error wraps ErrInvalidRef.
func (f Fence) Validate() error {
	if err := f.Ref.Validate(); err != nil {
		return err
	}
	if f.Generation < 1 {
		return fmt.Errorf("%w: fence generation %d is not positive", ErrInvalidRef, f.Generation)
	}
	return nil
}

// IndexTime is the one representation every Index implementation stores and
// compares: UTC, truncated to milliseconds. SQL columns are DATETIME(3), which
// rounds on insert, so truncating before binding keeps a value and its stored
// form identical and keeps the in-memory and SQL indexes in agreement. The
// SQL index lives in another package, hence the exported name.
func IndexTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

// ValidateGCArgs rejects a non-positive grace period or batch limit. A zero
// grace would make every object that has no link garbage the moment it is
// written, and a zero limit would make a collector pass do nothing without
// saying so. Implementations of Candidates, MarkDeleting, and PruneLinks call
// it so they all fail the same way. MarkDeleting has no batch, so it passes 1.
func ValidateGCArgs(grace time.Duration, limit int) error {
	if grace <= 0 {
		return fmt.Errorf("blob: grace %s is not positive", grace)
	}
	if limit <= 0 {
		return fmt.Errorf("blob: limit %d is not positive", limit)
	}
	return nil
}
