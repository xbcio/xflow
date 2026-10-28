package blob

import (
	"context"
	"errors"
	"fmt"
	"syscall"
)

// ErrTransient marks a failure of the Store or the Index that is not known to be
// permanent: an I/O error, a network or database error, a cancelled or expired
// context. Retrying may succeed. Every error Policy returns for a failure it
// cannot classify satisfies errors.Is(err, ErrTransient); the permanent
// conditions below (ErrNotFound, ErrCorrupt, ErrNamespaceMismatch, ErrInvalidRef,
// ErrTooLarge, ErrMetadataConflict) never do.
var ErrTransient = errors.New("blob: transient store failure")

// permanentClasses lists the sentinel errors Policy keeps as a class of their
// own, with the fixed words its error text uses for each. A failure that matches
// none of them is transient.
var permanentClasses = []struct {
	err    error
	reason string
}{
	{ErrNamespaceMismatch, "ref belongs to another namespace"},
	{ErrNotFound, "object not found"},
	{ErrCorrupt, "content does not match ref"},
	{ErrInvalidRef, "invalid ref"},
	{ErrTooLarge, "data too large"},
	{ErrMetadataConflict, "recorded metadata conflicts with ref"},
}

// policyError is the error Policy returns for a failure of its Store or Index.
// Its text is built from fixed words and a short hint, never from the wrapped
// error: errors from the file system and from database drivers carry full paths
// and host names, and Policy errors end up in logs and in queue archives.
//
// It has no Unwrap method on purpose, so errors.As cannot reach the original
// error and the path inside it. errors.Is still works: it matches the class, or,
// for a transient failure, ErrTransient and anything in the original chain, so a
// cancelled context stays recognisable.
type policyError struct {
	op     string // "put", "link", or "get"
	hint   string // what the operation was about; never contains a path
	reason string // fixed words describing the failure
	class  error  // one of permanentClasses, or nil for a transient failure
	cause  error  // the original error, kept only when class is nil
}

// Error implements error. The text names the operation, what it was about, and a
// fixed reason. It never contains text from the wrapped error.
func (e *policyError) Error() string {
	return fmt.Sprintf("blob: %s %s: %s", e.op, e.hint, e.reason)
}

// GoString implements fmt.GoStringer. Without it, formatting an error with %#v
// would print the unexported cause field, path and all.
func (e *policyError) GoString() string { return e.Error() }

// Is implements the errors.Is protocol; see policyError for what it matches.
func (e *policyError) Is(target error) bool {
	if e.class != nil {
		return target == e.class
	}
	return target == ErrTransient || errors.Is(e.cause, target)
}

// policyFailure turns an error from the Store or the Index into the error Policy
// returns. A permanent condition keeps its sentinel as the class; anything else
// is transient. op is "put", "link", or "get"; hint says what the call was about
// and must be built with sizeHint or refHint so that it holds no path.
func policyFailure(op, hint string, err error) error {
	for _, c := range permanentClasses {
		if errors.Is(err, c.err) {
			return &policyError{op: op, hint: hint, reason: c.reason, class: c.err}
		}
	}
	return &policyError{op: op, hint: hint, reason: transientReason(err), cause: err}
}

// transientReason describes a transient failure with words that cannot carry a
// path: the context error, or the errno of an operating-system error (for
// example "permission denied"), or a fixed phrase.
func transientReason(err error) string {
	const base = "temporary store failure"
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return base + " (" + errno.Error() + ")"
	}
	return base
}

// sizeHint describes data about to be stored.
func sizeHint(n int) string {
	return fmt.Sprintf("of %d bytes", n)
}

// refHint describes the blob ref points at. Only a well-formed digest is quoted,
// and only its first 12 characters: a Ref read back from state is untrusted
// input, and a hostile Digest could hold a path or a log-forging line break.
func refHint(ref Ref) string {
	if !validDigest(ref.Digest) {
		return "for a malformed ref"
	}
	return fmt.Sprintf("for sha256:%s (%d bytes)", ref.Digest[:12], ref.Size)
}
