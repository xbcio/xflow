package types

import "errors"

// ErrPermanent is the sentinel marker for an error that must NOT be retried.
// Use errors.Is(err, ErrPermanent) to check, errors.Join(ErrPermanent, err) to
// stamp an arbitrary error as permanent.
//
// Handlers that want fine-grained control can also implement the
// PermanentError interface to decide per-error whether retries are warranted.
//
// Spec: .claude/docs/specs/retry-policy.md
var ErrPermanent = errors.New("xflow: permanent error")

// PermanentError lets handlers tag a specific error as non-retryable without
// using errors.Join. The engine checks Permanent() before honoring the
// surrounding workflow's RetrySettings.
type PermanentError interface {
	error
	Permanent() bool
}

// IsPermanent reports whether err should bypass retries. Either the error
// chain contains ErrPermanent, or it implements PermanentError with
// Permanent()==true.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return true
	}
	var p PermanentError
	if errors.As(err, &p) {
		return p.Permanent()
	}
	return false
}
