package types

import "errors"

// ErrPermanent is the sentinel marker for an error that must NOT be retried.
// Use errors.Is(err, ErrPermanent) to check, errors.Join(ErrPermanent, err) to
// stamp an arbitrary error as permanent. Unmarked errors are treated as
// transient by retry-capable engines and queues.
var ErrPermanent = errors.New("xflow: permanent error")

// IsPermanent reports whether err should bypass retries.
func IsPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}
