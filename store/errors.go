package store

import "errors"

// ErrNotFound is returned when a requested resource does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrTransient marks a store error as safe to retry: the operation failed for
// a reason that is expected to clear on its own (e.g. a MySQL deadlock,
// error 1213), not because the request itself was invalid or because another
// caller legitimately won a race. Callers should check with errors.Is(err,
// store.ErrTransient) rather than inspecting driver-specific error types, so
// they do not need to import a concrete SQL driver package to decide whether
// to retry. Only true deadlocks are classified this way; lock-wait-timeout
// (MySQL 1205) is deliberately excluded because it can indicate a long-running
// transaction rather than a genuine deadlock, and blindly retrying it could
// make contention worse.
var ErrTransient = errors.New("store: transient error, safe to retry")
