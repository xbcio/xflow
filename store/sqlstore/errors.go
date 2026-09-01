package sqlstore

import (
	"errors"
	"fmt"
	"sync"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// transientClassifier pairs a dialect entry point's "is this driver error
// safe to retry" predicate with the dialect name it was registered under.
// The name exists only so HasTransientClassifierFor can answer "is dialect X
// wired at all" for a deterministic wiring probe (see its own doc comment);
// wrapDBErr itself does not and cannot filter by dialect -- it is a free
// function with no DB handle in scope, so it has no way to know which
// dialect produced the error it is classifying. It must consult every
// registered classifier regardless of name.
type transientClassifier struct {
	dialect  string
	classify func(error) bool
}

// transientClassifiers holds every dialect entry point's registered
// classifier, in registration order. wrapDBErr consults this list instead of
// knowing about any concrete SQL driver's error type itself -- that is the
// whole point of the registration point below: this package (sqlstore) is
// documented as dialect-agnostic (store/sqlstore/mysqlstore/mysqlstore.go's
// package comment), and a dialect-specific error number hardcoded here would
// silently contradict that claim for every OTHER dialect's caller.
//
// Guarded by transientClassifiersMu rather than left a bare slice: wrapDBErr
// runs on every request path that touches the store, so reads happen far
// more often than the (typically one-time, at process-init) write from a
// dialect package's init(). A sync.RWMutex is the simplest correct tool for
// that shape -- readers do not block each other, and the rare writer only
// blocks readers for the length of an append, not a whole request.
var (
	transientClassifiersMu sync.RWMutex
	transientClassifiers   []transientClassifier
)

// RegisterTransientClassifier lets a dialect entry point (e.g.
// store/sqlstore/mysqlstore) teach this dialect-agnostic core which
// driver-specific errors are safe to retry, without this package importing
// that driver's error type itself.
//
// This is the same idiom as database/sql.Register: the generic core exposes
// a registration point, and each concrete dialect package calls it -- from
// its own init(), the same place database/sql driver packages register
// themselves -- to announce its own capability.
//
// This used to be documented as "never from an init()", on the reasoning
// that "no MySQL connection has been opened" should mean "no MySQL error
// classification". That reasoning was wrong and has been retracted: this
// registers a pure error-TYPE classifier, not a live capability tied to any
// particular connection. isMySQLDeadlock (mysqlstore's predicate) only ever
// matches a *mysql.MySQLError with a specific error number -- a value that
// can only ever come from a real MySQL driver error in the first place, so
// registering it unconditionally at import time is completely inert for any
// deployment that never opens a MySQL connection, and any deployment that
// constructs its own *gorm.DB and calls sqlstore.New directly (the
// documented, supported embedding path -- see sdk/xflow/cluster.go's own
// example) never goes through this package's New at all, so gating
// registration on New running was gating it behind a call path an entire
// class of legitimate callers does not take. See
// store/sqlstore/mysqlstore/mysqlstore.go's package comment for the concrete
// fix.
//
// dialect identifies which dialect package performed the registration (e.g.
// "mysql"), consulted only by HasTransientClassifierFor -- see that
// function's doc comment for why classification itself does not filter by
// it.
//
// f may be called concurrently by wrapDBErr on any request goroutine once
// registered; it must be safe for concurrent use (in practice, this only
// matters if f closes over mutable state -- a pure type-switch/field-compare
// predicate like mysqlstore's needs no additional care). A nil f is ignored.
func RegisterTransientClassifier(dialect string, f func(error) bool) {
	if f == nil {
		return
	}
	transientClassifiersMu.Lock()
	defer transientClassifiersMu.Unlock()
	transientClassifiers = append(transientClassifiers, transientClassifier{dialect: dialect, classify: f})
}

// HasTransientClassifierFor reports whether a classifier has been registered
// under the given dialect name.
//
// This exists purely as a deterministic wiring probe for callers who embed
// this package indirectly through a dialect entry point's blank import (see
// store/sqlstore/mysqlstore's package comment): unlike a real-deadlock-driven
// test, it needs no live database and no actual collision to answer "is the
// dialect's classifier actually registered in this binary", which is exactly
// what makes it possible to pin a blank import against being silently deleted
// (a goimports "unused import" cleanup would otherwise remove it with no
// build break, since a blank import by definition has no referencing
// identifier for the compiler to miss).
func HasTransientClassifierFor(dialect string) bool {
	transientClassifiersMu.RLock()
	defer transientClassifiersMu.RUnlock()
	for _, c := range transientClassifiers {
		if c.dialect == dialect {
			return true
		}
	}
	return false
}

// isRegisteredTransient reports whether any registered classifier recognizes
// err as safe to retry, regardless of which dialect registered it: wrapDBErr
// has no DB handle in scope and so no way to know which dialect actually
// produced err, so it must ask every classifier rather than filtering by
// name.
func isRegisteredTransient(err error) bool {
	transientClassifiersMu.RLock()
	defer transientClassifiersMu.RUnlock()
	for _, c := range transientClassifiers {
		if c.classify(err) {
			return true
		}
	}
	return false
}

// wrapDBErr normalizes a GORM error for a given operation. gorm.ErrRecordNotFound
// is mapped to store.ErrNotFound so callers see a consistent sentinel regardless
// of whether the missing row surfaced from a First (read) or, defensively, from
// a write path. If any classifier registered via RegisterTransientClassifier
// recognizes err as retryable (e.g. mysqlstore registers a MySQL-deadlock
// check), it is additionally wrapped with store.ErrTransient so callers can
// decide to retry without this package depending on a concrete SQL driver
// type. Any other non-nil error is wrapped with the operation name and %w so
// errors.Is/As keep working. A nil err returns (nil, nil).
func wrapDBErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%s: %w", op, store.ErrNotFound)
	}
	if isRegisteredTransient(err) {
		return fmt.Errorf("%s: %w: %w", op, store.ErrTransient, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}
