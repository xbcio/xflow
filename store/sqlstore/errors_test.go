package sqlstore

import (
	"errors"
	"fmt"
	"testing"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// TestWrapDBErr_RecordNotFoundStillMapsToErrNotFound is a non-regression
// check: gorm.ErrRecordNotFound must still map to store.ErrNotFound, and must
// not incidentally be classified as transient.
func TestWrapDBErr_RecordNotFoundStillMapsToErrNotFound(t *testing.T) {
	got := wrapDBErr("op", gorm.ErrRecordNotFound)
	if !errors.Is(got, store.ErrNotFound) {
		t.Fatalf("wrapDBErr(gorm.ErrRecordNotFound) = %v, want errors.Is(_, store.ErrNotFound) to be true", got)
	}
	if errors.Is(got, store.ErrTransient) {
		t.Fatalf("wrapDBErr(gorm.ErrRecordNotFound) = %v, want errors.Is(_, store.ErrTransient) to be false", got)
	}
}

// TestWrapDBErr_NilReturnsNil is a non-regression check on the existing nil
// fast path.
func TestWrapDBErr_NilReturnsNil(t *testing.T) {
	if got := wrapDBErr("op", nil); got != nil {
		t.Fatalf("wrapDBErr(nil) = %v, want nil", got)
	}
}

// TestWrapDBErr_UnmatchedErrorIsNotTransient pins the baseline this package
// must always have: a plain, unclassified error that no registered
// classifier recognizes must never be marked retryable just because it isn't
// gorm.ErrRecordNotFound. This deliberately uses a value no OTHER test in
// this file registers a classifier for (see registerTestClassifier's doc
// comment on why registrations from earlier tests are never reverted), so
// the assertion holds regardless of test execution order -- including the
// simplest case this guards, a caller before any dialect entry point (e.g.
// mysqlstore.New) has registered anything at all.
func TestWrapDBErr_UnmatchedErrorIsNotTransient(t *testing.T) {
	plain := errors.New("boom")
	got := wrapDBErr("op", plain)
	if errors.Is(got, store.ErrTransient) {
		t.Fatalf("wrapDBErr(%v) = %v, want errors.Is(_, store.ErrTransient) to be false for an error no registered classifier matches", plain, got)
	}
	if !errors.Is(got, plain) {
		t.Fatalf("wrapDBErr(%v) = %v, want errors.Is(_, plain) to still be true (the original error must stay reachable via %%w)", plain, got)
	}
}

// TestWrapDBErr_RegisteredClassifierMarksErrorTransient and
// TestWrapDBErr_RegisteredClassifierLeavesOtherErrorsAlone together pin
// wrapDBErr's generic dispatch contract -- the property every dialect
// package (mysqlstore included) relies on -- using a fake classifier instead
// of any concrete driver's error type. This keeps this package's own tests
// free of any SQL driver import: the mysql-specific classification itself is
// tested where it is now defined, store/sqlstore/mysqlstore's own test file.
//
// registerTestClassifier below appends to the package-level
// transientClassifiers slice for the duration of one test and is not
// reverted afterward (there is no unregister -- see its own doc comment);
// each test below installs its own classifier keyed to a value unique to
// that test's fake error so an earlier test's leftover registration cannot
// produce a false positive for a later one. The dialect name passed here is
// an arbitrary fake ("test-dialect-n") distinct from "mysql" -- these tests
// must not accidentally satisfy HasTransientClassifierFor("mysql"), which
// mysqlstore's own test file (store/sqlstore/mysqlstore) owns exclusively.
func registerTestClassifier(t *testing.T, dialect string, match func(error) bool) {
	t.Helper()
	RegisterTransientClassifier(dialect, match)
}

func TestWrapDBErr_RegisteredClassifierMarksErrorTransient(t *testing.T) {
	sentinel := errors.New("fake-driver-deadlock-marker")
	registerTestClassifier(t, "test-dialect-1", func(err error) bool { return errors.Is(err, sentinel) })

	wrapped := fmt.Errorf("driver: %w", sentinel)
	got := wrapDBErr("op", wrapped)
	if !errors.Is(got, store.ErrTransient) {
		t.Fatalf("wrapDBErr(%v) = %v, want errors.Is(_, store.ErrTransient) to be true once a registered classifier matches", wrapped, got)
	}
	if !errors.Is(got, sentinel) {
		t.Fatalf("wrapDBErr(%v) = %v, want the original error still reachable via errors.Is", wrapped, got)
	}
}

func TestWrapDBErr_RegisteredClassifierLeavesOtherErrorsAlone(t *testing.T) {
	sentinel := errors.New("fake-driver-deadlock-marker-2")
	registerTestClassifier(t, "test-dialect-2", func(err error) bool { return errors.Is(err, sentinel) })

	unrelated := errors.New("some other db error, not the deadlock marker")
	got := wrapDBErr("op", unrelated)
	if errors.Is(got, store.ErrTransient) {
		t.Fatalf("wrapDBErr(%v) = %v, want errors.Is(_, store.ErrTransient) to be false for an error the registered classifier does not match", unrelated, got)
	}
}

// TestHasTransientClassifierFor_UnregisteredDialectReportsFalse pins
// HasTransientClassifierFor's own contract in isolation from any real dialect
// package: a name nothing has ever registered under must report false. Uses
// a dialect name no other test in this package ever registers, for the same
// cross-test-order-independence reason as registerTestClassifier above.
func TestHasTransientClassifierFor_UnregisteredDialectReportsFalse(t *testing.T) {
	if HasTransientClassifierFor("no-such-dialect-ever-registered") {
		t.Fatalf("HasTransientClassifierFor(%q) = true, want false for a dialect name nothing registered", "no-such-dialect-ever-registered")
	}
}

// TestHasTransientClassifierFor_RegisteredDialectReportsTrue pins the other
// half: once RegisterTransientClassifier is called under a given dialect
// name, HasTransientClassifierFor for that same name must report true.
func TestHasTransientClassifierFor_RegisteredDialectReportsTrue(t *testing.T) {
	registerTestClassifier(t, "test-dialect-3", func(error) bool { return false })
	if !HasTransientClassifierFor("test-dialect-3") {
		t.Fatalf("HasTransientClassifierFor(%q) = false, want true right after RegisterTransientClassifier(%q, ...)", "test-dialect-3", "test-dialect-3")
	}
}
