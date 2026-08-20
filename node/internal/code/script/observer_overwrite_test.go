package script

import (
	"testing"
)

// TestSetObserverSecondNonNilRegistrationPanics is a probe that demonstrates
// the silent-overwrite defect: a second non-nil SetObserver call overwrites the
// first without any complaint, making the first observer invisible and any
// metric it was supposed to collect silently lost.
//
// After the fix, the second non-nil call must panic. SetObserver(nil) (the
// test-teardown reset path) must still be allowed at any time.
func TestSetObserverSecondNonNilRegistrationPanics(t *testing.T) {
	// Restore noop on exit regardless of outcome.
	defer SetObserver(nil)

	type minObserver struct{ noopObserver }
	first := minObserver{}
	second := minObserver{}

	SetObserver(first)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("second non-nil SetObserver must panic, but did not")
		}
	}()
	// This must panic after the fix; before the fix it silently overwrites.
	SetObserver(second)
}

// TestSetObserverNilResetAlwaysAllowed verifies that SetObserver(nil) is
// always a legal reset, even when called multiple times. Tests rely on this
// as their teardown path.
func TestSetObserverNilResetAlwaysAllowed(t *testing.T) {
	defer SetObserver(nil)

	type minObserver struct{ noopObserver }
	SetObserver(minObserver{})
	// Reset to noop — must not panic.
	SetObserver(nil)
	// A subsequent non-nil registration from noop is also fine.
	SetObserver(minObserver{})
}
