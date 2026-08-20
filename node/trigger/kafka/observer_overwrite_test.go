package kafka

import (
	"testing"
)

// TestSetObserverSecondNonNilRegistrationPanics is a probe that demonstrates
// the silent-overwrite defect in the kafka package's global observer: a second
// non-nil SetObserver call overwrites the first without any complaint.
//
// After the fix, the second non-nil call must panic. SetObserver(nil) (the
// test-teardown reset path) must still be allowed at any time.
func TestSetObserverSecondNonNilRegistrationPanics(t *testing.T) {
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
	// Must panic after the fix; before the fix silently overwrites.
	SetObserver(second)
}

// TestSetObserverKafkaNilResetAlwaysAllowed verifies that SetObserver(nil) is
// always a legal reset, and that a non-nil registration after a nil reset
// succeeds — the test-teardown pattern must keep working.
func TestSetObserverKafkaNilResetAlwaysAllowed(t *testing.T) {
	defer SetObserver(nil)

	type minObserver struct{ noopObserver }
	SetObserver(minObserver{})
	SetObserver(nil)
	SetObserver(minObserver{})
}
