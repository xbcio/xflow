package runner

import (
	"testing"
)

// TestSupplyGateSetObserverSecondNonNilRegistrationPanics probes the
// silent-overwrite defect on SupplyGate.SetObserver. After the fix, a second
// non-nil call must panic.
func TestSupplyGateSetObserverSecondNonNilRegistrationPanics(t *testing.T) {
	g := NewSupplyGate(nil, nil, nil)

	type minObserver struct{ recordingGateObserver }
	first := &minObserver{}
	second := &minObserver{}

	g.SetObserver(first)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("second non-nil SetObserver must panic, but did not")
		}
	}()
	g.SetObserver(second)
}

// TestSupplyGateSetObserverNilResetAlwaysAllowed verifies SetObserver(nil)
// resets cleanly, and a subsequent non-nil registration succeeds.
func TestSupplyGateSetObserverNilResetAlwaysAllowed(t *testing.T) {
	g := NewSupplyGate(nil, nil, nil)

	type minObserver struct{ recordingGateObserver }
	g.SetObserver(&minObserver{})
	g.SetObserver(nil)
	g.SetObserver(&minObserver{})
}
