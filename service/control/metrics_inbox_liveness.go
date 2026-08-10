package control

import (
	"context"
	"time"
)

// directoryLiveness answers IsRunnerLive from the same directory snapshot and
// the same selector the dispatcher uses. Reusing that verdict — rather than
// inventing a metrics-specific staleness rule — is what keeps metric
// visibility and scheduling availability from diverging: a runner's series
// stops exactly when the scheduler stops sending it work.
type directoryLiveness struct {
	runners  RunnerDirectory
	selector RunnerSelector
}

// NewDirectoryLiveness adapts a RunnerDirectory + RunnerSelector pair to
// RunnerLiveness. now is passed straight through to RunnerSelector.IsLive
// rather than read from the wall clock here, so the caller (MetricsInbox) owns
// the clock and expiry stays testable without sleeping.
func NewDirectoryLiveness(runners RunnerDirectory, selector RunnerSelector) RunnerLiveness {
	return directoryLiveness{runners: runners, selector: selector}
}

func (d directoryLiveness) IsRunnerLive(ctx context.Context, runnerID string, now time.Time) bool {
	if d.runners == nil || runnerID == "" {
		return false
	}
	snap, ok := d.runners.Runner(ctx, runnerID)
	if !ok {
		// Unknown means not live. Failing closed here only costs visibility for
		// one scrape; failing open would keep emitting series for runners the
		// directory has already forgotten.
		return false
	}
	return d.selector.IsLive(snap, now)
}
