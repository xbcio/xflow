package control

// ReapResult is what one optional-reaper pass inspected and what it released.
//
// It is a struct rather than a third return value because the two counts belong
// together — a release with no denominator cannot say whether a pass is draining
// its backlog or walking a shape wider than the one it drains — and because a
// later count can be added to it without changing the method signature again.
//
// Both fields carry whatever the pass had accumulated when it returned, so a
// pass that failed part-way still reports what it inspected and released before
// the failure (the same contract the released count alone already had). Inspected
// is never smaller than Released: every release is decided on a candidate that
// was inspected first.
type ReapResult struct {
	// Inspected is the number of candidate records the pass examined. What
	// counts as a candidate is defined per pass, on the capability that
	// implements it, because the shapes differ: for one pass it is every field
	// of the structure it scans, for another only the entries of it that carry
	// the state the pass drains.
	Inspected int
	// Released is the number of records the pass released, reaped or reclaimed.
	Released int
}
