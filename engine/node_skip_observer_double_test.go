package engine

import (
	"context"
	"sync"
)

// recordedSkip is one OnNodeSkip call, kept verbatim so a test can assert the
// flow/node labels and the count rather than only a total.
type recordedSkip struct {
	flow  string
	node  string
	count int
}

// recordingSkipObserver is the NodeSkipObserver test double: it records every
// call verbatim, so tests can assert the literal label values the engine
// classified each skip into and the count it forwarded.
//
// It is deliberately its own double rather than a method added to
// recordingGroupObserver. A skip is not a group event, and a shared double
// would let a change that wrongly routed skips through the group observer stay
// green.
type recordingSkipObserver struct {
	mu     sync.Mutex
	skips  []recordedSkip
	frozen bool
}

func (r *recordingSkipObserver) OnNodeSkip(_ context.Context, flow string, node string, count int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		// A panic-recovering hook runs this after the test body on some paths;
		// the flag keeps a late call from racing the assertions.
		return
	}
	r.skips = append(r.skips, recordedSkip{flow: flow, node: node, count: count})
}

// take returns the recorded calls and freezes the double, so a later delivery
// cannot append to the slice a test is about to assert on. It is for the final
// assertion of a test.
//
// A test that needs to inspect an intermediate state uses snapshot instead:
// freezing after the first read would silently stop recording, and the failure
// that produced would be a real skip reported as missing.
func (r *recordingSkipObserver) take() []recordedSkip {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
	return r.copyLocked()
}

// snapshot returns the recorded calls without freezing, for progress checks
// part-way through a test.
func (r *recordingSkipObserver) snapshot() []recordedSkip {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.copyLocked()
}

func (r *recordingSkipObserver) copyLocked() []recordedSkip {
	out := make([]recordedSkip, len(r.skips))
	copy(out, r.skips)
	return out
}

var _ NodeSkipObserver = (*recordingSkipObserver)(nil)
