package runner

import "sync"

// activeLeases is the set of leases this runner's workers currently hold. It
// travels on every poll so the control plane can tell a lease that never
// reached its runner (lost poll response, restarted process — both must be
// replayed) apart from one a worker is mid-handler on (must not be). The
// server cannot make that distinction: from its side both are simply "leased
// to this runner, this session".
//
// A lease joins the set the moment pollLoop accepts it — before it reaches a
// worker — because the poll that would duplicate it can happen in that gap.
// It leaves when execution finishes, whether the report landed or not: once
// the handler has returned, a replay is a legitimate redelivery of unreported
// work rather than a second concurrent execution.
type activeLeases struct {
	mu  sync.Mutex
	ids map[string]int
}

func newActiveLeases() *activeLeases {
	return &activeLeases{ids: make(map[string]int)}
}

// add marks a lease as executing. Counted rather than a plain set: a replayed
// lease legitimately arrives twice in a row (the server redelivers unreported
// work), and a naive delete on the first completion would unmark a lease the
// second worker still holds.
func (a *activeLeases) add(id string) {
	if id == "" {
		return
	}
	a.mu.Lock()
	a.ids[id]++
	a.mu.Unlock()
}

func (a *activeLeases) remove(id string) {
	if id == "" {
		return
	}
	a.mu.Lock()
	if n := a.ids[id]; n > 1 {
		a.ids[id] = n - 1
	} else {
		delete(a.ids, id)
	}
	a.mu.Unlock()
}

// snapshot returns the currently held lease IDs, or nil when none are held so
// the field is omitted from the poll body entirely.
func (a *activeLeases) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(a.ids))
	for id := range a.ids {
		out = append(out, id)
	}
	return out
}
