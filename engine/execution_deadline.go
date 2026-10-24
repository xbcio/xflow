package engine

import "time"

// stampExecutionDeadline sets snap.Deadline to CreatedAt + Graph.Timeout() when
// the graph declares settings.timeout, and leaves the snapshot untouched
// otherwise (zero Deadline means no execution-level budget).
//
// The backends own CreatedAt, but they stamp it inside the create call, after
// the engine has already built the snapshot, so the engine cannot read the
// instant it is about to be given. It therefore stamps CreatedAt itself, from
// the same clock reading the deadline is derived from, and a backend that
// preserves a non-zero CreatedAt (the local one does) then reports exactly
// Deadline - Timeout as the start. Only a graph with a timeout pays this: any
// other snapshot reaches the backend unchanged.
func stampExecutionDeadline(snap *ExecutionSnapshot) {
	if snap.Graph == nil || snap.Graph.Timeout() <= 0 {
		return
	}
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	snap.Deadline = snap.CreatedAt.Add(snap.Graph.Timeout())
}
