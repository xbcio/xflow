package control

import "context"

// QueueLaneLegacy is the lane name the assignment queue depth report uses for
// the shared legacy queue. It is a metric label value, so it is part of the
// observable surface: the queues themselves name the legacy queue with the
// empty string, and that empty name never reaches a gauge label.
const QueueLaneLegacy = "legacy"

// AssignmentQueueDepthReporter is the runner-directory capability that reports
// how many assignments are waiting on each assignment queue: one entry per
// configured lane, keyed by the lane's node type, plus QueueLaneLegacy for the
// shared legacy queue.
//
// It exists because the runner-directory LIST is a blind spot that the queue
// depth metrics inherited from the broker cannot cover: xflow_queue_depth
// samples asynq's queues, which is a different backlog entirely, so an
// assignment sitting in this LIST for two minutes — the shape behind the
// webscan sink residency investigation — had no series at all. A directory
// that does not implement this capability simply reports nothing; the depth is
// a diagnostic, never a correctness requirement.
type AssignmentQueueDepthReporter interface {
	// AssignmentQueueDepths reads every queue's depth. The legacy queue is
	// always present, even when it is empty; a lane that was configured but
	// never written reads as a present zero, so an empty lane is visible as
	// "configured and idle" rather than a missing series.
	AssignmentQueueDepths(ctx context.Context) (map[string]int64, error)
}

var _ AssignmentQueueDepthReporter = (*RedisRunnerDirectory)(nil)

var _ AssignmentQueueDepthReporter = (*MemoryRunnerDirectory)(nil)

// AssignmentQueueDepths reads the depth of every assignment queue in one round
// trip: the configured lanes and the legacy queue. It reuses the claim walk's
// batch reader, so the keys reported are exactly the keys a claim walks — a
// lane a writer offers an assignment to cannot go unreported here.
//
// During a dual-write rollout one assignment is counted once on its lane and
// once on legacy. The depths are queue lengths, not distinct assignment counts,
// which is what the write mode makes them; read the two together as the split
// of the backlog rather than summing them.
func (d *RedisRunnerDirectory) AssignmentQueueDepths(ctx context.Context) (map[string]int64, error) {
	keys := d.laneQueueKeys()
	lengths, err := d.claimQueueLengths(ctx, keys)
	if err != nil {
		return nil, err
	}
	depths := make(map[string]int64, len(keys))
	depths[QueueLaneLegacy] = lengths[d.keys.queue]
	for _, lane := range d.lanes {
		depths[lane] = lengths[d.keys.laneQueueKey(lane)]
	}
	return depths, nil
}
