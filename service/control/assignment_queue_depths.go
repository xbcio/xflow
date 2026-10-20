package control

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

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

// assignmentQueueAgeSample is how many entries from the head of each queue the
// oldest-age report reads. The head is the oldest end — enqueue appends at the
// tail and every requeue pushes back at the head — so the first stamped entry
// is normally the oldest. It is a sample rather than the head alone because an
// entry an older control plane queued carries no stamp; a few entries are
// enough to step past such residue without turning the read into a scan.
const assignmentQueueAgeSample = 8

// AssignmentQueueOldestAgeReporter is the runner-directory capability that
// reports, per assignment queue, how long the oldest sampled entry has been
// waiting since it last entered the queue (enqueue or requeue).
//
// Depth alone cannot tell a large backlog that moves from a small one that is
// stuck: the webscan sink residency was minutes on a queue a few entries deep.
// The age is the direct reading of that residency.
type AssignmentQueueOldestAgeReporter interface {
	// AssignmentQueueOldestAges returns an entry for every empty queue (age
	// zero: nothing is waiting, so a drained queue does not leave the last age
	// frozen) and for every queue whose head sample carried at least one
	// queued-at stamp. A non-empty queue whose sampled entries all predate the
	// stamp is absent: it has waiters whose age is unknown, and zero would
	// claim otherwise.
	AssignmentQueueOldestAges(ctx context.Context) (map[string]time.Duration, error)
}

var _ AssignmentQueueOldestAgeReporter = (*RedisRunnerDirectory)(nil)

var _ AssignmentQueueOldestAgeReporter = (*MemoryRunnerDirectory)(nil)

// redisAssignmentQueueOldestAgesLua reads the head sample of every queue in
// KEYS[1..#KEYS-1] against the queued-at hash KEYS[#KEYS], and returns the
// server's TIME in milliseconds followed by each queue's earliest stamp (-2
// when the queue is empty, -1 when it is not but no sampled entry is
// stamped). It is read-only, and it uses the server clock for "now" so the
// age never mixes the writer's clock with the reader's.
const redisAssignmentQueueOldestAgesLua = `
local sample = tonumber(ARGV[1])
local stamps = KEYS[#KEYS]
local now = redis.call('TIME')
local result = { string.format('%.0f', tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)) }
for i = 1, #KEYS - 1 do
  local oldest = -2
  local ids = redis.call('LRANGE', KEYS[i], 0, sample - 1)
  if #ids > 0 then
    oldest = -1
    local values = redis.call('HMGET', stamps, unpack(ids))
    for j = 1, #values do
      local v = tonumber(values[j])
      if v and (oldest < 0 or v < oldest) then oldest = v end
    end
  end
  result[#result + 1] = string.format('%.0f', oldest)
end
return result
`

var redisAssignmentQueueOldestAgesScript = redis.NewScript(redisAssignmentQueueOldestAgesLua)

// AssignmentQueueOldestAges reads every queue's head sample in one round trip.
// It walks the same keys AssignmentQueueDepths reports, so a depth series and
// an age series always name the same lanes.
func (d *RedisRunnerDirectory) AssignmentQueueOldestAges(ctx context.Context) (map[string]time.Duration, error) {
	queues := d.laneQueueKeys()
	keys := append(append(make([]string, 0, len(queues)+1), queues...), d.keys.assignmentQueuedAt)
	raw, err := redisAssignmentQueueOldestAgesScript.Run(ctx, d.rdb, keys, assignmentQueueAgeSample).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("read assignment queue oldest ages: %w", err)
	}
	if len(raw) != len(queues)+1 {
		return nil, fmt.Errorf("read assignment queue oldest ages: unexpected result length %d", len(raw))
	}
	now, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("read assignment queue oldest ages: parse server time: %w", err)
	}
	laneOf := make(map[string]string, len(queues))
	laneOf[d.keys.queue] = QueueLaneLegacy
	for _, lane := range d.lanes {
		laneOf[d.keys.laneQueueKey(lane)] = lane
	}
	ages := make(map[string]time.Duration, len(queues))
	for i, key := range queues {
		oldest, err := strconv.ParseInt(raw[i+1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("read assignment queue oldest ages: parse stamp: %w", err)
		}
		if oldest == -2 {
			ages[laneOf[key]] = 0
			continue
		}
		if oldest < 0 {
			continue
		}
		age := time.Duration(now-oldest) * time.Millisecond
		if age < 0 {
			age = 0
		}
		ages[laneOf[key]] = age
	}
	return ages, nil
}
