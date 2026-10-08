package control

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

const (
	// RedisSupplyObservedKeyPrefix is the shared key space for runner-reported
	// applied supply hashes. Like RedisMetricsKeyPrefix it carries the {control}
	// hash tag so the payload keys and their index land in the same Cluster
	// slot: a cross-slot SMEMBERS+MGET pair is a Cluster error, and a bare SCAN
	// would only walk the node the connection happens to be pinned to —
	// silently missing most of the fleet.
	RedisSupplyObservedKeyPrefix = "xflow:supply:observed:{control}"

	// DefaultSupplyObservedRetention is how long a runner's last report is kept.
	// It mirrors DefaultMetricsRetention (3 × DefaultRunnerLiveTTL): retention
	// only governs when a stale report stops answering, and 3 heartbeat
	// intervals is long enough that a runner which merely missed a round is
	// still counted, short enough that a runner which is gone stops speaking
	// for the fleet well before a human notices the fleet view is wrong.
	DefaultSupplyObservedRetention = 3 * DefaultRunnerLiveTTL

	// supplyObservedOpTimeout bounds every internal Redis round trip. The sink
	// interface carries no context — Record rides a heartbeat and Snapshot
	// rides a management read — so a stalled Redis must degrade to "no
	// observation" within a bounded time instead of hanging whichever caller
	// arrived, on a path that is diagnostic and must never gate anything.
	supplyObservedOpTimeout = 2 * time.Second
)

// RedisSupplyObserved is the multi-replica SupplyObservedSink. Every server
// replica reads and writes the same keys, so a runner's report that lands on
// replica A through a load balancer is visible when a management read lands on
// replica B. The process-local MemorySupplyObserved cannot answer that: a
// report is only ever seen by the replica that happened to receive the
// heartbeat, so "has everyone applied revision N" would flap with the load
// balancer's choice of replica.
//
// Best-effort by contract: the sink interface has no error return (an observed
// report is diagnostic, never a gate on the heartbeat succeeding), so a
// failure is logged and swallowed. A failed write costs one stale-or-absent
// observation until the runner's next heartbeat; a failed read degrades to
// "no observation".
type RedisSupplyObserved struct {
	rdb       redis.Cmdable
	retention time.Duration
	logger    engine.Logger
}

// NewRedisSupplyObserved returns a Redis-backed sink. A non-positive retention
// adopts DefaultSupplyObservedRetention. The logger may be nil.
func NewRedisSupplyObserved(rdb redis.Cmdable, retention time.Duration, logger engine.Logger) *RedisSupplyObserved {
	if retention <= 0 {
		retention = DefaultSupplyObservedRetention
	}
	return &RedisSupplyObserved{rdb: rdb, retention: retention, logger: logger}
}

func (s *RedisSupplyObserved) indexKey() string { return RedisSupplyObservedKeyPrefix + ":index" }

func (s *RedisSupplyObserved) payloadKey(runnerID string) string {
	return RedisSupplyObservedKeyPrefix + ":payload:" + runnerID
}

// warn reports one failure, nil-safely. It is logged per failed call rather
// than rate-limited, the same shape the supply hinter gives its own failed
// reads: the heartbeat cadence that makes this hot is also what makes the
// failure self-healing, so a run of identical lines is the signal that Redis,
// not the report, is the problem.
func (s *RedisSupplyObserved) warn(msg string, err error) {
	if s.logger != nil {
		s.logger.Warn(msg, "error", err)
	}
}

// Record replaces runnerID's entire reported set with observed, mirroring
// MemorySupplyObserved: a supply no longer in observed (the runner stopped
// hosting its consumer, or the workflow was removed) must not linger — a
// merge would leave a permanently stale entry that nothing ever clears.
//
// The index SET deliberately has no TTL — an expired payload leaves a dangling
// member that Snapshot prunes, which is cheaper and simpler than trying to keep
// a collection's expiry in step with its members' (the same trade
// RedisMetricsStore.Put makes).
func (s *RedisSupplyObserved) Record(runnerID string, observed map[string]string) {
	if s == nil || s.rdb == nil || runnerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), supplyObservedOpTimeout)
	defer cancel()
	pipe := s.rdb.TxPipeline()
	if len(observed) == 0 {
		pipe.Del(ctx, s.payloadKey(runnerID))
		pipe.SRem(ctx, s.indexKey(), runnerID)
		if _, err := pipe.Exec(ctx); err != nil {
			s.warn("supply observed: clear report failed", err)
		}
		return
	}
	payload, err := json.Marshal(observed)
	if err != nil {
		// Unreachable for map[string]string, but a sink that discards a report
		// without saying so is the failure mode this type exists to avoid.
		s.warn("supply observed: encode report failed", err)
		return
	}
	pipe.Set(ctx, s.payloadKey(runnerID), payload, s.retention)
	pipe.SAdd(ctx, s.indexKey(), runnerID)
	if _, err := pipe.Exec(ctx); err != nil {
		s.warn("supply observed: record report failed", err)
	}
}

// Snapshot returns runner ID → (supply name → applied hash) for the whole
// fleet in one round trip after the index read, rather than one GET per
// runner. A failure degrades to nil, never to a partial fleet view: the caller
// is deciding "has everyone converged", and an answer built from some runners
// is worse than no answer.
//
// Expired payloads leave their index members behind (the index has no TTL), so
// a read prunes what it finds. That is the same read-path write
// RedisMetricsStore.List performs, and it is idempotent for the same reason:
// concurrent Snapshot calls across replicas cannot corrupt each other.
func (s *RedisSupplyObserved) Snapshot() map[string]map[string]string {
	if s == nil || s.rdb == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), supplyObservedOpTimeout)
	defer cancel()
	ids, err := s.rdb.SMembers(ctx, s.indexKey()).Result()
	if err != nil {
		s.warn("supply observed: list runners failed", err)
		return nil
	}
	if len(ids) == 0 {
		return map[string]map[string]string{}
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, s.payloadKey(id))
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		s.warn("supply observed: read reports failed", err)
		return nil
	}
	out := make(map[string]map[string]string, len(ids))
	var stale []any
	for i, id := range ids {
		if i >= len(vals) || vals[i] == nil {
			stale = append(stale, id)
			continue
		}
		var raw string
		switch v := vals[i].(type) {
		case string:
			raw = v
		case []byte:
			raw = string(v)
		default:
			stale = append(stale, id)
			continue
		}
		var report map[string]string
		if err := json.Unmarshal([]byte(raw), &report); err != nil {
			// A payload this store did not write is not worth failing the whole
			// snapshot over; drop the member like an expired payload, and the
			// runner's next heartbeat restores a well-formed one.
			stale = append(stale, id)
			continue
		}
		out[id] = report
	}
	if len(stale) > 0 {
		// Best effort: a failed prune only means the next Snapshot prunes again.
		_ = s.rdb.SRem(ctx, s.indexKey(), stale...).Err()
	}
	return out
}

var _ SupplyObservedSink = (*RedisSupplyObserved)(nil)
