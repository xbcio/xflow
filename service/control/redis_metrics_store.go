package control

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// RedisMetricsKeyPrefix carries the {control} hash tag so the payload keys
	// and their index land in the same Cluster slot. That co-location is what
	// makes the index SET usable at all: a cross-slot SMEMBERS+MGET pair is a
	// Cluster error, and a bare SCAN would only walk the node the connection
	// happens to be pinned to — silently missing most of the fleet.
	RedisMetricsKeyPrefix = "xflow:runner:metrics:{control}"

	// DefaultMetricsRetention is how long a report is retained. It is 3x
	// DefaultRunnerLiveTTL: retention only governs when the KEY disappears
	// (keeping Redis from accumulating garbage), while IsLive governs when the
	// SERIES stops being emitted. IsLive fires first by design, so there is
	// never a window where the key is present but nobody judges the runner dead.
	DefaultMetricsRetention = 3 * DefaultRunnerLiveTTL
)

// RedisMetricsStore is the multi-replica MetricsStore. Every server replica
// reads the same keys, so a report that lands on replica A through a load
// balancer is visible when Prometheus scrapes replica B.
type RedisMetricsStore struct {
	rdb       redis.Cmdable
	retention time.Duration
}

func NewRedisMetricsStore(rdb redis.Cmdable, retention time.Duration) *RedisMetricsStore {
	if retention <= 0 {
		retention = DefaultMetricsRetention
	}
	return &RedisMetricsStore{rdb: rdb, retention: retention}
}

func (s *RedisMetricsStore) indexKey() string { return RedisMetricsKeyPrefix + ":index" }

func (s *RedisMetricsStore) payloadKey(runnerID string) string {
	return RedisMetricsKeyPrefix + ":payload:" + runnerID
}

// Put replaces the runner's retained report and refreshes its TTL. The index
// SET deliberately has no TTL — an expired payload leaves a dangling member
// that List prunes, which is cheaper and simpler than trying to keep a
// collection's expiry in step with its members'.
func (s *RedisMetricsStore) Put(ctx context.Context, runnerID string, stamped []byte) error {
	if s == nil || s.rdb == nil {
		return errors.New("control: redis metrics store not configured")
	}
	if runnerID == "" {
		return ErrRunnerIDRequired
	}
	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, s.payloadKey(runnerID), stamped, s.retention)
	pipe.SAdd(ctx, s.indexKey(), runnerID)
	_, err := pipe.Exec(ctx)
	return err
}

// List returns every retained payload in one round trip after the index read,
// rather than one GET per runner.
func (s *RedisMetricsStore) List(ctx context.Context) (map[string][]byte, error) {
	if s == nil || s.rdb == nil {
		return nil, errors.New("control: redis metrics store not configured")
	}
	ids, err := s.rdb.SMembers(ctx, s.indexKey()).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, s.payloadKey(id))
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(ids))
	var stale []any
	for i, id := range ids {
		if i >= len(vals) || vals[i] == nil {
			stale = append(stale, id)
			continue
		}
		switch v := vals[i].(type) {
		case string:
			out[id] = []byte(v)
		case []byte:
			out[id] = v
		default:
			stale = append(stale, id)
		}
	}
	if len(stale) > 0 {
		// Best effort: a failed prune only means the next List prunes again.
		// This is the read path's only write, and it is idempotent, so
		// concurrent Gather calls across replicas cannot corrupt each other.
		_ = s.rdb.SRem(ctx, s.indexKey(), stale...).Err()
	}
	return out, nil
}

var _ MetricsStore = (*RedisMetricsStore)(nil)
