package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var _ engine.OutboxFailureRecorder = (*Store)(nil)
var _ engine.OutboxLeaser = (*Store)(nil)
var _ engine.OutboxReleaser = (*Store)(nil)
var _ engine.OutboxMetricsReader = (*Store)(nil)

type redisOutboxEntry struct {
	ID          string      `json:"id"`
	Task        engine.Task `json:"task"`
	AutoDepth   int         `json:"auto_depth,omitempty"`
	Activation  int         `json:"activation_id,omitempty"`
	UnitIdx     *int        `json:"unit_idx,omitempty"`
	AvailableAt int64       `json:"available_at_ms,omitempty"`
	CreatedAt   int64       `json:"created_at_ms,omitempty"`
}

// leaseOutboxLua claims ready entries by pushing their ready-ZSET score forward
// to now+visibility, and returns each claimed entry's id, body, and PREVIOUS
// score so the caller can hand the entry straight back on backpressure.
//
// The score dimension already means "not deliverable before this instant", so a
// lease reuses it rather than introducing a second index. Crucially the member
// is NOT removed: ack remains the only removal path outside
// recordOutboxFailureLua, so the body-presence ⇔ ready-membership equivalence
// that script's ghost dead-letter guard relies on still holds.
//
// An entry whose body is gone was already acked; its stray ready member is
// dropped here rather than leased, which is what the pre-lease read loop did.
//
// KEYS: 1=outbox:ready 2=outbox:body
// ARGV: 1=cutoff_ms (availability cutoff, also the lease clock) 2=limit
//
//	3=visibility_ms
var leaseOutboxLua = redis.NewScript(`
local cutoff = tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff, 'LIMIT', 0, tonumber(ARGV[2]))
local out = {}
for i = 1, #ids do
    local entryID = ids[i]
    local body = redis.call('HGET', KEYS[2], entryID)
    if body then
        local previous = redis.call('ZSCORE', KEYS[1], entryID)
        redis.call('ZADD', KEYS[1], cutoff + tonumber(ARGV[3]), entryID)
        out[#out + 1] = entryID
        out[#out + 1] = body
        out[#out + 1] = previous
    else
        redis.call('ZREM', KEYS[1], entryID)
        redis.call('HDEL', KEYS[2], entryID)
    end
end
return out
`)

// renewOutboxLua extends the leases a live deliverer still holds.
//
// Renewal is fenced on the deadline the caller believes it holds, because the
// lease has no other identity. Presenting a deadline that no longer matches the
// entry's score means this caller's lease already lapsed and someone else
// claimed the entry; pushing the score forward then would yank it back
// mid-flight and deliver it twice — the exact duplication the lease exists to
// prevent, in precisely the case it exists for (a deliverer that stalls and
// then comes back). Such a renewal is refused.
//
// A caller that lost its lease learns so from the renewed count, which is why
// the script returns it rather than a bare OK.
//
// KEYS: 1=outbox:ready 2=outbox:body
// ARGV: 1=now_ms 2=visibility_ms 3..N=entryID,expectedDeadlineMs pairs
var renewOutboxLua = redis.NewScript(`
local now = tonumber(ARGV[1])
local visibility = tonumber(ARGV[2])
local renewed = 0
for i = 3, #ARGV, 2 do
    local entryID = ARGV[i]
    local expected = tonumber(ARGV[i + 1])
    if redis.call('HGET', KEYS[2], entryID) then
        local score = redis.call('ZSCORE', KEYS[1], entryID)
        if score and tonumber(score) == expected and expected > now then
            redis.call('ZADD', KEYS[1], now + visibility, entryID)
            renewed = renewed + 1
        end
    end
end
return renewed
`)

// releaseOutboxLua hands a leased entry back for immediate redelivery by
// restoring its original availability score.
//
// The body check is load-bearing: without it a release that races an ack would
// ZADD a member whose body is gone, creating exactly the ghost ready entry
// recordOutboxFailureLua's guard reasons cannot exist.
//
// KEYS: 1=outbox:ready 2=outbox:body
// ARGV: 1=entryID 2=score_ms
var releaseOutboxLua = redis.NewScript(`
if not redis.call('HGET', KEYS[2], ARGV[1]) then
    return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), ARGV[1])
return 1
`)

// readOutboxLua reports ready entries without claiming any of them.
//
// The cutoff filters out both entries that are not due yet and entries another
// deliverer currently holds a lease on — both express themselves as a score in
// the future, which is precisely what "not ready" means.
//
// It self-heals a body-less ready member the same way leaseOutboxLua does: such
// a member is the residue of an ack, and leaving it would break the
// body-presence ⇔ ready-membership equivalence the dead-letter guard relies on.
//
// KEYS: 1=outbox:ready 2=outbox:body
// ARGV: 1=cutoff_ms 2=limit
var readOutboxLua = redis.NewScript(`
local cutoff = tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff, 'LIMIT', 0, tonumber(ARGV[2]))
local out = {}
for i = 1, #ids do
    local entryID = ids[i]
    local body = redis.call('HGET', KEYS[2], entryID)
    if body then
        out[#out + 1] = entryID
        out[#out + 1] = body
    else
        redis.call('ZREM', KEYS[1], entryID)
        redis.call('HDEL', KEYS[2], entryID)
    end
end
return out
`)

// ListOutbox reports ready delivery intents without changing any state. Entries
// another deliverer currently holds a lease on are not ready and are omitted.
// Claiming for delivery is LeaseOutbox's job.
func (s *Store) ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	t := namespace.FromContext(ctx)
	raw, err := readOutboxLua.Run(ctx, s.rdb,
		[]string{outboxReadyKey(t, id), outboxBodyKey(t, id)},
		before.UnixMilli(), limit,
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("list outbox %q: %w", id, err)
	}
	out := make([]engine.OutboxEntry, 0, len(raw)/2)
	for i := 0; i+1 < len(raw); i += 2 {
		entryID, _ := raw[i].(string)
		body, _ := raw[i+1].(string)
		entry, err := unmarshalRedisOutboxEntry(body)
		if err != nil {
			return out, fmt.Errorf("decode outbox %q/%q: %w", id, entryID, err)
		}
		out = append(out, entry)
	}
	return out, nil
}

// LeaseOutbox claims ready entries for exclusive delivery, hiding each from
// other callers for engine.OutboxDeliveryLeaseTTL. Entries stay in Redis until
// AckOutbox, so enqueue/ack response loss is still retried — once the lease
// lapses or is released.
func (s *Store) LeaseOutbox(ctx context.Context, id types.ExecutionID, now time.Time, limit int) ([]engine.OutboxEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	t := namespace.FromContext(ctx)
	deadline := now.UnixMilli() + engine.OutboxDeliveryLeaseTTL.Milliseconds()
	raw, err := leaseOutboxLua.Run(ctx, s.rdb,
		[]string{outboxReadyKey(t, id), outboxBodyKey(t, id)},
		now.UnixMilli(), limit, engine.OutboxDeliveryLeaseTTL.Milliseconds(),
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("lease outbox %q: %w", id, err)
	}
	out := make([]engine.OutboxEntry, 0, len(raw)/3)
	for i := 0; i+2 < len(raw); i += 3 {
		entryID, _ := raw[i].(string)
		body, _ := raw[i+1].(string)
		entry, err := unmarshalRedisOutboxEntry(body)
		if err != nil {
			return out, fmt.Errorf("decode outbox %q/%q: %w", id, entryID, err)
		}
		entry.LeasedFromScoreMs = leasedScoreOf(raw[i+2])
		entry.LeaseDeadlineMs = deadline
		out = append(out, entry)
	}
	return out, nil
}

// RenewOutbox extends the leases this deliverer still holds, proving it is
// alive so engine.OutboxDeliveryLeaseTTL can stay short.
//
// It reports back the entries whose renewal was granted, each carrying its new
// deadline for the next renewal to fence on. An entry missing from the result
// is one this caller no longer holds — see renewOutboxLua. Because the script
// returns only a count, the grant is all-or-nothing per batch: a partial renewal
// cannot be attributed to specific entries, so the caller must treat the whole
// batch as lost and let those leases lapse, which redelivers rather than loses.
func (s *Store) RenewOutbox(ctx context.Context, id types.ExecutionID, entries []engine.OutboxEntry, now time.Time) ([]engine.OutboxEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	t := namespace.FromContext(ctx)
	deadline := now.UnixMilli() + engine.OutboxDeliveryLeaseTTL.Milliseconds()
	argv := make([]any, 0, 2*len(entries)+2)
	argv = append(argv, now.UnixMilli(), engine.OutboxDeliveryLeaseTTL.Milliseconds())
	for _, entry := range entries {
		argv = append(argv, entry.ID, entry.LeaseDeadlineMs)
	}
	renewed, err := renewOutboxLua.Run(ctx, s.rdb,
		[]string{outboxReadyKey(t, id), outboxBodyKey(t, id)},
		argv...,
	).Int64()
	if err != nil {
		return nil, fmt.Errorf("renew outbox %q: %w", id, err)
	}
	if int(renewed) != len(entries) {
		return nil, nil
	}
	out := make([]engine.OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		entry.LeaseDeadlineMs = deadline
		out = append(out, entry)
	}
	return out, nil
}

// leasedScoreOf reads back the pre-lease ready score. Redis returns ZSCORE as a
// string from Lua; a value we cannot parse would make ReleaseOutbox restore a
// wrong availability time, so fall back to 0 (immediately available) — the same
// score every non-delayed producer writes.
func leasedScoreOf(v any) int64 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int64(f)
}

// ReleaseOutbox returns a leased entry to the ready set immediately instead of
// waiting out its visibility timeout. Backpressure and a failed handoff are both
// retry-now conditions; holding the lease for the full timeout would stall a
// perfectly good intent.
func (s *Store) ReleaseOutbox(ctx context.Context, id types.ExecutionID, entry engine.OutboxEntry) error {
	t := namespace.FromContext(ctx)
	if err := releaseOutboxLua.Run(ctx, s.rdb,
		[]string{outboxReadyKey(t, id), outboxBodyKey(t, id)},
		entry.ID, entry.LeasedFromScoreMs,
	).Err(); err != nil {
		return fmt.Errorf("release outbox %q/%q: %w", id, entry.ID, err)
	}
	return nil
}

// AckOutbox removes an already-enqueued entry atomically and idempotently.
func (s *Store) AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error {
	t := namespace.FromContext(ctx)
	if err := ackOutboxLua.Run(ctx, s.rdb, []string{outboxReadyKey(t, id), outboxBodyKey(t, id), outboxAttemptsKey(t, id)}, entryID).Err(); err != nil {
		return fmt.Errorf("ack outbox %q/%q: %w", id, entryID, err)
	}
	return nil
}

// RecordOutboxFailure records a failed queue handoff and moves an intent to
// execution-scoped dead-letter storage after maxAttempts failures. It also
// writes compact node/activation metadata so later replay can guard against
// stale activations without parsing the entry body.
func (s *Store) RecordOutboxFailure(ctx context.Context, id types.ExecutionID, entry engine.OutboxEntry, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
	if maxAttempts <= 0 {
		maxAttempts = engine.DefaultOutboxMaxDeliveryAttempts
	}
	ttl := s.getExecTTL(ctx, id)
	t := namespace.FromContext(ctx)
	result, err := recordOutboxFailureLua.Run(ctx, s.rdb, []string{
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		outboxDeadMetaKey(t, id, entry.ID),
		outboxReplayEntryIdxKey(t, id), // KEYS[7]: clear stale replay index on re-dead-letter
	}, entry.ID, maxAttempts, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		entry.Task.NodeName, entry.Task.ActivationID,
		deadLetterIntent(entry.ID), int(entry.Task.Type)).Slice()
	if err != nil {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: %w", id, entry.ID, err)
	}
	if len(result) != 2 {
		return engine.OutboxDeliveryFailure{}, fmt.Errorf("record outbox failure %q/%q: unexpected result %v", id, entry.ID, result)
	}
	failure := engine.OutboxDeliveryFailure{Attempts: int(redisResultInt(result[0])), DeadLettered: redisResultInt(result[1]) == 1}
	if err := s.refreshTransientTTL(ctx, id,
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
		outboxAttemptsKey(t, id),
		outboxDeadKey(t, id),
		outboxDeadBodyKey(t, id),
		outboxDeadMetaKey(t, id, entry.ID),
		outboxReplayEntryIdxKey(t, id),
	); err != nil {
		return engine.OutboxDeliveryFailure{}, err
	}
	return failure, nil
}

// OutboxMetrics scans durable pending and dead-letter indexes to provide
// aggregate backlog metrics. It is a recovery/observability path, not part of
// the task-delivery hot path.
func (s *Store) OutboxMetrics(ctx context.Context) (engine.OutboxMetricsSnapshot, error) {
	var snapshot engine.OutboxMetricsSnapshot
	namespaces, err := s.listNamespaces(ctx)
	if err != nil {
		return engine.OutboxMetricsSnapshot{}, fmt.Errorf("list namespaces for outbox metrics: %w", err)
	}
	for _, t := range namespaces {
		if err := s.scanOutboxMetricsForTenant(ctx, t, &snapshot); err != nil {
			return engine.OutboxMetricsSnapshot{}, err
		}
	}
	return snapshot, nil
}

// outboxMetricsHeadCap bounds how many ready members this scan reads per
// execution when looking for the oldest creation time.
//
// The ready-ZSET is ordered by available-at, not by creation time, so the
// first member is not necessarily the oldest entry: a lease pushes a member's
// score forward and a retry backoff does the same, both reordering the set
// against creation order. Reading a bounded head and taking the minimum
// CreatedAt across it is the compromise — an execution whose oldest entry sits
// past this cap reports the oldest within the head, which understates the
// backlog age but never invents one.
const outboxMetricsHeadCap = 16

// outboxBodyKeyFromReadyKey derives the body-hash key from its ready-ZSET key.
//
// Both are execKey(t, id, ...) with different suffixes, and the scan yields
// the ready key without the execution ID it was built from. Rebuilding by
// suffix substitution avoids parsing the ID back out of the key.
func outboxBodyKeyFromReadyKey(readyKey string) string {
	return strings.TrimSuffix(readyKey, "outbox:ready") + "outbox:body"
}

// oldestOutboxCreatedAt returns the earliest CreatedAt across the given ready
// members, read from their entry bodies.
//
// It reads the body rather than the ZSET score because the score means
// available-at: entries enqueued by the entry-seed path carry a literal 0
// (which reads back as 1970), and a leased or retry-delayed entry carries a
// future instant (which clamps the reported age to zero). CreatedAt is written
// once at marshal time and never moves.
//
// A member whose body is missing was already acked and is skipped; a body that
// predates the CreatedAt field decodes it as 0 and is skipped too, so an old
// backlog reports no age rather than 1970.
func (s *Store) oldestOutboxCreatedAt(ctx context.Context, bodyKey string, members []string) (time.Time, error) {
	raw, err := s.rdb.HMGet(ctx, bodyKey, members...).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("read pending outbox bodies %q: %w", bodyKey, err)
	}
	var oldest time.Time
	for _, v := range raw {
		body, ok := v.(string)
		if !ok {
			continue
		}
		var entry redisOutboxEntry
		if err := json.Unmarshal([]byte(body), &entry); err != nil {
			continue
		}
		if entry.CreatedAt <= 0 {
			continue
		}
		at := time.UnixMilli(entry.CreatedAt).UTC()
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	return oldest, nil
}

func (s *Store) scanOutboxMetricsForTenant(ctx context.Context, t namespace.Namespace, snapshot *engine.OutboxMetricsSnapshot) error {
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:ready"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan pending outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("count pending outbox %q: %w", key, err)
			}
			snapshot.Pending += int(count)
			if count == 0 {
				continue
			}
			head, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key: key, Start: "-inf", Stop: "+inf", ByScore: true,
				Offset: 0, Count: outboxMetricsHeadCap,
			}).Result()
			if err != nil {
				return fmt.Errorf("read pending outbox head %q: %w", key, err)
			}
			if len(head) == 0 {
				continue
			}
			oldestAt, err := s.oldestOutboxCreatedAt(ctx, outboxBodyKeyFromReadyKey(key), head)
			if err != nil {
				return err
			}
			if !oldestAt.IsZero() && (snapshot.OldestPendingAt.IsZero() || oldestAt.Before(snapshot.OldestPendingAt)) {
				snapshot.OldestPendingAt = oldestAt
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	cursor = 0
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:dead"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan dead-letter outbox indexes: %w", err)
		}
		for _, key := range keys {
			count, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("count dead-letter outbox %q: %w", key, err)
			}
			snapshot.DeadLettered += int(count)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// ListOutboxExecutions scans execution-scoped ready indexes. The index is
// authoritative per execution; scanning is only a recovery discovery path.
func (s *Store) ListOutboxExecutions(ctx context.Context, limit int) ([]types.ExecutionID, error) {
	if limit <= 0 {
		return nil, nil
	}
	ids := make(map[types.ExecutionID]struct{})
	namespaces, err := s.listNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("list namespaces for outbox discovery: %w", err)
	}
	for _, t := range namespaces {
		if len(ids) >= limit {
			break
		}
		if err := s.scanOutboxExecutionsForTenant(ctx, t, limit, ids); err != nil {
			return nil, err
		}
	}
	out := make([]types.ExecutionID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *Store) scanOutboxExecutionsForTenant(ctx context.Context, t namespace.Namespace, limit int, ids map[types.ExecutionID]struct{}) error {
	var cursor uint64
	for len(ids) < limit {
		keys, next, err := s.rdb.Scan(ctx, cursor, execScanPattern(t, "outbox:ready"), 128).Result()
		if err != nil {
			return fmt.Errorf("scan outbox indexes: %w", err)
		}
		for _, key := range keys {
			id, ok := executionIDFromKey(key)
			if ok {
				ids[id] = struct{}{}
			}
			if len(ids) >= limit {
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

func marshalRedisOutboxEntry(id string, task engine.Task, availableAt time.Time) (string, error) {
	entry := redisOutboxEntry{
		ID:          id,
		Task:        task,
		AutoDepth:   task.AutoDepth,
		Activation:  task.ActivationID,
		UnitIdx:     redisUnitIdxPtr(task.UnitIdx),
		AvailableAt: availableAt.UnixMilli(),
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	if availableAt.IsZero() {
		entry.AvailableAt = 0
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("marshal outbox %q: %w", id, err)
	}
	return string(data), nil
}

// redisUnitIdxPtr omits the wire field when the task's UnitIdx is the
// "unknown" sentinel, so absence on decode is distinguishable from a real
// unit index of 0. See engine.UnitIdxUnknown.
func redisUnitIdxPtr(unitIdx int) *int {
	if unitIdx == engine.UnitIdxUnknown {
		return nil
	}
	v := unitIdx
	return &v
}

func unmarshalRedisOutboxEntry(raw string) (engine.OutboxEntry, error) {
	var encoded redisOutboxEntry
	if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
		return engine.OutboxEntry{}, err
	}
	encoded.Task.AutoDepth = encoded.AutoDepth
	encoded.Task.ActivationID = encoded.Activation
	if encoded.UnitIdx != nil {
		encoded.Task.UnitIdx = *encoded.UnitIdx
	} else {
		encoded.Task.UnitIdx = engine.UnitIdxUnknown
	}
	entry := engine.OutboxEntry{ID: encoded.ID, Task: encoded.Task}
	if encoded.AvailableAt > 0 {
		entry.AvailableAt = time.UnixMilli(encoded.AvailableAt).UTC()
	}
	if encoded.CreatedAt > 0 {
		entry.CreatedAt = time.UnixMilli(encoded.CreatedAt).UTC()
	}
	return entry, nil
}

func redisAdvanceOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("advance/%s/%s/%d", id, name, activationID)
}
func redisExecuteOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("execute/%s/%s/%d", id, name, activationID)
}
func redisSkipOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("skip/%s/%s/%d", id, name, activationID)
}
