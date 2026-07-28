package rstate

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var _ engine.DeadLetterStore = (*Store)(nil)

// listDeadLettersAfterFetchHook is a test-only hook invoked after
// ListDeadLetters fetches the dead-letter index but before it constructs the
// next cursor. It exists solely to exercise races that are no longer possible
// after the WithScores fetch made score capture atomic.
var listDeadLettersAfterFetchHook func()

type deadLetterIndexEntry struct {
	Member string
	Score  float64
}

// ListDeadLetters returns one page of dead-lettered outbox entries for one
// execution, ordered oldest-first by (score, member) — i.e. dead-letter
// timestamp, with entry ID as the tie-break so same-millisecond dead-letters
// still paginate stably. It reads the dead-letter body hash directly; entries
// are not removed. Pagination uses an opaque HMAC-signed cursor carrying the
// (score, member) resume point; an empty cursor starts from the oldest entry.
// The cursor is bound to the execution and signed with a process-local key,
// so a tampered, cross-execution, or stale (post-restart) cursor returns
// ErrCursorExpired and the caller must restart from the first page. limit<=0
// defaults to a bounded page size. The returned NextCursor is empty when the
// page is the last.
func (s *Store) ListDeadLetters(ctx context.Context, id types.ExecutionID, page engine.DeadLetterPage) (engine.DeadLetterList, error) {
	const defaultLimit, maxLimit = 100, 500
	if page.Limit <= 0 {
		page.Limit = defaultLimit
	}
	if page.Limit > maxLimit {
		page.Limit = maxLimit
	}
	t := namespace.FromContext(ctx)
	deadKey := outboxDeadKey(t, id)

	cursorScore, cursorMember, err := s.decodeDeadLetterCursor(page.Cursor, id)
	if err != nil {
		return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, err)
	}

	entries, err := s.listDeadLetterIndexEntries(ctx, deadKey, cursorScore, cursorMember, page.Cursor != "", page.Limit+1)
	if err != nil {
		return engine.DeadLetterList{}, fmt.Errorf("list dead letters %q: %w", id, err)
	}

	if listDeadLettersAfterFetchHook != nil {
		listDeadLettersAfterFetchHook()
	}

	var nextCursor string
	if len(entries) > page.Limit {
		// Build the next cursor from the (score, member) of the last entry on
		// this page. The score was captured atomically with the member in the
		// ZRangeWithScores fetch above, so a concurrent replay/delete of the
		// boundary entry cannot leave the cursor empty or drop later entries.
		boundary := entries[page.Limit-1]
		nextCursor = s.encodeDeadLetterCursor(id, boundary.Score, boundary.Member)
		entries = entries[:page.Limit]
	}

	out := make([]engine.OutboxEntry, 0, len(entries))
	for _, e := range entries {
		raw, err := s.rdb.HGet(ctx, outboxDeadBodyKey(t, id), e.Member).Result()
		if err == redis.Nil {
			// body missing while index still references it: self-heal by removing the stale index entry
			_ = s.rdb.ZRem(ctx, deadKey, e.Member).Err()
			continue
		}
		if err != nil {
			return engine.DeadLetterList{Entries: out}, fmt.Errorf("read dead letter %q/%q: %w", id, e.Member, err)
		}
		entry, err := unmarshalRedisOutboxEntry(raw)
		if err != nil {
			return engine.DeadLetterList{Entries: out}, fmt.Errorf("decode dead letter %q/%q: %w", id, e.Member, err)
		}
		out = append(out, entry)
	}
	return engine.DeadLetterList{Entries: out, NextCursor: nextCursor}, nil
}

func (s *Store) listDeadLetterIndexEntries(ctx context.Context, key string, cursorScore float64, cursorMember string, hasCursor bool, limit int) ([]deadLetterIndexEntry, error) {
	if !hasCursor {
		zs, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key: key, Start: "-inf", Stop: "+inf", ByScore: true, Offset: 0, Count: int64(limit),
		}).Result()
		if err != nil {
			return nil, err
		}
		return deadLetterIndexEntriesFromZ(zs), nil
	}

	const scanBatch = int64(512)
	entries := make([]deadLetterIndexEntry, 0, limit)
	score := formatScore(cursorScore)
	var offset int64
	for len(entries) < limit {
		zs, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key: key, Start: score, Stop: score, ByScore: true, Offset: offset, Count: scanBatch,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(zs) == 0 {
			break
		}
		for _, z := range zs {
			m, ok := z.Member.(string)
			if !ok {
				continue
			}
			if m > cursorMember {
				entries = append(entries, deadLetterIndexEntry{Member: m, Score: z.Score})
				if len(entries) == limit {
					break
				}
			}
		}
		if int64(len(zs)) < scanBatch {
			break
		}
		offset += int64(len(zs))
	}
	if len(entries) == limit {
		return entries, nil
	}

	higher, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key: key, Start: "(" + score, Stop: "+inf", ByScore: true, Offset: 0, Count: int64(limit - len(entries)),
	}).Result()
	if err != nil {
		return nil, err
	}
	entries = append(entries, deadLetterIndexEntriesFromZ(higher)...)
	return entries, nil
}

func deadLetterIndexEntriesFromZ(zs []redis.Z) []deadLetterIndexEntry {
	entries := make([]deadLetterIndexEntry, 0, len(zs))
	for _, z := range zs {
		m, ok := z.Member.(string)
		if !ok {
			continue
		}
		entries = append(entries, deadLetterIndexEntry{Member: m, Score: z.Score})
	}
	return entries
}

// formatScore renders a ZSET score as the exact string Redis round-trips, so a
// cursor's exclusive/inclusive score bounds match the stored member score
// byte-for-byte. Redis scores are IEEE-754 doubles; 'f' with -1 precision
// produces the decimal representation Go and Redis agree on for millisecond
// timestamps.
func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}

// ReplayDeadLetter moves a dead-lettered entry atomically and activation-safely
// back to the ready set so the OutboxDispatcher redelivers it. Concurrent or
// retried replays of the same entry (or RequestID) collapse to a single
// ReplayReplayed and return ReplayAlreadyReplayed with the original AuditID.
// Replays of terminal/expired executions, terminal nodes, or stale activations
// are rejected without mutating state.
func (s *Store) ReplayDeadLetter(ctx context.Context, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	if req.EntryID == "" {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayNotFound, ExecutionID: req.ExecutionID}, nil
	}
	requestID := req.RequestID
	if requestID == "" {
		requestID = req.EntryID
	}
	ttl := s.getExecTTL(req.ExecutionID)
	t := namespace.FromContext(ctx)
	result, err := replayDeadLetterLua.Run(ctx, s.rdb, []string{
		outboxDeadKey(t, req.ExecutionID),
		outboxDeadBodyKey(t, req.ExecutionID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
		outboxAttemptsKey(t, req.ExecutionID),
		execKey(t, req.ExecutionID, "status"),
		outboxDeadMetaKey(t, req.ExecutionID, req.EntryID),
		outboxReplayEntryIdxKey(t, req.ExecutionID),
	}, req.EntryID, time.Now().UTC().UnixMilli(), int(ttl.Seconds()),
		requestID, req.Operator, req.Reason, string(req.ExecutionID), string(t)).Slice()
	if err != nil {
		return engine.ReplayDeadLetterResult{}, fmt.Errorf("replay dead letter %q/%q: %w", req.ExecutionID, req.EntryID, err)
	}
	if len(result) != 4 {
		return engine.ReplayDeadLetterResult{}, fmt.Errorf("replay dead letter %q/%q: unexpected result %v", req.ExecutionID, req.EntryID, result)
	}
	outcome := replayOutcomeFromInt(redisResultInt(result[0]))
	return engine.ReplayDeadLetterResult{
		Outcome:      outcome,
		AuditID:      redisResultString(result[1]),
		ExecutionID:  req.ExecutionID,
		NodeID:       redisResultString(result[2]),
		ActivationID: redisResultString(result[3]),
	}, nil
}

func replayOutcomeFromInt(n int64) engine.DeadLetterReplayOutcome {
	switch n {
	case 1:
		return engine.ReplayReplayed
	case 2:
		return engine.ReplayRejectedTerminal
	case 3:
		return engine.ReplayRejectedInactive
	case 4:
		return engine.ReplayRejectedNodeTerminal
	case 5:
		return engine.ReplayRejectedActivationMismatch
	case 6:
		return engine.ReplayAlreadyReplayed
	case 7:
		return engine.ReplayRejectedMetadataMissing
	default:
		return engine.ReplayNotFound
	}
}

// deadLetterIntent extracts the intent prefix from an outbox entry ID
// (root/retry/requeue/resume/advance/execute/skip). It is written into the
// dead-letter meta hash at dead-letter time so replayDeadLetterLua can branch
// its guard rules by intent source instead of applying a single
// running/committing/waiting allowlist that wrongly rejects the typical
// initial/retry/requeue/resume dead-letters (whose node status is pending,
// suspended, or absent at dead-letter time). An empty/unknown intent falls
// back to "" and is treated by replay as legacy metadata → outcome 7.
func deadLetterIntent(entryID string) string {
	for i := 0; i < len(entryID); i++ {
		if entryID[i] == '/' {
			return entryID[:i]
		}
	}
	return ""
}
