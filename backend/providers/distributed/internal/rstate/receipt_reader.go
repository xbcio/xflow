package rstate

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// replayReceiptScanPattern is the per-namespace SCAN glob for replay receipt
// hashes. It matchesxflow:ns:<namespace>:exec:{*}:replay:receipt:* — every
// authoritative receipt written atomically by the replay Lua script. The
// {*} covers the execution id hash tag (no hash tag of its own, so SCAN does
// not collapse to one slot).
const replayReceiptScanPattern = "xflow:ns:%s:exec:{*}:replay:receipt:*"

// replayReceiptKeyPrefix is the literal prefix up to and including the
// requestID position; used to split the requestID out of a scanned key.
const replayReceiptKeyPrefix = "xflow:ns:"

// ScanReplayReceipts iterates every authoritative Redis replay receipt across
// all known namespaces and executions, invoking fn once per decoded receipt. It
// is read-only: it never mutates Redis, so the authoritative receipts survive
// regardless of the SQL projection state.
//
// The scan fans out per namespace (via the namespace registry) and uses SCAN (not
// KEYS) so a large Redis is not blocked. A receipt hash that is empty or
// malformed (legacy/partial) is skipped — fn is not called for it — because
// the projector's idempotency key (audit_id) would be empty and the row would
// never be queryable. Such skips are observable as a no-op here; T9's general
// reconcile worker will surface them as metrics.
//
// fn returning a non-nil error aborts the scan and propagates the error.
func (s *Store) ScanReplayReceipts(ctx context.Context, fn func(engine.ReplayReceipt) error) error {
	namespaces, err := s.listNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("list namespaces for receipt scan: %w", err)
	}
	for _, t := range namespaces {
		if err := s.scanReplayReceiptsForNamespace(ctx, t, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) scanReplayReceiptsForNamespace(ctx context.Context, t namespace.Namespace, fn func(engine.ReplayReceipt) error) error {
	pattern := fmt.Sprintf(replayReceiptScanPattern, t)
	keys, err := redisx.ScanAll(ctx, s.rdb, pattern, 256)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("scan replay receipts for namespace %q: %w", t, err)
	}
	for _, key := range keys {
		r, ok := decodeReplayReceiptKey(key, t)
		if !ok {
			continue
		}
		if err := s.decodeAndEmitReceipt(ctx, key, r, fn); err != nil {
			return err
		}
	}
	return nil
}

// decodeReplayReceiptKey splitsxflow:ns:<namespace>:exec:{<id>}:replay:receipt:<requestID>
// into its namespace (verified against the scan namespace), execution id, and
// request id. Returns ok=false when the key does not match the receipt shape.
func decodeReplayReceiptKey(key string, scanNamespace namespace.Namespace) (engine.ReplayReceipt, bool) {
	if !strings.HasPrefix(key, replayReceiptKeyPrefix) {
		return engine.ReplayReceipt{}, false
	}
	execID, requestID, ok := parseReplayReceiptKey(key)
	if !ok {
		return engine.ReplayReceipt{}, false
	}
	return engine.ReplayReceipt{
		Namespace:   string(scanNamespace),
		ExecutionID: string(execID),
		RequestID:   requestID,
	}, true
}

// parseReplayReceiptKey reversesxflow:ns:<namespace>:exec:{<id>}:replay:receipt:<requestID>
// into (executionID, requestID). The requestID is the trailing segment after
// the last ':' following the receipt marker; it may itself contain colons
// (receipts use requestID which is opaque to Redis), so the split takes the
// substring after the literal 'replay:receipt:' marker.
func parseReplayReceiptKey(key string) (types.ExecutionID, string, bool) {
	const marker = ":replay:receipt:"
	idx := strings.Index(key, marker)
	if idx < 0 {
		return "", "", false
	}
	head := key[:idx]
	requestID := key[idx+len(marker):]
	if requestID == "" {
		return "", "", false
	}
	// head isxflow:ns:<namespace>:exec:{<id>}; reuse the namespace-exec parser to
	// extract the execution id.
	_, execID, ok := parseNamespaceExecKey(head)
	if !ok {
		return "", "", false
	}
	return execID, requestID, true
}

// decodeAndEmitReceipt HGETALLs the receipt hash, decodes it, and invokes fn.
// A missing or empty receipt (e.g. expired between SCAN and HGETALL) is
// skipped silently — it will simply not be projected, which is correct: an
// expired receipt means the replay is outside the retention window.
func (s *Store) decodeAndEmitReceipt(ctx context.Context, key string, r engine.ReplayReceipt, fn func(engine.ReplayReceipt) error) error {
	fields, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("hgetall receipt %q: %w", key, err)
	}
	if len(fields) == 0 {
		return nil
	}
	auditID := fields["audit_id"]
	if auditID == "" {
		// Without an audit_id there is no idempotency key; skip rather than
		// project an un-correlatable row. T9 will surface these as metrics.
		return nil
	}
	tsMs, _ := strconv.ParseInt(fields["ts_ms"], 10, 64)
	r.AuditID = auditID
	r.NodeID = fields["node"]
	r.ActivationID = fields["activation"]
	r.Outcome = engine.DeadLetterReplayOutcome(fields["outcome"])
	r.Operator = fields["operator"]
	r.Reason = fields["reason"]
	r.EntryID = fields["entry_id"]
	r.TimestampMs = tsMs
	return fn(r)
}
