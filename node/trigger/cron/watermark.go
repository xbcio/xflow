package cron

import (
	"context"
	"log/slog"
	"time"

	"github.com/xbcio/xflow/types"
)

// The watermark is the last scheduled instant this trigger has ACKNOWLEDGED: the
// firing it either admitted, skipped, or dropped. It is written through the
// runtime's TriggerState, so it survives a restart and outlives the process that
// wrote it — which is the point: without it a restarted trigger has no way to
// know which slots of its own schedule went by while nobody was hosting it, and
// a firing that was never admitted leaves no other trace (no execution, no
// offset, no state).
//
// It is deliberately NOT what a firing's identity is derived from. The identity
// comes from the schedule (see scheduleCursor), and the watermark records what
// was already accounted for; deriving an identity from a watermark that a crash
// left stale would republish instants whose firings already ran.

const (
	// watermarkKey is the key the record lives under inside its scope. One key,
	// because the record is one instant — the state store is a key/value map per
	// scope, not a document store.
	watermarkKey = "last_scheduled"

	// watermarkScopePrefix namespaces the scope to this trigger kind. The scope
	// is per (workflow, node), so two cron nodes of one workflow — and the same
	// node of two workflows — never share a record.
	watermarkScopePrefix = "cron"
)

// watermarkScope is the TriggerState scope one (workflow, node) pair owns.
func watermarkScope(workflowID types.WorkflowID, nodeName string) string {
	return watermarkScopePrefix + ":" + string(workflowID) + ":" + nodeName
}

// openWatermark resolves the state handle a firing writes its watermark through.
//
// A nil handle is a supported outcome rather than a fault: a runtime whose state
// lives somewhere else (or nowhere) still fires, it just cannot say what it
// already fired. The caller reports that once per activation — the cluster
// runner's entry-seed runtime is the runtime this happens with today — and then
// runs exactly as it did before the watermark existed.
func openWatermark(ctx context.Context, rt types.TriggerRuntime, workflowID types.WorkflowID, nodeName string) types.TriggerState {
	if rt == nil {
		return nil
	}
	return rt.State(ctx, watermarkScope(workflowID, nodeName))
}

// readWatermark returns the last acknowledged scheduled instant, or ok=false
// when the record is absent, unreadable, or written by a build that spelled the
// instant differently.
//
// Every failure is the same answer on purpose. The record is an optimization for
// the operations that FOLLOW a firing: a firing itself is admitted on its own
// identity, so a missing or corrupt watermark costs at most the knowledge of
// which instants were already accounted for — never a wrong admission. A caller
// that treated "unreadable" as "the epoch" instead would turn one corrupt value
// into a catch-up of every instant the schedule ever had.
func readWatermark(ctx context.Context, state types.TriggerState) (time.Time, bool) {
	if state == nil {
		return time.Time{}, false
	}
	raw, err := state.Get(ctx, watermarkKey)
	if err != nil || len(raw) == 0 {
		return time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339Nano, string(raw))
	if err != nil {
		return time.Time{}, false
	}
	return at.UTC(), true
}

// writeWatermark records one acknowledged firing. The encoding is the fire key's
// own rendering of the instant (UTC, RFC3339Nano), so a watermark read out of
// the state store can be matched against the fire keys in a log or an admission
// table without a conversion step.
//
// An error is returned rather than logged so the caller decides: a firing has
// already been admitted by the time this runs, and a failed watermark write must
// not undo that. There is no TTL — the record is the trigger's own history, and
// expiring it would silently reintroduce the amnesia it exists to remove.
func writeWatermark(ctx context.Context, state types.TriggerState, scheduled time.Time) error {
	if state == nil {
		return nil
	}
	return state.Set(ctx, watermarkKey, []byte(cronFireID(scheduled)), 0)
}

// openActivationWatermark resolves one activation's watermark and logs the one
// thing the activation can say about its own past.
//
// A runtime that keeps no trigger state returns a nil handle, and that is logged
// as the capability gap it is rather than as an error: the trigger fires
// correctly either way, and the gap only costs the ability to identify firings
// missed while no activation was hosting the schedule. Once per activation is
// the right cadence — an activation is a rare event, and a per-firing line would
// bury the fact in the noise it is meant to stand out from.
func openActivationWatermark(ctx context.Context, in *types.TriggerActivateInput) types.TriggerState {
	state := openWatermark(ctx, in.Runtime, in.WorkflowID, in.NodeName)
	if state == nil {
		slog.Warn("cron trigger keeps no fire watermark: this runtime has no trigger state, "+
			"so firings that it was not running to observe cannot be identified after a restart",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName)
		return nil
	}
	if last, ok := readWatermark(ctx, state); ok {
		slog.Info("cron trigger resuming from its last scheduled fire",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"last_scheduled", cronFireID(last),
			"stale_for", time.Since(last).Round(time.Second).String())
	}
	return state
}

// recordFiring moves the watermark past one firing that has been handled.
//
// It runs for a firing that was admitted, one that was refused, and one that was
// dropped: in all three the instant is behind the trigger, and the watermark's
// job is to say which instants are behind it. A firing that was never attempted
// — a store failure before the attempt — is the one case that must NOT advance
// it, and the callers therefore only call this once the attempt happened. That
// distinction is what lets a later activation still see the slot.
//
// A failed write is logged, not returned, and never undoes the firing: by this
// point the firing has been decided, and the record of it is strictly less
// important than the decision.
func recordFiring(ctx context.Context, state types.TriggerState, in *types.TriggerActivateInput, scheduled time.Time) {
	if err := writeWatermark(ctx, state, scheduled); err != nil {
		slog.Warn("cron trigger could not record its fire watermark",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"scheduled", cronFireID(scheduled),
			"err", err)
	}
}
