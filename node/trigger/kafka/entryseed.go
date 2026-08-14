package kafka

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// buildBatchAdmissionKey builds the admission key for a batch of messages
// from one partition. The range is the ACTUAL first..last offset of the batch,
// not an aligned window: batch boundaries are not reproducible across a reader
// rebuild (they depend on broker fetch timing), so an aligned key would buy
// nothing while costing the timeout-flush path. Delivery is at-least-once by
// design — see the spec §4.1.
//
// Namespace is left empty: the server sets it (see seedEntryBatch).
func buildBatchAdmissionKey(workflowID types.WorkflowID, workflowVersion, entryUnitID string, messages []Message) string {
	if len(messages) == 0 {
		return ""
	}
	first := messages[0]
	last := messages[len(messages)-1]
	return fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d-%d",
		"", workflowID, workflowVersion, entryUnitID,
		first.Topic, first.Partition, first.Offset, last.Offset)
}

// buildBatchExits builds the single boundary exit carrying a whole batch.
// The shape mirrors batchEvent's Data so a downstream node sees the same
// keys whether the trigger ran on the legacy Emit path or the entry-seed path.
func buildBatchExits(nodeName string, messages []Message) []types.BoundaryExit {
	if len(messages) == 0 {
		return nil
	}
	first := messages[0]
	last := messages[len(messages)-1]
	return []types.BoundaryExit{{
		NodeName: nodeName,
		Port:     "main",
		Data: map[string]any{
			"topic":        first.Topic,
			"partition":    first.Partition,
			"start_offset": first.Offset,
			"end_offset":   last.Offset,
			"count":        len(messages),
			"messages":     messageDataList(messages),
		},
	}}
}

// seedEntryBatchMessages admits a whole batch through the entry-seed path.
// It returns true when the admission was HANDLED and the caller may commit the
// batch's offsets.
//
// The false cases are what keep messages from vanishing:
//   - transport error → the control plane may not have the result; redeliver.
//   - generation fence rejection (surfaced as an error by entrySeedRuntime, NOT
//     as Conflict) → the current-generation owner has not seen these messages.
//     Committing here would mean Kafka never redelivers them and the new owner
//     never processes them: silent message loss.
//
// Conflict, by contrast, DOES commit: another runner already admitted a result
// for this key, so the messages are accounted for.
func seedEntryBatchMessages(ctx context.Context, in *types.TriggerActivateInput, rt types.EntrySeedRuntime, messages []Message) bool {
	if len(messages) == 0 {
		return true
	}

	entryUnitID, _ := in.Params["entry_unit_id"].(string)
	if entryUnitID == "" {
		// Single-node entry unit ID = node name (spec §11.5).
		entryUnitID = in.NodeName
	}
	workflowVersion, _ := in.Params["workflow_version"].(string)

	req := types.EntrySeedRequest{
		AdmissionKey:    buildBatchAdmissionKey(in.WorkflowID, workflowVersion, entryUnitID, messages),
		WorkflowID:      in.WorkflowID,
		WorkflowVersion: workflowVersion,
		EntryUnitID:     entryUnitID,
		Outcome:         "success",
		Exits:           buildBatchExits(in.NodeName, messages),
	}

	resp, err := rt.SeedExecutionFromEntry(ctx, req)
	topic := messages[0].Topic
	if err != nil {
		obs().OnBatchAdmission(ctx, topic, "error")
		return false
	}
	switch {
	case resp.Duplicate:
		obs().OnBatchAdmission(ctx, topic, "duplicate")
	case resp.Conflict:
		obs().OnBatchAdmission(ctx, topic, "conflict")
	case resp.Accepted:
		obs().OnBatchAdmission(ctx, topic, "accepted")
	}
	return resp.Accepted || resp.Duplicate || resp.Conflict
}

// seedEntryBatchViaGroupExec is the trigger-group counterpart of
// seedEntryBatchMessages: instead of synthesizing exits from the raw
// batch (buildBatchExits), it runs the group's real member nodes
// locally via rt.ExecuteGroup and admits the exits THAT execution actually
// produced (spec 2026-08-07 §3.3-§3.4). rt must implement both
// types.EntrySeedRuntime (for the admission round trip) and
// types.GroupExecRuntime (for the local execution) — the caller
// (partitionAggregator.flush) type-asserts for both together before
// calling this function.
//
// Return value and offset-commit semantics mirror seedEntryBatchMessages
// exactly for the transport/fence-rejection cases, PLUS one new case: a group
// outcome other than "success" (a member node failed, or the batch's internal
// deadline was exceeded) also withholds the commit — Kafka must redeliver the
// batch rather than have it silently disappear because a member failed.
func seedEntryBatchViaGroupExec(ctx context.Context, in *types.TriggerActivateInput, rt interface {
	types.EntrySeedRuntime
	types.GroupExecRuntime
}, messages []Message) bool {
	if len(messages) == 0 {
		return true
	}
	first := messages[0]
	last := messages[len(messages)-1]
	topic := first.Topic

	execRes, err := rt.ExecuteGroup(ctx, map[string]any{
		"topic":        first.Topic,
		"partition":    first.Partition,
		"start_offset": first.Offset,
		"end_offset":   last.Offset,
		"count":        len(messages),
		"messages":     messageDataList(messages),
	})
	if err != nil {
		obs().OnBatchAdmission(ctx, topic, "error")
		return false
	}
	if execRes.Outcome != "success" {
		// The group ran but did not succeed. Retry semantics depend on whether
		// the failure is deterministic (retrying won't help) or transient.
		if execRes.Deterministic {
			// Permanent failure (compile error, schema validation, etc.):
			// commit offset to skip this batch — redelivery would fail identically.
			obs().OnBatchAdmission(ctx, topic, "deterministic_skip")
			return true
		}
		// Transient failure (timeout, member I/O error, etc.): do NOT admit —
		// this batch must be redelivered.
		obs().OnBatchAdmission(ctx, topic, "error")
		return false
	}

	entryUnitID, _ := in.Params["entry_unit_id"].(string)
	if entryUnitID == "" {
		entryUnitID = in.NodeName
	}
	workflowVersion, _ := in.Params["workflow_version"].(string)

	req := types.EntrySeedRequest{
		AdmissionKey:    buildBatchAdmissionKey(in.WorkflowID, workflowVersion, entryUnitID, messages),
		WorkflowID:      in.WorkflowID,
		WorkflowVersion: workflowVersion,
		EntryUnitID:     entryUnitID,
		Outcome:         "success",
		Exits:           execRes.Exits,
	}

	resp, err := rt.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		obs().OnBatchAdmission(ctx, topic, "error")
		return false
	}
	switch {
	case resp.Duplicate:
		obs().OnBatchAdmission(ctx, topic, "duplicate")
	case resp.Conflict:
		obs().OnBatchAdmission(ctx, topic, "conflict")
	case resp.Accepted:
		obs().OnBatchAdmission(ctx, topic, "accepted")
	}
	return resp.Accepted || resp.Duplicate || resp.Conflict
}

// ---------------------------------------------------------------------------
// Entry-seed mode: admission-based emit (Milestone G)
// ---------------------------------------------------------------------------

// seedEntryBatch processes one message through the entry-unit (single node
// or group node) seed admission path. Instead of Emit+Dedup, it calls
// SeedExecutionFromEntry on the runtime. Only accepted/duplicate-accepted/conflict
// responses commit the Kafka offset. Transient errors return false (no commit →
// Kafka redelivery).
//
// This function is the entry-seed analogue of emitMessage for the
// per-partition serial worker. It is NOT used by the legacy Emit path.
func seedEntryBatch(ctx context.Context, in *types.TriggerActivateInput, consumer Consumer, msg Message) bool {
	rt, ok := in.Runtime.(types.EntrySeedRuntime)
	if !ok {
		// Fallback: runtime does not support entry-seed. This should not happen
		// in a properly configured entry-seed activation.
		return false
	}

	entryUnitID, _ := in.Params["entry_unit_id"].(string)
	if entryUnitID == "" {
		// Single-node entry unit ID = node name (spec §11.5).
		entryUnitID = in.NodeName
	}
	workflowVersion, _ := in.Params["workflow_version"].(string)

	// Build the admission key from the message's stable source identity.
	admissionKey := fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d-%d",
		"", // namespace is set server-side
		in.WorkflowID, workflowVersion, entryUnitID,
		msg.Topic, msg.Partition, msg.Offset, msg.Offset)

	// Build exits — for a single-message entry unit, the output is the message data.
	exits := []types.BoundaryExit{{
		NodeName: in.NodeName,
		Port:     "main",
		Data: map[string]any{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"key":       string(msg.Key),
			"value":     string(msg.Value),
		},
	}}

	req := types.EntrySeedRequest{
		AdmissionKey:    admissionKey,
		WorkflowID:      in.WorkflowID,
		WorkflowVersion: workflowVersion,
		EntryUnitID:     entryUnitID,
		Outcome:         "success",
		Exits:           exits,
	}

	resp, err := rt.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		// Transient error (network timeout, etc.) — do NOT commit offset.
		// Kafka will redeliver the message.
		return false
	}

	// Accepted, duplicate-accepted, or conflict: the admission was handled.
	// Commit the Kafka offset regardless — for conflict, another runner already
	// admitted a result for this key, so the message is consumed.
	if resp.Accepted || resp.Duplicate || resp.Conflict {
		if commitErr := commitMessages(ctx, consumer, msg); commitErr != nil {
			// Commit failed — the message will be redelivered. On redelivery,
			// SeedExecutionFromEntry returns duplicate-accepted, which is safe.
			return false
		}
		return true
	}

	// Unknown state — defensive: don't commit.
	return false
}
