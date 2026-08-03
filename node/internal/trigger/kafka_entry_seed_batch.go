package trigger

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// buildKafkaBatchAdmissionKey builds the admission key for a batch of messages
// from one partition. The range is the ACTUAL first..last offset of the batch,
// not an aligned window: batch boundaries are not reproducible across a reader
// rebuild (they depend on broker fetch timing), so an aligned key would buy
// nothing while costing the timeout-flush path. Delivery is at-least-once by
// design — see the spec §4.1.
//
// Namespace is left empty: the server sets it (see seedKafkaEntryBatch).
func buildKafkaBatchAdmissionKey(workflowID types.WorkflowID, workflowVersion, entryUnitID string, messages []KafkaMessage) string {
	if len(messages) == 0 {
		return ""
	}
	first := messages[0]
	last := messages[len(messages)-1]
	return fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d-%d",
		"", workflowID, workflowVersion, entryUnitID,
		first.Topic, first.Partition, first.Offset, last.Offset)
}

// buildKafkaBatchExits builds the single boundary exit carrying a whole batch.
// The shape mirrors kafkaBatchEvent's Data so a downstream node sees the same
// keys whether the trigger ran on the legacy Emit path or the entry-seed path.
func buildKafkaBatchExits(nodeName string, messages []KafkaMessage) []types.BoundaryExit {
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
			"messages":     kafkaMessageDataList(messages),
		},
	}}
}

// seedKafkaEntryBatchMessages admits a whole batch through the entry-seed path.
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
func seedKafkaEntryBatchMessages(ctx context.Context, in *types.TriggerActivateInput, rt types.EntrySeedRuntime, messages []KafkaMessage) bool {
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
		AdmissionKey:    buildKafkaBatchAdmissionKey(in.WorkflowID, workflowVersion, entryUnitID, messages),
		WorkflowID:      in.WorkflowID,
		WorkflowVersion: workflowVersion,
		EntryUnitID:     entryUnitID,
		Outcome:         "success",
		Exits:           buildKafkaBatchExits(in.NodeName, messages),
	}

	resp, err := rt.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		return false
	}
	return resp.Accepted || resp.Duplicate || resp.Conflict
}
