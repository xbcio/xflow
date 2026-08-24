package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xbcio/xflow/types"
)

// admissionCauseLimit bounds how much of a failure cause reaches the log.
const admissionCauseLimit = 512

// Admission reason labels. These narrow OnBatchAdmission's state from WHAT
// happened to WHY, and they are the whole point of the second label: a
// scenario-A run showed 599 of 634 admissions in state="error", which called
// for four different responses depending on which of these it was, and the
// counter could not tell them apart. The cause text distinguishes them in the
// log, but free text supports no rate and no alert — you cannot page on it,
// graph it, or say whether the mix changed after a deploy.
//
// Every value here is a compile-time constant chosen in this package.
// Cardinality is len(reasons) x len(states) x len(topics), all bounded. A
// runtime's error string or a group's Outcome field must never reach this
// label: see admissionReasonForOutcome.
const (
	// admissionReasonNone is the reason for a state that needs no narrowing —
	// accepted, duplicate, conflict. Prometheus has no absent label within a
	// metric family, so a placeholder is required; an empty string would render
	// as reason="" and read like a bug.
	admissionReasonNone = "none"

	// admissionReasonExecuteGroup: ExecuteGroup itself returned an error, so
	// the group never ran. Package resolution, compilation, engine wiring —
	// infrastructure, not traffic. A rate here means this runner cannot run
	// this workflow at all, and redelivery elsewhere may well succeed.
	admissionReasonExecuteGroup = "execute_group"

	// admissionReasonGroupOutcome: the group RAN and a member node failed or
	// the batch deadline was exceeded. This is the data-and-logic bucket —
	// a guest trap, a schema rejection, a slow downstream. Distinguishing it
	// from execute_group is the difference between "fix the deployment" and
	// "fix the rule".
	admissionReasonGroupOutcome = "group_outcome"

	// admissionReasonSeedTransport: the control-plane admission round trip
	// failed. The group's work is DONE and is being thrown away, and the whole
	// batch will be redelivered and re-executed. Of the three error reasons
	// this is the one that wastes the most downstream capacity per occurrence,
	// which is exactly what the undifferentiated counter could not show.
	admissionReasonSeedTransport = "seed_transport"

	// admissionReasonUnknownState: the control plane answered with none of
	// accepted/duplicate/conflict set. Defensive: the batch is not committed.
	// A nonzero rate here means this runner and the control plane disagree
	// about the protocol, which no other signal reports.
	admissionReasonUnknownState = "unknown_state"

	// admissionReasonUnknownOutcome: the group returned an Outcome string this
	// build does not know. Kept separate from unknown_state — collapsing the
	// two would merge a runner-versus-control-plane protocol gap with a
	// runner-versus-engine one, which is the exact conflation this whole label
	// exists to undo.
	admissionReasonUnknownOutcome = "unknown_outcome"
)

// admissionReasonForOutcome maps a group outcome to its reason label, and is
// the guard that keeps this label bounded. execRes.Outcome is a string that
// arrives from a runtime implementation, so using it directly would let an
// unexpected value open unbounded label cardinality in a process-lifetime
// registry — the one metrics failure that cannot be undone without a restart.
//
// Every known outcome collapses to admissionReasonGroupOutcome: the group ran
// and did not succeed, which is the distinction this label exists to draw.
// Which KIND of non-success it was stays in the log's cause field, where an
// unbounded value belongs. An outcome this build does not know about is
// reported as unknown_outcome rather than passed through.
func admissionReasonForOutcome(outcome string) string {
	switch outcome {
	case "failed", "timeout", "canceled":
		return admissionReasonGroupOutcome
	default:
		return admissionReasonUnknownOutcome
	}
}

// logBatchAdmission emits a throttled log line for a batch admission that did
// not succeed. It exists because OnBatchAdmission carries only a state enum:
// an error string cannot be a metric label (unbounded cardinality), so without
// this the counter says 599 batches failed and nothing says why. A withheld
// commit is correct behaviour, but a correct behaviour with no diagnosis is an
// outage no one can shorten.
//
// Message content is never logged (see logInvalidMessage). The cause is the
// engine's own error text, not the record — but a guest that quotes its input
// in an error message will put that fragment here, so treat this line with the
// same trust as a stack trace and keep it bounded.
func logBatchAdmission(topic string, partition int, first, last int64, count int, state, cause string) {
	emit, occurrences := discardLog.allow(time.Now(), topic+"\x00admission_"+state)
	if !emit {
		return
	}
	if len(cause) > admissionCauseLimit {
		cause = cause[:admissionCauseLimit] + "…(truncated)"
	}
	slog.Warn("kafka batch admission did not succeed",
		"topic", topic,
		"partition", partition,
		"start_offset", first,
		"end_offset", last,
		"count", count,
		"state", state,
		"cause", cause,
		"committed", false,
		"occurrences", occurrences,
	)
}

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
			// false: these exits are marshalled onto the control-plane wire and
			// persisted server-side, where `value` is expected to be a string.
			// The spliced form is only safe on the group path below, which
			// never leaves this process.
			"messages": messageDataList(messages, false),
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
		obs().OnBatchAdmission(ctx, topic, "error", admissionReasonSeedTransport)
		logBatchAdmission(topic, messages[0].Partition, messages[0].Offset,
			messages[len(messages)-1].Offset, len(messages), "error", err.Error())
		return false
	}
	switch {
	case resp.Duplicate:
		obs().OnBatchAdmission(ctx, topic, "duplicate", admissionReasonNone)
	case resp.Conflict:
		obs().OnBatchAdmission(ctx, topic, "conflict", admissionReasonNone)
	case resp.Accepted:
		obs().OnBatchAdmission(ctx, topic, "accepted", admissionReasonNone)
	default:
		// None of the three set: the control plane and this runner disagree
		// about the protocol. The batch is not committed (the return below),
		// and previously that happened without a single observation — the
		// counter recorded nothing at all for this path.
		obs().OnBatchAdmission(ctx, topic, "error", admissionReasonUnknownState)
		logBatchAdmission(topic, messages[0].Partition, messages[0].Offset,
			messages[len(messages)-1].Offset, len(messages), "error",
			"admission response set none of accepted/duplicate/conflict")
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
// The valueJSON argument is honoured only here, and only because of where this
// runs: rt.ExecuteGroup executes the group's members on an in-process
// in-memory backend, so the batch it is handed never crosses a JSON boundary
// between this call and the member node that reads it. Every other producer of
// message data in this package passes false — on those paths the item is
// marshalled into Redis, SQL or the control-plane wire, and a spliced value
// would come back as a parsed object rather than the string those readers
// expect. See messageData.
func seedEntryBatchViaGroupExec(ctx context.Context, in *types.TriggerActivateInput, rt interface {
	types.EntrySeedRuntime
	types.GroupExecRuntime
}, messages []Message, valueJSON bool) bool {
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
		"messages":     messageDataList(messages, valueJSON),
	})
	if err != nil {
		obs().OnBatchAdmission(ctx, topic, "error", admissionReasonExecuteGroup)
		logBatchAdmission(topic, first.Partition, first.Offset, last.Offset,
			len(messages), "error", "ExecuteGroup: "+err.Error())
		return false
	}
	if execRes.Outcome != "success" {
		// The group ran but did not succeed. Retry semantics depend on whether
		// the failure is deterministic (retrying won't help) or transient.
		cause := fmt.Sprintf("group outcome=%s: %s", execRes.Outcome, execRes.Error)
		reason := admissionReasonForOutcome(execRes.Outcome)
		if execRes.Deterministic {
			// Retrying cannot repair a deterministic failure, but committing it would
			// silently discard the entire batch. Keep the partition frontier here;
			// recovery requires fixing the workflow or an explicit operator policy.
			obs().OnBatchAdmission(ctx, topic, "deterministic_error", reason)
			logBatchAdmission(topic, first.Partition, first.Offset, last.Offset,
				len(messages), "deterministic_error", cause)
			return false
		}
		// Transient failure (timeout, member I/O error, etc.): do NOT admit —
		// this batch must be redelivered.
		obs().OnBatchAdmission(ctx, topic, "error", reason)
		logBatchAdmission(topic, first.Partition, first.Offset, last.Offset,
			len(messages), "error", cause)
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
		obs().OnBatchAdmission(ctx, topic, "error", admissionReasonSeedTransport)
		logBatchAdmission(topic, first.Partition, first.Offset, last.Offset,
			len(messages), "error", "SeedExecutionFromEntry: "+err.Error())
		return false
	}
	switch {
	case resp.Duplicate:
		obs().OnBatchAdmission(ctx, topic, "duplicate", admissionReasonNone)
	case resp.Conflict:
		obs().OnBatchAdmission(ctx, topic, "conflict", admissionReasonNone)
	case resp.Accepted:
		obs().OnBatchAdmission(ctx, topic, "accepted", admissionReasonNone)
	default:
		// See the identical branch in seedEntryBatchMessages. On this path the
		// waste is larger: the group already RAN, so a protocol disagreement
		// here throws away completed work and redelivers the whole batch.
		obs().OnBatchAdmission(ctx, topic, "error", admissionReasonUnknownState)
		logBatchAdmission(topic, first.Partition, first.Offset, last.Offset,
			len(messages), "error",
			"admission response set none of accepted/duplicate/conflict")
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
