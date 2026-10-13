// Package seedadmit is the entry-seed admission path shared by the trigger
// kinds that are hosted by a remote runner: it builds the admission request a
// time-based or Redis trigger sends for one event, and applies the one retry
// policy those kinds use.
//
// It exists as a package rather than as a copy per trigger because the parts it
// holds must not drift between them. The fire key decides whether two replicas
// firing the same tick produce one execution or two; the retry bound decides
// how long a tick may keep asking; the exit shape decides what a downstream
// node sees. Three private copies of those would be three places a change lands
// in one and is missed in another — the same argument the kafka trigger's
// observer makes for one shared admission classifier.
//
// It is internal so only the trigger packages can import it: the runner is the
// only caller, and a capability this specific does not belong in the public
// node surface.
package seedadmit

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/types"
)

// Kind labels, one per trigger kind that admits through entry seed. They are
// segments of the fire key, so they are wire vocabulary: two replicas of the
// same kind must spell them identically, and a label that changed would let one
// tick be admitted twice.
const (
	KindTimer = "timer"
	KindCron  = "cron"
	KindRedis = "redis"
)

// Enabled reports whether this activation must route its events through the
// entry-seed admission instead of Emit/Dedup.
//
// Both conditions are required, and the runtime capability is checked first on
// purpose: entry_seed=true against a runtime that cannot seed (the embedded
// engine's TriggerRuntime) must NOT take the seed path, because there is no
// control plane to admit to and the trigger would silently stop firing. The
// params are the runner's declaration; the runtime is the fact.
func Enabled(in *types.TriggerActivateInput) bool {
	if in == nil {
		return false
	}
	if _, ok := in.Runtime.(types.EntrySeedRuntime); !ok {
		return false
	}
	if cast.ToBool(in.Params["entry_seed"]) {
		return true
	}
	if id, _ := in.Params["entry_unit_id"].(string); id != "" {
		return true
	}
	return false
}

// EntryUnitID resolves the entry unit this activation admits against. The
// runner stamps entry_unit_id for both single-node and group entry units; the
// node name is the fallback for a single-node unit, whose entry-unit ID is its
// own node name (spec §11.5).
func EntryUnitID(in *types.TriggerActivateInput) string {
	if in == nil {
		return ""
	}
	if id, _ := in.Params["entry_unit_id"].(string); id != "" {
		return id
	}
	return in.NodeName
}

// WorkflowVersion returns the version this activation was delivered for, or ""
// when the runner did not declare one. The control plane rejects a remote seed
// with an empty version, so an empty value here is a mis-wired activation
// rather than a version-less workflow.
func WorkflowVersion(in *types.TriggerActivateInput) string {
	if in == nil {
		return ""
	}
	return cast.ToString(in.Params["workflow_version"])
}

// FireKey builds the admission key for one fired event:
//
//	trigger:<workflow>:<entry_unit>:<kind>:<id>
//
// The server deduplicates on it, so every replica that observes the same event
// must derive the same key from it and no replica may derive a key another
// event would also produce. That is why id is the SCHEDULED time for a
// schedule-driven kind and the source's own identity for a message-driven one:
// it is the identity of the event, not of the firing.
func FireKey(workflowID types.WorkflowID, entryUnitID, kind, id string) string {
	return "trigger:" + string(workflowID) + ":" + entryUnitID + ":" + kind + ":" + id
}

// Result classifies what happened to one fire's admission.
type Result int

const (
	// Admitted: the control plane accepted the fire — accepted, duplicate,
	// queued, or conflict. All four mean the event is accounted for and the
	// trigger is done with it. Conflict is included deliberately: another
	// replica already admitted this same fire, which is the outcome the fire
	// key exists to produce.
	Admitted Result = iota
	// Refused: the workflow's own concurrency policy declined the admission and
	// nothing was persisted. A schedule-driven trigger drops the tick (the next
	// one is a fresh attempt, and replaying this one would turn a throttled
	// workflow into a backlog of stale ticks); a message-driven one withholds
	// its acknowledgement instead, which is the caller's decision, not this
	// package's.
	Refused
	// Dropped: the admission failed for a reason that is not a decision —
	// a transport fault past the retry bound, or a response that set no state
	// this build understands. The fire is lost, and the caller's log line is
	// the only record of it.
	Dropped
)

// errNoState is returned with Dropped when the control plane answered with none
// of accepted/duplicate/conflict/refused. It is not retried: the two sides
// disagree about the protocol, and asking again cannot repair that.
var errNoState = errors.New("entry seed: response set none of accepted/duplicate/conflict/refused")

// Retry bounds one fire's transport-failure retries. It is a value rather than
// a constant so a test can shrink the window; the triggers that admit on a
// schedule pass DefaultRetry.
type Retry struct {
	// MaxRetries is how many ADMISSION ATTEMPTS may follow the first one. Zero
	// means a single attempt.
	MaxRetries int
	// Delay is the pause before each retry.
	Delay time.Duration
}

// DefaultRetry is the retry policy every schedule- and message-driven trigger
// uses: three retries, then the fire is dropped.
//
// The bound is what keeps a control plane that is down from turning into a
// goroutine that never returns: a tick's admission may not outlive the tick's
// own window, and a retrying loop without a bound is indistinguishable from a
// hung trigger. Three is chosen against the transport, not against the
// workflow: a refused connection, a reset, or a proxied 502 usually resolves
// within one retry, and a control plane that is genuinely unreachable is not
// reached by the fourth attempt either.
var DefaultRetry = Retry{MaxRetries: 3, Delay: 200 * time.Millisecond}

// Admit submits one fire through the entry-seed path and applies the retry
// policy. Every attempt carries the same request, admission key included — the
// key is derived from the event's identity, so a retry is a retry of that event
// and not a new admission, which is what lets the control plane answer the
// second attempt with Duplicate.
//
// The returned error is non-nil only with Dropped, and is for the caller's log
// line. Refused carries no error: it is a decision, not a fault, and the two
// must not be counted together.
func Admit(ctx context.Context, rt types.EntrySeedRuntime, req types.EntrySeedRequest, retry Retry) (Result, error) {
	var lastErr error
	for attempt := 0; attempt <= retry.MaxRetries; attempt++ {
		if attempt > 0 && !waitRetry(ctx, retry.Delay) {
			// The window closed (the caller's context is the tick's deadline).
			// Reported as lastErr rather than as a deadline error of its own:
			// the admission that failed is the one an operator needs named.
			break
		}
		resp, err := rt.SeedExecutionFromEntry(ctx, req)
		switch {
		case err != nil:
			// A transport fault — including the generation-fence rejection the
			// runtime reports as an error. Retried within the bound: a fence
			// rejection cannot succeed on retry, but it is indistinguishable
			// from a transport fault at this layer, and the cost of retrying it
			// three times is bounded while the cost of not retrying a genuine
			// fault is a dropped tick.
			lastErr = err
		case resp.Accepted || resp.Duplicate || resp.Conflict:
			return Admitted, nil
		case resp.Refused:
			return Refused, nil
		default:
			return Dropped, errNoState
		}
	}
	if lastErr == nil {
		// The retry budget was exhausted without a single attempt being made,
		// which only happens when the context was already done.
		lastErr = ctx.Err()
	}
	return Dropped, lastErr
}

// waitRetry pauses before a retry and reports whether the pause completed. A
// canceled or expired context ends the retry loop instead of waiting out the
// delay.
func waitRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Exits builds the single boundary exit a trigger's fire carries into the
// seeded execution.
//
// Port is "main", the output port the trigger nodes declare: the server
// resolves the entry unit's out-edges by (node index, port), so an exit naming
// a port the node does not have would be admitted and fan out to nothing.
//
// Data is the event under the same "trigger" key the embedded engine passes to
// engine.Invoke, so a downstream node reads $input.trigger identically whether
// its workflow runs in process or on a runner. The event is carried as the
// decoded-map JSON shape (types.TriggerEventData), which is what the wire
// produces in both modes: the local path hands the engine a *types.TriggerEvent
// in process and a map[string]any over the wire, and a definition may rely on
// neither.
func Exits(in *types.TriggerActivateInput, event *types.TriggerEvent) []types.BoundaryExit {
	return []types.BoundaryExit{{
		NodeName: in.NodeName,
		Port:     "main",
		Data:     map[string]any{"trigger": types.TriggerEventData(event)},
	}}
}

// NewRequest assembles the admission request for one fire. kind is the fire-key
// segment for this trigger kind; id is the event's own identity (see FireKey).
func NewRequest(in *types.TriggerActivateInput, kind, id string, event *types.TriggerEvent) types.EntrySeedRequest {
	entryUnitID := EntryUnitID(in)
	return types.EntrySeedRequest{
		AdmissionKey:    FireKey(in.WorkflowID, entryUnitID, kind, id),
		WorkflowID:      in.WorkflowID,
		WorkflowVersion: WorkflowVersion(in),
		EntryUnitID:     entryUnitID,
		Outcome:         "success",
		Exits:           Exits(in, event),
	}
}
