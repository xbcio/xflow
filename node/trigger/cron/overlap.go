package cron

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/types"
)

// overlapPolicy is what a cron trigger does when a scheduled instant arrives
// while the run IT started last has not finished.
//
//   - allow starts the run anyway. Two runs of one schedule are in flight at
//     once, which is what a schedule whose runs are short relative to its cadence
//     wants, and what every cron trigger did before this parameter existed.
//   - skip drops the firing instead. A schedule that must not run twice at once —
//     a slow reconciliation, a report that walks a table — says so here rather
//     than by stretching its cadence past its worst-case runtime, which would make
//     the cadence a claim about running time rather than about the moments the
//     work is due.
//
// "Queue the firing behind the run in flight" is deliberately NOT a value of this
// parameter. Serialising runs is not a decision a trigger makes: a workflow that
// wants it says so once, in settings.concurrency (mode queue, limit 1), and every
// entry into that workflow obeys it — a trigger parameter could only say it about
// the firings of one node, and would then have to be enforced against a setting it
// does not own. parseOverlapPolicy names that alternative in its error for the
// definition that asks for it by name.
type overlapPolicy string

const (
	overlapAllow overlapPolicy = "allow"
	overlapSkip  overlapPolicy = "skip"
)

// The value an absent param resolves to. Declared in
// node/internal.BuiltinFallbacks rather than as ParamSpec.Default for the same
// reason the catch-up params are: a Default is written into a workflow's params
// by the SDK, and adding one moves the definition hash of every workflow using
// this node.
const defaultOverlap = overlapAllow

// lastExecutionKey is the TriggerState key, inside the cron scope, that records
// the execution this trigger started last.
//
// It lives in the watermark's scope (see watermarkScope) under its own key: the
// watermark answers "which instants are behind me" and this answers "what did I
// start last", and one value cannot answer both. It is written without a TTL —
// the record is only ever compared against the store's own status for that
// execution, so a record left behind by a process that died costs a status read
// that answers "gone", never a wrong decision.
const lastExecutionKey = "last_execution_id"

// readLastExecution returns the execution this trigger started last, or
// ok=false when there is no record, it is unreadable, or there is no state to
// read it from. Every failure is the same answer on purpose: the policy treats
// "nothing recorded" as "nothing of mine is running", which is the only reading
// that cannot stop a schedule on the strength of a state-store fault.
func readLastExecution(ctx context.Context, state types.TriggerState) (types.ExecutionID, bool) {
	if state == nil {
		return "", false
	}
	raw, err := state.Get(ctx, lastExecutionKey)
	if err != nil || len(raw) == 0 {
		return "", false
	}
	return types.ExecutionID(raw), true
}

// recordLastExecution records the execution one admitted firing started. A
// failed write is logged, not returned: the run is already admitted, and the
// record of it is strictly less important than the run. The cost of losing it is
// that the next firing sees no run of ours in flight and therefore runs — the
// same concurrency the policy is trying to bound, missed once rather than
// enforced with a stale id.
func recordLastExecution(ctx context.Context, state types.TriggerState, in *types.TriggerActivateInput, id types.ExecutionID) {
	if state == nil || id == "" {
		return
	}
	if err := state.Set(ctx, lastExecutionKey, []byte(id), 0); err != nil {
		slog.Warn("cron trigger could not record the execution it started",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"execution_id", id,
			"err", err)
	}
}

// parseOverlapPolicy maps the param value to a policy. An absent value is the
// default (allow); an unknown one is an error rather than a silent default, for
// the same reason a malformed catch-up window is: the value was written on
// purpose, and a typo in "overlap" must not quietly become "run them all at
// once" — or, worse, the other way round, with a schedule that stops firing for
// a reason nobody wrote down.
func parseOverlapPolicy(v any) (overlapPolicy, error) {
	policy := overlapPolicy(cast.ToString(v))
	switch policy {
	case "":
		return defaultOverlap, nil
	case overlapAllow, overlapSkip:
		return policy, nil
	case "queue":
		return "", fmt.Errorf("overlap=queue is not a trigger setting: serialising runs is settings.concurrency "+
			"mode=queue with limit=1, which every entry into the workflow obeys; use overlap=%s or overlap=%s here",
			overlapAllow, overlapSkip)
	default:
		return "", fmt.Errorf("overlap must be one of: %s, %s", overlapAllow, overlapSkip)
	}
}

// overlapPlan is one activation's answer to "may this firing start": the policy
// it will enforce, and the reader that enforces it. The reader is non-nil exactly
// when the policy is skip.
type overlapPlan struct {
	policy overlapPolicy
	reader types.ExecutionStatusReader
}

// resolveOverlap reads the overlap param and pairs it with the capability that
// can enforce it. It runs once per activation, so a definition with a mistyped
// policy is refused at activation rather than at the first instant the policy
// would have decided anything.
//
// The capability is optional (types.ExecutionStatusReader) because a runtime with
// no execution store cannot answer the question the policy asks. When it is
// absent the activation says so once and runs as allow: a trigger that cannot see
// the other run must not claim it decided to skip, and dropping firings on a
// question it cannot ask would turn a missing capability into a schedule that
// silently stops.
func resolveOverlap(in *types.TriggerActivateInput) (overlapPlan, error) {
	policy, err := parseOverlapPolicy(in.Params["overlap"])
	if err != nil {
		return overlapPlan{}, err
	}
	if policy != overlapSkip {
		return overlapPlan{policy: policy}, nil
	}
	reader, capable := in.Runtime.(types.ExecutionStatusReader)
	if !capable {
		slog.Warn("cron trigger asked for overlap=skip but this runtime cannot read execution status; "+
			"runs of this schedule may overlap",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName)
		return overlapPlan{policy: overlapAllow}, nil
	}
	return overlapPlan{policy: overlapSkip, reader: reader}, nil
}

// admits reports whether one firing may start the workflow, and is where the
// policy's single question is asked: is the run this trigger started last
// finished?
//
// Every way the answer can be unclear resolves in favour of starting the run:
// nothing recorded (this trigger has started nothing), an execution the store no
// longer knows (found=false, the same "not active" the engine reads from a
// missing record), a terminal status, and any failure to read the status at all.
// The alternative — a firing dropped because a status read failed — is a schedule
// that stops for a reason the workflow's own history cannot show, while the
// parameter's purpose, bounding concurrency, is still served by skipping exactly
// when the previous run is known to still be going.
func (p overlapPlan) admits(ctx context.Context, state types.TriggerState, in *types.TriggerActivateInput) bool {
	if p.policy != overlapSkip || p.reader == nil {
		return true
	}
	last, recorded := readLastExecution(ctx, state)
	if !recorded {
		return true
	}
	status, found, err := p.reader.GetExecutionStatus(ctx, last)
	if err != nil {
		slog.Warn("cron trigger could not read the status of the execution it started last; "+
			"this firing starts anyway",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"execution_id", last,
			"err", err)
		return true
	}
	if !found || types.IsTerminalExecutionStatus(status) {
		return true
	}
	return false
}
