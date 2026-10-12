package cron

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	cronlib "github.com/robfig/cron/v3"
	"github.com/spf13/cast"
	"github.com/xbcio/xflow/node/trigger/internal/seedadmit"
	"github.com/xbcio/xflow/types"
)

// Node is the xflow.trigger.cron trigger: it emits one event per cron
// expression firing, in a configurable timezone.
type Node struct {
	nodeinternal.BaseTrigger
	Expression string
	Timezone   string
}

func New() *Node {
	return &Node{Expression: "* * * * *"}
}

func (n *Node) Cron(expression string) *Node {
	n.Expression = expression
	return n
}

func (n *Node) InTimezone(timezone string) *Node {
	n.Timezone = timezone
	return n
}

func (n *Node) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.cron",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Cron Trigger",
		Params: []types.ParamSpec{
			// Parsed by robfig/cron's standard parser: five fields plus the
			// @every / @hourly descriptors.
			{Name: "expression", DisplayName: "Expression", Type: types.ParamString, Required: true, Description: "Cron schedule, five fields (e.g. \"*/5 * * * *\") or a descriptor such as \"@every 1m\"",
				Widget: nodeinternal.WidgetCron, Constraints: nodeinternal.Format(nodeinternal.FormatCron)},
			{Name: "timezone", DisplayName: "Timezone", Type: types.ParamString, Required: false, Default: "", Description: "IANA time zone name the schedule is evaluated in; empty inherits the workflow settings.timezone (UTC if none)"},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *Node) NodeType() string { return "xflow.trigger.cron" }
func (n *Node) RawParams() any {
	params := map[string]any{"expression": n.Expression}
	if n.Timezone != "" {
		params["timezone"] = n.Timezone
	}
	return params
}
func (n *Node) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *Node) TriggerHandler() types.TriggerHandler { return n }

func (n *Node) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	expr := cast.ToString(in.Params["expression"])
	if expr == "" {
		return nil, fmt.Errorf("cron expression is required")
	}
	// Precedence: the node's own timezone, then the workflow's
	// settings.timezone, then UTC.
	tz := cast.ToString(in.Params["timezone"])
	if tz == "" {
		tz = in.WorkflowTimezone
	}
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	// The schedule is parsed here rather than left to AddFunc because the firing's
	// identity comes from it: the cursor needs the schedule to answer which
	// instant a firing belongs to, and both paths must answer that the same way.
	sched, err := cronlib.ParseStandard(expr)
	if err != nil {
		return nil, err
	}
	if seedadmit.Enabled(in) {
		return n.activateEntrySeed(ctx, in, sched, loc)
	}
	runCtx, cancel := context.WithCancel(ctx)
	cursor := newScheduleCursor(sched, time.Now().In(loc))
	watermark := openActivationWatermark(ctx, in)
	c := cronlib.New(cronlib.WithLocation(loc))
	c.Schedule(sched, cronlib.FuncJob(func() {
		observed := time.Now().In(loc)
		fire := cursor.next(observed)
		event := newCronTriggerEvent(in.WorkflowID, in.NodeName, fire)
		// handled distinguishes "this instant has been decided" from "the store
		// failed before anything was attempted": only the first moves the
		// watermark, so a firing that never happened is still visible to a later
		// activation.
		handled := false
		if ok, err := in.Runtime.Dedup(runCtx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, 2*time.Minute); err == nil {
			handled = true
			if ok {
				if _, err := in.Emit(runCtx, event); err != nil {
					// A firing that was refused is DROPPED, and that is the whole
					// handling: a schedule is a stream of moments, so the next firing
					// is a fresh attempt and replaying this one would turn a busy
					// workflow into a backlog of stale ticks. The refusal an operator
					// will see here is the workflow's own concurrency limit
					// (engine.ErrConcurrencyLimit); a QUEUE-mode workflow does not
					// produce it, because its over-limit firings are parked and
					// admitted later instead of refused.
					//
					// The log is not decoration. The watermark records which instant
					// this was, but not that it was refused — so without this line a
					// schedule that is being refused is indistinguishable from a
					// schedule that never fired.
					slog.Warn("cron trigger firing was not admitted; the tick is dropped",
						"workflow_id", in.WorkflowID,
						"node", in.NodeName,
						"event", event.ID,
						"err", err)
				}
			}
		}
		if handled {
			recordFiring(runCtx, watermark, in, fire.scheduled)
		}
	}))
	c.Start()
	return types.CloseFunc(func(context.Context) error {
		cancel()
		stopCtx := c.Stop()
		select {
		case <-stopCtx.Done():
		case <-ctx.Done():
		}
		return nil
	}), nil
}

// admitRetry is the retry policy one firing's admission uses. It is a variable
// so a test can shrink the window: the production cadence is 200ms per retry,
// which no test should have to wait out.
var admitRetry = seedadmit.DefaultRetry

// activateEntrySeed hosts the cron on a runner that admits through the control
// plane's entry-seed endpoint instead of emitting into an embedded engine.
// Every replica runs the same schedule and submits the same fire key for a
// scheduled instant, and the server's admission-key deduplication is what turns
// that into one execution per firing — so there is no leader election and no
// runner-side lock (spec 2026-10-12 §4).
func (n *Node) activateEntrySeed(ctx context.Context, in *types.TriggerActivateInput, sched cronlib.Schedule, loc *time.Location) (types.TriggerSubscription, error) {
	rt, ok := in.Runtime.(types.EntrySeedRuntime)
	if !ok {
		// seedadmit.Enabled proved the capability a moment ago; fail closed
		// rather than fall back onto Emit, which this runtime cannot serve.
		return nil, fmt.Errorf("cron trigger: entry-seed activation requires an entry-seed runtime")
	}
	runCtx, cancel := context.WithCancel(ctx)
	cursor := newScheduleCursor(sched, time.Now().In(loc))
	watermark := openActivationWatermark(ctx, in)
	c := cronlib.New(cronlib.WithLocation(loc))
	c.Schedule(sched, cronlib.FuncJob(func() {
		observed := time.Now().In(loc)
		n.admitScheduledFire(runCtx, in, rt, sched, cursor.next(observed), watermark)
	}))
	c.Start()
	return types.CloseFunc(func(context.Context) error {
		cancel()
		stopCtx := c.Stop()
		select {
		case <-stopCtx.Done():
		case <-ctx.Done():
		}
		return nil
	}), nil
}

// admitScheduledFire admits one firing.
//
// It is a method rather than an inline closure body so a test can fire it at a
// chosen instant: a five-field cron fires at minute granularity, so there is no
// way to reach the next firing inside a test without waiting a minute, and no
// way to assert what the fire key is for a given instant without one.
func (n *Node) admitScheduledFire(ctx context.Context, in *types.TriggerActivateInput, rt types.EntrySeedRuntime, sched cronlib.Schedule, fire cronFire, watermark types.TriggerState) {
	event := newCronTriggerEvent(in.WorkflowID, in.NodeName, fire)
	req := seedadmit.NewRequest(in, seedadmit.KindCron, cronFireID(fire.scheduled), event)
	admitCtx, admitCancel := context.WithTimeout(ctx, firingWindow(sched, fire.scheduled))
	defer admitCancel()
	result, err := seedadmit.Admit(admitCtx, rt, req, admitRetry)
	switch result {
	case seedadmit.Refused:
		// A refusal is a DECISION, not a fault: the workflow is at its own
		// concurrency limit and nothing was persisted. The firing is dropped —
		// the next one is a fresh attempt — and this line is the only record of
		// why, because the watermark below says only that the instant passed.
		slog.Warn("cron trigger firing was refused; the tick is dropped",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"fire_key", req.AdmissionKey)
	case seedadmit.Dropped:
		slog.Warn("cron trigger firing was not admitted; the tick is dropped",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"fire_key", req.AdmissionKey,
			"err", err)
	}
	// The watermark moves for all three outcomes by design: an admitted firing is
	// behind us, and so is one the workflow refused or the transport lost — the
	// tick is dropped rather than retried, and the watermark's job is to say which
	// instants are behind the trigger, not which of them produced an execution.
	recordFiring(ctx, watermark, in, fire.scheduled)
}

// newCronTriggerEvent builds the event for one firing. It is a function rather
// than an inline closure body for the same reason timer's newTimerTriggerEvent
// is: the scheduled instant decides the event ID, and the event ID is the
// deduplication key; inside a closure driven by the real clock there is no way to
// assert which instant a firing was attributed to without racing the boundary.
//
// The scheduled instant — not the observation — is the identity, because that is
// the only thing two replicas of one schedule necessarily agree on: their
// callbacks run at different moments, and one of them may run late enough to
// cross into the next minute. Publishing the scheduled instant as `scheduled_time`
// and the observation as Time keeps both facts available instead of trading one
// for the other.
func newCronTriggerEvent(workflowID types.WorkflowID, nodeName string, fire cronFire) *types.TriggerEvent {
	scheduled := fire.scheduled.Format(time.RFC3339Nano)
	return &types.TriggerEvent{
		ID:     fmt.Sprintf("%s/%s/%s", workflowID, nodeName, scheduled),
		Kind:   "cron",
		Source: nodeName,
		Time:   fire.observed,
		Data:   map[string]any{"scheduled_time": scheduled},
	}
}

// cronFireID is the entry-seed fire-key id of one firing: the scheduled instant
// in UTC RFC3339Nano.
//
// It is derived from the same instant the event ID is, so the identity the
// embedded engine's deduper sees and the identity the control plane's admission
// sees cannot drift apart. Nanoseconds are carried because a schedule can name a
// sub-second cadence (@every 1500ms); for the whole-second cadences every cron
// expression not spelled in milliseconds produces, the formatting is identical to
// RFC3339.
func cronFireID(scheduled time.Time) string {
	return scheduled.UTC().Format(time.RFC3339Nano)
}

func init() { registry.RegisterTrigger(&Node{}) }
