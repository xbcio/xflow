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
	if seedadmit.Enabled(in) {
		return n.activateEntrySeed(ctx, in, expr, loc)
	}
	runCtx, cancel := context.WithCancel(ctx)
	c := cronlib.New(cronlib.WithLocation(loc))
	if _, err := c.AddFunc(expr, func() {
		event := newCronTriggerEvent(in.WorkflowID, in.NodeName, time.Now().In(loc))
		if ok, err := in.Runtime.Dedup(runCtx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, 2*time.Minute); err == nil && ok {
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
				// The log is not decoration. Nothing durable records the dropped
				// tick — no execution, no offset, no state — so without this line
				// a schedule that is being refused is indistinguishable from a
				// schedule that never fired.
				slog.Warn("cron trigger firing was not admitted; the tick is dropped",
					"workflow_id", in.WorkflowID,
					"node", in.NodeName,
					"event", event.ID,
					"err", err)
			}
		}
	}); err != nil {
		cancel()
		return nil, err
	}
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
// scheduled minute, and the server's admission-key deduplication is what turns
// that into one execution per firing — so there is no leader election and no
// runner-side lock (spec 2026-10-12 §4).
//
// The schedule is parsed explicitly rather than left to cronlib's AddFunc
// because the admit window is the interval to the NEXT firing: a retry that
// outlived it would let two firings contend for one retry budget.
func (n *Node) activateEntrySeed(ctx context.Context, in *types.TriggerActivateInput, expression string, loc *time.Location) (types.TriggerSubscription, error) {
	rt, ok := in.Runtime.(types.EntrySeedRuntime)
	if !ok {
		// seedadmit.Enabled proved the capability a moment ago; fail closed
		// rather than fall back onto Emit, which this runtime cannot serve.
		return nil, fmt.Errorf("cron trigger: entry-seed activation requires an entry-seed runtime")
	}
	sched, err := cronlib.ParseStandard(expression)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	c := cronlib.New(cronlib.WithLocation(loc))
	c.Schedule(sched, cronlib.FuncJob(func() {
		n.admitScheduledFire(runCtx, in, rt, sched, time.Now().In(loc))
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

// admitScheduledFire admits one firing at firedAt, the instant the callback
// ran.
//
// It is a method rather than an inline closure body so a test can fire it at a
// chosen instant: a five-field cron fires at minute granularity, so there is no
// way to reach the next firing inside a test without waiting a minute, and no
// way to assert what the fire key is for a given instant without one.
func (n *Node) admitScheduledFire(ctx context.Context, in *types.TriggerActivateInput, rt types.EntrySeedRuntime, sched cronlib.Schedule, firedAt time.Time) {
	event := newCronTriggerEvent(in.WorkflowID, in.NodeName, firedAt)
	req := seedadmit.NewRequest(in, seedadmit.KindCron, cronFireID(firedAt), event)
	window := sched.Next(firedAt).Sub(firedAt)
	if window <= 0 {
		// A schedule with no next firing gives no upper bound; the fire's own
		// minute is the smallest cadence a five-field cron can express, so it is
		// the tightest bound available rather than an arbitrary one.
		window = time.Minute
	}
	admitCtx, admitCancel := context.WithTimeout(ctx, window)
	defer admitCancel()
	result, err := seedadmit.Admit(admitCtx, rt, req, admitRetry)
	switch result {
	case seedadmit.Refused:
		// A refusal is a DECISION, not a fault: the workflow is at its own
		// concurrency limit and nothing was persisted. The firing is dropped —
		// the next one is a fresh attempt — and this line is the only record,
		// because nothing durable saw the attempt.
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
}

// newCronTriggerEvent builds the event for one firing. It is a function rather
// than an inline closure body for the same reason timer's newTimerTriggerEvent
// is: the minute truncation below decides the event ID, and the event ID is the
// deduplication key. Inside a closure driven by the real clock there is no way
// to assert that truncation without racing a minute boundary.
//
// The truncation is what makes two replicas firing the same schedule agree on
// one identity. Widening it to an hour would fold every firing in an hour into
// one ID and the deduper would drop all but the first; removing it would give
// each replica a distinct sub-second ID and every firing would run twice.
func newCronTriggerEvent(workflowID types.WorkflowID, nodeName string, t time.Time) *types.TriggerEvent {
	scheduled := cronScheduledMinute(t)
	return &types.TriggerEvent{
		ID:     fmt.Sprintf("%s/%s/%s", workflowID, nodeName, scheduled.Format(time.RFC3339)),
		Kind:   "cron",
		Source: nodeName,
		Time:   t,
		Data:   map[string]any{"scheduled_time": scheduled.Format(time.RFC3339)},
	}
}

// cronScheduledMinute is the minute a firing belongs to. Minute truncation is
// timezone-independent (every IANA zone differs from UTC by a whole number of
// minutes), so two replicas evaluating the same schedule in the same zone — or
// in different zones that happen to be configured — derive the same instant.
func cronScheduledMinute(t time.Time) time.Time {
	return t.Truncate(time.Minute)
}

// cronFireID is the entry-seed fire-key id of one firing: the scheduled minute
// in UTC RFC3339. It is derived from the same truncation the event ID uses, so
// the identity the embedded engine's deduper sees and the identity the control
// plane's admission sees cannot drift apart.
func cronFireID(firedAt time.Time) string {
	return cronScheduledMinute(firedAt).UTC().Format(time.RFC3339)
}

func init() { registry.RegisterTrigger(&Node{}) }
