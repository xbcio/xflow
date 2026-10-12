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
	scheduled := t.Truncate(time.Minute)
	return &types.TriggerEvent{
		ID:     fmt.Sprintf("%s/%s/%s", workflowID, nodeName, scheduled.Format(time.RFC3339)),
		Kind:   "cron",
		Source: nodeName,
		Time:   t,
		Data:   map[string]any{"scheduled_time": scheduled.Format(time.RFC3339)},
	}
}

func init() { registry.RegisterTrigger(&Node{}) }
