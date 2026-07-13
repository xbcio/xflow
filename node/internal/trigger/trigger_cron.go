package trigger

import (
	"context"
	"fmt"
	. "github.com/xbcio/xflow/node/internal"
	"time"

	"github.com/xbcio/xflow/types"
	cronlib "github.com/robfig/cron/v3"
	"github.com/spf13/cast"
)

type CronTriggerNode struct {
	BaseNode
	Expression string
	Timezone   string
}

func CronTrigger() *CronTriggerNode {
	return &CronTriggerNode{Expression: "* * * * *", Timezone: "UTC"}
}

func (n *CronTriggerNode) Cron(expression string) *CronTriggerNode {
	n.Expression = expression
	return n
}

func (n *CronTriggerNode) InTimezone(timezone string) *CronTriggerNode {
	n.Timezone = timezone
	return n
}

func (n *CronTriggerNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.cron",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Cron Trigger",
		Params: []types.ParamSpec{
			{Name: "expression", DisplayName: "Expression", Type: types.ParamString, Required: true},
			{Name: "timezone", DisplayName: "Timezone", Type: types.ParamString, Required: false, Default: "UTC"},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *CronTriggerNode) NodeType() string { return "xflow.trigger.cron" }
func (n *CronTriggerNode) RawParams() any {
	return map[string]any{"expression": n.Expression, "timezone": n.Timezone}
}
func (n *CronTriggerNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *CronTriggerNode) TriggerHandler() types.TriggerHandler { return n }
func (n *CronTriggerNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return ExecuteTriggerEntry(input)
}

func (n *CronTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	expr := cast.ToString(in.Params["expression"])
	if expr == "" {
		return nil, fmt.Errorf("cron expression is required")
	}
	tz := cast.ToString(in.Params["timezone"])
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
		t := time.Now().In(loc)
		scheduled := t.Truncate(time.Minute)
		event := &types.TriggerEvent{
			ID:     fmt.Sprintf("%s/%s/%s", in.WorkflowID, in.NodeName, scheduled.Format(time.RFC3339)),
			Kind:   "cron",
			Source: in.NodeName,
			Time:   t,
			Data:   map[string]any{"scheduled_time": scheduled.Format(time.RFC3339)},
		}
		if ok, err := in.Runtime.Dedup(runCtx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, 2*time.Minute); err == nil && ok {
			_, _ = in.Emit(runCtx, event)
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

func init() { RegisterTrigger(&CronTriggerNode{}) }
