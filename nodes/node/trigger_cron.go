package node

import (
	"context"
	"fmt"
	"time"

	cronlib "github.com/robfig/cron/v3"

	"github.com/xbcio/xflow/types"
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

func (n *CronTriggerNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.trigger.cron",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Cron Trigger",
		Params: []ParamSpec{
			{Name: "expression", DisplayName: "Expression", Type: ParamString, Required: true},
			{Name: "timezone", DisplayName: "Timezone", Type: ParamString, Required: false, Default: "UTC"},
		},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *CronTriggerNode) NodeType() string { return "xflow.trigger.cron" }
func (n *CronTriggerNode) RawParams() any {
	return map[string]any{"expression": n.Expression, "timezone": n.Timezone}
}
func (n *CronTriggerNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}
func (n *CronTriggerNode) TriggerHandler() TriggerHandler { return n }

func (n *CronTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	expr, _ := in.Params["expression"].(string)
	if expr == "" {
		return nil, fmt.Errorf("cron expression is required")
	}
	tz, _ := in.Params["timezone"].(string)
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
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
		if ok, _ := in.Runtime.Dedup(context.Background(), "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, 2*time.Minute); ok {
			_, _ = in.Emit(context.Background(), event)
		}
	}); err != nil {
		return nil, err
	}
	c.Start()
	return types.CloseFunc(func(context.Context) error {
		stopCtx := c.Stop()
		select {
		case <-stopCtx.Done():
		case <-ctx.Done():
		}
		return nil
	}), nil
}

func init() { RegisterTrigger(&CronTriggerNode{}) }
