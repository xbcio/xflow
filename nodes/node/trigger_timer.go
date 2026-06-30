package node

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"
)

type TimerTriggerNode struct {
	BaseNode
	Interval time.Duration
}

func TimerTrigger() *TimerTriggerNode {
	return &TimerTriggerNode{Interval: time.Minute}
}

func (n *TimerTriggerNode) Every(interval time.Duration) *TimerTriggerNode {
	n.Interval = interval
	return n
}

func (n *TimerTriggerNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.trigger.timer",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Timer Trigger",
		Params: []ParamSpec{
			{Name: "interval", DisplayName: "Interval", Type: ParamString, Required: true},
		},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *TimerTriggerNode) NodeType() string { return "xflow.trigger.timer" }
func (n *TimerTriggerNode) RawParams() any {
	return map[string]any{"interval": n.Interval.String()}
}
func (n *TimerTriggerNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}
func (n *TimerTriggerNode) TriggerHandler() TriggerHandler { return n }
func (n *TimerTriggerNode) Execute(_ context.Context, input *Input) (*Output, error) {
	return executeTriggerEntry(input)
}

func (n *TimerTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	interval, err := triggerDurationParam(in.Params["interval"])
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case t := <-ticker.C:
				event := newTimerTriggerEvent(in.WorkflowID, in.NodeName, interval, t)
				if ok, err := in.Runtime.Dedup(runCtx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, interval*2); err == nil && ok {
					_, _ = in.Emit(runCtx, event)
				}
			case <-runCtx.Done():
				return
			}
		}
	}()
	return types.CloseFunc(func(context.Context) error {
		cancel()
		return nil
	}), nil
}

func newTimerTriggerEvent(workflowID types.WorkflowID, nodeName string, interval time.Duration, tick time.Time) *types.TriggerEvent {
	scheduled := tick.UTC()
	if interval > 0 {
		scheduled = scheduled.Truncate(interval)
	}
	return &types.TriggerEvent{
		ID:     fmt.Sprintf("%s/%s/%d", workflowID, nodeName, scheduled.UnixNano()),
		Kind:   "timer",
		Source: nodeName,
		Time:   tick,
		Data:   map[string]any{"scheduled_time": scheduled.Format(time.RFC3339Nano)},
	}
}

func triggerDurationParam(v any) (time.Duration, error) {
	if v == nil || cast.ToString(v) == "" {
		return 0, fmt.Errorf("duration is required")
	}
	d, err := cast.ToDurationE(v)
	if err != nil {
		return 0, fmt.Errorf("duration is required")
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return d, nil
}

func init() { RegisterTrigger(&TimerTriggerNode{}) }
