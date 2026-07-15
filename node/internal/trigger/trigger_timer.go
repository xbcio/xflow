package trigger

import (
	"context"
	"fmt"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/xbcio/xflow/types"
)

type TimerTriggerNode struct {
	nodeinternal.BaseNode
	Interval time.Duration
}

func TimerTrigger() *TimerTriggerNode {
	return &TimerTriggerNode{Interval: time.Minute}
}

func (n *TimerTriggerNode) Every(interval time.Duration) *TimerTriggerNode {
	n.Interval = interval
	return n
}

func (n *TimerTriggerNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.timer",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Timer Trigger",
		Params: []types.ParamSpec{
			{Name: "interval", DisplayName: "Interval", Type: types.ParamString, Required: true},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *TimerTriggerNode) NodeType() string { return "xflow.trigger.timer" }
func (n *TimerTriggerNode) RawParams() any {
	return map[string]any{"interval": n.Interval.String()}
}
func (n *TimerTriggerNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *TimerTriggerNode) TriggerHandler() types.TriggerHandler { return n }
func (n *TimerTriggerNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return nodeinternal.ExecuteTriggerEntry(input)
}

func (n *TimerTriggerNode) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	interval, err := conv.PositiveDuration(in.Params["interval"])
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

func init() { registry.RegisterTrigger(&TimerTriggerNode{}) }
