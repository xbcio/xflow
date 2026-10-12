package timer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/xbcio/xflow/types"
)

// Node is the xflow.trigger.timer trigger: it emits one event per tick of a
// fixed interval.
type Node struct {
	nodeinternal.BaseTrigger
	Interval time.Duration
}

func New() *Node {
	return &Node{Interval: time.Minute}
}

func (n *Node) Every(interval time.Duration) *Node {
	n.Interval = interval
	return n
}

func (n *Node) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.timer",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Timer Trigger",
		Params: []types.ParamSpec{
			{Name: "interval", DisplayName: "Interval", Type: types.ParamString, Required: true, Description: "Positive tick interval (e.g. \"1m\")",
				Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *Node) NodeType() string { return "xflow.trigger.timer" }
func (n *Node) RawParams() any {
	return map[string]any{"interval": n.Interval.String()}
}
func (n *Node) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *Node) TriggerHandler() types.TriggerHandler { return n }

func (n *Node) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
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
					if _, err := in.Emit(runCtx, event); err != nil {
						// A refused firing is dropped: a tick is a moment, and the
						// next one carries its own event. Dropping it silently would
						// be indistinguishable from a timer that never fired —
						// nothing durable records the attempt, so this line is the
						// only evidence. The refusal an operator will see here is
						// the workflow's own concurrency limit
						// (engine.ErrConcurrencyLimit); a QUEUE-mode workflow never
						// produces it, because its over-limit firings are parked and
						// admitted later instead of refused.
						slog.Warn("timer trigger firing was not admitted; the tick is dropped",
							"workflow_id", in.WorkflowID,
							"node", in.NodeName,
							"event", event.ID,
							"err", err)
					}
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

func init() { registry.RegisterTrigger(&Node{}) }
