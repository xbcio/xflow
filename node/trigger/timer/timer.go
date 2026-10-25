package timer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/xbcio/xflow/node/trigger/internal/seedadmit"
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
		Description: "按固定间隔周期性触发工作流。",
		Category:    "trigger",
		Examples: []types.DescriptorExample{{
			Name:   "每 30 秒触发",
			Params: map[string]any{"interval": "30s"},
		}},
		CardSummary: []string{"interval"},
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
	if seedadmit.Enabled(in) {
		return n.activateEntrySeed(ctx, in, interval)
	}
	runCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case t := <-ticker.C:
				event := newTimerTriggerEvent(in.WorkflowID, in.NodeName, interval, t, t)
				if ok, err := in.Runtime.Dedup(runCtx, timerDedupKey(in.WorkflowID, in.NodeName, event), interval*2); err == nil && ok {
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
						// Reported as emit_error with the error attached, like the cron
						// trigger's: node/ may not import package engine, so which
						// engine sentinel this is gets decided by the observer.
						fireDrops.Observe(runCtx, types.TriggerFireDroppedEmitError, err)
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

// admitRetry is the retry policy one tick's admission uses. It is a variable so
// a test can shrink the window: the production cadence is 200ms per retry,
// which no test should have to wait out.
var admitRetry = seedadmit.DefaultRetry

// activateEntrySeed hosts the timer on a runner that admits through the
// control plane's entry-seed endpoint instead of emitting into an embedded
// engine. Every replica runs the same ticker and submits the same fire key, and
// the server's admission-key deduplication is what turns that into one
// execution per tick — so there is no leader election and no runner-side lock
// (spec 2026-10-12 §4). The cost is N admissions per tick, which the default
// single replica does not pay at all.
func (n *Node) activateEntrySeed(ctx context.Context, in *types.TriggerActivateInput, interval time.Duration) (types.TriggerSubscription, error) {
	rt, ok := in.Runtime.(types.EntrySeedRuntime)
	if !ok {
		// seedadmit.Enabled proved the capability a moment ago; a false here
		// means the runtime changed between the two checks, which no
		// implementation does. Fail closed rather than fall back onto Emit,
		// which this runtime cannot serve anyway.
		return nil, fmt.Errorf("timer trigger: entry-seed activation requires an entry-seed runtime")
	}
	runCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case t := <-ticker.C:
				event := newTimerTriggerEvent(in.WorkflowID, in.NodeName, interval, t, t)
				scheduled := scheduledTick(interval, t)
				req := seedadmit.NewRequest(in, seedadmit.KindTimer, strconv.FormatInt(scheduled.Unix(), 10), event)
				// The tick's own window bounds the admission: a retry that
				// outlives the next tick would make two admissions of two
				// different ticks contend for the same retry budget, and the
				// later one is the one an operator would rather have on time.
				admitCtx, admitCancel := context.WithTimeout(runCtx, interval)
				result, err := seedadmit.Admit(admitCtx, rt, req, admitRetry)
				admitCancel()
				switch result {
				case seedadmit.Refused:
					// A refusal is the workflow's own concurrency policy declining
					// the tick — a decision, not a fault. Logged and counted for the
					// same reason the cron trigger logs and counts its own: nothing
					// durable records that the tick was ever offered, so without them
					// a throttled schedule is indistinguishable from an idle one.
					slog.Warn("timer trigger firing was refused; the tick is dropped",
						"workflow_id", in.WorkflowID,
						"node", in.NodeName,
						"fire_key", req.AdmissionKey)
					fireDrops.Observe(runCtx, types.TriggerFireDroppedRefused, nil)
				case seedadmit.Dropped:
					// The tick is LOST — nothing durable records the attempt, so
					// this line is the only evidence it ever happened.
					slog.Warn("timer trigger tick was not admitted; the tick is dropped",
						"workflow_id", in.WorkflowID,
						"node", in.NodeName,
						"fire_key", req.AdmissionKey,
						"err", err)
					fireDrops.Observe(runCtx, types.TriggerFireDroppedTransport, err)
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

// newTimerTriggerEvent builds the event for one firing: the tick its identity is
// derived from, and the moment the firing is being admitted.
//
// The two are separate parameters because they are separate facts, and they come
// apart exactly on the backfill path — a firing admitted now for a tick the
// passage of time named earlier. A live tick passes the same value twice. It is a
// function rather than an inline closure body so the tick truncation below can be
// asserted without racing the clock.
func newTimerTriggerEvent(workflowID types.WorkflowID, nodeName string, interval time.Duration, tick, observed time.Time) *types.TriggerEvent {
	scheduled := scheduledTick(interval, tick)
	return &types.TriggerEvent{
		ID:     fmt.Sprintf("%s/%s/%d", workflowID, nodeName, scheduled.UnixNano()),
		Kind:   "timer",
		Source: nodeName,
		Time:   observed,
		Data:   map[string]any{"scheduled_time": scheduled.Format(time.RFC3339Nano)},
	}
}

// ScheduledEvent builds the firing of one tick this node's timeline names, at a
// moment that is not now — the instant a backfill was asked to run.
//
// It is a method on the node rather than a function the caller can reimplement
// because a tick's identity comes from the interval's own grid: the event ID is
// the truncated tick, and only the kind that reads the interval parameter can
// reproduce it. params are the node's activation parameters, so the interval is
// the effective one — an operator who retimed the node backfills against the
// schedule the node is actually running.
//
// Time carries the moment of admission (now) while `scheduled_time` carries the
// tick itself, exactly as a live firing does.
func (n *Node) ScheduledEvent(workflowID types.WorkflowID, nodeName string, params map[string]any, instant time.Time) (*types.TriggerEvent, error) {
	interval, err := conv.PositiveDuration(params["interval"])
	if err != nil {
		return nil, fmt.Errorf("timer trigger: interval: %w", err)
	}
	return newTimerTriggerEvent(workflowID, nodeName, interval, instant, time.Now()), nil
}

// timerDedupKey is the deduplicator key one tick claims. The event ID is the
// identity; this is that identity qualified by the trigger it belongs to.
func timerDedupKey(workflowID types.WorkflowID, nodeName string, event *types.TriggerEvent) string {
	return "trigger:" + string(workflowID) + ":" + nodeName + ":" + event.ID
}

// scheduledTick is the tick one delivered firing belongs to: the ticker's
// delivery time truncated to the interval, in UTC.
//
// Both the event ID and the entry-seed fire key are derived from it, and they
// must agree: the ID identifies the event to the embedded engine's deduper, the
// fire key identifies it to the control plane's admission. The truncation is
// also what makes two replicas that fire the same tick derive the same
// identity — their ticker deliveries differ by microseconds, and without it
// each would produce a distinct sub-second ID and every tick would run twice.
func scheduledTick(interval time.Duration, tick time.Time) time.Time {
	scheduled := tick.UTC()
	if interval > 0 {
		scheduled = scheduled.Truncate(interval)
	}
	return scheduled
}

func init() { registry.RegisterTrigger(&Node{}) }
