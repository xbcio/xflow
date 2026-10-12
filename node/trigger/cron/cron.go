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
	// MisfireValue is the misfire policy name; empty means the default
	// (misfireSkip).
	MisfireValue string
	// CatchupWindowValue bounds how far back a catch-up reaches; zero means
	// defaultCatchupWindow.
	CatchupWindowValue time.Duration
	// MaxCatchupValue bounds how many firings one activation catches up; zero
	// means defaultMaxCatchup.
	MaxCatchupValue int
	// OverlapValue is the overlap policy name; empty means the default
	// (overlapAllow).
	OverlapValue string
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

// Misfire sets what happens to the scheduled instants that passed while no
// trigger was hosting this schedule.
func (n *Node) Misfire(policy string) *Node {
	n.MisfireValue = policy
	return n
}

// CatchupWindow bounds how far back a catch-up reaches.
func (n *Node) CatchupWindow(window time.Duration) *Node {
	n.CatchupWindowValue = window
	return n
}

// MaxCatchup bounds how many firings one activation catches up.
func (n *Node) MaxCatchup(max int) *Node {
	n.MaxCatchupValue = max
	return n
}

// Overlap sets what happens when an instant arrives while the run this trigger
// started last has not finished.
func (n *Node) Overlap(policy string) *Node {
	n.OverlapValue = policy
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
			// The three catch-up params. None of them declares a Default: the
			// handlers' fallbacks are declared in node/internal.BuiltinFallbacks
			// instead, because a Default would be written into every workflow's
			// params and move its definition hash.
			{Name: "misfire", DisplayName: "Misfire Policy", Type: types.ParamString, Required: false,
				Description: "What to do with the scheduled instants that passed while no trigger was hosting this schedule; empty is skip",
				Enum: []types.EnumOption{
					{Value: string(misfireSkip), DisplayName: "Skip", Description: "Acknowledge the missed instants and fire none of them (default)"},
					{Value: string(misfireFireOnce), DisplayName: "Fire Once", Description: "Fire the most recent missed instant only"},
					{Value: string(misfireFireAll), DisplayName: "Fire All", Description: "Fire the missed instants, oldest first, within the catch-up bounds"},
				}},
			{Name: "catchup_window", DisplayName: "Catch-up Window", Type: types.ParamString, Required: false,
				Description: "How far back a catch-up reaches (e.g. \"1h\"); instants older than this are never fired",
				Widget:      nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
			{Name: "max_catchup", DisplayName: "Max Catch-up", Type: types.ParamNumber, Required: false,
				Description: "Most firings one activation catches up; the newest ones are kept",
				Constraints: &types.Constraints{Min: nodeinternal.Float(1)}},
			// No "queue" option: serialising runs is a workflow setting
			// (settings.concurrency mode=queue, limit=1), and a trigger parameter
			// could not enforce it against a workflow it does not configure. See
			// parseOverlapPolicy.
			{Name: "overlap", DisplayName: "Overlap Policy", Type: types.ParamString, Required: false,
				Description: "What to do when an instant arrives while the run this trigger started last has not finished; empty is allow",
				Enum: []types.EnumOption{
					{Value: string(overlapAllow), DisplayName: "Allow", Description: "Start the run anyway; two runs of this schedule may be in flight at once (default)"},
					{Value: string(overlapSkip), DisplayName: "Skip", Description: "Drop this firing if the execution this trigger started last has not finished"},
				}},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *Node) NodeType() string { return "xflow.trigger.cron" }
func (n *Node) RawParams() any {
	params := map[string]any{"expression": n.Expression}
	// Every optional param is omitted when unset, like the webhook's secret
	// supply and cron's own timezone: a definition that names no catch-up
	// behaviour is a definition whose parameters — and therefore whose workflow
	// hash — must stay what they were before these parameters existed.
	if n.Timezone != "" {
		params["timezone"] = n.Timezone
	}
	if n.MisfireValue != "" {
		params["misfire"] = n.MisfireValue
	}
	if n.CatchupWindowValue > 0 {
		params["catchup_window"] = n.CatchupWindowValue.String()
	}
	if n.MaxCatchupValue > 0 {
		params["max_catchup"] = n.MaxCatchupValue
	}
	if n.OverlapValue != "" {
		params["overlap"] = n.OverlapValue
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
	// The catch-up settings are read before either path starts, so a definition
	// with a mistyped policy is refused at activation rather than at the first
	// restart that would have needed it.
	cfg, err := catchupConfigFrom(in.Params)
	if err != nil {
		return nil, err
	}
	// The overlap policy is resolved here for the same reason, and it decides
	// which of the two firing paths can enforce it: the policy needs the runtime
	// to be able to read an execution's status, so a runtime that cannot says so
	// once, now, instead of at the first instant it was meant to decide.
	overlap, err := resolveOverlap(in)
	if err != nil {
		return nil, err
	}
	watermark := openActivationWatermark(ctx, in)
	now := time.Now().In(loc)
	// The plan is computed once, from the record the previous activation left
	// and the instant this one started, and both hosting paths run the same
	// plan: which of them admits the firings is the only difference.
	last, recorded := readWatermark(ctx, watermark)
	plan := missedFires(sched, last, recorded, now, cfg)
	if seedadmit.Enabled(in) {
		if overlap.policy == overlapSkip {
			// The control-plane admission path cannot enforce the policy: the
			// admission result it gets back does not carry the admitted
			// execution's id, so there is nothing to record and nothing to ask
			// about at the next instant. Said once per activation, because an
			// operator who asked for serialised runs and is getting overlapping
			// ones has no other way to find out.
			slog.Warn("cron trigger cannot enforce overlap=skip over the control-plane admission path; "+
				"runs of this schedule may overlap",
				"workflow_id", in.WorkflowID,
				"node", in.NodeName)
		}
		return n.activateEntrySeed(ctx, in, sched, loc, plan, watermark, now)
	}
	runCtx, cancel := context.WithCancel(ctx)
	cursor := newScheduleCursor(sched, now)
	c := cronlib.New(cronlib.WithLocation(loc))
	applyCatchup(runCtx, in, plan, now, watermark, func(fire cronFire) {
		n.emitScheduledFire(runCtx, in, sched, fire, cfg, overlap, watermark)
	})
	c.Schedule(sched, cronlib.FuncJob(func() {
		observed := time.Now().In(loc)
		n.emitScheduledFire(runCtx, in, sched, cursor.next(observed), cfg, overlap, watermark)
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

// emitScheduledFire admits one firing through the embedded engine: the local
// path's own admission, as opposed to the control plane's entry seed.
func (n *Node) emitScheduledFire(ctx context.Context, in *types.TriggerActivateInput, sched cronlib.Schedule, fire cronFire, cfg catchupConfig, overlap overlapPlan, watermark types.TriggerState) {
	event := newCronTriggerEvent(in.WorkflowID, in.NodeName, fire)
	// handled distinguishes "this instant has been decided" from "the store
	// failed before anything was attempted": only the first moves the
	// watermark, so a firing that never happened is still visible to a later
	// activation.
	handled := false
	if ok, err := in.Runtime.Dedup(ctx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, fireDedupTTL(sched, fire.scheduled, cfg)); err == nil {
		handled = true
		if ok {
			if !overlap.admits(ctx, watermark, in) {
				// A firing the overlap policy dropped is DECIDED, like one the
				// workflow refused: the instant is behind the trigger and the next
				// one is a fresh attempt, so the watermark below moves past it. The
				// log is the only trace — the workflow's own history shows a gap,
				// not a reason.
				slog.Warn("cron trigger skipped a firing; the execution it started last has not finished",
					"workflow_id", in.WorkflowID,
					"node", in.NodeName,
					"event", event.ID)
			} else if id, err := in.Emit(ctx, event); err != nil {
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
			} else {
				// The id is recorded for the overlap policy's next decision. It is
				// recorded from the admission rather than from the event, because
				// only the runtime knows which execution this firing became.
				recordLastExecution(ctx, watermark, in, id)
			}
		}
	}
	if handled {
		recordFiring(ctx, watermark, in, fire.scheduled)
	}
}

// fireDedupTTL is how long one firing's identity stays in the deduplicator.
//
// Two collisions need the key to be there, and they happen at very different
// distances from the firing: two replicas observing the same instant (seconds
// apart, which is what the cadence-doubled floor covers), and a catch-up firing
// of an instant whose ORIGINAL firing was admitted by a process that died before
// it could record the watermark (up to the whole catch-up window later). The
// original firing's key is the one that must survive, so the window is the
// floor, and it applies to every firing rather than to the catch-up ones alone —
// a catch-up can only be folded if the key it collides with is still there.
func fireDedupTTL(sched cronlib.Schedule, scheduled time.Time, cfg catchupConfig) time.Duration {
	ttl := 2 * firingWindow(sched, scheduled)
	if cfg.window > ttl {
		ttl = cfg.window
	}
	return ttl
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
func (n *Node) activateEntrySeed(ctx context.Context, in *types.TriggerActivateInput, sched cronlib.Schedule, loc *time.Location, plan catchupPlan, watermark types.TriggerState, now time.Time) (types.TriggerSubscription, error) {
	rt, ok := in.Runtime.(types.EntrySeedRuntime)
	if !ok {
		// seedadmit.Enabled proved the capability a moment ago; fail closed
		// rather than fall back onto Emit, which this runtime cannot serve.
		return nil, fmt.Errorf("cron trigger: entry-seed activation requires an entry-seed runtime")
	}
	runCtx, cancel := context.WithCancel(ctx)
	cursor := newScheduleCursor(sched, now)
	c := cronlib.New(cronlib.WithLocation(loc))
	applyCatchup(runCtx, in, plan, now, watermark, func(fire cronFire) {
		n.admitScheduledFire(runCtx, in, rt, sched, fire, watermark)
	})
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
//
// `catchup` says whether this firing is a moment the schedule named while no
// trigger was hosting it. A workflow that must not confuse a catch-up with a
// punctual firing — one that closes a book, or bills a period — branches on it;
// everything else can ignore it.
func newCronTriggerEvent(workflowID types.WorkflowID, nodeName string, fire cronFire) *types.TriggerEvent {
	scheduled := fire.scheduled.Format(time.RFC3339Nano)
	return &types.TriggerEvent{
		ID:     fmt.Sprintf("%s/%s/%s", workflowID, nodeName, scheduled),
		Kind:   "cron",
		Source: nodeName,
		Time:   fire.observed,
		Data:   map[string]any{"scheduled_time": scheduled, "catchup": fire.catchup},
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
