package control

import (
	"fmt"
	"time"

	cronlib "github.com/robfig/cron/v3"

	"github.com/xbcio/xflow/store"
)

// A "schedule" is a trigger node whose timing an operator can see — and, through
// an override, retime — without touching the workflow definition. There is no
// schedule entity: the schedule IS the trigger node, and the overlay is the
// store.ScheduleOverride row an operator has written against it.
//
// The vocabulary lives here, in the control package, rather than in the manager
// alone, because two very different callers must agree on it down to the
// parameter names:
//
//   - the manager (this package) derives the API's view and validates what may
//     be overridden;
//   - the trigger HOST (sdk/xflow's trigger runtime) merges an override into the
//     activation params before it activates the node.
//
// A host that disagreed with the API about which param an override's
// `expression` maps onto would produce the worst possible failure: an override
// that the API accepts, stores, audits, displays as in effect, and the trigger
// never applies. So the mapping is defined once, here, and the manager's
// validation is derived from the same table the host merges with.

// Node types that carry a schedule. They are the trigger kinds with a timing
// rule made of parameters; a webhook or a Kafka trigger has no schedule to
// retime, and a timer or cron node inside a co-location group is not an
// independently activated entry unit, so neither is a schedule this API owns.
const (
	NodeTypeCronTrigger  = "xflow.trigger.cron"
	NodeTypeTimerTrigger = "xflow.trigger.timer"
)

// Schedule kind labels. They are what the API reports (`kind`), one per node
// type, and they are wire vocabulary.
const (
	ScheduleKindCron  = "cron"
	ScheduleKindTimer = "timer"
)

// ScheduleKindOf maps a trigger node type to its schedule kind. It reports false
// for every node that has no schedule — including trigger kinds that do have an
// activation but no timing rule.
func ScheduleKindOf(nodeType string) (string, bool) {
	switch nodeType {
	case NodeTypeCronTrigger:
		return ScheduleKindCron, true
	case NodeTypeTimerTrigger:
		return ScheduleKindTimer, true
	default:
		return "", false
	}
}

// ScheduleCapabilities says which overridable fields a kind actually has.
//
// It exists because the two kinds are NOT the same shape: a cron node declares
// timezone, misfire and overlap, and a timer node declares only its interval.
// Accepting an override the node cannot apply would be a silent no-op, so the
// API refuses it instead (400) — an operator who PATCHes a timezone onto a timer
// and gets 200 back has been told something false.
type ScheduleCapabilities struct {
	// Timezone, Misfire and Overlap are cron-only: they are parameters of the
	// cron handler, which evaluates a calendar expression in a zone and has
	// policies for missed and overlapping firings.
	//
	// Backfill is NOT here even though it is the other operator-facing verb.
	// Both kinds can be backfilled — each identifies its instants from its own
	// rule alone, a timer's being the interval's grid — so it is not a property
	// that distinguishes them, and a field that is always true for every kind
	// that exists would only invite a caller to check it and be wrong later.
	Timezone bool
	Misfire  bool
	Overlap  bool
}

// ScheduleCapabilitiesFor returns the capability set of one kind. An unknown
// kind has none: every field is refused rather than assumed.
func ScheduleCapabilitiesFor(kind string) ScheduleCapabilities {
	switch kind {
	case ScheduleKindCron:
		return ScheduleCapabilities{Timezone: true, Misfire: true, Overlap: true}
	case ScheduleKindTimer:
		return ScheduleCapabilities{}
	default:
		return ScheduleCapabilities{}
	}
}

// ScheduleExpressionParam is the node parameter an override's `expression`
// writes: the cron node's own `expression`, and the timer node's `interval`.
//
// The overlay has ONE expression field rather than a per-kind one because what
// an operator changes about a schedule is its timing rule. The rule's spelling
// differs by kind, and this is where that difference is recorded — nowhere else,
// so the API and the host cannot disagree.
func ScheduleExpressionParam(kind string) string {
	switch kind {
	case ScheduleKindCron:
		return "expression"
	case ScheduleKindTimer:
		return "interval"
	default:
		return ""
	}
}

// ScheduleMisfirePolicies and ScheduleOverlapPolicies are the accepted values of
// the two policy fields. They mirror the cron node's own enums; they are
// restated here because the control plane must refuse an unknown policy at write
// time rather than store one the trigger will reject at activation, which would
// leave the schedule silently dead until an operator noticed the trigger had
// stopped firing.
//
// `queue` is deliberately absent from the overlap policies even though the
// design mentions it: serialising runs is the workflow's concurrency setting
// (settings.concurrency mode=queue, limit=1), not a property of the trigger, and
// the cron node rejects it for exactly that reason.
var (
	ScheduleMisfirePolicies = []string{"skip", "fire_once", "fire_all"}
	ScheduleOverlapPolicies = []string{"allow", "skip"}
)

// ScheduleExpression is a parsed timing rule: the cron schedule, or the timer's
// interval. Parsing happens in the control plane so that a write can be refused
// before it is stored, and so a view can answer `next_fire` without activating
// anything.
type ScheduleExpression struct {
	kind     string
	location *time.Location
	cron     cronlib.Schedule
	interval time.Duration
}

// ParseScheduleExpression parses one kind's expression. expression is the
// literal the node would be activated with; timezone applies to cron only (a
// timer's grid is UTC, matching scheduledTick in the timer handler).
func ParseScheduleExpression(kind, expression, timezone string) (ScheduleExpression, error) {
	switch kind {
	case ScheduleKindCron:
		if expression == "" {
			return ScheduleExpression{}, fmt.Errorf("cron expression is required")
		}
		loc := time.UTC
		if timezone != "" {
			parsed, err := time.LoadLocation(timezone)
			if err != nil {
				return ScheduleExpression{}, fmt.Errorf("timezone %q: %w", timezone, err)
			}
			loc = parsed
		}
		// ParseStandard is the parser the cron trigger itself uses (five fields
		// plus @every/@hourly descriptors): a schedule this accepts and the
		// trigger does not would be an override that breaks activation.
		sched, err := cronlib.ParseStandard(expression)
		if err != nil {
			return ScheduleExpression{}, fmt.Errorf("cron expression %q: %w", expression, err)
		}
		return ScheduleExpression{kind: kind, location: loc, cron: sched}, nil

	case ScheduleKindTimer:
		d, err := time.ParseDuration(expression)
		if err != nil {
			return ScheduleExpression{}, fmt.Errorf("interval %q: %w", expression, err)
		}
		if d <= 0 {
			return ScheduleExpression{}, fmt.Errorf("interval %q must be positive", expression)
		}
		return ScheduleExpression{kind: kind, interval: d}, nil

	default:
		return ScheduleExpression{}, fmt.Errorf("node type %q is not a schedule", kind)
	}
}

// Next returns the first instant strictly after `after`, in the schedule's own
// zone (UTC for a timer).
//
// For a timer this is the interval grid's next point, which is the same instant
// the timer handler would derive for its next tick (scheduledTick truncates the
// delivery time to the interval). It is exact rather than approximate because
// the grid is absolute: a tick's identity is `truncate(now, interval)`, not
// "last firing + interval", so a process that was down for an hour resumes on
// the grid rather than an hour behind it.
func (e ScheduleExpression) Next(after time.Time) time.Time {
	switch e.kind {
	case ScheduleKindCron:
		if e.cron == nil {
			return time.Time{}
		}
		return e.cron.Next(after.In(e.location)).In(e.location)
	case ScheduleKindTimer:
		return after.UTC().Truncate(e.interval).Add(e.interval)
	default:
		return time.Time{}
	}
}

// MergeScheduleOverrideParams applies an override to a node's activation params
// and returns the params the node must be activated with.
//
// It is the host's half of the contract described at the top of this file: the
// manager stores what an operator asked for, and this is what turns that into
// what the trigger runs. Fields the override does not set are left exactly as
// the definition rendered them, which is what makes DELETE .../override (drop
// the row, re-activate) restore the definition's own timing.
//
// The input map is not mutated: an activation's params may be shared with the
// compiled graph, and a host that wrote into them would change the definition
// the process has in memory rather than one activation of it.
func MergeScheduleOverrideParams(kind string, params map[string]any, ov *store.ScheduleOverride) map[string]any {
	if ov == nil {
		return params
	}
	out := make(map[string]any, len(params)+4)
	for k, v := range params {
		out[k] = v
	}
	if ov.Expression != nil {
		if param := ScheduleExpressionParam(kind); param != "" {
			out[param] = *ov.Expression
		}
	}
	caps := ScheduleCapabilitiesFor(kind)
	if caps.Timezone && ov.Timezone != nil {
		out["timezone"] = *ov.Timezone
	}
	if caps.Misfire && ov.Misfire != nil {
		out["misfire"] = *ov.Misfire
	}
	if caps.Overlap && ov.Overlap != nil {
		out["overlap"] = *ov.Overlap
	}
	return out
}
