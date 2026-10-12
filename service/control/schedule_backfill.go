package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// MaxScheduleBackfillInstants caps how many instants one backfill may plan.
//
// The cap is a refusal, not a truncation: a backfill is a request to have a
// named set of instants run, and silently planning the oldest thousand of two
// thousand would leave the operator believing they asked for less than they did.
// A dry run reports that plan without firing anything, but it is bound by the
// same cap — the plan is the thing being capped and it is the expensive part, so
// a range too wide to run is also too wide to preview. A wide range is explored
// by narrowing it until the plan fits, and the error names the cap to say how
// far off the caller is.
//
// The number is the design's. It bounds both the work one request can queue and
// the size of the response that reports it.
const MaxScheduleBackfillInstants = 1000

// Errors a backfill can be refused with, distinct from each other because the
// API answers them differently: a bad range is the caller's mistake (400), no
// emitter is a deployment that cannot do this at all (501), and a missing
// schedule is a 404 like every other schedule operation's.
var (
	// ErrScheduleBackfillRange reports a range that cannot be planned: inverted,
	// empty, reaching into the future, or holding more instants than the cap.
	ErrScheduleBackfillRange = errors.New("control: invalid backfill range")
	// ErrScheduleNoEmitter reports a deployment with no way to fire an instant
	// itself. Firing outside an activation is the trigger host's job, and a
	// control plane whose triggers are hosted elsewhere (the cluster mode the
	// design defers) has nothing here to call.
	ErrScheduleNoEmitter = errors.New("control: no schedule emitter is configured")
)

// ScheduleEmitter fires one instant of a schedule's own timeline. It is the
// backfill's whole outbound surface: what a backfill needs from a host is not
// "start this workflow" but "be the trigger, for this instant".
//
// The instant is passed as the instant, not as an event: the identity of a
// firing — its event ID, spelled in the owning trigger kind's own format (see
// types.ScheduledTrigger) — is assembled by the host, which asks the kind that
// owns the schedule's timeline. A caller outside that kind cannot spell it, and
// an ID assembled anywhere else would be a firing of nothing in particular.
//
// A host fires an instant under its OWN deduplication, not the one a live
// activation claims: that key means "this instant has been decided", and it is
// claimed for the firings the live trigger drops — while paused, while the
// overlap policy holds one back, or when the workflow's concurrency limit
// refuses it. A backfill exists to run exactly those instants.
//
// duplicate reports an instant the host has already fired for this backfill: its
// claim was still held, so nothing was admitted. It is a success, not a failure —
// it is what makes a repeated backfill idempotent — and it is reported
// separately because "skipped, already ran" and "skipped, refused" call for
// different operator actions.
type ScheduleEmitter interface {
	EmitScheduleInstant(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string, instant time.Time) (types.ExecutionID, bool, error)
}

// ScheduleBackfillRequest is one backfill: the range to plan, and whether to
// plan it only.
type ScheduleBackfillRequest struct {
	// From is exclusive and To inclusive: an instant at From is the one the
	// schedule last acknowledged, and asking for its re-run is a different
	// request than asking for a range.
	From time.Time
	To   time.Time
	// DryRun plans without firing. It is the only way to see what a range would
	// do before it does it; it is not a way around the cap, which counts the plan
	// itself and so refuses a dry run as readily as a run.
	DryRun bool
}

// ScheduleBackfillResult reports what a backfill planned and what happened to
// each instant. Every instant ends up in exactly one of Emitted or Skipped, so
// the two lists are the whole answer — nothing is reported as a count alone.
type ScheduleBackfillResult struct {
	From   time.Time
	To     time.Time
	DryRun bool
	// Planned is every instant in the range the schedule identifies, oldest
	// first: what a dry run is for, and what a real run then reports item by
	// item.
	Planned []time.Time
	Emitted []ScheduleBackfillEmission
	Skipped []ScheduleBackfillSkip
}

// ScheduleBackfillEmission is one instant that was admitted, with the execution
// it started.
type ScheduleBackfillEmission struct {
	Instant     time.Time
	ExecutionID types.ExecutionID
}

// Why an instant was skipped. The reasons are wire vocabulary: an operator
// reading the response decides what to do next from them.
const (
	// ScheduleBackfillDuplicate is an instant that had already fired. Nothing is
	// wrong and nothing needs doing.
	ScheduleBackfillDuplicate = "duplicate"
	// ScheduleBackfillRefused is an instant the workflow's own admission refused
	// — its concurrency limit, in reject mode, or a full concurrency queue. The
	// trigger's live firings are dropped for the same reason; a backfill reports
	// it per instant instead, because an operator who asked for a specific range
	// needs to know which parts of it did not run.
	ScheduleBackfillRefused = "refused"
	// ScheduleBackfillFailed is an instant whose admission failed for any other
	// reason.
	ScheduleBackfillFailed = "failed"
)

// ScheduleBackfillSkip is one instant that was not admitted, and why.
type ScheduleBackfillSkip struct {
	Instant time.Time
	Reason  string
	// Detail is the underlying failure, as the engine reported it. It is empty
	// for a duplicate, which is not a failure.
	Detail string
}

// Backfill plans a schedule's instants in a range and fires them.
//
// It fires one instant at a time, oldest first, and never retries: a schedule is
// a stream of moments, and a backfill that retried would be replaying a history
// the operator asked to have run once. Each result is reported as it happens, so
// a range that partly succeeded is partly reported — and is safe to re-run,
// because an instant that already fired is recognised as a duplicate.
//
// A PAUSED schedule can be backfilled. Pausing means "do not fire on your own",
// and an explicit backfill is the operator saying which instants they want
// anyway; that is the one hole the pause's "resuming is not a catch-up" rule
// leaves open, and it is deliberate.
//
// The instants are the EFFECTIVE schedule's: the overlay's expression where the
// operator has retimed the node, the definition's where they have not. A
// backfill of a range therefore plans what the schedule the operator is looking
// at would have fired, not what the definition originally said.
func (m *ScheduleManager) Backfill(ctx context.Context, wf ScheduleWorkflow, node string, req ScheduleBackfillRequest) (ScheduleBackfillResult, error) {
	if m == nil || m.overrides == nil {
		return ScheduleBackfillResult{}, errors.New("control: no schedule store configured")
	}
	result := ScheduleBackfillResult{From: req.From, To: req.To, DryRun: req.DryRun}
	nm, err := m.node(wf.Graph, node)
	if err != nil {
		return result, err
	}
	if nm == nil {
		return result, fmt.Errorf("%w: node %q is not a trigger node of this workflow", ErrScheduleNotFound, node)
	}
	kind, _ := ScheduleKindOf(nm.Type)
	params, err := m.params(wf, nm)
	if err != nil {
		return result, err
	}
	ov, err := m.overlay(ctx, wf, node)
	if err != nil {
		return result, err
	}
	expression := effectiveParam(params, ov, ScheduleExpressionParam(kind), ScheduleFieldExpression)
	timezone := effectiveParam(params, ov, ScheduleFieldTimezone, ScheduleFieldTimezone)
	expr, err := ParseScheduleExpression(kind, expression, timezone)
	if err != nil {
		return result, fmt.Errorf("schedule %s/%s cannot be planned: %w", wf.ID, node, err)
	}

	// The range is validated before anything is planned, and the future is
	// refused rather than clamped. An instant the schedule has not reached yet
	// belongs to the live trigger: firing it here would either suppress the real
	// firing (the identity is claimed, so the live one is deduplicated away) or
	// be forgotten by the deduplicator before the live firing comes — and which
	// of the two happens is a function of how far ahead the range reaches, which
	// is not something an operator can reason about.
	now := m.now()
	if !req.To.After(req.From) {
		return result, fmt.Errorf("%w: to must be after from", ErrScheduleBackfillRange)
	}
	if req.To.After(now) {
		return result, fmt.Errorf("%w: to (%s) is in the future; a backfill reaches only instants the schedule has already reached",
			ErrScheduleBackfillRange, req.To.UTC().Format(time.RFC3339))
	}
	result.Planned = expr.Instants(req.From, req.To, MaxScheduleBackfillInstants+1)
	if len(result.Planned) > MaxScheduleBackfillInstants {
		return ScheduleBackfillResult{From: req.From, To: req.To, DryRun: req.DryRun},
			fmt.Errorf("%w: the range holds more than %d instants; narrow it (or dry-run a narrower range first)",
				ErrScheduleBackfillRange, MaxScheduleBackfillInstants)
	}
	if req.DryRun {
		return result, nil
	}
	if m.emitter == nil {
		return result, ErrScheduleNoEmitter
	}
	for _, instant := range result.Planned {
		id, duplicate, err := m.emitter.EmitScheduleInstant(ctx, wf.Namespace, wf.ID, node, instant)
		switch {
		case err != nil:
			reason, detail := classifyBackfillError(err)
			result.Skipped = append(result.Skipped, ScheduleBackfillSkip{Instant: instant, Reason: reason, Detail: detail})
		case duplicate:
			result.Skipped = append(result.Skipped, ScheduleBackfillSkip{
				Instant: instant,
				Reason:  ScheduleBackfillDuplicate,
			})
		default:
			result.Emitted = append(result.Emitted, ScheduleBackfillEmission{Instant: instant, ExecutionID: id})
		}
	}
	return result, nil
}

// classifyBackfillError decides whether an instant was refused by the workflow's
// own admission or failed for another reason. The distinction is the operator's:
// a refused instant is the workflow being busy, which needs no investigation,
// while any other failure is something to look at.
func classifyBackfillError(err error) (reason, detail string) {
	switch {
	case errors.Is(err, engine.ErrConcurrencyLimit), errors.Is(err, engine.ErrConcurrencyQueueFull):
		return ScheduleBackfillRefused, err.Error()
	default:
		return ScheduleBackfillFailed, err.Error()
	}
}

// params renders a node's activation parameters, exactly as the trigger host
// does before it merges an overlay into them: the two must agree, or a
// backfill's instants would be planned against a schedule the trigger is not
// running.
func (m *ScheduleManager) params(wf ScheduleWorkflow, nm *graph.NodeMeta) (map[string]any, error) {
	return graph.EvaluateActivationParams(wf.Graph, nm.Name, nm.Type, nm.Parameters)
}
